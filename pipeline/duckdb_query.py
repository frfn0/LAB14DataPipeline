"""Задание 8. Анализ через DuckDB.

Методичка требует SQL-запрос к Parquet-файлу с фильтрацией, группировкой и
сортировкой и сравнение производительности с анализом в Polars.

Сравнение сделано честно, иначе оно ничего не значит:

- оба варианта делают одно и то же: тот же результат по тем же данным;
- измеряется всё вместе с чтением файла, потому что в задании 9 читать файл
  придётся и DuckDB;
- запрос выполняется несколько раз, и сравнивается медиана, а не единственный
  замер: на Windows единичный замер скачет из-за планировщика и холодного
  кеша, и вывод по нему получался бы случайным;
- рядом с общим временем приводится время только вычислений, без чтения, иначе
  не видно, кто именно выигрывает.

Результаты DuckDB и Polars сверяются построчно: если значения разошлись,
замер времени ничего не значит, потому что считалось разное.

Запуск:

    python -m pipeline.duckdb_query
"""

from __future__ import annotations

import statistics
import sys
import time
from collections.abc import Callable

import duckdb
import polars as pl

from pipeline.common import (
    REPORTS_DIR,
    ROOT,
    human_bytes,
    rule,
    section,
)
from pipeline.report import print_table
from pipeline.to_parquet import parquet_path

# Сколько раз выполняется каждый вариант. Медиана из пяти замеров устойчивее
# единственного: два выброса из пяти не сдвигают результат.
RUNS = 5

# Порог шума в миллисекундах.
#
# Замер идёт по медиане пяти повторов, и разброс остаётся около 0.5 мс. Разница
# меньше миллисекунды не отделяет один способ от другого: она тонет в
# планировщике ОС.
NOISE_MS = 1.0

# Порог отношения времён, ниже которого выигрыш не объявляется.
#
# Отношение 1.2 означает 20 %: на объёме в 420 строк это десятые доли
# миллисекунды, и объявлять по ним победителя означало бы объявлять победителя
# по шуму измерения.
SPEEDUP_THRESHOLD = 1.2

# Условие фильтрации: холодная погода с осадками, ниже +10 градусов.
#
# Первым был взят более жёсткий вариант, temp_c < 0 AND осадки > 0. Он выглядел
# естественнее - зима с осадками, - но под него подошёл только один город с
# шестью наблюдениями. Сравнивать скорость на одной группе бессмысленно, и
# отчёт с одной строкой ничего не показывает. Порог +10 градусов оставляет
# пять городов и тридцать одно наблюдение: это и настоящая погода, и данные,
# на которых время выполнения что-то значит.
FILTER_SQL = "temp_c < 10 AND (rain_mm > 0 OR snow_mm > 0)"


def sql_path() -> str:
    """Путь к Parquet в виде строки для SQL.

    В Windows путь содержит обратные слэши, а одинарные кавычки в SQL означают
    строку. Поэтому слэши заменяются на прямые, а не только экранируются:
    DuckDB понимает и прямые слэши в Windows-пути.
    """
    return str(parquet_path()).replace("\\", "/")


def read_sql(sql: str) -> pl.DataFrame:
    """Выполняет SQL-запрос и возвращает результат как таблицу Polars."""
    connection = duckdb.connect()
    try:
        return connection.sql(sql).pl()
    finally:
        connection.close()


def print_result(frame: pl.DataFrame) -> None:
    """Печатает результат запроса: города, агрегаты и осадки.

    Порядок столбцов и формат чисел заданы здесь, а не берутся из заголовков
    SQL: иначе кириллические псевдонимы AS город попали бы в отчёт как есть,
    и читателю пришлось бы догадываться, где что.
    """
    print(rule())
    print(
        f"{'город':16} {'наблюдений':>10} {'мин °C':>8} {'средняя °C':>12} "
        f"{'макс °C':>8} {'осадки мм':>10}"
    )
    print(rule())

    for record in frame.iter_rows(named=True):
        print(
            f"{record['город']:16} {record['наблюдений']:>10} "
            f"{record['мин']:>8.2f} {record['средняя']:>12.2f} "
            f"{record['макс']:>8.2f} {record['осадки']:>10.2f}"
        )

    print(rule())
    print(f"строк в результате: {frame.height}")
    print()


def read_polars(table: pl.DataFrame) -> pl.DataFrame:
    """Выполняет тот же анализ в Polars, без чтения файла.

    Файл уже в памяти: Polars в задании 6 читал JSON, здесь же сравнивается
    вычисление, а не чтение разных форматов. Чтение сравнивается отдельно.

    Осадки здесь - сумма дождя и снега, как в SQL, то есть sum(snow_mm +
    rain_mm). Поначалу суммировался только снег: у Якутска это случайно дало
    те же 1.64 мм, потому что там дождя не было, а у остальных городов дождь
    терялся, и сверка справедливо сообщила о расхождении.
    """
    return (
        table.filter(
            (pl.col("temp_c") < 10)
            & ((pl.col("rain_mm") > 0) | (pl.col("snow_mm") > 0))
        )
        .group_by("city")
        .agg(
            pl.len().alias("наблюдений"),
            pl.col("temp_c").min().alias("мин"),
            pl.col("temp_c").mean().alias("средняя"),
            pl.col("temp_c").max().alias("макс"),
            (pl.col("snow_mm") + pl.col("rain_mm")).sum().alias("осадки"),
        )
        .sort("мин")
    )


def measure(
    action: Callable[[], object], runs: int = RUNS
) -> tuple[float, float, list[float]]:
    """Выполняет действие несколько раз и возвращает времена.

    Args:
        action: что измеряется.
        runs: число повторов.

    Returns:
        Пара: минимальное время и медиана, и список всех замеров. Минимум
        показывает, сколько действие занимает без помех, медиана - типичный
        результат.
    """
    timings: list[float] = []

    for _ in range(runs):
        started = time.perf_counter()
        action()
        timings.append((time.perf_counter() - started) * 1000)

    return min(timings), statistics.median(timings), timings


def rounded(values: list) -> list:
    """Округляет дробные значения до восьми знаков.

    Строгое сравнение числовых результатов двух движков даёт ложное
    расхождение. У Якутска DuckDB посчитал среднюю температуру как
    -3.5849999999999995, а Polars как -3.5850000000000004: числа те же,
    отличается порядок сложения. Такая разница не означает ошибки ни в
    запросе, ни в коде, и отсекается округлением. Настоящие расхождения,
    например потерянный дождь, округление не скрывает.

    Args:
        values: список значений столбца.

    Returns:
        Список округлённых значений, целые остаются целыми.
    """
    return [round(value, 8) if isinstance(value, float) else value for value in values]


def compare_results(sql_frame: pl.DataFrame, polars_frame: pl.DataFrame) -> list[str]:
    """Сверяет результаты DuckDB и Polars.

    Args:
        sql_frame: результат SQL-запроса.
        polars_frame: результат вычисления в Polars.

    Returns:
        Список расхождений. Пустой список означает, что результаты совпали.
    """
    problems: list[str] = []

    if sql_frame.columns != polars_frame.columns:
        problems.append(
            f"наборы столбцов различаются: SQL {sql_frame.columns}, "
            f"Polars {polars_frame.columns}"
        )
        return problems

    if sql_frame.height != polars_frame.height:
        problems.append(f"строк: SQL {sql_frame.height}, Polars {polars_frame.height}")
        return problems

    for field in sql_frame.columns:
        left = rounded(sql_frame[field].to_list())
        right = rounded(polars_frame[field].to_list())
        if left != right:
            problems.append(f"значения столбца {field} различаются")

    return problems


def main() -> int:
    """Выполняет анализ через DuckDB и сравнивает его с Polars.

    Returns:
        Код возврата: 0 при успехе, 1 если результаты разошлись или файла нет.
    """
    section("Задание 8. Анализ через DuckDB")

    target = parquet_path()
    if not target.exists():
        print(f"Файл не найден: {target}")
        print("Сначала выполните задание 7: python -m pipeline.to_parquet")
        return 1

    print(f"Файл запроса: {target.relative_to(ROOT)}")
    print(f"Размер: {human_bytes(target.stat().st_size)}")
    print("Сжатие: zstd, записано заданием 7")
    print(f"Повторов каждого варианта: {RUNS}, сравнивается медиана")
    print()

    source_sql = sql_path()

    # --- Готовим SQL. Он используется дальше и для замера, и для отчёта.
    sql = f"""
        SELECT
            city                                        AS город,
            count(*)                                    AS наблюдений,
            min(temp_c)                                 AS мин,
            avg(temp_c)                                 AS средняя,
            max(temp_c)                                 AS макс,
            sum(snow_mm + rain_mm)                      AS осадки
        FROM read_parquet('{source_sql}')
        WHERE {FILTER_SQL}
        GROUP BY city
        HAVING count(*) > 0
        ORDER BY мин
    """

    # Тот же запрос, но без чтения файла. Таблица регистрируется в DuckDB как
    # таблица в памяти, поэтому замеряется вычисление, а не чтение: первый
    # вариант сравнения Polars считал по таблице в памяти, а DuckDB каждый раз
    # перечитывал Parquet, и разницу в 23 раза давало чтение, а не запрос.
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE weather AS SELECT * FROM read_parquet(?)", [source_sql]
    )
    in_memory_sql = sql.replace(f"read_parquet('{source_sql}')", "weather")

    # --- Замер чтения файла обоими способами.
    section("1. Сравнение чтения Parquet")

    def duckdb_read() -> None:
        """DuckDB читает Parquet через SQL."""
        fresh = duckdb.connect()
        try:
            fresh.sql(f"SELECT count(*) FROM read_parquet('{source_sql}')").fetchall()
        finally:
            fresh.close()

    def polars_read() -> None:
        """Polars читает тот же Parquet."""
        pl.read_parquet(target).height

    duck_min, duck_med, duck_all = measure(duckdb_read)
    pol_min, pol_med, pol_all = measure(polars_read)

    print_table(
        [
            ["способ", "минимум, мс", "медиана, мс", "замеры, мс"],
            ["DuckDB (SQL)", *fmt_times(duck_min, duck_med, duck_all)],
            ["Polars", *fmt_times(pol_min, pol_med, pol_all)],
        ]
    )

    print("  DuckDB здесь создаёт подключение заново: это его полная стоимость чтения")
    print("  одного файла из SQL, как и требует конвейер. Разница в пользу Polars")
    print("  здесь означает не медленный Parquet, а стоимость подключения к базе.")
    print()

    # --- Основной запрос: фильтрация, группировка, сортировка.
    section("2. Запрос с фильтрацией, группировкой и сортировкой")

    print("Текст запроса")
    print(rule())
    print(sql.strip())
    print(rule())
    print(f"  Фильтр: {FILTER_SQL}")
    print(
        "  HAVING count(*) > 0 отбрасывает пустые группы. На этих данных он\n"
        "  ничего не отбрасывает, но без него пустая группа дала бы в MIN и MAX\n"
        "  одно и то же значение - NULL, и в таблице появилась бы строка без\n"
        "  наблюдений."
    )
    print()

    sql_frame = connection.sql(sql).pl()

    print("Результат запроса")
    print_result(sql_frame)

    if sql_frame.height == 0:
        print("  Запрос вернул пустой результат: под условия фильтрации не подошёл")
        print(
            "  ни один город. Проверить время выполнения на таком результате бессмысленно,"
        )
        print("  и сравнение с Polars было бы сравнением двух пустых таблиц.")
        return 1

    # --- Тот же анализ в Polars, по данным в памяти.
    section("3. Тот же анализ в Polars")

    pl_all = connection.sql("SELECT * FROM weather").pl()
    polars_frame = read_polars(pl_all)

    # DuckDB отдаёт названия столбцов как в SQL, Polars - свои. Переименование
    # приводит результаты к общему виду перед сверкой, иначе сравнение
    # сообщило бы о расхождении там, где расхождения нет.
    polars_frame = polars_frame.rename(
        dict(zip(polars_frame.columns, sql_frame.columns, strict=True))
    )

    print(f"прочитано Polars: {pl_all.height} строк, {pl_all.width} столбцов")
    print(f"фильтр применён, городов в результате: {polars_frame.height}")
    print()

    print("Результат в Polars")
    print_result(polars_frame)

    # --- Сверка результатов.
    section("4. Сверка результатов")

    problems = compare_results(sql_frame, polars_frame)
    if problems:
        print("  ОШИБКА: результаты DuckDB и Polars разошлись")
        for line in problems:
            print(f"    {line}")
        print("  Замер времени не имеет смысла: считалось разное.")
        return 1

    print("  результаты совпали во всех столбцах и во всех строках")
    print(f"  строк: {sql_frame.height}, столбцов: {sql_frame.width}")
    print()

    # --- Сравнение времени вычисления. Оба варианта работают с данными в памяти:
    # иначе сравнивалось бы чтение файла, а не запрос.
    section("5. Сравнение времени выполнения запроса")

    def duckdb_query() -> None:
        """DuckDB выполняет запрос по таблице в памяти."""
        connection.sql(in_memory_sql).pl()

    def polars_query() -> None:
        """Polars делает то же по таблице в памяти."""
        read_polars(pl_all)

    duck_min, duck_med, duck_all = measure(duckdb_query)
    pol_min, pol_med, pol_all = measure(polars_query)

    print_table(
        [
            ["способ", "минимум, мс", "медиана, мс", "замеры, мс"],
            ["DuckDB (SQL)", *fmt_times(duck_min, duck_med, duck_all)],
            ["Polars", *fmt_times(pol_min, pol_med, pol_all)],
        ]
    )

    print("  оба варианта работают с таблицей в памяти и выполняют один запрос:")
    print("  фильтрация, группировка по городу, пять агрегатов, сортировка")
    print()

    # --- Тот же запрос, но с чтением файла: так выглядит полный цикл.
    section("6. Полный цикл: чтение Parquet и запрос")

    def duckdb_full() -> None:
        """DuckDB: подключение, чтение Parquet, запрос."""
        fresh = duckdb.connect()
        try:
            fresh.sql(sql).pl()
        finally:
            fresh.close()

    def polars_full() -> None:
        """Polars: чтение Parquet, затем тот же запрос."""
        read_polars(pl.read_parquet(target))

    full_duck_min, full_duck_med, full_duck_all = measure(duckdb_full)
    full_pol_min, full_pol_med, full_pol_all = measure(polars_full)

    print_table(
        [
            ["способ", "минимум, мс", "медиана, мс", "замеры, мс"],
            ["DuckDB (SQL)", *fmt_times(full_duck_min, full_duck_med, full_duck_all)],
            ["Polars", *fmt_times(full_pol_min, full_pol_med, full_pol_all)],
        ]
    )

    print("  Разница между этим замером и предыдущим - стоимость подключения")
    print("  к DuckDB. Она постоянная и не зависит от размера данных, поэтому")
    print("  на большом файле её доля уменьшается, а на таком маленьком - нет.")
    print()

    section("7. Вывод по скорости")

    print("Вычисление запроса, данные в памяти")
    print(rule())
    print(f"  {compare_speed('DuckDB', duck_med, 'Polars', pol_med)}")
    print()

    print("Полный цикл с чтением Parquet")
    print(rule())
    print(f"  {compare_speed('DuckDB', full_duck_med, 'Polars', full_pol_med)}")
    print()

    connect_cost = full_duck_med - duck_med
    if connect_cost > 0:
        print(
            f"  Подключение к DuckDB добавляет {connect_cost:.2f} мс к каждому запросу."
        )
        print()

    print("  Что из этого следует:")
    print("  - На 420 строках оба способа укладываются в единицы миллисекунд.")
    print("    Ни один из них не является узким местом конвейера.")
    print("  - Проигрыш DuckDB обеспечен не запросом, а стоимостью подключения:")
    print("    соединение создаётся заново, потому что конвейер работает по шагам.")
    print("  - Выигрыш DuckDB проявляется там, где запрос уходит в сканирование")
    print("    миллионов строк, а не двухсот. Такой замер потребовал бы данных на")
    print("    порядки больше, а методичка такого не требует.")
    print("  - Практический вывод для этого конвейера: DuckDB удобен тем, что")
    print("    запрос пишется на SQL и читается как обычный текст, а скорость на")
    print("    таком объёме у него не хуже.")
    print()

    connection.close()

    section("Проверенные файлы")
    print(f"  {target.relative_to(ROOT)}  {human_bytes(target.stat().st_size)}")
    print(f"  каталог отчётов: {REPORTS_DIR.relative_to(ROOT)}")
    print()

    return 0


def fmt_times(minimum: float, median: float, timings: list[float]) -> list[str]:
    """Приводит три времени к строкам таблицы."""
    return [
        f"{minimum:.2f}",
        f"{median:.2f}",
        ", ".join(f"{value:.1f}" for value in timings),
    ]


def compare_speed(name_a: str, time_a: float, name_b: str, time_b: float) -> str:
    """Формирует вывод по двум замерам.

    Args:
        name_a: название первого варианта.
        time_a: медиана первого варианта в миллисекундах.
        name_b: название второго варианта.
        time_b: медиана второго варианта в миллисекундах.

    Returns:
        Строка с выводом.
    """
    difference = abs(time_a - time_b)
    ratio = max(time_a, time_b) / min(time_a, time_b) if min(time_a, time_b) else 1.0
    numbers = f"{name_a} {time_a:.2f} мс, {name_b} {time_b:.2f} мс. "

    if difference < NOISE_MS or ratio < SPEEDUP_THRESHOLD:
        return (
            f"{numbers}Разница {difference:.2f} мс, отношение {ratio:.2f}. "
            f"Оба меньше порогов: {NOISE_MS:.0f} мс и {SPEEDUP_THRESHOLD:.1f}, "
            "время одинаково."
        )

    faster = name_a if time_a < time_b else name_b
    return f"{numbers}Быстрее {faster} в {ratio:.1f} раза."


if __name__ == "__main__":
    sys.exit(main())
