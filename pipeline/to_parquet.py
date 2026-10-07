"""Задание 7. Сохранение очищенных данных в Parquet.

Методичка требует сохранить очищенный DataFrame в Parquet одним файлом.
Просто записать файл - половина задачи: остаётся убедиться, что из
Parquet читается то же самое, что записывалось. Поэтому после записи файл
читается обратно и сравнивается построчно: состав полей, типы, количество
строк и значения. Round-trip-проверка ловит то, что тихая запись скрыла бы:
потерю строк при сжатии, смену типа целого на дробное, обрезание текста.

Дополнительно измеряется, во сколько раз Parquet меньше JSON, и сравниваются
три способа сжатия. Это не украшение: выбор сжатия влияет и на размер, и на
скорость следующего шага, а DuckDB в задании 8 читает именно этот файл.

Запуск:

    python -m pipeline.to_parquet
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

from pipeline.common import (
    REPORTS_DIR,
    ROOT,
    clean_out_path,
    ensure_dir,
    human_bytes,
    load_clean,
    rule,
    section,
    timed,
)

# Имя файла Parquet. Один файл, как требует методичка: разбиение на части
# нужно, когда данных больше, чем помещается в память, а это не наш случай.
PARQUET_FILE = "weather.parquet"

# Способы сжатия для сравнения. Имя без сжатия у Polars именно
# uncompressed, а не none: с none запрос падает с ValueError и перечисление
# нельзя было бы использовать как есть.
#
# zstd жмёт сильнее и распакует быстрее, чем gzip; несжатый вариант нужен,
# чтобы увидеть разницу.
COMPRESSIONS = ["uncompressed", "snappy", "zstd", "gzip"]

# Колонки, которые показываются в отчёте. Все тридцать одна не помещаются, а
# список подобран так, чтобы в нём был представитель каждого типа, который
# важно пережить запись: текст, время с часовым поясом, местное время, дата,
# дробное число, целое число и булево значение. Без такого подбора читатель не
# увидит, что время и логические значения не пострадали.
CHECK_FIELDS = [
    "city",
    "endpoint",
    "observed_at",
    "observed_local",
    "local_date",
    "temp_c",
    "humidity_pct",
    "is_day",
    "weather_main",
]


def parquet_path() -> Path:
    """Путь к файлу Parquet."""
    return REPORTS_DIR / PARQUET_FILE


def save_parquet(frame: pl.DataFrame, target: Path, compression: str = "zstd") -> int:
    """Записывает таблицу в Parquet и возвращает размер файла в байтах.

    Args:
        frame: очищенные данные.
        target: путь к файлу.
        compression: способ сжатия.

    Returns:
        Размер записанного файла в байтах.
    """
    ensure_dir(target.parent)
    frame.write_parquet(target, compression=compression)
    return target.stat().st_size


def verify_roundtrip(frame: pl.DataFrame, target: Path) -> list[str]:
    """Читает Parquet обратно и сравнивает с исходной таблицей.

    Сравнение построчное, а не по агрегатам: сумма может случайно совпасть и
    при потерянных строках, если в других строках значения изменились.

    Args:
        frame: таблица до записи.
        target: записанный файл.

    Returns:
        Список расхождений. Пустой список означает, что данные совпали.
    """
    problems: list[str] = []
    restored = pl.read_parquet(target)

    if restored.width != frame.width:
        problems.append(f"столбцов: было {frame.width}, стало {restored.width}")

    if restored.height != frame.height:
        problems.append(f"строк: было {frame.height}, стало {restored.height}")

    missing = [field for field in frame.columns if field not in restored.columns]
    extra = [field for field in restored.columns if field not in frame.columns]
    if missing:
        problems.append(f"нет полей после чтения: {', '.join(missing)}")
    if extra:
        problems.append(f"лишние поля после чтения: {', '.join(extra)}")

    if problems:
        return problems

    changed = [
        field
        for field, dtype in frame.schema.items()
        if restored.schema.get(field) != dtype
    ]
    if changed:
        for field in changed:
            problems.append(
                f"тип поля {field}: было {frame.schema[field]}, "
                f"стало {restored.schema[field]}"
            )

    # Сравнение значений построчно. equals сравнивает значения, но не порядок
    # строк: файл читается в порядке записи, а полагаться на это в проверке
    # рано, поэтому порядок сравнивается отдельно.
    if restored.columns != frame.columns:
        problems.append("порядок столбцов изменился")

    same = frame.equals(restored)
    if not same:
        for field in frame.columns:
            if frame[field].to_list() != restored[field].to_list():
                problems.append(f"значения поля {field} отличаются")
                break

    return problems


def print_schema(frame: pl.DataFrame) -> None:
    """Печатает схему файла Parquet.

    Ширина колонки типа считается по самому длинному значению: у полей времени
    это Datetime(time_unit='us', time_zone='UTC'), и при фиксированной ширине
    колонки счётчики значений съезжали бы вправо по одной строке за раз.
    """
    print("Схема файла")
    print(rule())

    nulls = frame.null_count()
    row = nulls.row(0)
    counts = {name: count for name, count in zip(nulls.columns, row, strict=True)}

    type_width = max(len(str(dtype)) for dtype in frame.schema.values())
    name_width = max(len(field) for field in frame.columns)

    print(f"{'поле':{name_width}} {'тип':{type_width}} {'значений':>10} {'нулей':>8}")
    print(rule())

    for name, dtype in frame.schema.items():
        value = frame.height - counts[name]
        print(
            f"{name:{name_width}} {str(dtype):{type_width}} "
            f"{value:>10} {counts[name]:>8}"
        )

    print(rule())
    print(f"столбцов: {frame.width}, строк: {frame.height}")


def print_sample(frame: pl.DataFrame, rows: int = 5) -> None:
    """Печатает первые строки, прочитанные из Parquet.

    Показываются не все тридцать одно поле, а шесть: по одному на каждый тип
    данных, который важно пережить запись без потерь, - текст, время, дробное
    число, целое число и булево значение.
    """
    head = frame.head(rows)

    print(f"Первые {rows} строк из {frame.height}")
    print(rule())

    fields = list(CHECK_FIELDS)
    name_width = max(len(field) for field in fields)
    cell_width = 24

    print(
        "поле".ljust(name_width)
        + "".join(f"№{index + 1}".rjust(cell_width) for index in range(head.height))
    )
    print(rule(width=name_width + cell_width * head.height - 1))

    for field in fields:
        values = []
        for value in head[field].to_list():
            text = "" if value is None else str(value)
            if len(text) > cell_width - 1:
                text = text[: cell_width - 2] + "…"
            values.append(text.rjust(cell_width))
        print(field.ljust(name_width) + "".join(values))


def print_compression(source: pl.DataFrame, target: Path) -> None:
    """Сравнивает способы сжатия и размер файла с исходным JSON."""
    json_bytes = clean_out_path().stat().st_size
    raw_bytes = source.estimated_size()

    print("Сравнение сжатия")
    print(rule())
    print(f"{'способ':12} {'размер':>10} {'к JSON':>9} {'в памяти':>10}")
    print(rule())

    sizes: dict[str, int] = {}
    for compression in COMPRESSIONS:
        size = save_parquet(source, target, compression)
        sizes[compression] = size
        ratio = json_bytes / size if size else 0
        print(
            f"{compression:12} {human_bytes(size):>10} {ratio:8.1f}x "
            f"{human_bytes(raw_bytes):>10}"
        )

    print(rule())
    best = min(sizes, key=lambda name: sizes[name])
    worst = max(sizes, key=lambda name: sizes[name])
    print(f"самый компактный: {best}")
    print(f"наименее компактный: {worst}")
    print(f"разница между худшим и лучшим: {human_bytes(sizes[worst] - sizes[best])}")
    print()
    print(
        f"JSON занимает {human_bytes(json_bytes)}, в памяти таблица "
        f"{human_bytes(raw_bytes)}"
    )
    print(
        "Parquet меньше JSON и потому, что хранит только столбцы, и потому, "
        "что сжимает их по частям: одинаковые значения рядом схлопываются."
    )
    print(
        "JSON хранит имена полей в каждой строке, поэтому каждая из "
        f"{source.height} строк повторяет названия всех {source.width} полей."
    )


def main() -> int:
    """Сохраняет очищенные данные в Parquet и проверяет результат.

    Returns:
        Код возврата: 0 при успехе, 1 если данные не совпали после чтения.
    """
    section("Задание 7. Сохранение в Parquet")

    frame = load_clean()
    print(f"Прочитано очищенных данных: {frame.height} строк, {frame.width} столбцов")
    print()

    section("1. Схема и данные до записи")
    print_schema(frame)
    print()
    print_sample(frame)
    print()

    section("2. Сравнение способов сжатия")

    target = parquet_path()
    with timed("запись пяти вариантов"):
        print_compression(frame, target)
    print()

    section("3. Запись в выбранном формате")

    with timed("запись Parquet со сжатием zstd"):
        size = save_parquet(frame, target, "zstd")

    print(f"  файл: {target.relative_to(ROOT)}")
    print(f"  размер: {human_bytes(size)}")
    print(f"  строк: {frame.height}, столбцов: {frame.width}")
    print()

    section("4. Проверка после чтения")

    with timed("чтение Parquet обратно"):
        restored = pl.read_parquet(target)

    problems = verify_roundtrip(frame, target)

    print(f"  прочитано строк: {restored.height}")
    print(f"  прочитано столбцов: {restored.width}")
    print(
        f"  полей по схеме: {sum(1 for f, d in frame.schema.items() if restored.schema.get(f) == d)}"
    )
    print(f"  пропусков: {int(restored.null_count().sum_horizontal().item())}")
    print(f"  размер в памяти: {human_bytes(restored.estimated_size())}")
    print()

    if problems:
        print("  ОШИБКА: данные не совпали после чтения")
        for line in problems:
            print(f"    {line}")
        return 1

    print("  значения всех полей совпали построчно")
    print("  порядок столбцов сохранён")
    print()

    # Разница в размерах Parquet и таблицы в памяти объяснима: в файле данные
    # сжаты, в памяти лежат распакованными.
    in_memory = frame.estimated_size()
    print(
        f"  в файле {human_bytes(size)}, в памяти {human_bytes(in_memory)}, "
        f"в {in_memory / size:.1f} раза больше"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
