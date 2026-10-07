"""Очистка и валидация данных (задание 5).

Шаги выполняются по очереди, каждый считает, сколько строк он изменил или
удалил. Порядок выбран так, чтобы каждая проверка работала с уже приведёнными
данными: сначала дубликаты, потом пропуски, затем типы и только потом
диапазоны значений.

Политика обработки:

- полные дубликаты удаляются;
- повторные наблюдения того же города и того же времени удаляются, остаётся
  самая свежая запись по времени сбора;
- время разбирается в три поля: момент наблюдения в UTC, местное время
  города и местная дата. Местная дата нужна для суточных агрегатов:
  сутки в городе заканчиваются в полночь по его времени, а не по UTC;
- строки без города, времени наблюдения или температуры удаляются: такую
  запись нельзя ни сгруппировать, ни показать на графике;
- остальные пропуски заполняются нулём: так означает «показатель не
  сообщён», а для осадков, порывов и облачности ноль означает то же самое,
  что и отсутствие значения;
- значения вне правдоподобных диапазонов удаляются вместе со строкой.

Запуск:

    python -m pipeline.clean_data
"""

from __future__ import annotations

import sys

import polars as pl

from pipeline.common import (
    CLEAN_SCHEMA,
    INTERIM_DIR,
    clean_out_path,
    ensure_dir,
    human_bytes,
    load_raw,
    raw_files,
    rule,
    section,
)

# Ключ наблюдения: город, эндпоинт и момент наблюдения однозначно
# определяют запись, город может прийти из двух сборов.
DEDUP_KEY = ["city", "endpoint", "observed_at"]

# Поля, без которых запись бесполезна.
REQUIRED_FIELDS = ["city", "observed_at", "temp_c"]

# Числовые поля, где отсутствие значения равнозначно нулю: осадков не было,
# порывов не ожидалось, облачность неизвестна. Ноль для них - честное
# значение, а пустое означало бы пробел в отчёте.
NULL_IS_ZERO = [
    "wind_gust_ms",
    "rain_mm",
    "snow_mm",
    "pop_prob",
    "visibility_m",
    "clouds_pct",
    "dew_point_c",
    "pressure_hpa",
    "humidity_pct",
    "weather_id",
    "request_ms",
]

# Текстовые поля заполняются пустой строкой. Ноль здесь означал бы строку
# "0" вместо описания погоды, и она попала бы в графики и отчёты как
# настоящее название.
NULL_IS_EMPTY_TEXT = [
    "weather_main",
    "weather_desc",
    "country",
]

# Правдоподобные диапазоны. Границы взяты шире наблюдавшихся значений:
# задача проверять явную нелепость, а не отсекать редкие, но настоящие
# погодные условия.
VALUE_RANGES = {
    "temp_c": (-80.0, 60.0),
    "feels_like_c": (-90.0, 70.0),
    "pressure_hpa": (850.0, 1100.0),
    "humidity_pct": (0.0, 100.0),
    "clouds_pct": (0.0, 100.0),
    "pop_prob": (0.0, 1.0),
    "wind_speed_ms": (0.0, 120.0),
    "rain_mm": (0.0, 200.0),
    "snow_mm": (0.0, 200.0),
    "visibility_m": (0.0, 100000.0),
    "request_ms": (0.0, 60000.0),
}

# Границы для широты и долготы: за их пределами точка не может быть городом.
COORD_LIMITS = {"lat": (-90.0, 90.0), "lon": (-180.0, 180.0)}


class Step:
    """Результат одного шага очистки."""

    def __init__(self, title: str) -> None:
        self.title = title
        self.notes: list[str] = []

    def note(self, text: str) -> None:
        """Добавляет пояснение к шагу."""
        self.notes.append(text)


def drop_full_duplicates(frame: pl.DataFrame, step: Step) -> pl.DataFrame:
    """Удаляет полностью совпадающие строки."""
    before = frame.height
    result = frame.unique(keep="first", maintain_order=True)
    removed = before - result.height

    step.note(f"удалено полных дубликатов: {removed}")
    return result


def drop_observation_duplicates(frame: pl.DataFrame, step: Step) -> pl.DataFrame:
    """Оставляет по одной записи на наблюдение, самую свежую.

    Сортировка по времени сбора выполняется до выборки: без неё
    `keep="last"` оставил бы произвольную из двух записей, а не новую.
    """
    before = frame.height
    keys = frame.select(DEDUP_KEY).n_unique()
    removed = before - keys

    result = frame.sort([*DEDUP_KEY, "collected_at"]).unique(
        subset=DEDUP_KEY, keep="last", maintain_order=True
    )

    step.note(
        f"уникальных наблюдений: {keys}, удалено повторов: {removed}, "
        "оставлена самая свежая запись по времени сбора"
    )
    return result


def parse_time(frame: pl.DataFrame, step: Step) -> pl.DataFrame:
    """Разбирает время наблюдения в три поля.

    В данных у городов разные часовые пояса: от +03:00 до +10:00. Из
    строки с часовым поясом получается момент в UTC, из той же строки без
    смещения - местное время города. Оба нужны: временной ряд строится по
    UTC, а суточные агрегаты считаются по местной дате.
    """
    # Время наблюдения приходит со смещением города: +03:00 и так далее.
    # Время сбора приходит в UTC и заканчивается буквой Z, которую формат с
    # часовым поясом не понимает: буква отбрасывается, а к разобранному
    # значению привязывается UTC.
    result = frame.with_columns(
        pl.col("observed_at")
        .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", time_zone="UTC")
        .alias("observed_utc"),
        pl.col("observed_at")
        .str.slice(0, 19)
        .str.to_datetime(format="%Y-%m-%dT%H:%M:%S")
        .alias("observed_local"),
        pl.col("collected_at")
        .str.strip_chars("Z")
        .str.to_datetime(format="%Y-%m-%dT%H:%M:%S")
        .dt.replace_time_zone("UTC")
        .alias("collected_utc"),
    )

    result = result.with_columns(
        pl.col("observed_local").dt.date().alias("local_date"),
    )

    step.note("добавлены поля observed_utc, observed_local, collected_utc и local_date")
    return result


def fill_nulls(frame: pl.DataFrame, step: Step) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Удаляет строки без обязательных полей и заполняет остальные пропуски.

    Returns:
        Пара: очищенная таблица и таблица удалённых строк.
    """
    missing = frame.filter(
        pl.any_horizontal([pl.col(field).is_null() for field in REQUIRED_FIELDS])
    )
    result = frame.filter(
        ~pl.any_horizontal([pl.col(field).is_null() for field in REQUIRED_FIELDS])
    )

    step.note(f"удалено строк без обязательных полей: {missing.height}")

    numbers = [field for field in NULL_IS_ZERO if field in result.columns]
    texts = [field for field in NULL_IS_EMPTY_TEXT if field in result.columns]
    before_nulls = int(result.null_count().sum_horizontal().item())

    if numbers:
        result = result.with_columns(
            [pl.col(field).fill_null(0).cast(CLEAN_SCHEMA[field]) for field in numbers]
        )

    if texts:
        result = result.with_columns(
            [pl.col(field).fill_null("").cast(CLEAN_SCHEMA[field]) for field in texts]
        )

    after_nulls = int(result.null_count().sum_horizontal().item())
    step.note(
        f"заполнено пропусков: {before_nulls - after_nulls} "
        f"(нулём в {len(numbers)} числовых полях, пустой строкой в {len(texts)} "
        f"текстовых), осталось пропусков: {after_nulls}"
    )

    return result, missing


def drop_out_of_range(
    frame: pl.DataFrame, step: Step
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Удаляет строки со значениями вне правдоподобных диапазонов."""
    conditions = []

    for field, (low, high) in {**VALUE_RANGES, **COORD_LIMITS}.items():
        if field not in frame.columns:
            continue
        conditions.append(((pl.col(field) < low) | (pl.col(field) > high)).alias(field))

    invalid = frame.filter(pl.any_horizontal(conditions))
    result = frame.filter(~pl.any_horizontal(conditions))

    offenders = []
    for field in {**VALUE_RANGES, **COORD_LIMITS}:
        if field not in frame.columns:
            continue
        low, high = {**VALUE_RANGES, **COORD_LIMITS}[field]
        count = frame.filter((pl.col(field) < low) | (pl.col(field) > high)).height
        if count:
            offenders.append(f"{field} вне [{low:g}, {high:g}] — {count}")

    step.note(f"удалено строк вне диапазонов: {invalid.height}")
    for line in offenders:
        step.note(f"  {line}")

    return result, invalid


def cast_types(frame: pl.DataFrame, step: Step) -> pl.DataFrame:
    """Приводит типы полей к схеме очищенных данных."""
    result = frame.with_columns(
        [
            pl.col(field).cast(dtype)
            for field, dtype in CLEAN_SCHEMA.items()
            if field in frame.columns
        ]
    )

    changed = [
        field
        for field, dtype in CLEAN_SCHEMA.items()
        if field in frame.columns and frame.schema[field] != dtype
    ]

    step.note(
        f"приведено к схеме полей: {len(CLEAN_SCHEMA)}, из них сменили тип: {len(changed)}"
    )
    return result


def sort_by_time(frame: pl.DataFrame, step: Step) -> pl.DataFrame:
    """Сортирует данные по городу и времени.

    Дальше по конвейеру идут агрегаты по городам и временные ряды, и оба
    требуют упорядоченных данных.
    """
    result = frame.sort(["city", "observed_utc"])
    step.note("сортировка по городу и моменту наблюдения")
    return result


def print_step(index: int, step: Step, rows_before: int, rows_after: int) -> None:
    """Печатает отчёт по шагу."""
    print(f"Шаг {index}. {step.title}")
    print(rule())
    print(
        f"  строк до и после: {rows_before} -> {rows_after} (изменено {rows_before - rows_after})"
    )
    for note in step.notes:
        print(f"  {note}")
    print()


def main() -> int:
    """Выполняет очистку и сохраняет результат.

    Returns:
        Код возврата: 0 при успехе, 1 если данные не прошли проверку.
    """
    section("Задание 5. Очистка и валидация данных")

    files = raw_files()
    if not files:
        print("Файлы с данными не найдены. Сначала запустите сборщик.")
        return 1

    frame = load_raw(files)
    rows_loaded = frame.height
    print(f"Загружено файлов: {len(files)}, строк: {rows_loaded}")
    print()

    rejected: list[pl.DataFrame] = []

    steps: list[Step] = []

    def remember(step: Step, before: int) -> None:
        """Сохраняет шаг и печатает его."""
        steps.append(step)
        print(f"Шаг {len(steps)}. {step.title}")
        print(rule())
        print(f"  строк до и после: {before} -> {frame.height}")
        for note in step.notes:
            print(f"  {note}")
        print()

    step = Step("Полные дубликаты")
    before = frame.height
    frame = drop_full_duplicates(frame, step)
    remember(step, before)

    step = Step("Повторные наблюдения того же города и времени")
    before = frame.height
    frame = drop_observation_duplicates(frame, step)
    remember(step, before)

    step = Step("Разбор времени")
    before = frame.height
    frame = parse_time(frame, step)
    remember(step, before)

    step = Step("Обработка пропусков")
    before = frame.height
    frame, missing_rows = fill_nulls(frame, step)
    if missing_rows.height:
        rejected.append(missing_rows)
    remember(step, before)

    step = Step("Проверка диапазонов значений")
    before = frame.height
    frame, invalid_rows = drop_out_of_range(frame, step)
    if invalid_rows.height:
        rejected.append(invalid_rows)
    remember(step, before)

    step = Step("Приведение типов")
    before = frame.height
    frame = cast_types(frame, step)
    remember(step, before)

    step = Step("Сортировка по городу и времени")
    before = frame.height
    frame = sort_by_time(frame, step)
    remember(step, before)

    section("Итог")

    print(f"  строк на входе          {rows_loaded}")
    print(f"  строк на выходе         {frame.height}")
    print(f"  удалено                 {rows_loaded - frame.height}")
    print(f"  столбцов                {frame.width}")
    print(f"  городов                 {frame['city'].n_unique()}")
    print(f"  наблюдений на город     {frame.height // frame['city'].n_unique()}")
    print(f"  период наблюдений (UTC) {frame['observed_utc'].min()}")
    print(f"                          {frame['observed_utc'].max()}")
    print(f"  местных дат             {frame['local_date'].n_unique()}")
    print(f"  размер в памяти         {human_bytes(frame.estimated_size())}")
    print()

    section("Проверка после очистки")

    mismatched = [
        (field, frame.schema.get(field), expected)
        for field, expected in CLEAN_SCHEMA.items()
        if frame.schema.get(field) != expected
    ]

    if mismatched:
        print("  ОШИБКА: типы не совпадают со схемой очищенных данных")
        for field, actual, expected in mismatched:
            print(f"    {field}: получено {actual}, ожидалось {expected}")
        return 1

    nulls = int(frame.null_count().sum_horizontal().item())
    keys = frame.select(DEDUP_KEY).n_unique()

    print(f"  полей по схеме          {len(CLEAN_SCHEMA)} совпадают")
    print(f"  пропусков               {nulls}")
    print(f"  повторов наблюдений     {frame.height - keys}")
    print(f"  городов                 {frame['city'].n_unique()}")
    print()

    if rejected:
        section("Отклонённые строки")
        sample = pl.concat(rejected, how="vertical_relaxed")
        print(f"  всего отклонено: {sample.height}")
        print(sample.select(["city", "observed_at", "temp_c"]).head(5))
        print()

    ensure_dir(INTERIM_DIR)
    target = clean_out_path()
    frame.write_json(target)

    section("Результат сохранён")
    print(f"  {target}  {human_bytes(target.stat().st_size)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
