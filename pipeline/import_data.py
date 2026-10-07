"""Задание 4. Импорт данных в Polars.

Загружает собранные сборщиком JSON-файлы в таблицу Polars и показывает
базовую информацию: первые строки, типы полей, количество строк и пропуски.

Запуск:

    python -m pipeline.import_data
"""

from __future__ import annotations

import sys

import polars as pl

from pipeline.common import (
    HEAD_ROWS,
    WEATHER_SCHEMA,
    human_bytes,
    load_raw,
    raw_files,
    rule,
    section,
    timed,
)


def print_head(frame: pl.DataFrame, rows: int = HEAD_ROWS) -> None:
    """Печатает первые строки таблицы.

    Таблица рисуется обычным текстом, а не средствами Polars: полей
    двадцать семь, и встроенный вывод усекает либо ширину, либо высоту, из-за
    чего часть данных просто не попадает в отчёт.
    """
    head = frame.head(rows)
    columns = [f"№{index + 1}" for index in range(head.height)]

    print(f"Первые {rows} строк из {frame.height}")
    print(rule())

    name_width = max(16, *(len(str(field)) for field in head.columns))
    cell_width = 15

    header = "поле".ljust(name_width) + "".join(
        title.rjust(cell_width) for title in columns
    )
    print(header)
    print(rule(width=len(header)))

    for field in head.columns:
        values = []
        for value in head[field].to_list():
            text_value = "" if value is None else str(value)
            if len(text_value) > cell_width - 1:
                text_value = text_value[: cell_width - 2] + "…"
            values.append(text_value.rjust(cell_width))

        print(str(field).ljust(name_width) + "".join(values))


def print_schema(frame: pl.DataFrame) -> None:
    """Печатает типы полей."""
    print("Типы полей")
    print(rule())
    print(f"{'поле':22} {'тип в данных':18} {'ожидается':12}")
    print(rule())

    for name, dtype in frame.schema.items():
        expected = WEATHER_SCHEMA.get(name)
        expected_name = str(expected) if expected is not None else "нет в схеме"
        print(f"{name:22} {str(dtype):18} {expected_name:12}")

    print(rule())
    print(f"всего столбцов: {frame.width}")


def print_nulls(frame: pl.DataFrame) -> None:
    """Печатает количество пропусков по полям."""
    counts = frame.null_count()
    # null_count возвращает таблицу в одну строку: сумма по ней считается
    # по горизонтали, иначе получилась бы новая таблица, а не число.
    total_nulls = int(counts.sum_horizontal().item())
    with_nulls = int((counts > 0).sum_horizontal().item())

    print("Пропуски")
    print(rule())
    print(f"{'поле':22} {'пропусков':10} {'доля':8}")
    print(rule())

    # У null_count нет метода items, поэтому столбцы и единственная строка
    # со счётчиками берутся раздельно.
    row = counts.row(0)

    for name, count in zip(counts.columns, row, strict=True):
        share = count / frame.height if frame.height else 0
        mark = "" if count == 0 else "  <-"
        print(f"{name:22} {count:<10} {share * 100:6.2f}%{mark}")

    print(rule())
    print(f"всего пропусков: {total_nulls} в {with_nulls} полях из {frame.width}")


def print_overview(frame: pl.DataFrame, files: list) -> None:
    """Печатает общие сведения о загруженных данных."""
    print("Общие сведения")
    print(rule())
    print(f"файлов прочитано        {len(files)}")
    for path in files:
        print(
            f"  {path.relative_to(path.parents[2])}  {human_bytes(path.stat().st_size)}"
        )
    print(f"строк                   {frame.height}")
    print(f"столбцов                {frame.width}")
    print(f"городов                 {frame['city'].n_unique()}")
    print(f"эндпоинтов              {sorted(frame['endpoint'].unique().to_list())}")
    print(f"размер в памяти         {human_bytes(frame.estimated_size())}")
    print(f"период наблюдений       {frame['observed_at'].min()}")
    print(f"                        {frame['observed_at'].max()}")


def main() -> int:
    """Загружает данные и печатает отчёт.

    Returns:
        Код возврата: 0 при успехе.
    """
    section("Задание 4. Импорт данных в Polars")

    print("Загрузка файлов")
    print(rule())

    files = raw_files()
    if not files:
        print("Файлы с данными не найдены.")
        print("Сначала запустите сборщик: go run ./cmd/collector")
        return 1

    with timed("чтение и разбор JSON"):
        frame = load_raw(files)

    print(f"прочитано файлов: {len(files)}")
    print()

    section("Базовая информация о данных")
    print_overview(frame, files)
    print()
    print_head(frame)
    print()
    print_schema(frame)
    print()
    print_nulls(frame)

    return 0


if __name__ == "__main__":
    sys.exit(main())
