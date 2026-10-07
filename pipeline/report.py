"""Печать таблиц отчёта: ячейки в несколько строк, подписи и единицы измерения.

Встроенный вывод Polars рисует рамки и не переносит длинные заголовки, а
заголовки агрегатов как раз длинные: «средняя температура, °C» в одну колонку
не помещается, а «средняя» без единицы измерения не читается. Поэтому таблицы
печатаются обычным текстом, а колонка занимает столько строк, сколько строк в
её заголовке.
"""

from __future__ import annotations

from collections.abc import Sequence

# Ширина строки отчёта и отступ при печати таблиц.
WIDTH = 78
INDENT = "  "

# Единица измерения для каждого агрегируемого поля.
UNIT = {
    "temp_c": "°C",
    "feels_like_c": "°C",
    "temp_min_c": "°C",
    "temp_max_c": "°C",
    "dew_point_c": "°C",
    "pressure_hpa": "гПа",
    "humidity_pct": "%",
    "wind_speed_ms": "м/с",
    "wind_gust_ms": "м/с",
    "clouds_pct": "%",
    "visibility_m": "м",
    "pop_prob": "",
    "rain_mm": "мм",
    "snow_mm": "мм",
    "request_ms": "мс",
}

# Подпись агрегата во второй строке заголовка.
AGG_LABEL = {"mean": "ср.", "min": "мин", "max": "макс", "sum": "сум.", "count": ""}


def format_value(value: object, kind: str) -> str:
    """Приводит значение агрегата к строке отчёта.

    Args:
        value: значение агрегата.
        kind: формат: d - целое, f - дробное с двумя знаками, pct - проценты,
            km - километры вместо метров, m - целые метры.

    Returns:
        Строка для печати или прочерк, если значения нет.
    """
    if value is None:
        return "-"

    if kind == "d":
        return str(int(value))
    if kind == "f":
        return f"{float(value):.2f}"
    if kind == "pct":
        return f"{float(value) * 100:.1f}%"
    if kind == "km":
        return f"{float(value) / 1000:.1f}"
    if kind == "m":
        return str(int(value))

    return str(value)


def header_of(title: str, field: str, agg: str) -> str:
    """Заголовок колонки с единицей измерения.

    Среднее, минимум, максимум и сумму подписываются по-разному: без подписи
    «средняя» и «мин» в одной таблице читаются как одно и то же число.

    Args:
        title: подпись агрегата, например «средняя».
        field: поле, из которого взят агрегат: единица измерения берётся из него.
        agg: вид агрегата.

    Returns:
        Заголовок с переносами строк, готовый для печати.
    """
    parts = [title]

    label = AGG_LABEL.get(agg, "")
    if label:
        parts.append(label)

    unit = UNIT.get(field, "")
    if unit:
        parts.append(unit)

    return "\n".join(parts)


def _split(value: object) -> list[str]:
    """Разбивает ячейку на строки по переносам строк."""
    if isinstance(value, str) and "\n" in value:
        return value.split("\n")
    return [str(value)]


def print_table(
    rows: Sequence[Sequence[object]], align_left: Sequence[int] = (0,)
) -> None:
    """Печатает таблицу с многострочными заголовками.

    Заголовки занимают столько строк, сколько нужно: «температура / ср. / °C».
    Данные выводятся ниже разделителя, по одной строке на запись.

    Args:
        rows: первая строка - заголовки, остальные - данные.
        align_left: номера колонок, выравниваемых по левому краю. По умолчанию
            только первая колонка: ключ группировки читают глазами, а числа
            сравнивают по разрядам, и для них нужен правый край.
    """
    if not rows:
        print("нет данных")
        return

    header = rows[0]
    body = rows[1:]
    left = set(align_left)

    # Ширина колонки считается и по заголовку, и по данным: заголовок
    # «вероятность осадков» шире любого процента в таблице.
    widths = [max(len(part) for part in _split(item)) + 2 for item in header]
    for row in body:
        for index, item in enumerate(row):
            widths[index] = max(
                widths[index], max(len(part) for part in _split(item)) + 2
            )

    def show(values: list[list[str]], position: int = 0) -> str:
        """Собирает одну строку таблицы из ячеек."""
        cells = [(cell[position] if position < len(cell) else "") for cell in values]
        line = [
            text.ljust(widths[index] - 1)
            if index in left
            else text.rjust(widths[index] - 1)
            for index, text in enumerate(cells)
        ]
        return (INDENT + "".join(line)).rstrip()

    # Заголовок печатается по своей высоте, а каждая строка данных - по своей:
    # у данных одна строка, и печатать для них столько же строк, сколько в
    # заголовке, значило бы выводить пустые строки между записями.
    for position in range(max(len(_split(item)) for item in header)):
        print(show([_split(item) for item in header], position))

    print("-" * min(WIDTH, sum(widths) + len(INDENT) - 1))

    for row in body:
        cells = [_split(item) for item in row]
        for position in range(max(len(cell) for cell in cells)):
            print(show(cells, position))

    print()
