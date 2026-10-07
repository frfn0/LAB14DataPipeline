"""Задание 6. Агрегационный анализ.

Группирует очищенные данные по городу и считает основные агрегаты: SUM, AVG,
MIN, MAX и COUNT. Методичка требует группировку по одному ключевому полю, но
по одному полю погоду не опишешь: сумма осадков за пять дней и суточная
динамика температуры отвечают на разные вопросы. Поэтому агрегаты считаются
пятью способами, и каждый отвечает на свой:

- по городу: какая погода стоит в городе за весь период;
- по городу и местной дате: как погода менялась по дням;
- по типу погоды: сколько наблюдений пришлось на каждый тип;
- по эндпоинту: чем наблюдение сейчас отличается от прогноза;
- ранжирование городов: кто где по температуре и влажности.

Суммы температур и влажности сами по себе бессмысленны: сложение градусов не
значит ничего. Поэтому в таблицах SUM приводится там, где сумма осмысленна, -
в осадках, снеге и количестве наблюдений.

Запуск:

    python -m pipeline.aggregate
"""

from __future__ import annotations

import sys

import polars as pl

from pipeline.common import load_clean, rule, section, timed
from pipeline.report import format_value, header_of, print_table

# Колонки таблицы по городам: подпись, имя в таблице, поле, агрегат, формат.
#
# Имя в таблице задаётся отдельно от имени поля: у температуры три агрегата,
# и без отдельных имён Polars отклонил бы запрос как повторяющийся столбец.
CityColumn = tuple[str, str, str, str, str]

CITY_COLUMNS: list[CityColumn] = [
    ("наблюдений", "count", "city", "count", "d"),
    ("температура", "temp_avg", "temp_c", "mean", "f"),
    ("мин", "temp_min", "temp_c", "min", "f"),
    ("макс", "temp_max", "temp_c", "max", "f"),
    ("ощущается", "feels_avg", "feels_like_c", "mean", "f"),
    ("влажность", "humidity_avg", "humidity_pct", "mean", "f"),
    ("давление", "pressure_avg", "pressure_hpa", "mean", "f"),
    ("облачность", "clouds_avg", "clouds_pct", "mean", "f"),
    ("ветер", "wind_avg", "wind_speed_ms", "mean", "f"),
    ("порывы", "wind_max", "wind_gust_ms", "max", "f"),
    ("видимость", "visibility_avg", "visibility_m", "mean", "km"),
    ("осадки", "rain_sum", "rain_mm", "sum", "f"),
    ("снег", "snow_sum", "snow_mm", "sum", "f"),
    ("вероятность осадков", "pop_avg", "pop_prob", "mean", "pct"),
]

# Колонки таблицы по городам и дням.
DAILY_COLUMNS: list[tuple[str, str, str]] = [
    ("наблюдений", "count", "d"),
    ("температура", "temp_mean", "f"),
    ("мин", "temp_min", "f"),
    ("макс", "temp_max", "f"),
    ("осадки", "rain_sum", "f"),
    ("снег", "snow_sum", "f"),
    ("влажность", "humidity_mean", "f"),
]

# Колонки таблицы по типам погоды.
CONDITION_COLUMNS: list[tuple[str, str, str]] = [
    ("наблюдений", "count", "d"),
    ("доля", "share", "pct"),
    ("температура", "temp_mean", "f"),
    ("мин", "temp_min", "f"),
    ("макс", "temp_max", "f"),
    ("влажность", "humidity_mean", "f"),
    ("осадки", "rain_sum", "f"),
    ("снег", "snow_sum", "f"),
    ("вероятность осадков", "pop_mean", "pct"),
]

# Колонки сравнения наблюдения и прогноза.
ENDPOINT_COLUMNS: list[tuple[str, str, str]] = [
    ("наблюдений", "count", "d"),
    ("температура", "temp_mean", "f"),
    ("влажность", "humidity_mean", "f"),
    ("давление", "pressure_mean", "f"),
    ("облачность", "clouds_mean", "f"),
    ("ветер", "wind_mean", "f"),
    ("вероятность осадков", "pop_mean", "pct"),
]

# Сколько городов показывать в суточной таблице. Все десять дадут семьдесят
# строк, и отчёт станет нечитаемым; в README приводится выборка.
DAILY_CITIES = 3


def city_table(frame: pl.DataFrame) -> pl.DataFrame:
    """Считает агрегаты по городам.

    Сортировка по средней температуре по убыванию: первым идёт самый тёплый
    город, и в отчёте сразу видно, кто где.
    """
    aggregations = [
        getattr(pl.col(field), agg)().alias(name)
        for _, name, field, agg, _ in CITY_COLUMNS
    ]

    return frame.group_by("city").agg(aggregations).sort("temp_avg", descending=True)


def print_city_report(frame: pl.DataFrame) -> None:
    """Печатает агрегаты по городам."""
    table = city_table(frame)

    rows: list[list[object]] = [
        [
            "город",
            *[header_of(title, field, agg) for title, _, field, agg, _ in CITY_COLUMNS],
        ]
    ]
    for record in table.iter_rows(named=True):
        rows.append(
            [
                record["city"],
                *[
                    format_value(record[name], kind)
                    for _, name, _, _, kind in CITY_COLUMNS
                ],
            ]
        )

    print_table(rows)

    warmest = table.row(0, named=True)
    coldest = table.row(-1, named=True)
    total_rain = frame["rain_mm"].sum()
    total_snow = frame["snow_mm"].sum()

    print(f"  городов: {table.height}, наблюдений: {table['count'].sum()}")
    print()
    print(f"  теплее всего: {warmest['city']}, средняя {warmest['temp_avg']:.2f}°C")
    print(f"  холоднее всего: {coldest['city']}, средняя {coldest['temp_avg']:.2f}°C")
    print(f"  осадков за период: {total_rain:.2f} мм, снега {total_snow:.2f} мм")


def daily_table(frame: pl.DataFrame) -> pl.DataFrame:
    """Считает агрегаты по городу и местной дате.

    Группировка идёт по местной дате, а не по дате в UTC: сутки в городе
    заканчиваются в полночь по его времени, и наблюдение в 00:30 по Москве
    относится к сегодняшним, а не к вчерашним суткам.
    """
    return (
        frame.group_by(["city", "local_date"])
        .agg(
            pl.len().alias("count"),
            pl.col("temp_c").mean().alias("temp_mean"),
            pl.col("temp_c").min().alias("temp_min"),
            pl.col("temp_c").max().alias("temp_max"),
            pl.col("rain_mm").sum().alias("rain_sum"),
            pl.col("snow_mm").sum().alias("snow_sum"),
            pl.col("humidity_pct").mean().alias("humidity_mean"),
        )
        .sort(["city", "local_date"])
    )


def print_daily_report(frame: pl.DataFrame, cities: int = DAILY_CITIES) -> None:
    """Печатает суточные агрегаты по нескольким городам."""
    table = daily_table(frame)
    shown = table.filter(
        pl.col("city").is_in(sorted(frame["city"].unique().to_list())[:cities])
    )

    rows: list[list[object]] = [
        ["город", "местная дата", *[header_of(t, "", "") for t, _, _ in DAILY_COLUMNS]]
    ]
    for record in shown.iter_rows(named=True):
        rows.append(
            [
                record["city"],
                str(record["local_date"]),
                *[format_value(record[name], kind) for _, name, kind in DAILY_COLUMNS],
            ]
        )

    print_table(rows)

    warmest_day = table.sort("temp_mean", descending=True).row(0, named=True)
    coldest_day = table.sort("temp_mean").row(0, named=True)

    print(f"  строк по всем городам: {table.height}")
    print(f"  показаны {cities} первых города по алфавиту")
    print()
    print(
        f"  самый тёплый город-день: {warmest_day['city']} {warmest_day['local_date']}, "
        f"средняя {warmest_day['temp_mean']:.2f}°C"
    )
    print(
        f"  самый холодный город-день: {coldest_day['city']} {coldest_day['local_date']}, "
        f"средняя {coldest_day['temp_mean']:.2f}°C"
    )


def conditions_table(frame: pl.DataFrame) -> pl.DataFrame:
    """Считает наблюдения по типам погоды.

    Тип погоды задаётся полем weather_main: Clear, Clouds, Rain, Snow.
    """
    total = frame.height

    table = frame.group_by("weather_main").agg(
        pl.len().alias("count"),
        pl.col("temp_c").mean().alias("temp_mean"),
        pl.col("temp_c").min().alias("temp_min"),
        pl.col("temp_c").max().alias("temp_max"),
        pl.col("humidity_pct").mean().alias("humidity_mean"),
        pl.col("rain_mm").sum().alias("rain_sum"),
        pl.col("snow_mm").sum().alias("snow_sum"),
        pl.col("pop_prob").mean().alias("pop_mean"),
    )

    return table.with_columns((pl.col("count") / total).alias("share")).sort(
        "count", descending=True
    )


def print_conditions_report(frame: pl.DataFrame) -> None:
    """Печатает распределение наблюдений по типам погоды."""
    table = conditions_table(frame)

    rows: list[list[object]] = [
        ["погода", *[header_of(t, "", "") for t, _, _ in CONDITION_COLUMNS]]
    ]
    for record in table.iter_rows(named=True):
        rows.append(
            [
                record["weather_main"],
                *[
                    format_value(record[name], kind)
                    for _, name, kind in CONDITION_COLUMNS
                ],
            ]
        )

    print_table(rows)

    print("  доля считается от всех наблюдений, поэтому сумма по типам равна 100%")


def endpoint_table(frame: pl.DataFrame) -> pl.DataFrame:
    """Сравнивает наблюдаемую погоду и прогноз."""
    return (
        frame.group_by("endpoint")
        .agg(
            pl.len().alias("count"),
            pl.col("temp_c").mean().alias("temp_mean"),
            pl.col("humidity_pct").mean().alias("humidity_mean"),
            pl.col("pressure_hpa").mean().alias("pressure_mean"),
            pl.col("clouds_pct").mean().alias("clouds_mean"),
            pl.col("wind_speed_ms").mean().alias("wind_mean"),
            pl.col("pop_prob").mean().alias("pop_mean"),
        )
        .sort("count", descending=True)
    )


def print_endpoint_report(frame: pl.DataFrame) -> None:
    """Печатает сравнение наблюдений и прогноза."""
    table = endpoint_table(frame)

    print("current - наблюдение сейчас, forecast - прогноз на пять суток вперёд.")
    print("Средние различаются прежде всего облачностью: в прогноз попадают все")
    print("будущие сутки с их погодой, а в current - один текущий момент.")
    print()

    rows: list[list[object]] = [
        ["эндпоинт", *[header_of(t, "", "") for t, _, _ in ENDPOINT_COLUMNS]]
    ]
    for record in table.iter_rows(named=True):
        rows.append(
            [
                record["endpoint"],
                *[
                    format_value(record[name], kind)
                    for _, name, kind in ENDPOINT_COLUMNS
                ],
            ]
        )

    print_table(rows)

    current = table.filter(pl.col("endpoint") == "current")
    forecast = table.filter(pl.col("endpoint") == "forecast")
    if current.height and forecast.height:
        difference = current["clouds_mean"][0] - forecast["clouds_mean"][0]
        if difference >= 0:
            print(
                f"  облачность в current выше прогноза на {difference:.2f} "
                "процентного пункта"
            )
        else:
            print(
                f"  облачность в current ниже прогноза на {-difference:.2f} "
                "процентного пункта"
            )


def ranking(frame: pl.DataFrame) -> pl.DataFrame:
    """Ранжирует города по средней температуре и влажности.

    Rank по убыванию, поэтому первое место получает самый тёплый город, а по
    влажности - самый сухой. Метод dense не оставляет пропусков в местах:
    обычный rank при равенстве дал бы первым двум городам место 1, а третьему
    сразу 3, и в таблице появился бы пропуск.

    Returns:
        Таблица с городами и их местами.
    """
    return (
        frame.group_by("city")
        .agg(
            pl.col("temp_c").mean().alias("temp_mean"),
            pl.col("humidity_pct").mean().alias("humidity_mean"),
            pl.len().alias("count"),
        )
        .with_columns(
            pl.col("temp_mean")
            .rank(descending=True, method="dense")
            .cast(pl.Int64)
            .alias("warm_rank"),
            pl.col("humidity_mean")
            .rank(method="dense")
            .cast(pl.Int64)
            .alias("humid_rank"),
        )
        .sort(["warm_rank", "city"])
    )


def print_ranking(frame: pl.DataFrame) -> None:
    """Печатает ранжирование городов."""
    table = ranking(frame)

    rows: list[list[object]] = [
        [
            "город",
            "место\nпо теплу",
            "температура\nср., °C",
            "место\nпо влажности",
            "влажность\nср., %",
        ]
    ]
    for record in table.iter_rows(named=True):
        rows.append(
            [
                record["city"],
                record["warm_rank"],
                format_value(record["temp_mean"], "f"),
                record["humid_rank"],
                format_value(record["humidity_mean"], "f"),
            ]
        )

    print_table(rows)
    print("  место 1 - самый тёплый город, по влажности - самый сухой")


def main() -> int:
    """Выполняет агрегационный анализ и печатает отчёт.

    Returns:
        Код возврата: 0 при успехе.
    """
    section("Задание 6. Агрегационный анализ")

    frame = load_clean()
    cities = frame["city"].n_unique()

    print(f"Прочитано очищенных данных: {frame.height} строк, {frame.width} столбцов")
    print(f"Городов: {cities}, наблюдений на город: {frame.height // cities}")
    print(
        f"Период наблюдений: {frame['observed_utc'].min()} - {frame['observed_utc'].max()}"
    )
    print(rule())

    print("Сумма температур или влажности сама по себе ни о чём не говорит, поэтому")
    print("SUM приводится только там, где сумма осмысленна: осадки, снег и количество")
    print("наблюдений. Для остальных полей показаны среднее, минимум и максимум.")
    print()

    section("1. Группировка по городу: SUM, AVG, MIN, MAX, COUNT")
    with timed("агрегаты по городам"):
        print_city_report(frame)

    section("2. Группировка по городу и местной дате")
    with timed("агрегаты по городам и дням"):
        print_daily_report(frame)

    section("3. Группировка по типу погоды")
    with timed("агрегаты по типам погоды"):
        print_conditions_report(frame)

    section("4. Группировка по эндпоинту")
    with timed("агрегаты по эндпоинтам"):
        print_endpoint_report(frame)

    section("5. Ранжирование городов")
    with timed("ранжирование городов"):
        print_ranking(frame)

    return 0


if __name__ == "__main__":
    sys.exit(main())
