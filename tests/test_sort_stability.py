"""Проверяет, что повторные запуски дают одинаковый порядок строк в таблицах.

Дефект, который ловят эти проверки: Polars не гарантирует порядок строк при
равных значениях ключа сортировки - он зависит от порядка завершения потоков.
На данных с равными значениями порядок менялся от запуска к запуску, и на
графике осадков местами менялись подписи под двумя последними столбцами:
Краснодар и Сочи оба дают ноль.

Таблица в тестах содержит все поля, которые нужны агрегатам задания 6, и
специально построена так, чтобы равных значений было много: шесть городов из
десяти имеют одну и ту же среднюю температуру, у половины городов одинаковая
сумма осадков.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from pipeline.aggregate import (
    city_table,
    conditions_table,
    daily_table,
    endpoint_table,
)
from pipeline.charts import cities_for_series

# Десять городов. Первые четыре имеют разные средние температуры, остальные
# шесть - одну и ту же: без этого сортировать по температуре было бы нечего.
CITIES = [
    "Алтай",
    "Брянск",
    "Владимир",
    "Глазов",
    "Дагестан",
    "Елец",
    "Жмеринка",
    "Златоуст",
    "Иркутск",
    "Керчь",
]

# Средняя температура по городам: у последних шести она одинакова.
BASE_TEMP = {
    "Алтай": -20.0,
    "Брянск": -10.0,
    "Владимир": 0.0,
    "Глазов": 10.0,
}
TIED_TEMP = 5.0


def city_of(index: int) -> str:
    """Город для строки с заданным номером."""
    return CITIES[index % len(CITIES)]


def temp_of(index: int) -> float:
    """Температура для строки с заданным номером."""
    return BASE_TEMP.get(city_of(index), TIED_TEMP)


def table(rows: int = 40, **overrides) -> pl.DataFrame:
    """Таблица со всеми полями, нужными агрегатам задания 6."""
    cities = [city_of(index) for index in range(rows)]

    data = {
        "city": cities,
        "endpoint": ["current" if index % 2 else "forecast" for index in range(rows)],
        "local_date": [date(2026, 10, 8 + index % 2) for index in range(rows)],
        "temp_c": [temp_of(index) for index in range(rows)],
        "feels_like_c": [temp_of(index) - 1.0 for index in range(rows)],
        "pressure_hpa": [1010 + (index % 2) for index in range(rows)],
        "humidity_pct": [50 + (index % 3) * 10 for index in range(rows)],
        "wind_speed_ms": [1.0 + (index % 4) for index in range(rows)],
        "wind_gust_ms": [3.0 + (index % 4) for index in range(rows)],
        "clouds_pct": [(index % 5) * 20 for index in range(rows)],
        "visibility_m": [10000] * rows,
        # Осадки: у половины строк ноль, и суммы по городам совпадают.
        "rain_mm": [0.0 if index % 2 else 1.0 for index in range(rows)],
        "snow_mm": [0.0] * rows,
        "pop_prob": [0.0 if index % 2 else 0.5 for index in range(rows)],
        "weather_main": [["Clear", "Rain"][index % 2] for index in range(rows)],
    }

    schema = {
        "city": pl.String,
        "endpoint": pl.String,
        "local_date": pl.Date,
        "temp_c": pl.Float64,
        "feels_like_c": pl.Float64,
        "pressure_hpa": pl.Int64,
        "humidity_pct": pl.Int64,
        "wind_speed_ms": pl.Float64,
        "wind_gust_ms": pl.Float64,
        "clouds_pct": pl.Int64,
        "visibility_m": pl.Int64,
        "rain_mm": pl.Float64,
        "snow_mm": pl.Float64,
        "pop_prob": pl.Float64,
        "weather_main": pl.String,
    }

    data.update(overrides)
    schema.update({name: _infer_type(data[name]) for name in overrides})

    return pl.DataFrame(data, schema=schema)


def _infer_type(values: list) -> pl.DataType:
    """Тип столбца по первому значению."""
    first = values[0]
    if isinstance(first, bool):
        return pl.Boolean
    if isinstance(first, int):
        return pl.Int64
    if isinstance(first, float):
        return pl.Float64
    return pl.String


class TestSortStability:
    """Порядок строк не зависит от запуска."""

    def test_city_table_order_is_stable(self) -> None:
        """Порядок городов с одинаковой средней не меняется.

        Шесть городов из десяти имеют одну и ту же среднюю температуру, и без
        второго ключа сортировки их порядок определялся порядком завершения
        потоков.
        """
        source = table(40)

        orders = [city_table(source)["city"].to_list() for _ in range(10)]

        assert all(order == orders[0] for order in orders)

    def test_tied_cities_are_ordered_alphabetically(self) -> None:
        """Города с равной средней идут по алфавиту."""
        source = table(40)

        result = city_table(source)
        names = result["city"].to_list()
        means = result["temp_avg"].to_list()

        groups: dict[float, list[str]] = {}
        for name, mean in zip(names, means, strict=True):
            groups.setdefault(mean, []).append(name)

        tied = [group for group in groups.values() if len(group) > 1]
        assert tied, "в таблице нет городов с равной средней"
        for group in tied:
            assert group == sorted(group), group

    def test_daily_table_order_is_stable(self) -> None:
        """Порядок строк суточной таблицы не меняется."""
        source = table(40)

        orders = [
            daily_table(source).select(["city", "local_date"]).rows() for _ in range(5)
        ]

        assert all(order == orders[0] for order in orders)

    def test_conditions_table_order_is_stable(self) -> None:
        """Порядок типов погоды при равных счётчиках не меняется.

        Все шесть типов получают ровно по одной записи, сортировать не по чему,
        и порядок определяется внутренним устройством группировки.
        """
        names = ["Rain", "Clear", "Snow", "Fog", "Mist", "Haze"]
        source = table(6, weather_main=names)

        orders = [conditions_table(source)["weather_main"].to_list() for _ in range(10)]

        assert all(order == orders[0] for order in orders)
        assert orders[0] == sorted(orders[0])

    def test_endpoint_table_order_is_stable(self) -> None:
        """Порядок эндпоинтов при равных счётчиках не меняется."""
        source = table(4, endpoint=["current", "forecast", "current", "forecast"])

        orders = [endpoint_table(source)["endpoint"].to_list() for _ in range(10)]

        assert all(order == orders[0] for order in orders)
        assert orders[0] == sorted(orders[0])

    def test_series_cities_order_is_stable(self) -> None:
        """Порядок городов временного ряда не меняется."""
        source = table(40)

        orders = [cities_for_series(source) for _ in range(10)]

        assert all(order == orders[0] for order in orders)


class TestTestDataHasTies:
    """Проверочные данные действительно содержат равные значения."""

    def test_source_table_has_equal_means(self) -> None:
        """В тестовой таблице есть города с одинаковой средней.

        Без этого тесты стабильности проходили бы вхолостую: сортировать
        нечего, и порядок всегда одинаковый.
        """
        source = table(40)

        means = (
            source.group_by("city")
            .agg(pl.col("temp_c").mean().alias("mean"))
            .sort("mean")
        )

        assert means["mean"].n_unique() < means.height

    def test_source_table_has_equal_precipitation(self) -> None:
        """Есть города с одинаковой суммой осадков.

        Именно этот случай проявился на настоящих данных: у Краснодара и
        Сочи было ноль осадков, и города менялись местами на графике.
        """
        source = table(40)

        totals = source.group_by("city").agg(
            (pl.col("rain_mm") + pl.col("snow_mm")).sum().alias("total")
        )

        assert totals["total"].n_unique() < totals.height
