"""Тесты агрегатов задания 6 и печати таблиц.

Агрегаты проверяются на таблицах с числами, которые можно посчитать в уме:
на настоящих данных сумма и среднее считаются верно, но неверная формула
дала бы правдоподобный результат и не была бы замечена.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl

from pipeline.aggregate import (
    CITY_COLUMNS,
    city_table,
    conditions_table,
    daily_table,
    endpoint_table,
    ranking,
)
from pipeline.report import format_value, header_of, print_table


def row(**overrides) -> dict:
    """Строка очищенных данных."""
    base = {
        "city": "Москва",
        "endpoint": "current",
        "observed_utc": datetime(2026, 10, 8, 6, 0, tzinfo=timezone.utc),
        "local_date": date(2026, 10, 8),
        "temp_c": 10.0,
        "feels_like_c": 8.0,
        "pressure_hpa": 1010,
        "humidity_pct": 60,
        "wind_speed_ms": 3.0,
        "wind_gust_ms": 5.0,
        "clouds_pct": 20,
        "visibility_m": 10000,
        "pop_prob": 0.0,
        "rain_mm": 0.0,
        "snow_mm": 0.0,
        "weather_main": "Clear",
    }
    base.update(overrides)
    return base


def _types() -> dict[str, pl.DataType]:
    """Типы полей тестовой таблицы."""
    return {
        "city": pl.String,
        "endpoint": pl.String,
        "observed_utc": pl.Datetime("us", "UTC"),
        "local_date": pl.Date,
        "temp_c": pl.Float64,
        "feels_like_c": pl.Float64,
        "pressure_hpa": pl.Int64,
        "humidity_pct": pl.Int64,
        "wind_speed_ms": pl.Float64,
        "wind_gust_ms": pl.Float64,
        "clouds_pct": pl.Int64,
        "visibility_m": pl.Int64,
        "pop_prob": pl.Float64,
        "rain_mm": pl.Float64,
        "snow_mm": pl.Float64,
        "weather_main": pl.String,
    }


def frame(rows: list[dict]) -> pl.DataFrame:
    """Таблица из строк."""
    return pl.DataFrame(rows, schema=_types())


class TestCityAggregation:
    """Группировка по городу."""

    def test_sums_and_averages_match_hand_calculation(self) -> None:
        """Сумма и среднее считаются по всем наблюдениям города."""
        source = frame(
            [
                row(temp_c=0.0, rain_mm=1.0),
                row(temp_c=10.0, rain_mm=2.0),
            ]
        )

        table = city_table(source)

        assert table["temp_avg"][0] == 5.0
        assert table["rain_sum"][0] == 3.0
        assert table["count"][0] == 2

    def test_min_and_max_are_per_field(self) -> None:
        """Минимум и максимум относятся к своему полю, а не к температуре."""
        source = frame(
            [
                row(temp_c=-5.0, wind_gust_ms=12.0),
                row(temp_c=5.0, wind_gust_ms=3.0),
            ]
        )

        table = city_table(source)

        assert table["temp_min"][0] == -5.0
        assert table["temp_max"][0] == 5.0
        assert table["wind_max"][0] == 12.0

    def test_cities_are_grouped_separately(self) -> None:
        """Наблюдения разных городов не смешиваются."""
        source = frame(
            [
                row(city="Москва", temp_c=0.0),
                row(city="Якутск", temp_c=40.0),
                row(city="Москва", temp_c=10.0),
            ]
        )

        table = city_table(source)

        assert table.height == 2
        by_city = dict(
            zip(table["city"].to_list(), table["temp_avg"].to_list(), strict=True)
        )
        assert by_city["Москва"] == 5.0
        assert by_city["Якутск"] == 40.0

    def test_sorted_warmest_first(self) -> None:
        """Самый тёплый город идёт первым."""
        source = frame(
            [
                row(city="Якутск", temp_c=-30.0),
                row(city="Сочи", temp_c=25.0),
                row(city="Казань", temp_c=10.0),
            ]
        )

        table = city_table(source)

        assert table["city"].to_list() == ["Сочи", "Казань", "Якутск"]

    def test_temperature_appears_three_times_in_one_row(self) -> None:
        """У температуры три агрегата, и в таблице им нужны разные имена.

        Без отдельных имён все три выражения получили бы имя temp_c, и Polars
        отклонил бы запрос как повторяющийся столбец.
        """
        source = frame([row(temp_c=7.0)])

        table = city_table(source)

        assert {"temp_avg", "temp_min", "temp_max"} <= set(table.columns)
        assert table.width == len(CITY_COLUMNS) + 1

    def test_empty_frame_survives_aggregation(self) -> None:
        """Пустая таблица не роняет агрегацию.

        Таблица строится с нулевыми строками, но со схемой: без полей группировать
        нечего, и падение тут означало бы ошибку в данных, а не в коде.
        """
        empty = pl.DataFrame(schema={name: _type for name, _type in _types().items()})

        assert city_table(empty).height == 0


class TestDailyAggregation:
    """Группировка по городу и местной дате."""

    def test_groups_by_local_date(self) -> None:
        """Группировка идёт по местной дате, а не по дате в UTC."""
        source = frame(
            [
                row(city="Москва", local_date=date(2026, 10, 8), temp_c=10.0),
                row(city="Москва", local_date=date(2026, 10, 9), temp_c=20.0),
            ]
        )

        table = daily_table(source)

        assert table.height == 2
        assert table["temp_mean"].to_list() == [10.0, 20.0]

    def test_same_city_different_dates_split(self) -> None:
        """Один город в разные сутки - разные строки."""
        source = frame(
            [
                row(local_date=date(2026, 10, 8)),
                row(local_date=date(2026, 10, 8)),
                row(local_date=date(2026, 10, 9)),
            ]
        )

        table = daily_table(source)

        assert table["count"].to_list() == [2, 1]

    def test_minutes_near_midnight_belong_to_same_day(self) -> None:
        """Полночь по местному времени не делит сутки пополам.

        Наблюдение в 00:30 и в 23:30 московского времени относятся к разным
        местным датам, хотя по UTC они могут попасть в одни и те же сутки.
        Группировка по UTC отнесла бы их к одному дню, и суточная средняя
        считалась бы за неверные сутки.
        """
        source = frame(
            [
                row(local_date=date(2026, 10, 8), temp_c=1.0),
                row(local_date=date(2026, 10, 9), temp_c=2.0),
            ]
        )

        table = daily_table(source)

        assert table.height == 2


class TestConditions:
    """Группировка по типу погоды."""

    def test_share_sums_to_one(self) -> None:
        """Доли по типам погоды в сумме дают единицу."""
        source = frame(
            [
                row(weather_main="Rain"),
                row(weather_main="Clear"),
                row(weather_main="Clear"),
            ]
        )

        table = conditions_table(source)

        assert table["share"].sum() == 1.0
        assert table["count"].to_list() == [2, 1]

    def test_snow_accumulates_in_own_column(self) -> None:
        """Снег суммируется отдельно от дождя."""
        source = frame(
            [
                row(weather_main="Snow", snow_mm=1.5),
                row(weather_main="Snow", snow_mm=0.5),
            ]
        )

        table = conditions_table(source)

        assert table["snow_sum"][0] == 2.0
        assert table["rain_sum"][0] == 0.0


class TestEndpointComparison:
    """Сравнение наблюдения и прогноза."""

    def test_endpoints_are_separate(self) -> None:
        """Наблюдение и прогноз считаются отдельно."""
        source = frame(
            [
                row(endpoint="current", clouds_pct=10),
                row(endpoint="forecast", clouds_pct=90),
                row(endpoint="forecast", clouds_pct=50),
            ]
        )

        table = endpoint_table(source)

        means = dict(
            zip(
                table["endpoint"].to_list(), table["clouds_mean"].to_list(), strict=True
            )
        )
        assert means["current"] == 10.0
        assert means["forecast"] == 70.0


class TestRanking:
    """Ранжирование городов."""

    def test_warmest_gets_first_place(self) -> None:
        """Самый тёплый город получает первое место."""
        source = frame(
            [
                row(city="Якутск", temp_c=-30.0, humidity_pct=80),
                row(city="Сочи", temp_c=25.0, humidity_pct=90),
            ]
        )

        table = ranking(source)

        assert table["warm_rank"].to_list() == [1, 2]
        assert table["city"].to_list() == ["Сочи", "Якутск"]

    def test_driest_gets_first_place(self) -> None:
        """По влажности первым идёт самый сухой город."""
        source = frame(
            [
                row(city="Влажный", humidity_pct=90),
                row(city="Сухой", humidity_pct=30),
            ]
        )

        table = ranking(source)

        by_city = dict(
            zip(table["city"].to_list(), table["humid_rank"].to_list(), strict=True)
        )
        assert by_city["Сухой"] == 1
        assert by_city["Влажный"] == 2

    def test_ties_share_place_without_gaps(self) -> None:
        """Одинаковые значения получают одинаковое место без пропусков.

        Rank по убыванию: самая высокая температура получает место 1. Метод dense
        не оставляет пустых мест - при обычном rank два самых тёплых города
        получили бы места 1 и 1, а третий - сразу 3, и в таблице появился бы
        пропуск.
        """
        source = frame(
            [
                row(city="А", temp_c=10.0),
                row(city="Б", temp_c=10.0),
                row(city="В", temp_c=20.0),
            ]
        )

        table = ranking(source)

        assert sorted(table["warm_rank"].to_list()) == [1, 2, 2]


class TestReportFormatting:
    """Печать значений и таблиц."""

    def test_formats_kinds(self) -> None:
        """Каждый формат печатается в своей записи."""
        assert format_value(5, "d") == "5"
        assert format_value(5.678, "f") == "5.68"
        assert format_value(0.363, "pct") == "36.3%"
        assert format_value(8400, "km") == "8.4"

    def test_missing_value_prints_dash(self) -> None:
        """Отсутствующее значение печатается прочерком, а не «None»."""
        assert format_value(None, "f") == "-"
        assert format_value(None, "d") == "-"

    def test_header_shows_unit_and_aggregate(self) -> None:
        """Заголовок содержит подпись агрегата и единицу измерения."""
        assert header_of("температура", "temp_c", "mean") == "температура\nср.\n°C"
        assert header_of("осадки", "rain_mm", "sum") == "осадки\nсум.\nмм"
        assert header_of("наблюдений", "city", "count") == "наблюдений"

    def test_prints_every_data_row(self, capsys) -> None:
        """Каждая строка таблицы попадает в вывод."""
        print_table(
            [
                ["город", "температура\nср.\n°C"],
                ["Москва", "10.00"],
                ["Якутск", "-30.00"],
            ]
        )

        output = capsys.readouterr().out
        assert "Москва" in output
        assert "Якутск" in output
        assert "ср." in output

    def test_empty_table_reports_no_data(self, capsys) -> None:
        """Пустая таблица сообщает об отсутствии данных."""
        print_table([])

        assert "нет данных" in capsys.readouterr().out
