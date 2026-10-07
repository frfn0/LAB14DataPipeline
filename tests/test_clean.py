"""Тесты шагов очистки данных.

На настоящих собранных данных пропусков и значений вне диапазонов нет, и
проверить было бы нечего. Поэтому эти тесты строят таблицы с проблемами
целенаправленно: так видна логика каждого шага.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from pipeline.clean_data import (
    DEDUP_KEY,
    cast_types,
    drop_full_duplicates,
    drop_observation_duplicates,
    drop_out_of_range,
    fill_nulls,
    parse_time,
    sort_by_time,
)
from pipeline.common import CLEAN_SCHEMA, WEATHER_SCHEMA
from pipeline.clean_data import Step


def row(**overrides) -> dict:
    """Строка со всеми полями сборщика."""
    base = {
        "city": "Москва",
        "country": "RU",
        "lat": 55.75,
        "lon": 37.61,
        "endpoint": "current",
        "observed_at": "2026-10-08T09:00:00+03:00",
        "collected_at": "2026-10-07T18:00:00Z",
        "temp_c": 8.0,
        "feels_like_c": 6.0,
        "temp_min_c": 7.0,
        "temp_max_c": 9.0,
        "pressure_hpa": 1015,
        "humidity_pct": 60,
        "dew_point_c": 1.0,
        "wind_speed_ms": 3.0,
        "wind_deg": 270,
        "wind_gust_ms": 5.0,
        "clouds_pct": 20,
        "visibility_m": 10000,
        "pop_prob": 0.0,
        "rain_mm": 0.0,
        "snow_mm": 0.0,
        "weather_id": 800,
        "weather_main": "Clear",
        "weather_desc": "ясно",
        "is_day": True,
        "request_ms": 500,
    }
    base.update(overrides)
    return base


def frame(rows: list[dict]) -> pl.DataFrame:
    """Таблица из строк со схемой сборщика."""
    return pl.DataFrame(rows, schema=WEATHER_SCHEMA)


class TestFullDuplicates:
    """Полные дубликаты."""

    def test_removes_identical_rows(self) -> None:
        """Совпадающие целиком строки удаляются."""
        source = frame([row(), row(), row(city="Казань")])

        result = drop_full_duplicates(source, Step("дубликаты"))

        assert result.height == 2
        assert sorted(result["city"].to_list()) == ["Казань", "Москва"]

    def test_keeps_rows_differing_in_any_field(self) -> None:
        """Записи, различающиеся хоть одним полем, сохраняются."""
        source = frame([row(), row(collected_at="2026-10-07T19:00:00Z")])

        result = drop_full_duplicates(source, Step("дубликаты"))

        assert result.height == 2


class TestObservationDuplicates:
    """Повторные наблюдения."""

    def test_keeps_freshest_record(self) -> None:
        """Из двух записей одного наблюдения остаётся самая свежая."""
        source = frame(
            [
                row(collected_at="2026-10-07T18:00:00Z", temp_c=8.0),
                row(collected_at="2026-10-07T19:30:00Z", temp_c=9.5),
            ]
        )

        result = drop_observation_duplicates(source, Step("повторы"))

        assert result.height == 1
        assert result["temp_c"][0] == 9.5

    def test_different_cities_are_not_duplicates(self) -> None:
        """Одинаковое время в разных городах - разные наблюдения."""
        source = frame([row(city="Москва"), row(city="Казань")])

        result = drop_observation_duplicates(source, Step("повторы"))

        assert result.height == 2

    def test_different_time_is_not_duplicate(self) -> None:
        """Один город в разное время - разные наблюдения."""
        source = frame(
            [
                row(observed_at="2026-10-08T09:00:00+03:00"),
                row(observed_at="2026-10-08T12:00:00+03:00"),
            ]
        )

        result = drop_observation_duplicates(source, Step("повторы"))

        assert result.height == 2

    def test_endpoints_do_not_collide(self) -> None:
        """Текущая погода и прогноз на один момент - разные записи."""
        source = frame(
            [
                row(endpoint="current"),
                row(endpoint="forecast"),
            ]
        )

        result = drop_observation_duplicates(source, Step("повторы"))

        assert result.height == 2

    def test_repeated_collection_is_cleaned(self) -> None:
        """Два сбора подряд дают одну запись на наблюдение."""
        first = row(collected_at="2026-10-07T18:00:00Z")
        second = row(collected_at="2026-10-07T19:00:00Z")

        result = drop_observation_duplicates(frame([first, second]), Step("повторы"))

        assert result.height == 1
        assert set(result.columns) == set(WEATHER_SCHEMA)


class TestParseTime:
    """Разбор времени."""

    def test_converts_local_to_utc(self) -> None:
        """Время с смещением города переводится в UTC."""
        source = frame([row(observed_at="2026-10-08T09:00:00+03:00")])

        result = parse_time(source, Step("время"))

        assert result["observed_utc"].dtype == CLEAN_SCHEMA["observed_utc"]
        # Момент в UTC на три часа раньше местного.
        assert result["observed_utc"][0].hour == 6

    def test_keeps_local_time(self) -> None:
        """Местное время остаётся доступным."""
        source = frame([row(observed_at="2026-10-08T09:00:00+03:00")])

        result = parse_time(source, Step("время"))

        assert result["observed_local"][0].hour == 9
        assert result["observed_local"].dtype.time_zone is None

    def test_local_date_differs_from_utc_date(self) -> None:
        """Местная дата считается по местному времени, а не по UTC.

        В полночь по московскому времени наблюдение ещё относится к
        предыдущим суткам по UTC: 00:30 третьего October в Москве это
        21:30 седьмого октября UTC. Группировка по UTC относила бы такое
        наблюдение к прошлому дню, и суточная средняя температура считалась
        бы не за тот день.
        """
        source = frame([row(observed_at="2026-10-08T00:30:00+03:00")])

        result = parse_time(source, Step("время"))

        assert result["local_date"][0] == date(2026, 10, 8)
        assert result["observed_utc"][0].day == 7
        assert result["observed_utc"][0].hour == 21

    def test_east_cities_stay_ahead_of_utc(self) -> None:
        """Город восточнее Москвы в полночь по своему времени уже перешёл
        на следующие сутки UTC, но по местной дате дата ещё сегодняшняя.
        """
        source = frame([row(observed_at="2026-10-08T00:30:00+10:00")])

        result = parse_time(source, Step("время"))

        assert result["local_date"][0] == date(2026, 10, 8)
        assert result["observed_utc"][0].day == 7
        assert result["observed_utc"][0].hour == 14

    def test_collected_at_z_suffix(self) -> None:
        """Время сбора заканчивается буквой Z, а не смещением."""
        source = frame([row(collected_at="2026-10-07T18:48:35Z")])

        result = parse_time(source, Step("время"))

        assert result["collected_utc"][0].hour == 18
        assert result["collected_utc"].dtype == CLEAN_SCHEMA["collected_utc"]

    def test_different_offsets_in_one_table(self) -> None:
        """Города с разными часовыми поясами разбираются вместе."""
        source = frame(
            [
                row(city="Москва", observed_at="2026-10-08T09:00:00+03:00"),
                row(city="Владивосток", observed_at="2026-10-08T09:00:00+10:00"),
            ]
        )

        result = parse_time(source, Step("время"))

        assert result.height == 2
        assert (
            result["observed_utc"].to_list()[0] != result["observed_utc"].to_list()[1]
        )


class TestNulls:
    """Обработка пропусков."""

    def test_drops_rows_without_required_fields(self) -> None:
        """Запись без города или температуры бесполезна и удаляется."""
        source = frame(
            [
                row(city="Москва"),
                row(city="Казань", temp_c=None),
                row(city="Сочи", observed_at=None),
            ]
        )

        result, dropped = fill_nulls(source, Step("пропуски"))

        assert result["city"].to_list() == ["Москва"]
        assert dropped.height == 2

    def test_fills_missing_measurements_with_zero(self) -> None:
        """Отсутствующие показатели заполняются нулём."""
        source = frame([row(wind_gust_ms=None, rain_mm=None, snow_mm=None)])

        result, _ = fill_nulls(source, Step("пропуски"))

        assert result["wind_gust_ms"][0] == 0
        assert result["rain_mm"][0] == 0
        assert result["snow_mm"][0] == 0
        assert int(result.null_count().sum_horizontal().item()) == 0

    def test_fills_text_fields_with_empty(self) -> None:
        """Текстовые поля заполняются пустой строкой, а не нулём."""
        source = frame([row(weather_main=None, weather_desc=None)])

        result, _ = fill_nulls(source, Step("пропуски"))

        assert result["weather_main"][0] == ""
        assert result["weather_desc"][0] == ""

    def test_required_fields_are_never_zero_filled(self) -> None:
        """Пропуск в обязательном поле не замаскировывается нулём."""
        source = frame([row(temp_c=None)])

        result, dropped = fill_nulls(source, Step("пропуски"))

        assert result.height == 0
        assert dropped.height == 1

    def test_every_filled_field_keeps_schema_type(self) -> None:
        """Заполнение не меняет тип поля: 0 в строковом поле остаётся строкой."""
        source = frame([row(weather_main=None, humidity_pct=None)])

        result, _ = fill_nulls(source, Step("пропуски"))

        assert result.schema["weather_main"] == CLEAN_SCHEMA["weather_main"]
        assert result.schema["humidity_pct"] == CLEAN_SCHEMA["humidity_pct"]


class TestRanges:
    """Проверка диапазонов."""

    @pytest.mark.parametrize(
        "field",
        ["temp_c", "humidity_pct", "pressure_hpa", "clouds_pct", "pop_prob"],
    )
    def test_out_of_range_is_dropped(self, field: str) -> None:
        """Явно неправдоподобное значение приводит к удалению строки."""
        source = frame([row(**{field: 9999}), row(city="Казань")])

        result, invalid = drop_out_of_range(source, Step("диапазоны"))

        assert result["city"].to_list() == ["Казань"]
        assert invalid.height == 1

    def test_boundary_values_survive(self) -> None:
        """Границы диапазона считаются допустимыми."""
        source = frame([row(humidity_pct=0), row(clouds_pct=100, city="Казань")])

        result, invalid = drop_out_of_range(source, Step("диапазоны"))

        assert result.height == 2
        assert invalid.height == 0

    def test_plausible_extremes_survive(self) -> None:
        """Редкие, но настоящие значения не отсекаются."""
        source = frame([row(temp_c=-45.0, pressure_hpa=880)])

        result, _ = drop_out_of_range(source, Step("диапазоны"))

        assert result.height == 1

    def test_coordinates_are_checked(self) -> None:
        """Точка вне земного шара не может быть городом."""
        source = frame([row(lat=200.0), row(lon=-400.0), row(city="Казань")])

        result, invalid = drop_out_of_range(source, Step("диапазоны"))

        assert result.height == 1
        assert invalid.height == 2


class TestCastAndSort:
    """Приведение типов и сортировка."""

    def test_casts_to_clean_schema(self) -> None:
        """Поля приводятся к типам очищенной схемы."""
        source = frame([row()]).with_columns(
            pl.col("temp_c").cast(pl.Int64),
            pl.col("is_day").cast(pl.Int8),
        )

        result = cast_types(source, Step("типы"))

        assert result.schema["temp_c"] == CLEAN_SCHEMA["temp_c"]
        assert result.schema["is_day"] == CLEAN_SCHEMA["is_day"]

    def test_sort_orders_by_city_and_time(self) -> None:
        """Данные упорядочиваются по городу и моменту наблюдения."""
        parsed = parse_time(
            frame(
                [
                    row(city="Сочи", observed_at="2026-10-08T15:00:00+03:00"),
                    row(city="Казань", observed_at="2026-10-08T15:00:00+03:00"),
                    row(city="Казань", observed_at="2026-10-08T09:00:00+03:00"),
                ]
            ),
            Step("время"),
        )

        result = sort_by_time(parsed, Step("сортировка"))

        pairs = list(zip(result["city"].to_list(), result["observed_utc"].to_list()))
        assert [city for city, _ in pairs] == ["Казань", "Казань", "Сочи"]
        assert pairs[0][1] < pairs[1][1]


class TestPipelineOrder:
    """Порядок шагов имеет значение."""

    def test_cleaned_data_has_no_duplicates(self) -> None:
        """После очистки у каждого наблюдения остаётся одна запись."""
        source = frame(
            [
                row(collected_at="2026-10-07T18:00:00Z"),
                row(collected_at="2026-10-07T19:00:00Z"),
            ]
        )

        cleaned = drop_observation_duplicates(source, Step("повторы"))

        assert cleaned.select(DEDUP_KEY).n_unique() == cleaned.height

    def test_empty_frame_survives_cleaning(self) -> None:
        """Пустая таблица не роняет очистку."""
        empty = frame([])

        assert drop_full_duplicates(empty, Step("дубликаты")).height == 0
        assert drop_out_of_range(empty, Step("диапазоны"))[0].height == 0
