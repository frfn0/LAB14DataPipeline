"""Тесты загрузки данных и схемы конвейера.

Загрузчик проверяется на настоящем собранном файле: он и есть контракт
между сборщиком на Go и анализом на Python.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from pipeline.common import (
    WEATHER_SCHEMA,
    ensure_dir,
    human_bytes,
    load_raw,
    raw_files,
)

DATA_FILE = Path("data/raw/weather.jsonl")


def write_jsonl(path: Path, rows: list[dict]) -> None:
    """Записывает строки в файл в формате JSON Lines."""
    path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n",
        encoding="utf-8",
    )


def sample_row(index: int = 0) -> dict:
    """Строка со всеми полями контракта."""
    return {
        "city": f"Город-{index}",
        "country": "RU",
        "lat": 55.75,
        "lon": 37.61,
        "endpoint": "current",
        "observed_at": "2026-10-07T21:00:00+03:00",
        "collected_at": "2026-10-07T18:00:00Z",
        "temp_c": 8.5,
        "feels_like_c": 6.1,
        "temp_min_c": 7.0,
        "temp_max_c": 10.0,
        "pressure_hpa": 1015,
        "humidity_pct": 62,
        "dew_point_c": 1.4,
        "wind_speed_ms": 4.2,
        "wind_deg": 270,
        "wind_gust_ms": 9.1,
        "clouds_pct": 10,
        "visibility_m": 10000,
        "pop_prob": 0.0,
        "rain_mm": 0.0,
        "snow_mm": 0.0,
        "weather_id": 800,
        "weather_main": "Clear",
        "weather_desc": "ясно",
        "is_day": True,
        "request_ms": 685,
    }


class TestSchema:
    """Проверки схемы."""

    def test_schema_matches_contract(self) -> None:
        """Схема описывает ровно поля WeatherRecord."""
        assert set(WEATHER_SCHEMA) == set(sample_row())

    def test_integer_fields_are_integers(self) -> None:
        """Счётчики объявлены целыми, а не дробными."""
        integers = {name for name, dtype in WEATHER_SCHEMA.items() if dtype == pl.Int64}

        assert "humidity_pct" in integers
        assert "pressure_hpa" in integers
        assert "request_ms" in integers

    def test_temperatures_are_floats(self) -> None:
        """Температуры дробные: они иначе теряли бы точность."""
        assert WEATHER_SCHEMA["temp_c"] == pl.Float64
        assert WEATHER_SCHEMA["dew_point_c"] == pl.Float64


class TestLoadRaw:
    """Проверки загрузки данных."""

    def test_loads_collected_data(self) -> None:
        """Собранный сборщиком файл читается без потерь."""
        if not DATA_FILE.exists():
            pytest.skip("данные не собраны: сначала запустите сборщик")

        frame = load_raw([DATA_FILE])

        assert frame.height > 0
        assert set(frame.columns) == set(WEATHER_SCHEMA)

    def test_types_match_schema(self) -> None:
        """Фактические типы совпадают с объявленной схемой."""
        if not DATA_FILE.exists():
            pytest.skip("данные не собраны: сначала запустите сборщик")

        frame = load_raw([DATA_FILE])

        for name, expected in WEATHER_SCHEMA.items():
            assert frame.schema[name] == expected, f"поле {name}"

    def test_loads_several_files(self, tmp_path: Path) -> None:
        """Несколько файлов объединяются в одну таблицу."""
        first = tmp_path / "a.jsonl"
        second = tmp_path / "b.jsonl"

        write_jsonl(first, [sample_row(1), sample_row(2)])
        write_jsonl(second, [sample_row(3)])

        frame = load_raw([first, second])

        assert frame.height == 3
        assert set(frame["city"].to_list()) == {"Город-1", "Город-2", "Город-3"}

    def test_order_of_files_is_preserved(self, tmp_path: Path) -> None:
        """Порядок файлов задаётся вызывающим, а не алфавитом."""
        first = tmp_path / "z.jsonl"
        second = tmp_path / "a.jsonl"

        write_jsonl(first, [sample_row(1)])
        write_jsonl(second, [sample_row(2)])

        frame = load_raw([first, second])

        assert frame["city"].to_list() == ["Город-1", "Город-2"]

    def test_missing_field_is_reported(self, tmp_path: Path) -> None:
        """Запись без поля контракта приводит к ошибке, а не к тихой потере.

        Polars при чтении с готовой схемой отсутствующее поле просто
        заполняет пустыми значениями, поэтому проверка контракта выполняется
        отдельно.
        """
        broken = tmp_path / "broken.jsonl"

        row = sample_row()
        del row["humidity_pct"]
        write_jsonl(broken, [row])

        with pytest.raises(ValueError, match="нет полей"):
            load_raw([broken])

    def test_extra_field_is_reported(self, tmp_path: Path) -> None:
        """Лишнее поле в данных тоже приводит к ошибке."""
        broken = tmp_path / "extra.jsonl"

        row = sample_row()
        row["unexpected"] = 1
        write_jsonl(broken, [row])

        with pytest.raises(ValueError, match="лишние поля"):
            load_raw([broken])

    def test_empty_file_is_reported(self, tmp_path: Path) -> None:
        """Пустой файл приводит к понятной ошибке."""
        empty = tmp_path / "empty.jsonl"
        empty.write_text("\n", encoding="utf-8")

        with pytest.raises(ValueError, match="пуст"):
            load_raw([empty])

    def test_missing_file_is_reported(self, tmp_path: Path) -> None:
        """Пустой каталог приводит к понятной ошибке."""
        with pytest.raises(FileNotFoundError):
            raw_files(tmp_path / "нет-такого")


class TestHelpers:
    """Проверки вспомогательных функций."""

    @pytest.mark.parametrize(
        ("size", "expected"),
        [(0, "0.0 Б"), (512, "512.0 Б"), (1024, "1.0 КБ"), (1536, "1.5 КБ")],
    )
    def test_human_bytes(self, size: int, expected: str) -> None:
        """Размер приводится к читаемому виду."""
        assert human_bytes(size) == expected

    def test_ensure_dir_creates(self, tmp_path: Path) -> None:
        """Каталог создаётся при отсутствии."""
        target = tmp_path / "reports" / "charts"

        ensure_dir(target)

        assert target.is_dir()
