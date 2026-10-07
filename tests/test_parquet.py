"""Тесты записи в Parquet и проверки чтения обратно.

Проверка round-trip - главное здесь. Тихая запись может потерять строки,
сменить тип или обрезать текст, и это обнаружится только в задании 8 или 9,
когда данные уже ушли дальше по конвейеру. Поэтому проверка строится так, чтобы
каждый вид повреждения был заранее известен: файл пишется испорченным, а
проверка обязана его заметить.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from pipeline.to_parquet import (
    CHECK_FIELDS,
    COMPRESSIONS,
    PARQUET_FILE,
    parquet_path,
    save_parquet,
    verify_roundtrip,
)


def frame(rows: int = 10) -> pl.DataFrame:
    """Таблица со всеми типами, которые важно пережить запись.

    Таблица строится с явной схемой. Без неё типы выводятся из первых строк, и
    конструкторы pl.date и pl.datetime дают объекты Python, которые Polars
    принимает как тип Object, а Object в Parquet не пишется. Явная схема
    избавляет от этого и делает ожидаемый тип каждого поля видимым в коде.
    """
    data = {
        "city": [f"Город {index}" for index in range(rows)],
        "country": ["RU"] * rows,
        "endpoint": ["current" if index % 2 else "forecast" for index in range(rows)],
        "observed_at": [f"2026-10-08T0{index}:00:00+03:00" for index in range(rows)],
        "temp_c": [10.5 + index / 10 for index in range(rows)],
        "humidity_pct": list(range(rows)),
        "weather_main": [["Clear", "Rain"][index % 2] for index in range(rows)],
        "is_day": [index % 2 == 0 for index in range(rows)],
        "observed_utc": [
            datetime(2026, 10, 8, index % 24, tzinfo=timezone.utc)
            for index in range(rows)
        ],
        "observed_local": [datetime(2026, 10, 8, index % 24) for index in range(rows)],
        "local_date": [date(2026, 10, 8 + index % 3) for index in range(rows)],
    }

    schema = {
        "city": pl.String,
        "country": pl.String,
        "endpoint": pl.String,
        "observed_at": pl.String,
        "temp_c": pl.Float64,
        "humidity_pct": pl.Int64,
        "weather_main": pl.String,
        "is_day": pl.Boolean,
        "observed_utc": pl.Datetime("us", "UTC"),
        "observed_local": pl.Datetime("us"),
        "local_date": pl.Date,
    }

    return pl.DataFrame(data, schema=schema)


class TestSaveParquet:
    """Запись файла."""

    def test_writes_single_file(self, tmp_path: Path) -> None:
        """Методичка требует один файл, а не набор частей."""
        target = tmp_path / PARQUET_FILE

        size = save_parquet(frame(), target)

        assert target.exists()
        assert size > 0
        assert list(tmp_path.iterdir()) == [target]

    @pytest.mark.parametrize("compression", COMPRESSIONS)
    def test_every_compression_writes_readable_file(
        self, tmp_path: Path, compression: str
    ) -> None:
        """Все перечисленные способы сжатия дают читаемый файл.

        Имя для записи без сжатия у Polars - uncompressed, а не none: с none
        запрос падает с ValueError, и проверка ловит это на самом списке.
        """
        target = tmp_path / "data.parquet"

        save_parquet(frame(), target, compression)
        restored = pl.read_parquet(target)

        assert restored.height == 10

    def test_compression_reduces_size(self, tmp_path: Path) -> None:
        """Сжатие уменьшает файл, иначе сравнивать нечего."""
        source = frame(200)

        plain = save_parquet(source, tmp_path / "plain.parquet", "uncompressed")
        packed = save_parquet(source, tmp_path / "packed.parquet", "zstd")

        assert packed < plain

    def test_repeated_calls_overwrite(self, tmp_path: Path) -> None:
        """Повторная запись заменяет файл, а не дописывает в него."""
        target = tmp_path / PARQUET_FILE

        save_parquet(frame(10), target)
        save_parquet(frame(30), target)

        assert pl.read_parquet(target).height == 30

    def test_creates_missing_directory(self, tmp_path: Path) -> None:
        """Каталог для файла создаётся, если его ещё нет."""
        target = tmp_path / "reports" / PARQUET_FILE

        save_parquet(frame(), target)

        assert target.exists()


class TestRoundTrip:
    """Проверка после чтения."""

    def test_matching_data_has_no_problems(self, tmp_path: Path) -> None:
        """Данные, записанные и прочитанные без потерь, проблем не дают."""
        source = frame()
        target = tmp_path / PARQUET_FILE
        save_parquet(source, target)

        assert verify_roundtrip(source, target) == []

    def test_all_types_survive(self, tmp_path: Path) -> None:
        """Время, дата, булево значение и числа переживают запись.

        Без такой проверки потеря типа была бы видна только в задании 9, где
        по колонке рисуется график.
        """
        source = frame()
        target = tmp_path / PARQUET_FILE
        save_parquet(source, target)

        restored = pl.read_parquet(target)

        for field, dtype in source.schema.items():
            assert restored.schema[field] == dtype, field

    def test_empty_frame_survives(self, tmp_path: Path) -> None:
        """Пустая таблица записывается и читается без ошибок."""
        source = frame(0)
        target = tmp_path / PARQUET_FILE
        save_parquet(source, target)

        assert verify_roundtrip(source, target) == []

    def test_detects_lost_rows(self, tmp_path: Path) -> None:
        """Потеря строк при чтении обнаруживается.

        Проверка строится подменой файла: сначала пишется полная таблица,
        затем на её месте - таблица без последних строк. Настоящая потеря при
        сжатии маловероятна, но проверка обязана её видеть.
        """
        source = frame(10)
        target = tmp_path / PARQUET_FILE
        save_parquet(source, target)
        save_parquet(source.head(4), target)

        problems = verify_roundtrip(source, target)

        assert any("строк" in line for line in problems)

    def test_detects_changed_values(self, tmp_path: Path) -> None:
        """Изменённое значение обнаруживается, а не считается совпадением."""
        source = frame(10)
        target = tmp_path / PARQUET_FILE
        save_parquet(source, target)
        save_parquet(source.with_columns(pl.col("temp_c").cast(pl.String)), target)

        problems = verify_roundtrip(source, target)

        assert problems

    def test_detects_missing_column(self, tmp_path: Path) -> None:
        """Потерянный столбец обнаруживается."""
        source = frame(10)
        target = tmp_path / PARQUET_FILE
        save_parquet(source, target)
        save_parquet(source.drop("humidity_pct"), target)

        problems = verify_roundtrip(source, target)

        assert any("нет полей" in line for line in problems)

    def test_detects_column_order_change(self, tmp_path: Path) -> None:
        """Перестановка столбцов обнаруживается.

        Значения те же самые, но читать их по именам полей пришлось бы, а
        такой файл молчал бы о расхождении.
        """
        source = frame(10)
        target = tmp_path / PARQUET_FILE
        save_parquet(source, target)
        save_parquet(source.select(["temp_c", "city"]), target)

        problems = verify_roundtrip(source, target)

        assert problems


class TestLayout:
    """Расположение файла и состав отчёта."""

    def test_default_path_under_reports(self) -> None:
        """Файл по умолчанию лежит в reports."""
        target = parquet_path()

        assert target.parent.name == "reports"
        assert target.name == PARQUET_FILE

    def test_check_fields_exist_in_data(self) -> None:
        """Все поля, показываемые в отчёте, есть в данных."""
        source = frame()

        for field in CHECK_FIELDS:
            assert field in source.columns

    def test_check_fields_cover_types(self) -> None:
        """Показываемые поля покрывают все типы, важные для записи.

        Список полей используется в отчёте как выборка: если в нём не окажется
        времени или булева значения, читатель не увидит, что они пережили
        запись.
        """
        source = frame()
        shown = {source.schema[field] for field in CHECK_FIELDS}

        for dtype in [pl.String, pl.Float64, pl.Int64, pl.Boolean]:
            assert dtype in shown
        assert any(isinstance(dtype, pl.Datetime) for dtype in shown)
        assert any(isinstance(dtype, pl.Date) for dtype in shown)
