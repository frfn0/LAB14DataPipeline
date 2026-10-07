"""Общие настройки конвейера анализа.

Скрипты заданий 4-10 лежат в этом каталоге и читают одни и те же данные,
поэтому пути, схема и вспомогательные функции вынесены сюда.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import polars as pl

# Корни проекта: пути считаются от расположения файла, чтобы скрипты
# работали из любого каталога.
ROOT = Path(__file__).resolve().parent.parent

# Собранные сборщиком данные: JSON Lines, один объект на строку.
RAW_DIR = ROOT / "data" / "raw"
RAW_PATTERN = "*.jsonl"

# Промежуточные результаты между шагами.
INTERIM_DIR = ROOT / "data" / "interim"

# Результаты для DuckDB и визуализации.
REPORTS_DIR = ROOT / "reports"

# Графики задания 9.
CHARTS_DIR = REPORTS_DIR / "charts"

# Число строк, показываемых в отчётах.
HEAD_ROWS = 5

# Схема собранных данных.
#
# Схема задана явно, а не выводится из первых строк файла: имена полей и их
# смысл зафиксированы в контракте pkg/messages/messages.go и в
# WeatherRecord. Явная схема даёт три вещи, которых не даёт вывод типов:
# целочисленные счётчики не превращаются в дробные из-за одного пустого
# значения, изменение контракта сборщиком приводит к ошибке импорта, а не
# к тихой смене типа, и состав полей виден в одном месте.
WEATHER_SCHEMA: dict[str, pl.DataType] = {
    "city": pl.String,
    "country": pl.String,
    "lat": pl.Float64,
    "lon": pl.Float64,
    "endpoint": pl.String,
    "observed_at": pl.String,
    "collected_at": pl.String,
    "temp_c": pl.Float64,
    "feels_like_c": pl.Float64,
    "temp_min_c": pl.Float64,
    "temp_max_c": pl.Float64,
    "pressure_hpa": pl.Int64,
    "humidity_pct": pl.Int64,
    "dew_point_c": pl.Float64,
    "wind_speed_ms": pl.Float64,
    "wind_deg": pl.Int64,
    "wind_gust_ms": pl.Float64,
    "clouds_pct": pl.Int64,
    "visibility_m": pl.Int64,
    "pop_prob": pl.Float64,
    "rain_mm": pl.Float64,
    "snow_mm": pl.Float64,
    "weather_id": pl.Int64,
    "weather_main": pl.String,
    "weather_desc": pl.String,
    "is_day": pl.Boolean,
    "request_ms": pl.Int64,
}


def raw_files(directory: Path = RAW_DIR) -> list[Path]:
    """Файлы с собранными данными, отсортированные по имени.

    Args:
        directory: каталог с JSON-файлами.

    Returns:
        Список файлов. Может быть пустым, если сборщик ещё не запускался.

    Raises:
        FileNotFoundError: если каталога нет.
    """
    if not directory.exists():
        raise FileNotFoundError(
            f"каталог с данными не найден: {directory}. "
            "Сначала запустите сборщик: go run ./cmd/collector"
        )

    return sorted(directory.glob(RAW_PATTERN))


def check_contract(path: Path, schema: dict[str, pl.DataType]) -> None:
    """Проверяет, что состав полей в файле совпадает со схемой.

    Polars при чтении с готовой схемой не требует наличия всех полей:
    отсутствующее поле просто становится пустым. Из-за этого изменение
    контракта сборщиком прошло бы молча, а данные появились бы в отчёте как
    пропуски. Проверка сравнивает имена полей первой записи со схемой.

    Args:
        path: файл с данными.
        schema: ожидаемый состав полей.

    Raises:
        ValueError: если в файле лишние или недостающие поля.
    """
    with path.open(encoding="utf-8") as source:
        for line in source:
            if not line.strip():
                continue

            record = json.loads(line)
            break
        else:
            raise ValueError(f"файл {path.name} пуст")

    actual = set(record)
    expected = set(schema)

    missing = sorted(expected - actual)
    unknown = sorted(actual - expected)

    if missing or unknown:
        details = []
        if missing:
            details.append(f"нет полей: {', '.join(missing)}")
        if unknown:
            details.append(f"лишние поля: {', '.join(unknown)}")

        raise ValueError(
            f"состав полей в {path.name} не совпадает с контрактом сборщика: "
            + "; ".join(details)
        )


def load_raw(
    files: list[Path] | None = None, schema: dict[str, pl.DataType] | None = None
) -> pl.DataFrame:
    """Загружает собранные JSON-файлы в таблицу Polars.

    Args:
        files: файлы для загрузки. По умолчанию берутся все файлы из
            каталога с сырыми данными.
        schema: схема полей. По умолчанию используется схема сборщика.

    Returns:
        Таблица со всеми записями.

    Raises:
        FileNotFoundError: если файлы не найдены.
    """
    sources = files if files is not None else raw_files()
    if not sources:
        raise FileNotFoundError("нет файлов с собранными данными")

    expected = schema if schema is not None else WEATHER_SCHEMA
    for path in sources:
        check_contract(path, expected)

    frames = [pl.read_ndjson(path, schema=expected) for path in sources]

    if len(frames) == 1:
        return frames[0]

    return pl.concat(frames, how="vertical_relaxed")


def ensure_dir(path: Path) -> Path:
    """Создаёт каталог, если его нет, и возвращает его."""
    path.mkdir(parents=True, exist_ok=True)
    return path


@contextmanager
def timed(label: str) -> Iterator[None]:
    """Измеряет время работы блока и печатает его.

    Используется в заданиях 8 и 9, где требуется сравнить время выполнения
    этапов конвейера.

    Args:
        label: что измеряется.
    """
    started = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - started
        print(f"  {label}: {elapsed:.3f} с")


def section(title: str) -> None:
    """Печатает заголовок раздела отчёта."""
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def rule(char: str = "-", width: int = 78) -> str:
    """Разделительная линия отчёта."""
    return char * width


def human_bytes(size: int) -> str:
    """Размер в байтах в человекочитаемом виде."""
    value = float(size)

    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if value < 1024 or unit == "ГБ":
            return f"{value:.1f} {unit}"
        value /= 1024

    return f"{value:.1f} ГБ"
