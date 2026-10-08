"""Тесты SQL-запроса задания 8 и сравнения с Polars.

Сравнение результатов проверяется на таблицах с числами, которые считаются
в уме. Ключевая часть тестов - сверка: если DuckDB и Polars посчитали разное,
замер времени ничего не значит, и сверка обязана это ловить.
"""

from __future__ import annotations

import time
from pathlib import Path

import duckdb
import polars as pl
import pytest

from pipeline.duckdb_query import (
    FILTER_SQL,
    NOISE_MS,
    SPEEDUP_THRESHOLD,
    compare_results,
    compare_speed,
    fmt_times,
    measure,
    read_polars,
    read_sql,
    rounded,
    sql_path,
)
from pipeline.to_parquet import save_parquet


def frame(rows: int = 12) -> pl.DataFrame:
    """Таблица с городами, температурами и осадками."""
    cities = ["Москва", "Якутск", "Казань", "Сочи"]
    return pl.DataFrame(
        {
            "city": [cities[index % len(cities)] for index in range(rows)],
            "temp_c": [-5.0 + index for index in range(rows)],
            "rain_mm": [float(index % 3) for index in range(rows)],
            "snow_mm": [0.5 if index % 4 == 0 else 0.0 for index in range(rows)],
        }
    )


def parquet(source: pl.DataFrame, tmp_path: Path) -> Path:
    """Записывает таблицу в Parquet во временном каталоге теста."""
    target = tmp_path / "data.parquet"
    save_parquet(source, target)
    return target


class TestSqlPath:
    """Путь к файлу в запросе."""

    def test_uses_forward_slashes(self) -> None:
        """В Windows путь приводится к прямому слэшу.

        Обратный слэш в SQL означает экранирование следующего символа, и путь
        с ним не открывается: DuckDB принял бы часть пути за
        escape-последовательность и сообщил бы об отсутствии файла.
        """
        path = sql_path()

        assert "\\" not in path
        assert path.endswith("reports/weather.parquet")


class TestReadSql:
    """Выполнение SQL-запроса."""

    def test_reads_and_aggregates(self, tmp_path: Path) -> None:
        """Запрос читает Parquet и считает агрегаты по группам."""
        path = parquet(frame(12), tmp_path)

        result = read_sql(
            f"SELECT city, count(*) AS n, min(temp_c) AS lo, max(temp_c) AS hi"
            f" FROM read_parquet('{str(path).replace(chr(92), '/')}')"
            f" GROUP BY city ORDER BY city"
        )

        assert set(result.columns) == {"city", "n", "lo", "hi"}
        assert result.height == 4
        assert result["n"].sum() == 12

    def test_filter_drops_rows(self, tmp_path: Path) -> None:
        """Фильтрация отбрасывает строки, не подходящие под условие."""
        path = parquet(frame(12), tmp_path)

        result = read_sql(
            f"SELECT count(*) AS n FROM read_parquet('{str(path).replace(chr(92), '/')}')"
            f" WHERE temp_c < 0"
        )

        assert result["n"][0] < 12

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        """Запрос к несуществующему файлу падает, а не возвращает пустую таблицу."""
        missing = str(tmp_path / "нет.parquet").replace("\\", "/")

        with pytest.raises(duckdb.Error):
            read_sql(f"SELECT * FROM read_parquet('{missing}')")


class TestReadPolars:
    """Тот же запрос в Polars."""

    def test_applies_filter_and_aggregates(self) -> None:
        """Фильтр и агрегаты применяются к таблице в памяти."""
        result = read_polars(frame(12))

        assert result.height > 0
        assert set(result.columns) == {
            "city",
            "наблюдений",
            "мин",
            "средняя",
            "макс",
            "осадки",
        }

    def test_sums_rain_and_snow_together(self) -> None:
        """Осадки - сумма дождя и снега.

        Раньше суммировался только снег: у Якутска это случайно давало те же
        1.64 мм, потому что дождя там не было, а у остальных городов дождь
        терялся, и сверка справедливо сообщала о расхождении.
        """
        source = pl.DataFrame(
            {
                "city": ["Москва", "Москва"],
                "temp_c": [-1.0, -2.0],
                "rain_mm": [3.0, 4.0],
                "snow_mm": [1.0, 2.0],
            }
        )

        result = read_polars(source)

        assert result["осадки"][0] == 10.0

    def test_sorted_by_minimum(self) -> None:
        """Строки отсортированы по минимальной температуре."""
        result = read_polars(frame(12))

        values = result["мин"].to_list()
        assert values == sorted(values)

    def test_empty_result_is_empty(self) -> None:
        """Отсутствие подходящих строк даёт пустой результат, а не ошибку."""
        source = pl.DataFrame(
            {
                "city": ["Москва"],
                "temp_c": [20.0],
                "rain_mm": [0.0],
                "snow_mm": [0.0],
            }
        )

        assert read_polars(source).height == 0


class TestRounded:
    """Округление дробных значений перед сверкой."""

    def test_float_noise_is_dropped(self) -> None:
        """Разница в последнем разряде дробной части не считается расхождением.

        DuckDB посчитал среднюю как -3.5849999999999995, а Polars как
        -3.5850000000000004. Числа те же, отличается порядок сложения.
        """
        assert rounded([-3.5849999999999995]) == rounded([-3.5850000000000004])

    def test_real_difference_survives(self) -> None:
        """Настоящее расхождение округление не скрывает."""
        assert rounded([0.0]) != rounded([6.84])

    def test_integers_stay_integers(self) -> None:
        """Целые значения не превращаются в дробные."""
        assert rounded([6, 15, 2]) == [6, 15, 2]


class TestCompareResults:
    """Сверка результатов DuckDB и Polars."""

    def test_identical_results_pass(self) -> None:
        """Одинаковые результаты считаются совпавшими."""
        source = pl.DataFrame({"город": ["Москва"], "наблюдений": [2], "осадки": [1.5]})

        assert compare_results(source, source.clone()) == []

    def test_float_noise_passes(self) -> None:
        """Разница в последнем разряде не считается расхождением."""
        sql = pl.DataFrame({"осадки": [-3.5849999999999995]})
        polars = pl.DataFrame({"осадки": [-3.5850000000000004]})

        assert compare_results(sql, polars) == []

    def test_lost_rain_is_detected(self) -> None:
        """Потерянный дождь обнаруживается, а не списывается на округление."""
        sql = pl.DataFrame({"осадки": [0.0]})
        polars = pl.DataFrame({"осадки": [6.84]})

        problems = compare_results(sql, polars)

        assert problems
        assert "осадки" in problems[0]

    def test_different_column_sets_detected(self) -> None:
        """Разный состав столбцов обнаруживается."""
        sql = pl.DataFrame({"город": ["Москва"]})
        polars = pl.DataFrame({"город": ["Москва"], "осадки": [1.0]})

        problems = compare_results(sql, polars)

        assert any("столбцов" in line for line in problems)

    def test_different_row_counts_detected(self) -> None:
        """Разное количество строк обнаруживается."""
        sql = pl.DataFrame({"город": ["Москва", "Якутск"]})
        polars = pl.DataFrame({"город": ["Москва"]})

        problems = compare_results(sql, polars)

        assert any("строк" in line for line in problems)


class TestMeasure:
    """Замер времени."""

    def test_returns_all_timings(self) -> None:
        """Возвращаются минимум, медиана и список всех замеров."""
        minimum, median, timings = measure(lambda: sum(range(1000)), runs=5)

        assert len(timings) == 5
        assert minimum <= median
        assert median <= max(timings)

    def test_median_resists_single_outlier(self) -> None:
        """Один выброс не сдвигает медиану.

        Единственный замер на Windows скачет из-за планировщика ОС, и по нему
        вывод получался бы случайным. Первый вызов действительно медленный,
        остальные одинаковые: медиана должна остаться типичной.
        """
        calls = {"n": 0}

        def action() -> None:
            calls["n"] += 1
            if calls["n"] == 1:
                time.sleep(0.05)

        _, median, timings = measure(action, runs=5)

        assert len(timings) == 5
        assert max(timings) > 40.0
        assert median < 10.0

    def test_single_run_gives_one_timing(self) -> None:
        """Один повтор даёт один замер."""
        _, _, timings = measure(lambda: None, runs=1)

        assert len(timings) == 1


class TestCompareSpeed:
    """Вывод по двум замерам."""

    def test_absolute_noise_wins_nothing(self) -> None:
        """Разница меньше миллисекунды не объявляет победителя.

        Такое отношение времён бывает при большом времени: 0.5 мс против
        0.9 мс - это разница 0.4 мс, но почти вдвое. Объявлять по ней
        победителя означало бы объявлять его по шуму измерения.
        """
        verdict = compare_speed("DuckDB", 0.90, "Polars", 0.50)

        assert "Быстрее" not in verdict
        assert "одинаково" in verdict

    def test_small_ratio_wins_nothing(self) -> None:
        """Отношение меньше порога не объявляет победителя.

        Отношение 1.2 означает 20 %: на 420 строках это десятые доли
        миллисекунды.
        """
        verdict = compare_speed("DuckDB", 1.15, "Polars", 1.0)

        assert "Быстрее" not in verdict

    def test_large_difference_declares_faster(self) -> None:
        """Заметная разница объявляет победителя.

        18.00 мс против 0.77 мс - это и абсолютно много, и по отношению 23
        раза, и по обоим порогам проходит.
        """
        verdict = compare_speed("Polars", 0.77, "DuckDB", 18.0)

        assert "Быстрее Polars" in verdict
        assert "23.4 раза" in verdict

    def test_faster_is_named_wherever_it_is(self) -> None:
        """Побеждает тот, у кого меньше времени, независимо от порядка аргументов."""
        first = compare_speed("DuckDB", 0.77, "Polars", 18.0)
        second = compare_speed("Polars", 18.0, "DuckDB", 0.77)

        assert "Быстрее DuckDB" in first
        assert "Быстрее DuckDB" in second

    def test_thresholds_are_positive(self) -> None:
        """Пороги заданы положительными числами, иначе сравнение сломается."""
        assert NOISE_MS > 0
        assert SPEEDUP_THRESHOLD > 1.0


class TestFormatting:
    """Форматирование времён."""

    def test_formats_three_columns(self) -> None:
        """Минимум, медиана и список замеров превращаются в строки таблицы."""
        cells = fmt_times(1.234, 2.345, [1.0, 2.0, 3.0])

        assert cells == ["1.23", "2.35", "1.0, 2.0, 3.0"]


class TestFilterDefinition:
    """Условие фильтрации."""

    def test_filter_matches_polars_expression(self, tmp_path: Path) -> None:
        """Условие в SQL и в Polars отбирают одинаковые строки.

        Поначалу порог был temp_c < 0: под него подходил один город с шестью
        наблюдениями, и сравнивать скорость на одной группе бессмысленно.
        Порог +10 оставляет пять городов и настоящую погоду.
        """
        source = frame(12)
        path = str(parquet(source, tmp_path)).replace("\\", "/")

        sql_rows = read_sql(
            f"SELECT count(*) AS n FROM read_parquet('{path}') WHERE {FILTER_SQL}"
        )["n"][0]

        polars_rows = read_polars(source)["наблюдений"].sum()

        assert sql_rows == polars_rows
        assert sql_rows > 0
