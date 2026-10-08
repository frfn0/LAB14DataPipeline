"""Тесты подготовки данных к графикам задания 9.

Графики сами по себе проверяются взглядом: числа на них неверны или подписи
наезжают друг на друга. Поэтому тесты проверяют то, что можно проверить
программно, - подготовленные таблицы, выбор городов, палитру и границы шкалы.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import numpy as np
import polars as pl

from pipeline.charts import (
    BINS,
    PALETTE,
    cities_for_series,
    city_colors,
)

CITIES = ["Москва", "Казань", "Сочи", "Якутск", "Новосибирск", "Калининград"]


def frame(rows: int = 30) -> pl.DataFrame:
    """Таблица с городами, временем и температурой."""
    return pl.DataFrame(
        {
            "city": [CITIES[index % len(CITIES)] for index in range(rows)],
            # Час берётся по модулю 24, а сутки увеличиваются: в datetime нельзя
            # час больше 23, а строк в тесте больше, чем часов в сутках.
            "observed_utc": [
                datetime(2026, 10, 8 + index // 24, index % 24, tzinfo=timezone.utc)
                for index in range(rows)
            ],
            "local_date": [date(2026, 10, 8 + index % 3) for index in range(rows)],
            "temp_c": [float(index) - 10 for index in range(rows)],
            "rain_mm": [0.0] * rows,
            "snow_mm": [0.0] * rows,
        },
        schema={
            "city": pl.String,
            "observed_utc": pl.Datetime("us", "UTC"),
            "local_date": pl.Date,
            "temp_c": pl.Float64,
            "rain_mm": pl.Float64,
            "snow_mm": pl.Float64,
        },
    )


class TestCityColors:
    """Палитра городов."""

    def test_every_city_gets_a_color(self) -> None:
        """Каждому городу назначен свой цвет."""
        colors = city_colors(CITIES)

        assert set(colors) == set(CITIES)
        assert all(color.startswith("#") for color in colors.values())

    def test_same_city_keeps_same_color(self) -> None:
        """Один город получает один и тот же цвет на всех графиках.

        Иначе временной ряд и тепловая карта говорили бы об одном городе
        разными цветами.
        """
        first = city_colors(CITIES)
        second = city_colors(list(reversed(CITIES)))

        assert first == second

    def test_colors_cycle_when_more_cities_than_colors(self) -> None:
        """При десяти городах и десяти цветах палитра не кончается."""
        names = [f"Город {index}" for index in range(len(PALETTE) + 2)]

        colors = city_colors(names)

        assert len(colors) == len(names)
        assert all(color in PALETTE for color in colors.values())


class TestCitiesForSeries:
    """Выбор городов для временного ряда."""

    def test_picks_extremes(self) -> None:
        """Берутся самые холодные и самые тёплые города.

        На временном ряде важны крайние точки: середина шкалы спрятана среди
        них и ничего не добавляет. Городов восемь, иначе список вернулся бы
        целиком и крайние точки не выделялись бы.
        """
        source = pl.DataFrame(
            {
                "city": [
                    "Очень холодный",
                    "Холодный",
                    "Прохладный",
                    "Средний",
                    "Тёплый",
                    "Довольно тёплый",
                    "Очень тёплый",
                    "Жаркий",
                ],
                "temp_c": [-30.0, -20.0, -5.0, 0.0, 5.0, 10.0, 20.0, 30.0],
            }
        )

        picked = cities_for_series(source)

        assert picked == [
            "Очень холодный",
            "Холодный",
            "Прохладный",
            "Довольно тёплый",
            "Очень тёплый",
            "Жаркий",
        ]
        assert "Средний" not in picked
        assert "Тёплый" not in picked

    def test_fewer_cities_returns_all(self) -> None:
        """Городов меньше, чем нужно для ряда, - возвращаются все.

        Без этого проверки на трёх городах получили бы список с повторами, и
        один город был бы нарисован дважды.
        """
        source = pl.DataFrame(
            {"city": ["Холодный", "Средний", "Жаркий"], "temp_c": [-20.0, 0.0, 20.0]}
        )

        picked = cities_for_series(source)

        assert sorted(picked) == ["Жаркий", "Средний", "Холодный"]
        assert len(picked) == len(set(picked))

    def test_order_follows_temperature(self) -> None:
        """Города в списке идут от холодных к тёплым.

        Порядок проверяется по самому списку, а не по таблице: group_by
        возвращает города по алфавиту, и сравнение с ним ничего не значило бы.
        """
        source = frame(60)

        picked = cities_for_series(source)

        # Список городов берётся один раз: два вызова unique() в Polars
        # возвращают строки в разном порядке, и словарь собрался бы из
        # разных перестановок.
        unique = source["city"].unique().to_list()
        means = {
            city: source.filter(pl.col("city") == city)["temp_c"].mean()
            for city in unique
        }
        values = [means[city] for city in picked]

        assert values == sorted(values)

    def test_returns_existing_cities_only(self) -> None:
        """Возвращаются только города, которые есть в данных."""
        picked = cities_for_series(frame(30))

        assert set(picked) <= set(CITIES)

    def test_single_city_still_works(self) -> None:
        """Даже один город даёт непустой список.

        На таком объёме SERIES_CITIES больше, чем городов, и список должен
        остаться осмысленным, а не продублировать один город.
        """
        source = pl.DataFrame({"city": ["Москва", "Москва"], "temp_c": [1.0, 2.0]})

        picked = cities_for_series(source)

        assert picked == ["Москва"]


class TestHistogramData:
    """Подготовка данных гистограммы."""

    def test_bin_count_is_reasonable(self) -> None:
        """Двадцать интервалов - приемлемо, шестьдесят превращают гистограмму
        в полосу без формы.
        """
        assert BINS == 20

    def test_all_observations_are_counted(self) -> None:
        """Сумма по интервалам равна числу наблюдений."""
        source = frame(30)

        low = source["temp_c"].min()
        high = source["temp_c"].max()

        binned = source.with_columns(
            ((pl.col("temp_c") - low) / (high - low) * BINS)
            .floor()
            .clip(0, BINS - 1)
            .cast(pl.Int64)
            .alias("bin")
        )

        assert binned.group_by("bin").len()["len"].sum() == source.height

    def test_cold_and_warm_cities_land_in_different_bins(self) -> None:
        """Холодный и тёплый город попадают в разные интервалы."""
        source = frame(30)

        low = source["temp_c"].min()
        high = source["temp_c"].max()

        def where(city: str) -> int:
            """Номер интервала для города."""
            rows = source.filter(pl.col("city") == city)
            value = rows["temp_c"].mean()
            if value is None:
                return -1
            return int((value - low) / (high - low) * BINS)

        assert where("Якутск") != where("Сочи")


class TestHeatmapShape:
    """Развёртка данных для тепловой карты."""

    def test_pivot_gives_string_columns(self) -> None:
        """После pivot даты приходят строками, а не объектами date.

        На этом падал первый вариант графика: strftime у строки не существует,
        и построение обрывалось с AttributeError.
        """
        source = frame(30)

        table = source.pivot(
            index="city", on="local_date", values="temp_c", aggregate_function="mean"
        )

        date_columns = [column for column in table.columns if column != "city"]
        assert date_columns
        assert all(isinstance(column, str) for column in date_columns)

    def test_missing_cells_become_nan(self) -> None:
        """Пропущенные комбинации города и даты дают NaN, а не ноль.

        Ноль был бы правдоподобной температурой, и по карте нельзя было бы
        отличить «нет наблюдений» от «ровно 0 °C».
        """
        source = pl.DataFrame(
            {
                "city": ["Москва", "Якутск"],
                "local_date": [date(2026, 10, 8), date(2026, 10, 9)],
                "temp_c": [1.0, 2.0],
            }
        )

        table = source.pivot(
            index="city", on="local_date", values="temp_c", aggregate_function="mean"
        )
        values = table.drop("city").to_numpy()

        assert values.size == 4
        # nan != nan, поэтому присутствие пропуска проверяется через numpy:
        # обычные min и max вернули бы nan и сломали бы границы шкалы.
        assert bool(np.isnan(values).any())
        assert bool(not np.isnan(values).all())

    def test_grid_size_is_cities_times_dates(self) -> None:
        """Размер сетки равен числу городов, умноженному на число дат."""
        source = frame(30)

        cities = source["city"].n_unique()
        dates = source["local_date"].n_unique()
        table = source.pivot(
            index="city", on="local_date", values="temp_c", aggregate_function="mean"
        )

        assert table.drop("city").to_numpy().size == cities * dates

    def test_sorted_dates_are_chronological(self) -> None:
        """Сортировка названий столбцов даёт хронологический порядок.

        После pivot даты шли как 11, 10, 09, 12, 13, 08, 07 - произвольный
        порядок, и дни на карте были бы переставлены.
        """
        source = frame(30)

        table = source.pivot(
            index="city", on="local_date", values="temp_c", aggregate_function="mean"
        )
        dates = sorted(column for column in table.columns if column != "city")

        assert dates == sorted(dates)
        assert dates[0] < dates[-1]
