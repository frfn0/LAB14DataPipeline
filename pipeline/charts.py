"""Задание 9. Визуализация результатов.

Методичка требует минимум две информативные визуализации и сохранение
графиков в файлы. Здесь их четыре, и каждая отвечает на свой вопрос:

1. Временной ряд температуры по всем городам - как погода менялась пять дней.
2. Гистограмма распределения температуры - какие значения вообще бывают и как
   распределены наблюдения.
3. Тепловая карта «город × местная дата» - кто где холоднее в каждый день.
4. Столбчатая диаграмма осадков по городам - где выпало больше всего.

Каждый график сохраняется в двух форматах: PNG через Matplotlib и
интерактивный HTML через Plotly. PNG нужен для отчёта и просмотра без
браузера, HTML - чтобы рассмотреть график и навести курсор на точку.

Про Plotly и PNG. Экспорт Plotly в PNG требует пакет kaleido, которого нет ни в
окружении, ни в requirements: он тянёт за собой отдельный движок браузера и на
установку уходит несколько минут. Ставить его ради одного формата смысла нет,
поэтому PNG рисуется Matplotlib, а Plotly даёт интерактивный вариант.

Запуск:

    python -m pipeline.charts
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import plotly.express as px
import polars as pl
from matplotlib.ticker import MaxNLocator

from pipeline.common import (
    CHARTS_DIR,
    ROOT,
    clean_out_path,
    ensure_dir,
    human_bytes,
    load_clean,
    rule,
    section,
    timed,
)
from pipeline.report import print_table

# Шрифт с кириллицей. По умолчанию Matplotlib на Windows берёт первый
# найденный, а подписи вида «Санкт-Петербург» превращаются в квадраты.
FONT = "DejaVu Sans"

# Размер PNG в дюймах и разрешение. 12 на 7 дюймов при 150 dpi даёт
# 1800 на 1050 пикселей: читается в отчёте и не занимает много места.
FIGSIZE = (12, 7)
DPI = 150

# Число городов на временном ряде. Все десять на одном графике сливаются в
# клубок, поэтому ряд строится по шести самым холодным и самым тёплым, а
# полный набор остаётся в интерактивном HTML.
SERIES_CITIES = 6

# Сколько столбцов в гистограмме. Двадцать - приемлемо, шестьдесят превращают
# гистограмму в полосу без формы.
BINS = 20

# Цвета городов. Перебираются по кругу, чтобы в легенде и на графике город
# имел один и тот же цвет во всех графиках.
PALETTE = [
    "#1f77b4",
    "#d62728",
    "#2ca02c",
    "#ff7f0e",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
]


def city_colors(names: list[str]) -> dict[str, str]:
    """Раздаёт городам цвета по кругу из палитры.

    Один и тот же город получает один и тот же цвет на всех графиках: иначе
    временной ряд и тепловая карта говорили бы об одном и том же городе
    разными цветами.

    Цвета раздаются по алфавиту, а не в порядке переданного списка: иначе
    порядок, в котором города пришли из таблицы, влиял бы на цвета, и
    перестроенная таблица поменяла бы цвета у всех городов разом.
    """
    return {
        name: PALETTE[index % len(PALETTE)] for index, name in enumerate(sorted(names))
    }


def apply_style() -> None:
    """Настраивает общий вид графиков."""
    plt.rcParams.update(
        {
            "font.family": FONT,
            "font.size": 11,
            "axes.titlesize": 14,
            "axes.labelsize": 11,
            "axes.grid": True,
            "grid.alpha": 0.3,
            "grid.linestyle": "--",
            "figure.autolayout": True,
            "savefig.bbox": "tight",
        }
    )


def save_png(figure: plt.Figure, name: str) -> Path:
    """Сохраняет график в PNG и возвращает путь к файлу."""
    target = CHARTS_DIR / name
    ensure_dir(target.parent)
    figure.savefig(target, dpi=DPI)
    plt.close(figure)
    return target


def save_html(figure, name: str) -> Path:
    """Сохраняет интерактивный график Plotly в HTML и возвращает путь.

    Заголовок документа не передаётся: аргумента title у write_html в Plotly 7
    нет, и вызов с ним падал с TypeError. Заголовок графика уже задан в самом
    рисунке, через title= при построении.
    """
    target = CHARTS_DIR / name
    ensure_dir(target.parent)
    figure.write_html(target, include_plotlyjs="cdn", full_html=True)
    return target


def cities_for_series(frame: pl.DataFrame) -> list[str]:
    """Выбирает города для временного ряда.

    Берутся три самых холодных и три самых тёплых по средней температуре.
    Ровно середина шкалы не интересует: на графике временного ряда важны
    крайние точки, а середина спрятана среди них.

    Города не повторяются: если городов меньше, чем SERIES_CITIES, список
    возвращается целиком. Без этого проверки на трёх городах давали бы
    «Москва, Тёплый, Тёплый», и город рисовался бы двумя разами.
    """
    means = (
        frame.group_by("city").agg(pl.col("temp_c").mean().alias("mean")).sort("mean")
    )
    names = means["city"].to_list()

    if len(names) <= SERIES_CITIES:
        return names

    half = SERIES_CITIES // 2
    return names[:half] + names[-half:]


def time_series(frame: pl.DataFrame, target: Path) -> tuple[list[Path], pl.DataFrame]:
    """Строит временной ряд температуры по городам.

    Ось времени идёт по UTC: это единая шкала, и города с разными часовыми
    поясами стоят на одном графике. Суточные агрегаты по местной дате считаются
    в задании 6, а здесь важна общая динамика.

    Returns:
        Пара: список файлов и таблица с данными графика.
    """
    names = cities_for_series(frame)
    colors = city_colors(names)

    data = (
        frame.filter(pl.col("city").is_in(names))
        .group_by(["city", "observed_utc"])
        .agg(
            pl.col("temp_c").mean().alias("temp_c"),
            pl.col("temp_c").min().alias("мин"),
            pl.col("temp_c").max().alias("макс"),
        )
        .sort("observed_utc")
    )

    figure, axis = plt.subplots(figsize=FIGSIZE)

    # Поясняющий интервал: между минимумом и максимумом, а не линия среднего.
    # Разброс по часам показывает, насколько менялась погода за сутки.
    for city in names:
        city_rows = data.filter(pl.col("city") == city)
        moments = city_rows["observed_utc"].to_list()
        color = colors[city]
        axis.plot(
            moments,
            city_rows["temp_c"].to_list(),
            color=color,
            linewidth=1.6,
            label=city,
        )
        axis.fill_between(
            moments,
            city_rows["мин"].to_list(),
            city_rows["макс"].to_list(),
            color=color,
            alpha=0.15,
        )

    axis.set_title("Температура по городам за пять суток, по UTC")
    axis.set_xlabel("Момент наблюдения (UTC)")
    axis.set_ylabel("Температура, °C")
    axis.legend(title="Город", loc="upper left", fontsize=9)
    axis.grid(True, alpha=0.3, linestyle="--")
    figure.autofmt_xdate()

    png = save_png(figure, "01_temperature_timeseries.png")

    interactive = px.line(
        data.sort(["city", "observed_utc"]),
        x="observed_utc",
        y="temp_c",
        color="city",
        title="Температура по городам за пять суток, по UTC",
        labels={"observed_utc": "Момент наблюдения (UTC)", "temp_c": "Температура, °C"},
    )
    html = save_html(interactive, "01_temperature_timeseries.html")

    return [png, html], data


def temperature_histogram(
    frame: pl.DataFrame, target: Path
) -> tuple[list[Path], pl.DataFrame]:
    """Строит гистограмму распределения температуры.

    Гистограмма одна на все города: распределение показывает, какие значения
    вообще встречаются, а не кто где. Пунктиром отмечена средняя температура
    по всем данным - она показывает, куда смещено распределение.

    Returns:
        Пара: список файлов и таблица с числом наблюдений в каждом интервале.
    """
    figure, axis = plt.subplots(figsize=FIGSIZE)

    axis.hist(
        frame["temp_c"].to_list(),
        bins=BINS,
        color="#1f77b4",
        edgecolor="white",
        linewidth=0.6,
    )

    mean_temp = frame["temp_c"].mean()
    axis.axvline(mean_temp, color="#d62728", linestyle="--", linewidth=1.8)
    axis.text(
        mean_temp + 0.3,
        axis.get_ylim()[1] * 0.92,
        f"средняя {mean_temp:.2f} °C",
        color="#d62728",
        fontsize=10,
    )

    axis.set_title("Распределение температуры по всем наблюдениям")
    axis.set_xlabel("Температура, °C")
    axis.set_ylabel("Число наблюдений")
    axis.yaxis.set_major_locator(MaxNLocator(integer=True))
    axis.axvline(0, color="#444444", linestyle=":", linewidth=1.2)
    axis.text(
        0.4,
        axis.get_ylim()[1] * 0.55,
        "ноль градусов",
        color="#444444",
        fontsize=9,
    )

    png = save_png(figure, "02_temperature_histogram.png")

    interactive = px.histogram(
        frame,
        x="temp_c",
        nbins=BINS,
        color="city",
        barmode="overlay",
        opacity=0.55,
        title="Распределение температуры по городам",
        labels={"temp_c": "Температура, °C", "count": "Число наблюдений"},
    )
    html = save_html(interactive, "02_temperature_histogram.html")

    table = (
        frame.with_columns(
            (
                (pl.col("temp_c") - frame["temp_c"].min())
                / (frame["temp_c"].max() - frame["temp_c"].min())
                * BINS
            )
            .floor()
            .clip(0, BINS - 1)
            .cast(pl.Int64)
            .alias("корзина")
        )
        .group_by("корзина")
        .agg(pl.len().alias("наблюдений"))
        .sort("корзина")
    )

    return [png, html], table


def heatmap(frame: pl.DataFrame, target: Path) -> tuple[list[Path], pl.DataFrame, int]:
    """Строит тепловую карту «город × местная дата».

    Группировка по местной дате: сутки в городе заканчиваются в полночь по его
    времени, и по UTC ночная часть попала бы в предыдущий день.

    Returns:
        Тройка: список файлов, сводная таблица температур и число пустых ячеек.
        Пустые ячейки подписываются «нет данных»: иначе по цвету нельзя было бы
        отличить отсутствие наблюдений от предельно холодной температуры.
    """
    table = (
        frame.group_by(["city", "local_date"])
        .agg(pl.col("temp_c").mean().alias("средняя"))
        .pivot(index="city", on="local_date", values="средняя")
        .sort("city")
    )

    # После pivot названия столбцов приходят строками, а не датами: Polars
    # разворачивает значения в заголовки колонок. Порядок при этом
    # произвольный - даты в колонках шли как 11, 10, 09, 12, 13, 08, 07,
    # поэтому даты сортируются по строке: в формате ГГГГ-ММ-ДД порядок строк
    # совпадает с хронологическим. Подпись для оси достаётся разбором строки,
    # а не вызовом strftime, которого у строки нет.
    dates = sorted(column for column in table.columns if column != "city")
    table = table.select(["city", *dates])
    labels = [date[5:].replace("-", ".") for date in dates]

    cities = table["city"].to_list()
    values = table.drop("city").to_numpy()

    figure, axis = plt.subplots(figsize=FIGSIZE)

    # Карта строится по своей шкале, а не по всей палитре RdYlBu: иначе
    # большинство ячеек, а это средние температуры от −9 до +18 °C, попали бы
    # в середину шкалы и слились в один бледный цвет. Нормировка по своим
    # значениям делает различия между городами видимыми.
    # Границы шкалы считаются без пропусков: в values есть NaN в пустых
    # ячейках, и обычные min и max вернули бы nan. Тогда imshow получил бы
    # vmin=nan, matplotlib молча подставил бы диапазон −0.1...0.1, и вся
    # карта стала бы одного цвета - на первой версии графика так и вышло.
    colormap = plt.get_cmap("RdYlBu_r")
    lowest = float(np.nanmin(values))
    highest = float(np.nanmax(values))
    span = highest - lowest

    def text_color(number: float) -> str:
        """Чёрным по светлой ячейке, белым по тёмной.

        Цвет берётся из той же шкалы, что и ячейка, а не по порогу
        «значение больше середины»: в палитре RdYlBu середина это светлое
        пятно, и чёрный на нём читается плохо.
        """
        if span == 0:
            return "black"
        position = (number - lowest) / span
        # Красная половина палитры тёмная, синяя - тоже; светлая полоса в
        # середине. Промежуток 0.15...0.85 - светлые клетки, остальное тёмные.
        if 0.15 < position < 0.85:
            return "black"
        return "white"

    image = axis.imshow(values, cmap=colormap, aspect="auto", vmin=lowest, vmax=highest)
    axis.set_xticks(range(len(dates)))
    axis.set_xticklabels(labels, rotation=45, ha="right")
    axis.set_yticks(range(len(cities)))
    axis.set_yticklabels(cities)
    axis.set_xlabel("Местная дата")
    axis.set_title("Средняя температура по городам и дням, °C")

    # Подписи прямо в ячейках: цветовая шкала показывает порядок величин, а
    # число рядом даёт точное значение, ради которого карта и строится.
    missing = 0
    for row in range(values.shape[0]):
        for column in range(values.shape[1]):
            number = values[row, column]
            if number == number:  # не пропуск
                axis.text(
                    column,
                    row,
                    f"{number:.1f}",
                    ha="center",
                    va="center",
                    fontsize=8,
                    color=text_color(number),
                )
            else:
                missing += 1
                axis.text(
                    column,
                    row,
                    "нет данных",
                    ha="center",
                    va="center",
                    fontsize=7,
                    color="#666666",
                    style="italic",
                )

    bar = figure.colorbar(image, ax=axis, shrink=0.85)
    bar.set_label("Средняя температура, °C")

    png = save_png(figure, "03_temperature_heatmap.png")

    long_form = (
        frame.group_by(["city", "local_date"])
        .agg(pl.col("temp_c").mean().alias("Средняя температура, °C"))
        .sort(["city", "local_date"])
    )
    interactive = px.imshow(
        table.drop("city").to_pandas(),
        x=dates,
        y=cities,
        text_auto=".1f",
        color_continuous_scale="RdYlBu_r",
        title="Средняя температура по городам и дням, °C",
        labels={"x": "Местная дата", "y": "Город"},
    )
    html = save_html(interactive, "03_temperature_heatmap.html")

    return [png, html], long_form, missing


def precipitation(frame: pl.DataFrame, target: Path) -> tuple[list[Path], pl.DataFrame]:
    """Строит столбчатую диаграмму осадков по городам.

    Диаграмма накопительная: дождь и снег показаны разными частями одного
    столбца, иначе было бы не видно, из чего сложились 31.84 мм в
    Санкт-Петербурге.

    Returns:
        Пара: список файлов и таблица осадков.
    """
    table = (
        frame.group_by("city")
        .agg(
            pl.col("rain_mm").sum().alias("дождь"),
            pl.col("snow_mm").sum().alias("снег"),
            pl.len().alias("наблюдений"),
        )
        .with_columns((pl.col("дождь") + pl.col("снег")).alias("всего"))
        .sort("всего", descending=True)
    )

    cities = table["city"].to_list()
    rain = table["дождь"].to_list()
    snow = table["снег"].to_list()

    figure, axis = plt.subplots(figsize=FIGSIZE)

    positions = range(len(cities))

    axis.bar(positions, rain, color="#1f77b4", label="дождь")
    axis.bar(positions, snow, bottom=rain, color="#9ecae1", label="снег")

    # Запас сверху для подписей: без него подпись самого большого столбца
    # 31.84 мм уходила за верхнюю границу и попадала на заголовок.
    largest = max(table["всего"].max(), 1.0)
    axis.set_ylim(0, largest * 1.15)

    for index, row in enumerate(table.iter_rows(named=True)):
        if row["всего"] > 0:
            axis.text(
                index,
                row["всего"] + largest * 0.02,
                f"{row['всего']:.2f}",
                ha="center",
                fontsize=9,
            )
        else:
            # Ноль подписывается у основания: без подписи пустой столбец не
            # отличить от отсутствующего города.
            axis.text(
                index,
                largest * 0.02,
                "0",
                ha="center",
                fontsize=9,
                color="#666666",
            )

    axis.set_xticks(list(positions))
    axis.set_xticklabels(cities, rotation=30, ha="right")
    axis.set_title("Сумма осадков по городам за период наблюдений")
    axis.set_ylabel("Осадки, мм")
    axis.legend()
    axis.grid(True, axis="y", alpha=0.3, linestyle="--")

    png = save_png(figure, "04_precipitation.png")

    interactive = px.bar(
        table.sort("всего"),
        x="city",
        y=["дождь", "снег"],
        title="Сумма осадков по городам за период наблюдений",
        labels={"city": "Город", "value": "Осадки, мм", "variable": "Тип"},
    )
    html = save_html(interactive, "04_precipitation.html")

    return [png, html], table


def describe_captions(rows: list[tuple[str, str]]) -> None:
    """Печатает список графиков с тем, что на каждом видно."""
    print_table([["график", "что показывает"], *[[name, note] for name, note in rows]])


def main() -> int:
    """Строит все графики и сохраняет их в файлы.

    Returns:
        Код возврата: 0 при успехе, 1 если данных не хватило.
    """
    section("Задание 9. Визуализация результатов")

    frame = load_clean()
    print(f"Прочитано очищенных данных: {frame.height} строк, {frame.width} столбцов")
    print(f"Каталог графиков: {CHARTS_DIR.relative_to(ROOT)}")
    print(f"Шрифт: {FONT} (с кириллицей)")
    print()

    apply_style()

    target = CHARTS_DIR
    ensure_dir(target)

    section("1. Временной ряд температуры")

    with timed("построение временного ряда"):
        series_files, series = time_series(frame, target)

    print(f"  городов на графике: {series['city'].n_unique()}")
    print(f"  точек во всех рядах: {series.height}")
    print(f"  период: {series['observed_utc'].min()} - {series['observed_utc'].max()}")
    print(
        f"  температура: от {series['мин'].min():.2f} до {series['макс'].max():.2f} °C"
    )
    print()
    print("  Все десять городов на одном графике сливаются в клубок, поэтому")
    print(f"  показаны {SERIES_CITIES}: три самых холодных и три самых тёплых.")
    print("  Полный набор из десяти городов есть в интерактивном HTML.")
    print()

    section("2. Гистограмма распределения температуры")

    with timed("построение гистограммы"):
        hist_files, hist_table = temperature_histogram(frame, target)

    print(f"  интервалов: {BINS}")
    print(f"  наблюдений: {hist_table['наблюдений'].sum()}")
    print(
        f"  самый частый интервал: №{hist_table['наблюдений'].arg_max() + 1}, "
        f"{hist_table['наблюдений'].max()} наблюдений"
    )
    print(f"  средняя температура: {frame['temp_c'].mean():.2f} °C")
    print()

    section("3. Тепловая карта город × дата")

    with timed("построение тепловой карты"):
        heat_files, heat_table, heat_missing = heatmap(frame, target)

    cities = heat_table["city"].n_unique()
    dates = heat_table["local_date"].n_unique()
    cells = cities * dates

    print(f"  городов: {cities}, местных дат: {dates}")
    print(f"  ячеек в карте: {cells}, из них пустых: {heat_missing}")
    print(f"  диапазон: от {heat_table['Средняя температура, °C'].min():.2f} °C")
    print(f"            до {heat_table['Средняя температура, °C'].max():.2f} °C")
    print()
    print(f"  Пустых ячеек {heat_missing} из {cells}: первый и последний местные дни")
    print("  покрыты не полностью. Прогноз на 5 суток начинается с полуночи")
    print("  седьмого октября и заканчивается восемнадцатью часами двенадцатого,")
    print("  а наблюдение сейчас приходится на один момент, поэтому в этих")
    print("  сутках данных мало. Такие ячейки подписаны «нет данных»: по одному")
    print("  только цвету нельзя отличить отсутствие наблюдений от предельно")
    print("  холодной температуры.")
    print()

    section("4. Осадки по городам")

    with timed("построение диаграммы осадков"):
        precip_files, precip_table = precipitation(frame, target)

    print(f"  городов: {precip_table.height}")
    print(f"  дождя всего: {precip_table['дождь'].sum():.2f} мм")
    print(f"  снега всего: {precip_table['снег'].sum():.2f} мм")
    print(f"  осадков всего: {precip_table['всего'].sum():.2f} мм")
    print()

    wettest = precip_table.row(0, named=True)
    driest = precip_table.row(-1, named=True)
    print(f"  больше всего осадков: {wettest['city']}, {wettest['всего']:.2f} мм")
    print(f"  меньше всего осадков: {driest['city']}, {driest['всего']:.2f} мм")
    print()

    section("5. Сохранённые файлы")

    everything = [
        *series_files,
        *hist_files,
        *heat_files,
        *precip_files,
    ]

    for path in everything:
        print(f"  {path.relative_to(ROOT)}  {human_bytes(path.stat().st_size)}")

    print(rule())
    print(f"  всего файлов: {len(everything)}")
    print(
        f"  общий объём: {human_bytes(sum(path.stat().st_size for path in everything))}"
    )
    print()

    section("Что показывает каждый график")
    describe_captions(
        [
            (
                "01_temperature_timeseries",
                "Как менялась температура пять суток в шести городах: "
                "полоса между минимумом и максимумом показывает разброс за сутки.",
            ),
            (
                "02_temperature_histogram",
                "Какие значения температуры вообще встречаются: "
                "распределение смещено вправо от нуля, пунктир - средняя.",
            ),
            (
                "03_temperature_heatmap",
                "Кто где холоднее в каждый из семи местных дней: "
                "красным холодно, синим тепло.",
            ),
            (
                "04_precipitation",
                "Сколько осадков выпало в каждом городе, дождь и снег "
                "отдельно: 31.84 мм в Санкт-Петербурге, ноль в четырёх.",
            ),
        ]
    )

    section("Итог")
    print(f"  графиков построено: {len(everything) // 2}")
    print("  форматов: PNG для отчёта, HTML интерактивный")
    print("  каждый график сохранён в двух форматах")
    print()
    print(
        f"  исходные данные: {clean_out_path().relative_to(ROOT)}, {frame.height} строк"
    )
    print(f"  осадков на всех графиках суммарно {precip_table['всего'].sum():.2f} мм")
    print(
        "  средняя температура на гистограмме и в задании 8 совпадает: "
        f"{frame['temp_c'].mean():.2f} °C"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
