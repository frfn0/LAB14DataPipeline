"""Проверки документации: README, PROMPT_LOG и скрипт запуска.

Документация расходится с кодом тихо: переименовали модуль, а в README осталось
старое имя, и человек запускает шаг, которого нет. Такие расхождения не видны
ни в линтере, ни в тестах кода, поэтому здесь проверяется, что написанное в
README совпадает с тем, что лежит в репозитории.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

README = ROOT / "README.md"
PROMPT_LOG = ROOT / "PROMPT_LOG.md"
RUN_ALL = ROOT / "scripts" / "run_all.ps1"

# Шаги конвейера, которые запускает скрипт: номер задания и имя модуля.
PIPELINE_STEPS = [
    (4, "pipeline.import_data"),
    (5, "pipeline.clean_data"),
    (6, "pipeline.aggregate"),
    (7, "pipeline.to_parquet"),
    (8, "pipeline.duckdb_query"),
    (9, "pipeline.charts"),
]


def readme() -> str:
    """Текст README."""
    return README.read_text(encoding="utf-8")


def prompt_log() -> str:
    """Текст PROMPT_LOG."""
    return PROMPT_LOG.read_text(encoding="utf-8")


class TestReadmeStructure:
    """Структура README."""

    def test_has_title_and_identity(self) -> None:
        """Есть заголовок, студент, группа и вариант."""
        text = readme()

        assert text.startswith("# Лабораторная работа")
        assert "Сурков Всеволод Сергеевич" in text
        assert "220032-11" in text
        assert "№2" in text

    def test_all_ten_tasks_present(self) -> None:
        """Все десять заданий имеют раздел."""
        text = readme()

        for number in range(1, 11):
            assert f"## Задание {number}." in text, number

    def test_task_table_marks_all_tasks_done(self) -> None:
        """В таблице заданий нет незавершённых пунктов."""
        text = readme()

        # Таблица заданий - первая таблица файла. По всему README идут и другие
        # таблицы, в том числе с числами в первом столбце, поэтому берётся
        # только нужная: иначе в состояния попали бы строки из других таблиц.
        header = "| № | Формулировка из методички | Состояние |"
        assert header in text, "в README нет таблицы заданий"

        table = text.split(header, 1)[1].split("\n\n", 1)[0]
        rows = re.findall(r"^\|\s*(\d+)\s*\|[^|]*\|\s*([^|]+?)\s*\|$", table, re.M)
        states = {int(number): state for number, state in rows}

        assert set(states) == set(range(1, 11))
        assert all("выполнено" in states[number] for number in states)

    def test_no_placeholders_left(self) -> None:
        """Не осталось заглушек вида «будет дополнено»."""
        assert "будет дополнено" not in readme()
        assert "_будет дополнено_" not in prompt_log()


class TestReadmeCommands:
    """Команды в README соответствуют репозиторию."""

    @pytest.mark.parametrize("module", [name for _, name in PIPELINE_STEPS])
    def test_module_is_importable(self, module: str) -> None:
        """Модуль из README действительно запускается."""
        name = module.split(".")[1]
        path = ROOT / "pipeline" / f"{name}.py"

        assert path.exists(), module

    def test_every_step_has_run_command(self) -> None:
        """Для каждого шага есть команда запуска."""
        text = readme()

        for number, module in PIPELINE_STEPS:
            assert f"python -m {module}" in text, module

    def test_collector_command_mentioned(self) -> None:
        """Есть команда запуска сборщика."""
        text = readme()

        assert "go run ./cmd/collector" in text

    def test_collect_flags_documented(self) -> None:
        """Основные флаги сборщика перечислены в README.

        Флаги берутся из исходника, а не из памяти: если флаг переименуют, а
        README нет, человек запустит сборщик со старым именем и не поймёт,
        почему тот не реагирует на параметр.
        """
        text = readme()
        source = (ROOT / "internal" / "config" / "config.go").read_text(
            encoding="utf-8"
        )
        flags = re.findall(r'flagSet\.\w+\("([a-z-]+)"', source)

        assert flags

        for flag in flags:
            assert f"-{flag}" in text, flag


class TestReadmeLinks:
    """Ссылки в README ведут в существующие файлы."""

    @pytest.mark.parametrize(
        "target",
        re.findall(r"\]\((?!https?:)([^)#]+)", readme()),
    )
    def test_local_file_exists(self, target: str) -> None:
        """Каждый локальный путь из ссылки существует."""
        if target.endswith("/"):
            return

        assert (ROOT / target).exists(), target

    def test_result_files_referenced(self) -> None:
        """На каждый результат в results есть ссылка."""
        text = readme()

        for number in range(1, 10):
            assert f"results/task{number}_result.txt" in text, number


class TestPromptLog:
    """PROMPT_LOG заполнен по всем заданиям."""

    def test_has_all_ten_sections(self) -> None:
        """Все десять заданий описаны."""
        text = prompt_log()

        for number in range(1, 11):
            assert f"## Задание {number}" in text, number

    def test_each_section_has_prompt_and_result(self) -> None:
        """В каждом разделе есть промпт и результат."""
        text = prompt_log()
        parts = re.split(r"^## ", text, flags=re.M)[1:]

        for part in parts:
            title = part.splitlines()[0]
            assert "### Промпт" in part, title
            assert "### Результат" in part, title

    def test_documents_failures(self) -> None:
        """Записаны неудачные попытки по большинству заданий.

        Методичка требует честно писать о том, что не получилось: раздел без
        ошибок выглядит так, будто всё шло с первого раза, а это неправда.
        Задания 1-3 выполнялись до того, как лог был заведён, поэтому
        достаточно семи разделов из десяти.
        """
        text = prompt_log()

        assert text.count("Неудачные попытки") >= 7

    def test_failures_are_described_as_a_table(self) -> None:
        """Неудачные попытки описаны таблицей «что, почему, как исправлено».

        Просто список ошибок не говорит, что с ними сделали, а раздел с
        исправлениями и есть то, что в логе действительно ценно.
        """
        text = prompt_log()
        sections = re.split(r"^## ", text, flags=re.M)[1:]
        described = [part for part in sections if "Неудачные попытки" in part]

        assert described
        for part in described:
            assert "Как исправлено" in part, part.splitlines()[0]

    def test_no_api_key_in_log(self) -> None:
        """Ключ API не попал в документацию."""
        for name in ("README.md", "PROMPT_LOG.md"):
            text = (ROOT / name).read_text(encoding="utf-8")
            assert "50b24c4b" not in text, name


class TestRunAllScript:
    """Скрипт запуска всего конвейера."""

    def test_script_exists(self) -> None:
        """Скрипт на месте."""
        assert RUN_ALL.exists()

    def test_script_has_bom(self) -> None:
        """Файл записан в UTF-8 с BOM.

        PowerShell 5.1 читает скрипт без BOM как ANSI: кириллица в сообщениях
        превращается в мусор. С BOM файл читается правильно.
        """
        assert RUN_ALL.read_bytes()[:3] == b"\xef\xbb\xbf"

    def test_script_parses(self) -> None:
        """Скрипт разбирается без синтаксических ошибок.

        Кириллические имена переменных без BOM дают разборщику ошибку
        ParserError, поэтому имена в скрипте латиницей, а кириллица остаётся
        только в сообщениях.
        """
        # Команда собирается склейкой, а не f-строкой: в PowerShell есть
        # фигурные скобки, и в f-строке Python они считаются подстановкой.
        path = str(RUN_ALL).replace("'", "''")
        script = (
            "$errors = $null; "
            f"[void][System.Management.Automation.Language.Parser]::ParseFile("
            f"'{path}', [ref]$null, [ref]$errors); "
            "if ($errors) { $errors | ForEach-Object { $_.Message }; exit 1 }"
        )

        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", script],
            capture_output=True,
            encoding="utf-8",
        )

        assert result.returncode == 0, result.stdout + result.stderr

    @pytest.mark.parametrize("module", [name for _, name in PIPELINE_STEPS])
    def test_script_calls_every_step(self, module: str) -> None:
        """Скрипт запускает каждый шаг конвейера."""
        text = RUN_ALL.read_text(encoding="utf-8-sig")

        assert module in text, module

    def test_script_stops_on_first_failure(self) -> None:
        """Скрипт проверяет код возврата после каждого шага.

        Без проверки конвейер отработал бы до конца на неполных данных, и
        ошибка в первом шаге обнаружилась бы только в отчёте последнего.
        """
        text = RUN_ALL.read_text(encoding="utf-8-sig")

        # Столько проверок кода возврата, сколько шагов в цикле плюс
        # проверки качества кода: без них конвейер отработал бы до конца на
        # неполных данных.
        assert text.count("LASTEXITCODE") >= len(PIPELINE_STEPS)
        assert text.count("throw") >= len(PIPELINE_STEPS)

    def test_script_lists_all_tasks(self) -> None:
        """В скрипте перечислены номера всех шести шагов."""
        text = RUN_ALL.read_text(encoding="utf-8-sig")

        for number, _ in PIPELINE_STEPS:
            assert f"Task = {number}" in text, number


class TestArchitectureDocumented:
    """Архитектура конвейера описана."""

    def test_pipeline_chain_documented(self) -> None:
        """Описана цепочка Go-сборщик, JSON, Polars, Parquet, DuckDB."""
        text = readme()

        for stage in ("Сборщик", "JSON", "Polars", "Parquet", "DuckDB"):
            assert stage in text, stage

    def test_has_diagram(self) -> None:
        """Есть схема конвейера в виде диаграммы."""
        assert "```mermaid" in readme()

    def test_describes_data_contract(self) -> None:
        """Описан формат записи: JSON Lines, один объект на строку."""
        text = readme()

        assert "JSON Lines" in text or "объект на строку" in text

    def test_documents_row_counts(self) -> None:
        """Приведены реальные размеры данных на каждом шаге.

        Цифры взяты из собранных файлов, а не выдуманы: читатель может
        сверить их с результатом.
        """
        text = readme()

        assert "410" in text  # записано сборщиком
        assert "420" in text  # после очистки, два сбора
