<#
.SYNOPSIS
    Запуск всего конвейера по шагам.

.DESCRIPTION
    Скрипт выполняет задания 4-9 подряд и останавливается на первом шаге, где
    вышел ненулевой код возврата: после очистки с ошибкой запускать Parquet и
    графики бессмысленно, и доводить конвейер до конца на неполных данных
    опаснее, чем остановиться и показать, где сломалось.

    Сбор данных (задания 1-3) не запускается: ему нужен ключ API, а шаги 4-9
    работают с уже собранным файлом. Сбор запускается отдельно, командой,
    указанной в README.

    Файл записан в UTF-8 с BOM. PowerShell 5.1 читает скрипт без BOM как ANSI,
    кириллица в сообщениях превращается в мусор.

    Имена переменных и ключи хешей латиницей: без BOM разборщик PowerShell
    не понимает кириллические имена и падает на первом же хеше.

.PARAMETER SkipTests
    Пропустить проверки качества кода перед запуском шагов.
#>

[CmdletBinding()]
param(
    [switch]$SkipTests
)

$ErrorActionPreference = 'Stop'

# Каталог проекта - на уровень выше каталога со скриптом.
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

# Кодировка вывода задаётся явно: PowerShell 5.1 печатает кириллицу в OEM,
# и сообщения об ошибках приходят нечитаемыми.
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONIOENCODING = 'utf-8'
$env:PYTHONUTF8 = '1'

$steps = @(
    @{ Task = 4; Title = 'Импорт данных в Polars'; Args = @('-m', 'pipeline.import_data') }
    @{ Task = 5; Title = 'Очистка и валидация'; Args = @('-m', 'pipeline.clean_data') }
    @{ Task = 6; Title = 'Агрегационный анализ'; Args = @('-m', 'pipeline.aggregate') }
    @{ Task = 7; Title = 'Сохранение в Parquet'; Args = @('-m', 'pipeline.to_parquet') }
    @{ Task = 8; Title = 'Анализ через DuckDB'; Args = @('-m', 'pipeline.duckdb_query') }
    @{ Task = 9; Title = 'Визуализация'; Args = @('-m', 'pipeline.charts') }
)

function Write-Step {
    <#
    .SYNOPSIS
        Печатает заголовок шага.

    .PARAMETER Task
        Номер задания.

    .PARAMETER Title
        Название шага.
    #>
    param(
        [int]$Task,
        [string]$Title
    )

    $line = '=' * 74
    Write-Output ''
    Write-Output $line
    Write-Output ("Шаг {0}. {1}" -f $Task, $Title)
    Write-Output $line
}

if (-not $SkipTests) {
    Write-Step 0 'Проверки качества кода'

    Write-Output '  go vet'
    & go vet ./...
    if ($LASTEXITCODE -ne 0) { throw 'go vet завершился с ошибкой' }

    Write-Output '  go test'
    & go test ./...
    if ($LASTEXITCODE -ne 0) { throw 'go test завершился с ошибкой' }

    Write-Output '  gofmt'
    $unformatted = & gofmt -l .
    if ($unformatted) {
        throw ("НЕ отформатированы файлы: {0}" -f ($unformatted -join ', '))
    }
    Write-Output '  все файлы отформатированы'

    Write-Output '  ruff check'
    & python -m ruff check pipeline tests
    if ($LASTEXITCODE -ne 0) { throw 'ruff check завершился с ошибкой' }

    Write-Output '  pytest'
    & python -m pytest
    if ($LASTEXITCODE -ne 0) { throw 'pytest завершился с ошибкой' }
}

$started = Get-Date

foreach ($step in $steps) {
    Write-Step $step.Task $step.Title
    & python @($step.Args)
    if ($LASTEXITCODE -ne 0) {
        throw ("Шаг {0} завершился с кодом {1}. Следующие шаги не запущены." -f $step.Task, $LASTEXITCODE)
    }
}

$elapsed = (Get-Date) - $started

Write-Output ''
Write-Output ('=' * 74)
Write-Output 'Конвейер отработал полностью'
Write-Output ('=' * 74)
Write-Output ("  времени: {0:N1} с" -f $elapsed.TotalSeconds)
Write-Output ''
Write-Output '  результаты:'
Write-Output '    data\interim\weather_clean.json   очищенные данные'
Write-Output '    reports\weather.parquet          данные для DuckDB'
Write-Output '    reports\charts\                  графики, PNG и HTML'
Write-Output ''