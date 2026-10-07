package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/frfn0/LAB14DataPipeline/internal/owm"
)

// WeatherSource - источник записей для сбора.
//
// Отдельный интерфейс нужен, чтобы проверять остановку без обращений к
// сети: в тестах подставляется источник-заглушка.
type WeatherSource interface {
	CollectOne(ctx context.Context, city string) ([]owm.WeatherRecord, error)
}

// CollectOutcome - что удалось собрать, а что нет.
type CollectOutcome struct {
	// Completed - города, собранные полностью.
	Completed []string
	// Failed - города, где источник ответил ошибкой или ничего не вернул.
	Failed []string
	// Skipped - города, не начатые из-за остановки.
	Skipped []string
	// Records - сколько записей отправлено в канал записи.
	Records int
	// Interrupted - остановка была запрошена.
	Interrupted bool
}

// cityState - состояние одного города во время сбора.
type cityState int

const (
	cityCompleted cityState = iota
	cityFailed
	citySkipped
)

// collect обходит города с ограничением параллелизма.
//
// Args:
//
//	ctx: контекст запросов. Отмена прерывает уже начатые запросы.
//	stop: закрывается при получении сигнала остановки. Города, которые
//	    ещё не начались, не запускаются, а начатые дорабатывают: это и
//	    есть дообработка текущих данных.
//	source: источник записей.
//	cities: города для сбора.
//	concurrency: сколько городов одновременно.
//	results: канал для собранных записей.
//	logger: логгер.
//
// Returns:
//
//	Что удалось собрать, а что нет.
func collect(
	ctx context.Context,
	stop <-chan struct{},
	force <-chan struct{},
	source WeatherSource,
	cities []string,
	concurrency int,
	results chan<- owm.WeatherRecord,
	logger *slog.Logger,
) CollectOutcome {
	if concurrency < 1 {
		concurrency = 1
	}

	states := make(map[string]cityState, len(cities))
	var mu sync.Mutex
	var records int

	setState := func(city string, state cityState) {
		mu.Lock()
		defer mu.Unlock()

		// Город мог быть отмечен двумя ветками: например, источник
		// вернул ошибку, а потом проверка на пустой результат.
		// Побеждает более позднее состояние.
		states[city] = state
	}

	addRecords := func(count int) {
		mu.Lock()
		defer mu.Unlock()

		records += count
	}

	var workers sync.WaitGroup

	semaphore := make(chan struct{}, concurrency)

	for _, city := range cities {
		select {
		case <-stop:
			// Остановка пришла до старта города: он не запускается вовсе.
			setState(city, citySkipped)
			continue
		default:
		}

		workers.Add(1)

		go func(name string) {
			defer workers.Done()

			// Ожидание места в группе тоже прерывается по сигналу: иначе
			// город, который ещё не начал собираться, всё равно стартовал
			// бы после остановки.
			select {
			case semaphore <- struct{}{}:
			case <-stop:
				setState(name, citySkipped)
				return
			}
			defer func() { <-semaphore }()

			fetched, err := source.CollectOne(ctx, name)
			if err != nil {
				logger.Warn("город собран частично", "city", name, "error", err)
			}

			if len(fetched) == 0 {
				setState(name, cityFailed)
				logger.Error("город не собран", "city", name)
				return
			}

			for _, record := range fetched {
				select {
				case results <- record:
				case <-force:
					// Обычная остановка записи не мешает: канал жив и
					// разберёт всё сам. Прерывает отправку только
					// аварийная остановка.
					setState(name, cityFailed)
					return
				}
			}

			addRecords(len(fetched))
			setState(name, cityCompleted)

			logger.Info("город собран", "city", name, "records", len(fetched))
		}(city)
	}

	workers.Wait()

	outcome := CollectOutcome{
		Interrupted: isClosed(stop),
		Records:     records,
	}

	for city, state := range states {
		switch state {
		case cityCompleted:
			outcome.Completed = append(outcome.Completed, city)
		case cityFailed:
			outcome.Failed = append(outcome.Failed, city)
		case citySkipped:
			outcome.Skipped = append(outcome.Skipped, city)
		}
	}

	return outcome
}

// isClosed проверяет, что канал уже закрыт.
func isClosed(channel <-chan struct{}) bool {
	select {
	case <-channel:
		return true
	default:
		return false
	}
}

// watchSignals следит за сигналами остановки.
//
// Первый сигнал закрывает stop: сборщик перестаёт начинать новые города и
// дописывает то, что уже собрано. Если города не доработают за grace,
// закрывается force и прерываются запросы и отправка. Второй сигнал делает
// то же немедленно.
//
// На Windows от операционной системы приходит только Ctrl+C, то есть
// os.Interrupt: SIGTERM там не поддерживается, но подписка на него нужна,
// чтобы тот же код работал в Linux без правок.
func watchSignals(
	signals <-chan os.Signal,
	stop chan<- struct{},
	force chan<- struct{},
	cancel context.CancelFunc,
	grace time.Duration,
	logger *slog.Logger,
) {
	first := <-signals
	if first == nil {
		return
	}

	logger.Warn("получен сигнал остановки, дорабатываю текущие данные",
		"signal", first.String(), "grace", grace.String())
	close(stop)

	timer := time.NewTimer(grace)
	defer timer.Stop()

	select {
	case second := <-signals:
		logger.Warn("повторный сигнал: прерываю немедленно", "signal", second.String())
		close(force)
		cancel()
	case <-timer.C:
		logger.Warn("время ожидания истекло, прерываю запросы",
			"grace", grace.String())
		close(force)
		cancel()
	}
}

// signalChannel создаёт канал сигналов с SIGINT и SIGTERM и функцию
// отмены подписки.
//
// Отмена вынесена наружу: иначе подписка осталась бы активной после выхода
// из программы.
func signalChannel() (chan os.Signal, func()) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	return signals, func() { signal.Stop(signals) }
}
