// Сборщик данных OpenWeatherMap.
//
// Задание 1: параллельный сбор данных из HTTP-источника с помощью
// горутин. Каждый город обрабатывается отдельной горутиной, готовые
// записи передаются в канал, пишущая горутина складывает их в
// JSON-файл, по одному объекту на строку.
//
// Пакетная запись и graceful shutdown добавляются в заданиях 2 и 3, здесь
// запись идёт сразу по мере поступления записи.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/frfn0/LAB14DataPipeline/internal/config"
	"github.com/frfn0/LAB14DataPipeline/internal/owm"
	"github.com/frfn0/LAB14DataPipeline/internal/writer"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ошибка:", err)
		os.Exit(1)
	}
}

// run собирает данные и пишет их в файл.
func run() error {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		return err
	}

	logger, closeLog, err := newLogger(cfg.LogFile)
	if err != nil {
		return err
	}
	defer closeLog()

	logger.Info("сборщик запущен",
		"cities", len(cfg.Cities),
		"concurrency", cfg.Concurrency,
		"output", cfg.Output,
		"api_key", config.RedactedKey(cfg.APIKey),
		"batch_size", cfg.Batch.BatchSize,
		"flush_interval", cfg.Batch.FlushInterval.String(),
		"channel_buffer", cfg.Batch.ChannelBuffer,
	)

	client, err := owm.New(cfg.APIKey, owm.WithHTTPClient(httpClient(cfg.Timeout)))
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(cfg.Output), 0o755); err != nil {
		return fmt.Errorf("каталог для данных не создан: %w", err)
	}

	file, err := os.Create(cfg.Output)
	if err != nil {
		return fmt.Errorf("файл %s не создан: %w", cfg.Output, err)
	}
	defer file.Close()

	started := time.Now()

	// results - канал готовых записей. Ёмкость задаётся параметром: буфер
	// позволяет сбору идти быстрее записи и не блокироваться на файле.
	results := make(chan owm.WeatherRecord, cfg.Batch.ChannelBuffer)

	// sink накапливает записи и пишет их пачками: одна операция записи на
	// пачку вместо одной на запись.
	sink, err := writer.New(file, writer.Options{
		BatchSize:     cfg.Batch.BatchSize,
		FlushInterval: cfg.Batch.FlushInterval,
	})
	if err != nil {
		return fmt.Errorf("писатель не создан: %w", err)
	}

	var writeGroup sync.WaitGroup

	// stop закрывается по сигналу остановки. Его получают и писатель, и
	// сбор городов: писатель дописывает накопленное, сборщик перестаёт
	// начинать новые города.
	// stop - обычная остановка: новые города не запускаются.
	stop := make(chan struct{})
	// force - аварийная остановка: прерываются и запросы, и запись.
	force := make(chan struct{})

	writeGroup.Add(1)
	go func() {
		defer writeGroup.Done()
		sink.Run(results, force)
	}()

	// Остановка по сигналу: первый сигнал перестаёт запускать новые
	// города и даёт дописать накопленное, по истечении времени ожидания
	// запросы прерываются.
	signals, stopSignals := signalChannel()
	defer stopSignals()

	// requestCtx отменяется только когда ждать больше нечего: до этого
	// момента текущие запросы дорабатывают.
	requestCtx, cancelRequests := context.WithCancel(context.Background())
	defer cancelRequests()

	watcherDone := make(chan struct{})

	go func() {
		defer close(watcherDone)
		watchSignals(signals, stop, force, cancelRequests, cfg.Grace, logger)
	}()

	outcome := collect(
		requestCtx, stop, force, client, cfg.Cities, cfg.Concurrency, results, logger,
	)

	// Сигнал мог не прийти: ждать наблюдателя незачем.
	close(stop)
	<-watcherDone

	close(results)
	writeGroup.Wait()

	logger.Info("сбор остановлен",
		"interrupted", outcome.Interrupted,
		"completed", len(outcome.Completed),
		"failed", len(outcome.Failed),
		"skipped", len(outcome.Skipped),
		"records", outcome.Records,
	)

	// Промежуточного буфера нет, поэтому дописывать нечего: проверяется
	// только, не было ли ошибки при записи пачек.
	if err := sink.Err(); err != nil {
		return fmt.Errorf("%w: %w", writer.ErrWrite, err)
	}

	stats := sink.Stats()

	logger.Info("запись завершена",
		"records", stats.Records,
		"write_ops", stats.WriteOps,
		"bytes", stats.Bytes,
		"avg_batch", fmt.Sprintf("%.1f", stats.AvgBatchSize()),
		"max_batch", stats.MaxBatch,
		"flush_by_size", stats.BySize,
		"flush_by_time", stats.ByTime,
		"flush_on_close", stats.ByClose,
	)

	logger.Info("сбор завершён",
		"cities", len(cfg.Cities),
		"elapsed_ms", time.Since(started).Milliseconds(),
		"output", cfg.Output,
		"grace", cfg.Grace.String(),
	)

	return nil
}
