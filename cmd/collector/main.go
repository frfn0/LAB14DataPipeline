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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/frfn0/LAB14DataPipeline/internal/config"
	"github.com/frfn0/LAB14DataPipeline/internal/owm"
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

	// results - канал готовых записей. Буфер небольшой: задача 2 добавит
	// управляемый буфер и пакетную запись.
	results := make(chan owm.WeatherRecord, 16)

	var workers sync.WaitGroup
	var writeGroup sync.WaitGroup

	// writer читает записи из канала и пишет их в файл. Отдельная
	// горутина нужна для того, чтобы запись шла параллельно со сбором.
	writer := bufio.NewWriterSize(file, 64*1024)
	writeGroup.Add(1)
	go func() {
		defer writeGroup.Done()
		writeRecords(results, writer, logger)
	}()

	// Сбор ограничен по числу городов одновременно.
	semaphore := make(chan struct{}, cfg.Concurrency)

	for _, city := range cfg.Cities {
		workers.Add(1)

		go func(name string) {
			defer workers.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			collectCity(context.Background(), client, name, results, logger)
		}(city)
	}

	workers.Wait()
	close(results)
	writeGroup.Wait()

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("буфер записи не сброшен: %w", err)
	}

	logger.Info("сбор завершён",
		"cities", len(cfg.Cities),
		"elapsed_ms", time.Since(started).Milliseconds(),
		"output", cfg.Output,
	)

	return nil
}

// collectCity собирает оба эндпоинта по одному городу и отправляет записи
// в канал.
//
// Ошибка одного города не останавливает сбор остальных: иначе одна
// опечатка в названии города обнуляла бы весь прогон.
func collectCity(
	ctx context.Context,
	client *owm.Client,
	city string,
	results chan<- owm.WeatherRecord,
	logger *slog.Logger,
) {
	cityStarted := time.Now()

	records, err := client.CollectOne(ctx, city)
	if err != nil {
		// Частичный результат сохраняется: текущая погода уже получена, и
		// выбрасывать её из-за недоступного прогноза незачем.
		logger.Warn("город собран частично", "city", city, "error", err)
	}

	if len(records) == 0 {
		logger.Error("город не собран", "city", city)
		return
	}

	for _, record := range records {
		results <- record
	}

	logger.Info("город собран",
		"city", city,
		"records", len(records),
		"elapsed_ms", time.Since(cityStarted).Milliseconds(),
	)
}

// writeRecords пишет записи в буфер по одной на строку.
func writeRecords(
	results <-chan owm.WeatherRecord,
	writer *bufio.Writer,
	logger *slog.Logger,
) {
	encoder := json.NewEncoder(writer)
	written := 0

	for record := range results {
		if err := encoder.Encode(record); err != nil {
			logger.Error("запись не записана", "city", record.City, "error", err)
			continue
		}
		written++
	}

	logger.Info("записей записано", "count", written)
}
