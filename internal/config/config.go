// Package config - настройки сборщика из переменных окружения и флагов.
//
// Секрет в конфигурацию не попадает: ключ API читается отдельно в
// переменную OWM_API_KEY, и в логи он не выводится.
package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config - параметры сбора данных.
type Config struct {
	// Cities - города для сбора.
	Cities []string
	// Output - путь к JSON-файлу с записями.
	Output string
	// Concurrency - сколько городов обрабатывать одновременно.
	Concurrency int
	// Timeout - предельное время одного HTTP-запроса.
	Timeout time.Duration
	// APIKey - ключ OpenWeatherMap.
	APIKey string
	// LogFile - путь к файлу лога; пустая строка означает stderr.
	LogFile string
	// EnvAPIKey - имя переменной с ключом, чтобы не зашивать имя ключа
	// в нескольких местах.
	EnvAPIKey string
	// Batch - параметры буферизации и пакетной записи.
	Batch Batch
	// Grace - сколько ждать дорабатывания текущих городов после сигнала
	// остановки, прежде чем прервать запросы.
	Grace time.Duration
}

// Parse разбирает флаги и переменные окружения.
//
// Ключ API обязателен: без него сборщик не может обратиться к источнику,
// и падать лучше сразу, до создания файла.
func Parse(args []string) (*Config, error) {
	flagSet := flag.NewFlagSet("collector", flag.ContinueOnError)

	cities := flagSet.String("cities", strings.Join(DefaultCities(), ","),
		"города для сбора через запятую")
	output := flagSet.String("out", "data/raw/weather.jsonl",
		"путь к JSON-файлу с записями")
	concurrency := flagSet.Int("concurrency", 5,
		"сколько городов обрабатывать одновременно")
	timeout := flagSet.Duration("timeout", 20*time.Second,
		"предельное время одного HTTP-запроса")
	logFile := flagSet.String("log", "", "путь к файлу лога")
	envKey := flagSet.String("env-key", "OWM_API_KEY",
		"имя переменной окружения с ключом API")
	batchSize := flagSet.Int("batch-size", DefaultBatchSize,
		"сколько записей накапливать перед записью; 1 отключает пакетную запись")
	flushInterval := flagSet.Duration("flush-interval", DefaultFlushInterval,
		"запись по таймеру, даже если пачка не набралась; 0 отключает")
	channelBuffer := flagSet.Int("channel-buffer", DefaultChannelBuffer,
		"ёмкость канала между сбором и записью")
	grace := flagSet.Duration("grace", DefaultGrace,
		"сколько ждать дорабатывания текущих городов после сигнала остановки")

	if err := flagSet.Parse(args); err != nil {
		return nil, err
	}

	list := splitCities(*cities)
	if len(list) == 0 {
		return nil, errors.New("не указан ни один город")
	}

	if *concurrency < 1 {
		return nil, fmt.Errorf("concurrency должен быть не меньше 1, получено %d", *concurrency)
	}

	if *timeout <= 0 {
		return nil, errors.New("timeout должен быть больше нуля")
	}

	if *grace <= 0 {
		return nil, errors.New("grace должен быть больше нуля")
	}

	apiKey := strings.TrimSpace(os.Getenv(*envKey))
	if apiKey == "" {
		return nil, fmt.Errorf("не задана переменная окружения %s с ключом API", *envKey)
	}

	batch := Batch{
		BatchSize:     *batchSize,
		FlushInterval: *flushInterval,
		ChannelBuffer: *channelBuffer,
	}

	if err := batch.Validate(); err != nil {
		return nil, err
	}

	return &Config{
		Cities:      list,
		Output:      *output,
		Concurrency: *concurrency,
		Timeout:     *timeout,
		APIKey:      apiKey,
		LogFile:     *logFile,
		EnvAPIKey:   *envKey,
		Batch:       batch,
		Grace:       *grace,
	}, nil
}

// splitCities разбирает список городов и убирает пустые элементы.
func splitCities(raw string) []string {
	fields := strings.Split(raw, ",")

	cities := make([]string, 0, len(fields))
	for _, field := range fields {
		city := strings.TrimSpace(field)
		if city != "" {
			cities = append(cities, city)
		}
	}

	return cities
}

// DefaultCities - города варианта 2: десять крупных городов России.
//
// Города разнесены по часовым поясам и климату, иначе анализ средних
// температур сравнивал бы города в одном поясе с городами на другом краю
// страны и разброс объяснялся бы временем, а не погодой.
func DefaultCities() []string {
	return []string{
		"Москва",
		"Санкт-Петербург",
		"Новосибирск",
		"Екатеринбург",
		"Казань",
		"Нижний Новгород",
		"Краснодар",
		"Владивосток",
		"Сочи",
		"Якутск",
	}
}

// RedactedKey возвращает ключ в виде, безопасном для логов.
//
// Сам ключ в логах не нужен, а его отсутствие среди записанных значений
// видно сразу: ключ не утёк, и его можно найти в файле.
func RedactedKey(key string) string {
	if key == "" {
		return "(не задан)"
	}

	if len(key) <= 8 {
		return strings.Repeat("*", len(key))
	}

	return key[:4] + "..." + key[len(key)-4:]
}

// LookupsToString превращает список городов в строку для логов.
func LookupsToString(cities []string) string {
	return strings.Join(cities, ", ")
}

// ParsePositiveInt разбирает положительное целое из строки.
//
// Нужен для проверки чисел из логов сборщика в тестах и в утилитах.
func ParsePositiveInt(raw string) (int, error) {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("ожидалось целое число, получено %q", raw)
	}

	if value <= 0 {
		return 0, fmt.Errorf("ожидалось положительное число, получено %d", value)
	}

	return value, nil
}
