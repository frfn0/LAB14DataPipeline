package writer

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frfn0/LAB14DataPipeline/internal/owm"
)

// countingSink считает операции записи и сохраняет накопленное.
type countingSink struct {
	mu       sync.Mutex
	writes   int
	contents []byte
}

func (s *countingSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.writes++
	s.contents = append(s.contents, p...)

	return len(p), nil
}

func (s *countingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.writes
}

func (s *countingSink) lines() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	text := strings.TrimRight(string(s.contents), "\n")
	if text == "" {
		return nil
	}

	return strings.Split(text, "\n")
}

// makeRecords собирает записей заданного количества.
func makeRecords(count int) []owm.WeatherRecord {
	records := make([]owm.WeatherRecord, 0, count)

	for index := range count {
		records = append(records, owm.WeatherRecord{
			City:     "Москва",
			Endpoint: owm.EndpointForecast,
			TempC:    float64(index),
		})
	}

	return records
}

// feed отправляет записи в канал и закрывает его.
func feed(records []owm.WeatherRecord) chan owm.WeatherRecord {
	channel := make(chan owm.WeatherRecord, len(records))

	for _, record := range records {
		channel <- record
	}
	close(channel)

	return channel
}

func TestValidateRejectsBadOptions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opts Options
	}{
		{"нулевой размер пачки", Options{BatchSize: 0, FlushInterval: time.Second}},
		{"отрицательный интервал", Options{BatchSize: 10, FlushInterval: -time.Second}},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if _, err := New(&countingSink{}, testCase.opts); err == nil {
				t.Error("неверные параметры приняты")
			}
		})
	}
}

func TestFlushesWhenBatchSizeReached(t *testing.T) {
	t.Parallel()

	sink := &countingSink{}
	collector, err := New(sink, Options{BatchSize: 50, FlushInterval: 0})
	if err != nil {
		t.Fatalf("писатель не создан: %v", err)
	}

	// 410 записей при пачке 50 - это восемь полных пачек и остаток 10.
	collector.Run(feed(makeRecords(410)), nil)

	stats := collector.Stats()
	if stats.Records != 410 {
		t.Errorf("принято записей: %d, ожидалось 410", stats.Records)
	}
	// Девять операций записи: восемь по размеру и одна по закрытию канала.
	if stats.WriteOps != 9 {
		t.Errorf("операций записи: %d, ожидалось 9 (8 по размеру и 1 остаток)", stats.WriteOps)
	}
	if stats.BySize != 8 {
		t.Errorf("пачек по размеру: %d, ожидалось 8", stats.BySize)
	}
	if stats.ByClose != 1 {
		t.Errorf("пачек по закрытию: %d, ожидалось 1", stats.ByClose)
	}
	if stats.MaxBatch != 50 {
		t.Errorf("наибольшая пачка: %d, ожидалось 50", stats.MaxBatch)
	}
	if len(sink.lines()) != 410 {
		t.Errorf("записано строк: %d, ожидалось 410", len(sink.lines()))
	}
}

func TestSingleRecordModeWritesEveryRecordSeparately(t *testing.T) {
	t.Parallel()

	sink := &countingSink{}
	collector, err := New(sink, Options{BatchSize: 1, FlushInterval: 0})
	if err != nil {
		t.Fatalf("писатель не создан: %v", err)
	}

	collector.Run(feed(makeRecords(100)), nil)

	// Размер пачки 1 - это режим без буферизации: одна операция на запись.
	if got := collector.Stats().WriteOps; got != 100 {
		t.Errorf("операций записи: %d, ожидалось 100", got)
	}
	if len(sink.lines()) != 100 {
		t.Errorf("записано строк: %d, ожидалось 100", len(sink.lines()))
	}
}

func TestBatchReducesWriteOperations(t *testing.T) {
	t.Parallel()

	records := makeRecords(410)

	plain := &countingSink{}
	plainWriter, _ := New(plain, Options{BatchSize: 1, FlushInterval: 0})
	plainWriter.Run(feed(records), nil)

	batched := &countingSink{}
	batchWriter, _ := New(batched, Options{BatchSize: 50, FlushInterval: time.Hour})
	batchWriter.Run(feed(records), nil)

	plainOps := plainWriter.Stats().WriteOps
	batchOps := batchWriter.Stats().WriteOps

	if plainOps != 410 {
		t.Errorf("без буферизации операций записи: %d, ожидалось 410", plainOps)
	}
	if batchOps != 9 {
		t.Errorf("с буферизацией операций записи: %d, ожидалось 9", batchOps)
	}

	// Данные в обоих случаях одинаковые: буферизация не должна менять
	// содержимое файла.
	if string(plain.contents) != string(batched.contents) {
		t.Error("содержимое файла отличается от варианта без буферизации")
	}
	if len(batched.lines()) != len(plain.lines()) {
		t.Errorf("число строк разошлось: %d и %d",
			len(batched.lines()), len(plain.lines()))
	}
}

func TestFlushesByTimerWhenBatchNotFull(t *testing.T) {
	t.Parallel()

	sink := &countingSink{}
	collector, err := New(sink, Options{BatchSize: 1000, FlushInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("писатель не создан: %v", err)
	}

	records := make(chan owm.WeatherRecord)

	done := make(chan struct{})
	go func() {
		collector.Run(records, nil)
		close(done)
	}()

	// Пачка в 1000 записей не наберётся, поэтому запись должна произойти
	// по таймеру - иначе данные ждали бы конца сбора.
	records <- owm.WeatherRecord{City: "Казань"}
	time.Sleep(150 * time.Millisecond)

	if sink.count() == 0 {
		t.Error("запись по таймеру не произошла")
	}

	close(records)
	<-done

	if got := collector.Stats().ByTime; got == 0 {
		t.Error("счётчик пачек по таймеру остался нулевым")
	}
}

func TestStopFlushesPendingRecords(t *testing.T) {
	t.Parallel()

	sink := &countingSink{}
	collector, err := New(sink, Options{BatchSize: 100, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("писатель не создан: %v", err)
	}

	records := make(chan owm.WeatherRecord, 4)
	stop := make(chan struct{})

	done := make(chan struct{})
	go func() {
		collector.Run(records, stop)
		close(done)
	}()

	records <- owm.WeatherRecord{City: "Казань"}
	records <- owm.WeatherRecord{City: "Казань"}
	stop <- struct{}{}

	<-done

	if len(sink.lines()) != 2 {
		t.Errorf("записано строк: %d, ожидалось 2 - накопленное не должно теряться", len(sink.lines()))
	}
	if got := collector.Stats().ByClose; got != 1 {
		t.Errorf("пачек по завершению: %d, ожидалось 1", got)
	}
}

func TestWritesOneObjectPerLine(t *testing.T) {
	t.Parallel()

	sink := &countingSink{}
	collector, err := New(sink, Options{BatchSize: 2, FlushInterval: 0})
	if err != nil {
		t.Fatalf("писатель не создан: %v", err)
	}

	collector.Run(feed(makeRecords(3)), nil)

	lines := sink.lines()
	if len(lines) != 3 {
		t.Fatalf("строк: %d, ожидалось 3", len(lines))
	}

	for index, line := range lines {
		var record owm.WeatherRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("строка %d не разобрана: %v", index, err)
		}
		if record.TempC != float64(index) {
			t.Errorf("строка %d: получено temp_c=%v, ожидалось %d",
				index, record.TempC, index)
		}
	}
}

func TestAvgBatchSize(t *testing.T) {
	t.Parallel()

	sink := &countingSink{}
	collector, _ := New(sink, Options{BatchSize: 50, FlushInterval: 0})
	collector.Run(feed(makeRecords(410)), nil)

	stats := collector.Stats()
	expected := float64(stats.Records) / float64(stats.WriteOps)

	if stats.AvgBatchSize() != expected {
		t.Errorf("средняя пачка: получено %.2f, ожидалось %.2f",
			stats.AvgBatchSize(), expected)
	}

	empty := Stats{}
	if empty.AvgBatchSize() != 0 {
		t.Errorf("средняя пачка при нуле записи: получено %.2f, ожидался 0", empty.AvgBatchSize())
	}
}

func TestWriteErrorIsReported(t *testing.T) {
	t.Parallel()

	collector, err := New(failingWriter{}, Options{BatchSize: 1})
	if err != nil {
		t.Fatalf("писатель не создан: %v", err)
	}

	// Ошибка записи не должна прерывать сбор: пачки продолжают
	// накапливаться, а ошибка возвращается через Err.
	collector.Run(feed(makeRecords(3)), nil)

	if collector.Err() == nil {
		t.Error("ошибка записи не вернулась из Err")
	}
	if collector.Stats().Records != 3 {
		t.Errorf("принято записей: %d, ожидалось 3", collector.Stats().Records)
	}
}

// failingWriter - приёмник, который всегда падает.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errWriteFailed
}

var errWriteFailed = errWrite("запись невозможна")

// errWrite - ошибка записи собственного типа.
type errWrite string

func (e errWrite) Error() string {
	return string(e)
}
