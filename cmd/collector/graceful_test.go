package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/frfn0/LAB14DataPipeline/internal/owm"
	"github.com/frfn0/LAB14DataPipeline/internal/writer"
)

// TestGracefulStopKeepsCollectedData проверяет весь путь остановки: сбор
// городов, пакетная запись и закрытие канала.
//
// Сигнал подставляется напрямую: на Windows другому процессу нельзя
// отправить Ctrl+C из скрипта, а жёсткое завершение процесса не даёт коду
// отработать вовсе. Обработка самого сигнала проверяется отдельно в
// TestWatchSignals.
func TestGracefulStopKeepsCollectedData(t *testing.T) {
	t.Parallel()

	source := newFakeSource(100 * time.Millisecond)

	cities := []string{"Москва", "Казань", "Сочи", "Якутск", "Владивосток"}

	stop := make(chan struct{})
	force := make(chan struct{})

	results := make(chan owm.WeatherRecord, 256)

	var file bytes.Buffer
	sink, err := writer.New(&file, writer.Options{
		BatchSize:     50,
		FlushInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("писатель не создан: %v", err)
	}

	done := make(chan struct{})
	go func() {
		sink.Run(results, force)
		close(done)
	}()

	// Сигнал приходит, пока первый город ещё в работе.
	go func() {
		time.Sleep(30 * time.Millisecond)
		close(stop)
	}()

	outcome := collect(
		context.Background(), stop, force, source, cities, 1, results, discardLogger(),
	)

	// Обычное завершение: канал записей закрывается, писатель дописывает
	// остаток и выходит.
	close(results)
	<-done

	if !outcome.Interrupted {
		t.Error("отчёт не отметил остановку")
	}
	if len(outcome.Completed) == 0 {
		t.Fatal("ни один город не успел собраться")
	}
	if len(outcome.Skipped) == 0 {
		t.Error("ожидались пропущенные города")
	}

	// В файле должны оказаться записи всех собранных городов.
	written := parseLines(t, file.String())
	if len(written) != outcome.Records {
		t.Errorf("в файле %d записей, а собрано %d", len(written), outcome.Records)
	}

	seen := map[string]bool{}
	for _, record := range written {
		seen[record.City] = true
	}

	for _, city := range outcome.Completed {
		if !seen[city] {
			t.Errorf("город %s собран, но его записей в файле нет", city)
		}
	}

	// Буфер дописан: последняя строка файла должна завершаться переводом
	// строки, иначе последняя запись осталась бы обрезанной.
	text := file.String()
	if !strings.HasSuffix(text, "\n") {
		t.Error("файл не заканчивается переводом строки: буфер дописан не полностью")
	}

	stats := sink.Stats()
	if stats.Records != int64(outcome.Records) {
		t.Errorf("счётчик писателя: %d, ожидалось %d", stats.Records, outcome.Records)
	}
	if stats.WriteOps == 0 {
		t.Error("записей в файл не попало ни одной операцией")
	}
}

// TestGracefulStopWritesPartialBatchWithoutSignal проверяет, что при
// остановке дописывается неполная пачка: накопленные записи не ждут
// момента, когда пачка наберётся.
func TestGracefulStopWritesPartialBatchWithoutSignal(t *testing.T) {
	t.Parallel()

	source := newFakeSource(0)
	stop := make(chan struct{})
	force := make(chan struct{})

	results := make(chan owm.WeatherRecord, 256)

	var file bytes.Buffer
	sink, err := writer.New(&file, writer.Options{
		// Пачка заведомо не наберётся: 3 города по 2 записи дают 6 записей.
		BatchSize:     100,
		FlushInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("писатель не создан: %v", err)
	}

	done := make(chan struct{})
	go func() {
		sink.Run(results, force)
		close(done)
	}()

	outcome := collect(context.Background(), stop, force, source,
		[]string{"Москва", "Казань", "Сочи"}, 1, results, discardLogger())

	close(results)
	<-done

	if outcome.Records != 6 {
		t.Fatalf("собрано записей: %d, ожидалось 6", outcome.Records)
	}

	written := parseLines(t, file.String())
	if len(written) != 6 {
		t.Errorf("в файле %d записей, ожидалось 6: неполная пачка не дописана", len(written))
	}

	stats := sink.Stats()
	if stats.ByClose != 1 {
		t.Errorf("пачек по завершению: %d, ожидалась 1", stats.ByClose)
	}
	if stats.BySize != 0 {
		t.Errorf("пачек по размеру быть не должно, получено %d", stats.BySize)
	}
}

// TestForceStopDropsRemainingRecords проверяет аварийную остановку: когда
// force закрыт, писатель дописывает то, что лежит, и выходит, а
// отправка оставшихся записей прерывается без зависания.
func TestForceStopDropsRemainingRecords(t *testing.T) {
	t.Parallel()

	force := make(chan struct{})

	results := make(chan owm.WeatherRecord, 256)

	var file bytes.Buffer
	sink, err := writer.New(&file, writer.Options{BatchSize: 100, FlushInterval: 0})
	if err != nil {
		t.Fatalf("писатель не создан: %v", err)
	}

	done := make(chan struct{})
	go func() {
		sink.Run(results, force)
		close(done)
	}()

	results <- owm.WeatherRecord{City: "Москва", TempC: 1}
	close(force)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("аварийная остановка не завершила писатель")
	}

	written := parseLines(t, file.String())
	if len(written) != 1 {
		t.Errorf("аварийная остановка должна дописать накопленное: %d записей", len(written))
	}
}

// parseLines разбирает файл с записями.
func parseLines(t *testing.T, text string) []owm.WeatherRecord {
	t.Helper()

	if text == "" {
		return nil
	}

	records := make([]owm.WeatherRecord, 0)
	scanner := bufio.NewScanner(strings.NewReader(text))

	for scanner.Scan() {
		var record owm.WeatherRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("строка не разобрана: %v", err)
		}
		records = append(records, record)
	}

	return records
}

// TestStopDoesNotLoseRecordsInFastRun проверяет, что быстрый сбор без
// сигналов не теряет ни одной записи.
func TestStopDoesNotLoseRecordsInFastRun(t *testing.T) {
	t.Parallel()

	source := newFakeSource(0)
	stop := make(chan struct{})
	force := make(chan struct{})

	results := make(chan owm.WeatherRecord, 256)

	var file bytes.Buffer
	sink, _ := writer.New(&file, writer.Options{BatchSize: 7, FlushInterval: time.Hour})

	done := make(chan struct{})
	go func() {
		sink.Run(results, force)
		close(done)
	}()

	outcome := collect(context.Background(), stop, force, source,
		[]string{"А", "Б", "В", "Г", "Д"}, 5, results, discardLogger())

	close(results)
	<-done

	if outcome.Interrupted {
		t.Error("остановки не было")
	}
	if len(outcome.Completed) != 5 {
		t.Errorf("городов собрано: %d, ожидалось 5", len(outcome.Completed))
	}
	if got := len(parseLines(t, file.String())); got != 10 {
		t.Errorf("записей в файле: %d, ожидалось 10", got)
	}
}
