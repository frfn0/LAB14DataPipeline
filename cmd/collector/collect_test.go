package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/frfn0/LAB14DataPipeline/internal/owm"
)

// fakeSource - источник-заглушка: отдаёт записи и умеет задерживаться.
type fakeSource struct {
	mu sync.Mutex
	// delay - сколько ждать перед ответом по городу.
	delay time.Duration
	// started - в каком порядке начинали собираться города.
	started []string
	// finished - в каком порядке закончили.
	finished []string
	// calls - сколько раз вызвали CollectOne.
	calls atomic.Int64
	// failFor - города, для которых источник отвечает ошибкой.
	failFor map[string]bool
	// emptyFor - города, для которых источник отдаёт пустой результат
	// без ошибки: город как будто собран, а данных нет.
	emptyFor map[string]bool
}

func newFakeSource(delay time.Duration) *fakeSource {
	return &fakeSource{
		delay:    delay,
		failFor:  map[string]bool{},
		emptyFor: map[string]bool{},
	}
}

func (s *fakeSource) CollectOne(ctx context.Context, city string) ([]owm.WeatherRecord, error) {
	s.calls.Add(1)

	s.mu.Lock()
	s.started = append(s.started, city)
	shouldFail := s.failFor[city]
	shouldBeEmpty := s.emptyFor[city]
	s.mu.Unlock()

	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if shouldFail {
		return nil, errSource
	}

	if shouldBeEmpty {
		return nil, nil
	}

	records := make([]owm.WeatherRecord, 0, 2)
	for index := range 2 {
		records = append(records, owm.WeatherRecord{
			City:     city,
			Endpoint: owm.EndpointForecast,
			TempC:    float64(index),
		})
	}

	s.mu.Lock()
	s.finished = append(s.finished, city)
	s.mu.Unlock()

	return records, nil
}

func (s *fakeSource) snapshot() ([]string, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.started...), append([]string(nil), s.finished...)
}

// errSource - ошибка источника-заглушки.
var errSource = errString("источник недоступен")

type errString string

func (e errString) Error() string {
	return string(e)
}

// forceNeverClosed - канал, который никогда не закрывается: в этих
// тестах аварийной остановки не происходит.
func forceNeverClosed() <-chan struct{} {
	return make(chan struct{})
}

// discardLogger возвращает логгер, который ничего не пишет.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discard{}, nil))
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func TestCollectWithoutStopCollectsEverything(t *testing.T) {
	t.Parallel()

	source := newFakeSource(0)
	stop := make(chan struct{})
	results := make(chan owm.WeatherRecord, 16)

	cities := []string{"Москва", "Казань", "Сочи"}

	outcome := collect(context.Background(), stop, forceNeverClosed(), source, cities, 2, results, discardLogger())
	close(results)

	if outcome.Interrupted {
		t.Error("остановки не было, но отчёт говорит об обрыве")
	}
	if len(outcome.Completed) != 3 {
		t.Errorf("городов собрано: %d, ожидалось 3 (%v)", len(outcome.Completed), outcome.Completed)
	}
	if outcome.Records != 6 {
		t.Errorf("записей: %d, ожидалось 6", outcome.Records)
	}

	drained := 0
	for range results {
		drained++
	}
	if drained != 6 {
		t.Errorf("в канале записей: %d, ожидалось 6", drained)
	}
}

func TestCollectSkipsCitiesAfterStop(t *testing.T) {
	t.Parallel()

	// Город отвечает 100 мс, сигнал приходит через 30 мс: часть городов
	// ещё не началась и должна быть пропущена.
	source := newFakeSource(100 * time.Millisecond)
	stop := make(chan struct{})

	cities := []string{"Москва", "Казань", "Сочи", "Якутск", "Владивосток"}

	results := make(chan owm.WeatherRecord, 64)
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(stop)
	}()

	outcome := collect(context.Background(), stop, forceNeverClosed(), source, cities, 1, results, discardLogger())

	if !outcome.Interrupted {
		t.Error("отчёт не отметил остановку")
	}
	if len(outcome.Skipped) == 0 {
		t.Error("пропущенные города не перечислены")
	}

	started, _ := source.snapshot()
	for _, city := range outcome.Skipped {
		for _, begun := range started {
			if city == begun {
				t.Errorf("город %s помечен пропущенным, но успел начаться", city)
			}
		}
	}

	if len(outcome.Skipped) == len(cities) {
		t.Error("не начался ни один город: дорабатывать было нечего")
	}
}

func TestCollectFinishesCurrentCityAfterStop(t *testing.T) {
	t.Parallel()

	// Источник отвечает 200 мс, сигнал приходит через 50 мс: город,
	// который уже начался, обязан доработать.
	source := newFakeSource(200 * time.Millisecond)
	stop := make(chan struct{})

	results := make(chan owm.WeatherRecord, 64)

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(stop)
	}()

	outcome := collect(context.Background(), stop, forceNeverClosed(), source, []string{"Москва"}, 1, results, discardLogger())

	if len(outcome.Completed) != 1 {
		t.Errorf("текущий город должен был доработать: %v", outcome)
	}
	if outcome.Records != 2 {
		t.Errorf("записей текущего города: %d, ожидалось 2", outcome.Records)
	}
}

func TestCollectRespectsConcurrency(t *testing.T) {
	t.Parallel()

	source := newFakeSource(30 * time.Millisecond)
	stop := make(chan struct{})
	results := make(chan owm.WeatherRecord, 256)

	cities := make([]string, 0, 6)
	for _, city := range []string{"Москва", "Казань", "Сочи", "Омск", "Тверь", "Пермь"} {
		cities = append(cities, city)
	}

	// При параллелизме 1 шесть городов по 30 мс идут не меньше 180 мс,
	// при параллелизме 3 - около 60 мс.
	started := time.Now()
	collect(context.Background(), stop, forceNeverClosed(), source, cities, 3, results, discardLogger())
	elapsed := time.Since(started)

	if len(outcome(cities, source)) == 0 {
		t.Fatal("города не собраны")
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("сбор занял %v: параллелизм не работает", elapsed)
	}
}

// outcome - помощник: сколько городов реально собиралось.
func outcome(cities []string, source *fakeSource) []string {
	_, finished := source.snapshot()
	return finished
}

func TestCollectReportsFailedCity(t *testing.T) {
	t.Parallel()

	source := newFakeSource(0)
	source.failFor["Казань"] = true
	stop := make(chan struct{})
	results := make(chan owm.WeatherRecord, 32)

	outcome := collect(context.Background(), stop, forceNeverClosed(), source,
		[]string{"Москва", "Казань"}, 2, results, discardLogger())

	if len(outcome.Failed) != 1 || outcome.Failed[0] != "Казань" {
		t.Errorf("неудачный город не отмечен: %+v", outcome)
	}
	if len(outcome.Completed) != 1 {
		t.Errorf("удачный город не отмечен: %+v", outcome)
	}
}

func TestCollectTreatsEmptyResultAsFailure(t *testing.T) {
	t.Parallel()

	source := newFakeSource(0)
	source.emptyFor["Пустой"] = true
	stop := make(chan struct{})
	results := make(chan owm.WeatherRecord, 32)

	// Источник возвращает пустой результат без ошибки: город как будто
	// собран, а данных нет.
	outcome := collect(context.Background(), stop, forceNeverClosed(), source,
		[]string{"Пустой"}, 1, results, discardLogger())

	if len(outcome.Failed) != 1 {
		t.Errorf("пустой результат должен считаться неудачей: %+v", outcome)
	}
	if outcome.Records != 0 {
		t.Errorf("записей: %d, ожидалось 0", outcome.Records)
	}
}

func TestCollectHonoursContextCancel(t *testing.T) {
	t.Parallel()

	// Контекст отменён заранее: источник не должен успеть ответить.
	source := newFakeSource(time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stop := make(chan struct{})
	results := make(chan owm.WeatherRecord, 16)

	outcome := collect(ctx, stop, forceNeverClosed(), source, []string{"Москва"}, 1, results, discardLogger())

	if len(outcome.Completed) != 0 {
		t.Errorf("при отменённом контексте город не может считаться собранным: %+v", outcome)
	}
}

func TestWatchSignalsStopsOnFirstSignal(t *testing.T) {
	t.Parallel()

	signals := make(chan os.Signal, 1)
	stop := make(chan struct{})
	force := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Заглушка, чтобы отмену можно было заметить.
	var cancelled atomic.Bool
	go func() {
		<-ctx.Done()
		cancelled.Store(true)
	}()

	go watchSignals(signals, stop, force, cancel, time.Minute, discardLogger())

	signals <- syscall.SIGINT

	select {
	case <-stop:
	case <-time.After(time.Second):
		t.Fatal("сигнал не закрыл канал остановки")
	}

	if cancelled.Load() {
		t.Error("по первому сигналу запросы прерываться не должны")
	}
}

func TestWatchSignalsCancelsWhenGraceExpires(t *testing.T) {
	t.Parallel()

	signals := make(chan os.Signal, 1)
	stop := make(chan struct{})
	force := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		watchSignals(signals, stop, force, cancel, 50*time.Millisecond, discardLogger())
		close(done)
	}()

	signals <- syscall.SIGINT

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("наблюдатель не завершился по истечении времени ожидания")
	}

	select {
	case <-ctx.Done():
	default:
		t.Error("по истечении времени ожидания запросы должны быть прерваны")
	}
}

func TestWatchSignalsSecondSignalCancelsImmediately(t *testing.T) {
	t.Parallel()

	signals := make(chan os.Signal, 2)
	stop := make(chan struct{})
	force := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		watchSignals(signals, stop, force, cancel, time.Minute, discardLogger())
		close(done)
	}()

	signals <- syscall.SIGINT
	// Второй сигнал сразу после первого: ждать минуту не нужно.
	signals <- syscall.SIGINT

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("повторный сигнал не прервал наблюдатель")
	}

	select {
	case <-ctx.Done():
	default:
		t.Error("повторный сигнал должен прервать запросы немедленно")
	}
}
