package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"
)

// httpClient создаёт HTTP-клиент с заданным таймаутом.
//
// Постоянные соединения выключены намеренно. На этой машине переиспользуемое
// соединение с api.openweathermap.org возвращало заголовки ответа и не
// отдавало тело: запрос зависал до таймаута, тело приходило нулевым.
// Наблюдение сделано на прогнозе (около 17 КБ), текущая погода (488 байт)
// проходила по тому же соединению. С DisableKeepAlives каждый запрос идёт по
// своему соединению, и все ответы приходят целиком за 0.9 с. Цена решения —
// новое TCP-соединение на запрос, что для двух запросов на город
// несущественно.
func httpClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
}

// newLogger создаёт логгер: записи идут в stderr и, если задан LOG_FILE,
// в файл.
//
// Возвращается функция закрытия файла: без неё лог оставался бы
// незакрытым до конца процесса.
func newLogger(logFile string) (*slog.Logger, func(), error) {
	writers := []any{os.Stderr}
	closers := []func(){}

	if logFile != "" {
		file, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, nil, fmt.Errorf("файл лога %s не открыт: %w", logFile, err)
		}

		writers = append(writers, file)
		closers = append(closers, func() { _ = file.Close() })
	}

	handler := slog.NewTextHandler(ioMultiWriter(writers), &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})

	closeAll := func() {
		for _, closer := range closers {
			closer()
		}
	}

	return slog.New(handler), closeAll, nil
}

// ioMultiWriter объединяет несколько писателей в один.
func ioMultiWriter(writers []any) *multiWriter {
	return &multiWriter{writers: writers}
}

// multiWriter пишет в несколько писателей по очереди.
//
// Свой тип вместо io.MultiWriter: тот возвращает ошибку от первого
// упавшего писателя, а логирование не должно падать из-за файла.
type multiWriter struct {
	writers []any
}

func (m *multiWriter) Write(p []byte) (int, error) {
	written := len(p)

	for _, writer := range m.writers {
		target, ok := writer.(interface{ Write([]byte) (int, error) })
		if !ok {
			continue
		}

		if _, err := target.Write(p); err != nil {
			return 0, err
		}
	}

	return written, nil
}
