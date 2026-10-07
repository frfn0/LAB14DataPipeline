package owm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// currentFixture - правдоподобный ответ текущей погоды.
const currentFixture = `{
  "coord": {"lat": 55.7558, "lon": 37.6176},
  "weather": [{"id": 800, "main": "Clear", "description": "ясно", "icon": "01d"}],
  "main": {"temp": 8.5, "feels_like": 6.1, "temp_min": 7.0, "temp_max": 10.0,
           "pressure": 1015, "humidity": 62, "dew_point": 1.4},
  "wind": {"speed": 4.2, "deg": 270, "gust": 9.1},
  "clouds": {"all": 10},
  "visibility": 10000,
  "rain": {"1h": 0.5},
  "dt": 1791406800,
  "sys": {"country": "RU", "pod": "d"},
  "timezone": 10800
}`

// forecastFixture - ответ прогноза с двумя точками.
const forecastFixture = `{
  "city": {"name": "Москва", "country": "RU", "timezone": 10800,
           "coord": {"lat": 55.7558, "lon": 37.6176}},
  "list": [
    {"dt": 1791406800,
     "main": {"temp": 9.0, "feels_like": 7.0, "temp_min": 8.0, "temp_max": 10.0,
              "pressure": 1012, "humidity": 70, "dew_point": 3.6},
     "weather": [{"id": 500, "main": "Rain", "description": "небольшой дождь"}],
     "wind": {"speed": 3.9, "deg": 269},
     "clouds": {"all": 80},
     "pop": 0.6, "rain": {"3h": 1.2},
     "sys": {"pod": "n"}},
    {"dt": 1791417600,
     "main": {"temp": 7.5, "feels_like": 5.0, "temp_min": 6.0, "temp_max": 8.0,
              "pressure": 1011, "humidity": 90, "dew_point": 5.9},
     "weather": [{"id": 601, "main": "Snow", "description": "снег"}],
     "wind": {"speed": 5.1, "deg": 180},
     "clouds": {"all": 95},
     "pop": 0.9, "snow": {"3h": 0.4},
     "sys": {"pod": "d"}}
  ]
}`

// newTestClient поднимает тестовый сервер и клиента на него.
func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	client, err := New("test-key",
		WithHTTPClient(server.Client()),
		WithEndpoints(server.URL+"/weather", server.URL+"/forecast"),
		WithNow(func() time.Time {
			return time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
		}),
	)
	if err != nil {
		t.Fatalf("клиент не создан: %v", err)
	}

	return client
}

func TestNewRequiresKey(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"", "   "} {
		if _, err := New(key); err == nil {
			t.Errorf("клиент создан с ключом %q", key)
		}
	}
}

func TestCurrentWeatherMapsFields(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "appid=test-key") {
			t.Errorf("ключ не передан в запросе: %s", r.URL.RawQuery)
		}
		if r.URL.Query().Get("units") != "metric" || r.URL.Query().Get("lang") != "ru" {
			t.Errorf("единицы или язык не заданы: %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(currentFixture))
	})

	record, elapsed, err := client.CurrentWeather(context.Background(), "Москва")
	if err != nil {
		t.Fatalf("запрос не выполнен: %v", err)
	}

	if elapsed <= 0 {
		t.Error("время запроса не измерено")
	}
	if record.City != "Москва" || record.Country != "RU" {
		t.Errorf("город или страна разобраны неверно: %+v", record)
	}
	if record.Endpoint != EndpointCurrent {
		t.Errorf("эндпоинт: получено %q, ожидалось %q", record.Endpoint, EndpointCurrent)
	}
	if record.TempC != 8.5 || record.FeelsLikeC != 6.1 {
		t.Errorf("температуры разобраны неверно: %+v", record)
	}
	if record.HumidityPct != 62 || record.PressureHpa != 1015 {
		t.Errorf("влажность или давление разобраны неверно: %+v", record)
	}
	if record.WindSpeedMs != 4.2 || record.WindDeg != 270 || record.WindGustMs != 9.1 {
		t.Errorf("ветер разобран неверно: %+v", record)
	}
	if record.CloudsPct != 10 || record.VisibilityM != 10000 {
		t.Errorf("облака или видимость разобраны неверно: %+v", record)
	}
	if record.RainMm != 0.5 || record.SnowMm != 0 {
		t.Errorf("осадки разобраны неверно: %+v", record)
	}
	if record.WeatherID != 800 || record.WeatherMain != "Clear" || record.WeatherDesc != "ясно" {
		t.Errorf("погодные условия разобраны неверно: %+v", record)
	}
	if !record.IsDay {
		t.Error("по sys.pod=d должен быть день")
	}
	if record.CollectedAt != "2026-10-07T12:00:00Z" {
		t.Errorf("время сбора: получено %q", record.CollectedAt)
	}
}

func TestCurrentWeatherUsesCityTimezone(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(currentFixture))
	})

	record, _, err := client.CurrentWeather(context.Background(), "Москва")
	if err != nil {
		t.Fatalf("запрос не выполнен: %v", err)
	}

	// Время 1791406800 - это 2026-10-07 21:00 UTC. При сдвиге +10800
	// (UTC+3) получается уже следующий день: без применения часового
	// пояса города дата в данных была бы на сутки раньше.
	if record.ObservedAt != "2026-10-08T00:00:00+03:00" {
		t.Errorf("время в часовом поясе города вычислено неверно: %q", record.ObservedAt)
	}
}

func TestForecastMapsEveryItem(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(forecastFixture))
	})

	records, _, err := client.Forecast(context.Background(), "Москва")
	if err != nil {
		t.Fatalf("запрос не выполнен: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("точек прогноза: получено %d, ожидалось 2", len(records))
	}

	first := records[0]
	if first.Endpoint != EndpointForecast {
		t.Errorf("эндпоинт: получено %q", first.Endpoint)
	}
	if first.TempC != 9.0 || first.HumidityPct != 70 {
		t.Errorf("показатели первой точки неверны: %+v", first)
	}
	if first.PopProb != 0.6 || first.RainMm != 1.2 {
		t.Errorf("вероятность осадков или дождь неверны: %+v", first)
	}
	if first.IsDay {
		t.Error("по sys.pod=n должно быть ночью")
	}

	second := records[1]
	if second.WeatherMain != "Snow" || second.SnowMm != 0.4 {
		t.Errorf("снег не разобран: %+v", second)
	}
	if !second.IsDay {
		t.Error("во второй точке по sys.pod=d должен быть день")
	}

	// Время сбора у всех точек одно: прогноз снят одним запросом.
	if records[0].CollectedAt != records[1].CollectedAt {
		t.Errorf("время сбора различается: %q и %q",
			records[0].CollectedAt, records[1].CollectedAt)
	}
}

func TestCollectOneKeepsCurrentWeatherWhenForecastFails(t *testing.T) {
	t.Parallel()

	// Источник отвечает на текущую погоду и отказывает на прогнозе.
	// Наблюдение текущей погоды должно сохраниться: иначе один сбойный
	// эндпоинт обнулял бы весь город.
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "forecast") {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"cod": 503, "message": "unavailable"}`))
			return
		}
		_, _ = w.Write([]byte(currentFixture))
	})

	records, err := client.CollectOne(context.Background(), "Москва")
	if err == nil {
		t.Fatal("ошибка прогноза не возвращена")
	}
	if len(records) != 1 {
		t.Fatalf("сохранённых записей: %d, ожидалась 1 (текущая погода)", len(records))
	}
	if records[0].Endpoint != EndpointCurrent {
		t.Errorf("сохранён неверный эндпоинт: %q", records[0].Endpoint)
	}
	if !strings.Contains(err.Error(), "Москва") {
		t.Errorf("в ошибке не назван город: %v", err)
	}
}

func TestCollectOneReturnsNothingWhenCurrentFails(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"cod": 401, "message": "Invalid API key"}`))
	})

	records, err := client.CollectOne(context.Background(), "Москва")
	if err == nil {
		t.Fatal("ошибка не возвращена")
	}
	if len(records) != 0 {
		t.Errorf("без текущей погоды записей быть не должно, получено %d", len(records))
	}
}

func TestCollectOneReturnsCurrentAndForecast(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "forecast") {
			_, _ = w.Write([]byte(forecastFixture))
			return
		}
		_, _ = w.Write([]byte(currentFixture))
	})

	records, err := client.CollectOne(context.Background(), "Москва")
	if err != nil {
		t.Fatalf("сбор города не выполнен: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("записей: получено %d, ожидалось 3 (1 текущая и 2 прогноза)", len(records))
	}
	if records[0].Endpoint != EndpointCurrent {
		t.Errorf("первой должна идти текущая погода: %q", records[0].Endpoint)
	}
}

func TestAPIErrorIsReported(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"cod": 401, "message": "Invalid API key"}`))
	})

	_, _, err := client.CurrentWeather(context.Background(), "Москва")
	if err == nil {
		t.Fatal("ошибка источника не возвращена")
	}

	var apiErr *ErrAPI
	if !errors.As(err, &apiErr) {
		t.Fatalf("ожидалась ErrAPI, получено %T", err)
	}
	if apiErr.StatusCode != http.StatusUnauthorized {
		t.Errorf("код источника: получен %d, ожидалось 401", apiErr.StatusCode)
	}
	if !strings.Contains(apiErr.Message, "Invalid API key") {
		t.Errorf("сообщение источника потеряно: %q", apiErr.Message)
	}
}

func TestEmptyBodyIsReported(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	_, _, err := client.CurrentWeather(context.Background(), "Москва")
	if err == nil || !strings.Contains(err.Error(), "пустой ответ") {
		t.Errorf("пустой ответ должен отклоняться, получено %v", err)
	}
}

func TestBrokenJSONIsReported(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{не json`))
	})

	_, _, err := client.CurrentWeather(context.Background(), "Москва")
	if err == nil || !strings.Contains(err.Error(), "не разобран") {
		t.Errorf("битый ответ должен отклоняться, получено %v", err)
	}
}

func TestMissingWeatherBlockDoesNotPanic(t *testing.T) {
	t.Parallel()

	// В ответе без блока weather обращаться к первому элементу нельзя.
	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"coord": {"lat": 1, "lon": 2}, "dt": 1791406800,
			"main": {"temp": 1}, "sys": {"country": "RU"}}`))
	})

	record, _, err := client.CurrentWeather(context.Background(), "Москва")
	if err != nil {
		t.Fatalf("запрос не выполнен: %v", err)
	}

	if record.WeatherMain != "" || record.WeatherDesc != "" || record.WeatherID != 0 {
		t.Errorf("пустые погодные условия должны остаться пустыми: %+v", record)
	}
}

func TestRecordIsJSONSerialisable(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(currentFixture))
	})

	record, _, err := client.CurrentWeather(context.Background(), "Москва")
	if err != nil {
		t.Fatalf("запрос не выполнен: %v", err)
	}

	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("запись не сериализована: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("запись не разобрана: %v", err)
	}

	// Имена полей в JSON совпадают с теми, что читает Python.
	for _, field := range []string{
		"city", "country", "endpoint", "observed_at", "collected_at",
		"temp_c", "feels_like_c", "pressure_hpa", "humidity_pct",
		"wind_speed_ms", "clouds_pct", "visibility_m", "pop_prob",
		"weather_main", "is_day", "request_ms",
	} {
		if _, ok := decoded[field]; !ok {
			t.Errorf("в JSON нет поля %q", field)
		}
	}
}

func TestContextCancellationStopsRequest(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * time.Second)
		_, _ = w.Write([]byte(currentFixture))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if _, _, err := client.CurrentWeather(ctx, "Москва"); err == nil {
		t.Error("запрос не прерван по контексту")
	}
}
