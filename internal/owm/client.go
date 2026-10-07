// Package owm - клиент OpenWeatherMap API.
//
// Сборщик обращается к двум эндпоинтам: текущая погода и прогноз на пять
// дней с шагом три часа. Ответы приводятся к единому типу WeatherRecord.
package owm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Базовые адреса эндпоинтов.
const (
	currentURL  = "https://api.openweathermap.org/data/2.5/weather"
	forecastURL = "https://api.openweathermap.org/data/2.5/forecast"
)

// ErrAPI - источник вернул ошибку или нечитаемый ответ.
//
// Отдельный тип нужен, чтобы отличать сбой источника от сбоя сети: в
// первом случае повторять запрос бессмысленно.
type ErrAPI struct {
	StatusCode int
	Message    string
}

func (e *ErrAPI) Error() string {
	return fmt.Sprintf("OpenWeatherMap вернул %d: %s", e.StatusCode, e.Message)
}

// Client - клиент OpenWeatherMap.
type Client struct {
	http     *http.Client
	apiKey   string
	units    string
	language string
	// now возвращает текущее время. Подменяется в тестах, иначе поле
	// collected_at невозможно проверить.
	now func() time.Time
	// Адреса эндпоинтов вынесены в поля: в тестах подставляется
	// httptest-сервер, иначе проверять разбор ответов пришлось бы против
	// настоящего источника.
	currentEndpoint  string
	forecastEndpoint string
}

// Option - настройка клиента.
type Option func(*Client)

// WithHTTPClient задаёт HTTP-клиент.
func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		c.http = client
	}
}

// WithNow задаёт источник текущего времени.
func WithNow(now func() time.Time) Option {
	return func(c *Client) {
		c.now = now
	}
}

// WithEndpoints задаёт адреса эндпоинтов. Используется в тестах.
func WithEndpoints(current, forecast string) Option {
	return func(c *Client) {
		c.currentEndpoint = current
		c.forecastEndpoint = forecast
	}
}

// New создаёт клиента.
//
// Args:
//
//	apiKey: ключ доступа, берётся из переменной OWM_API_KEY.
//	opts: дополнительные настройки.
func New(apiKey string, opts ...Option) (*Client, error) {
	if strings.TrimSpace(apiKey) == "" {
		return nil, fmt.Errorf("не задан ключ OpenWeatherMap (OWM_API_KEY)")
	}

	client := &Client{
		http:             &http.Client{Timeout: 20 * time.Second},
		apiKey:           apiKey,
		units:            "metric",
		language:         "ru",
		now:              time.Now,
		currentEndpoint:  currentURL,
		forecastEndpoint: forecastURL,
	}

	for _, opt := range opts {
		opt(client)
	}

	return client, nil
}

// get выполняет запрос и разбирает ответ в out.
func (c *Client) get(ctx context.Context, endpoint, city string, out any) (time.Duration, error) {
	query := url.Values{}
	query.Set("q", city)
	query.Set("appid", c.apiKey)
	query.Set("units", c.units)
	query.Set("lang", c.language)

	request, err := http.NewRequestWithContext(
		ctx, http.MethodGet, endpoint+"?"+query.Encode(), nil,
	)
	if err != nil {
		return 0, fmt.Errorf("не удалось создать запрос: %w", err)
	}

	started := time.Now()
	response, err := c.http.Do(request)
	elapsed := time.Since(started)
	if err != nil {
		return elapsed, fmt.Errorf("запрос к %s не выполнен: %w", endpoint, err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return elapsed, fmt.Errorf("не удалось прочитать ответ %s: %w", endpoint, err)
	}

	if len(body) == 0 {
		return elapsed, errors.New("источник вернул пустой ответ")
	}

	if response.StatusCode != http.StatusOK {
		return elapsed, &ErrAPI{
			StatusCode: response.StatusCode,
			Message:    apiMessage(body),
		}
	}

	if err := json.Unmarshal(body, out); err != nil {
		return elapsed, fmt.Errorf("ответ %s не разобран: %w", endpoint, err)
	}

	return elapsed, nil
}

// apiMessage достаёт текст ошибки из ответа API.
func apiMessage(body []byte) string {
	var payload struct {
		Message string `json:"message"`
	}

	if err := json.Unmarshal(body, &payload); err != nil || payload.Message == "" {
		return strings.TrimSpace(string(body))
	}

	return payload.Message
}

// CurrentWeather собирает наблюдение текущей погоды.
//
// Args:
//
//	ctx: контекст с отменой.
//	city: название города, как его понимает OpenWeatherMap.
//
// Returns:
//
//	Запись наблюдения и время выполнения запроса.
func (c *Client) CurrentWeather(ctx context.Context, city string) (WeatherRecord, time.Duration, error) {
	var payload currentResponse

	elapsed, err := c.get(ctx, c.currentEndpoint, city, &payload)
	if err != nil {
		return WeatherRecord{}, elapsed, err
	}

	record := WeatherRecord{
		City:        city,
		Country:     payload.Sys.Country,
		Lat:         payload.Coord.Lat,
		Lon:         payload.Coord.Lon,
		Endpoint:    EndpointCurrent,
		ObservedAt:  formatTime(payload.Dt, payload.Timezone),
		CollectedAt: c.now().UTC().Format(time.RFC3339),
		TempC:       payload.Main.Temp,
		FeelsLikeC:  payload.Main.FeelsLike,
		TempMinC:    payload.Main.TempMin,
		TempMaxC:    payload.Main.TempMax,
		PressureHpa: payload.Main.Pressure,
		HumidityPct: payload.Main.Humidity,
		DewPointC:   payload.Main.DewPoint,
		WindSpeedMs: payload.Wind.Speed,
		WindDeg:     payload.Wind.Deg,
		WindGustMs:  payload.Wind.Gust,
		CloudsPct:   payload.Clouds.All,
		VisibilityM: payload.Visibility,
		RainMm:      payload.Rain.OneHour,
		SnowMm:      payload.Snow.OneHour,
		WeatherID:   firstWeatherID(payload.Weather),
		WeatherMain: firstWeatherMain(payload.Weather),
		WeatherDesc: firstWeatherDesc(payload.Weather),
		IsDay:       isDay(payload.Sys.Pod),
		RequestMs:   elapsed.Milliseconds(),
	}

	return record, elapsed, nil
}

// Forecast собирает все точки прогноза на пять дней.
//
// Args:
//
//	ctx: контекст с отменой.
//	city: название города.
//
// Returns:
//
//	Записи прогноза по времени возрастания и время выполнения запроса.
func (c *Client) Forecast(ctx context.Context, city string) ([]WeatherRecord, time.Duration, error) {
	var payload forecastResponse

	elapsed, err := c.get(ctx, c.forecastEndpoint, city, &payload)
	if err != nil {
		return nil, elapsed, err
	}

	name := payload.City.Name
	if name == "" {
		name = city
	}

	collectedAt := c.now().UTC().Format(time.RFC3339)
	records := make([]WeatherRecord, 0, len(payload.List))

	for _, item := range payload.List {
		records = append(records, WeatherRecord{
			City:        name,
			Country:     payload.City.Country,
			Lat:         payload.City.Coord.Lat,
			Lon:         payload.City.Coord.Lon,
			Endpoint:    EndpointForecast,
			ObservedAt:  formatTime(item.Dt, payload.City.Timezone),
			CollectedAt: collectedAt,
			TempC:       item.Main.Temp,
			FeelsLikeC:  item.Main.FeelsLike,
			TempMinC:    item.Main.TempMin,
			TempMaxC:    item.Main.TempMax,
			PressureHpa: item.Main.Pressure,
			HumidityPct: item.Main.Humidity,
			DewPointC:   item.Main.DewPoint,
			WindSpeedMs: item.Wind.Speed,
			WindDeg:     item.Wind.Deg,
			WindGustMs:  item.Wind.Gust,
			CloudsPct:   item.Clouds.All,
			VisibilityM: item.Visibility,
			PopProb:     item.Pop,
			RainMm:      item.Rain.ThreeHour,
			SnowMm:      item.Snow.ThreeHour,
			WeatherID:   firstWeatherID(item.Weather),
			WeatherMain: firstWeatherMain(item.Weather),
			WeatherDesc: firstWeatherDesc(item.Weather),
			IsDay:       isDay(item.Sys.Pod),
			RequestMs:   elapsed.Milliseconds(),
		})
	}

	return records, elapsed, nil
}

// CollectOne собирает оба эндпоинта для одного города.
//
// Возвращает всё, что удалось собрать, вместе с ошибкой. Если прогноз
// недоступен, наблюдение текущей погоды не выбрасывается: иначе один
// сбойный эндпоинт обнулял бы весь город, а источник временами отвечает на
// текущую погоду и не отвечает на прогноз.
//
// Это то, из чего состоит работа одной горутины сбора.
func (c *Client) CollectOne(ctx context.Context, city string) ([]WeatherRecord, error) {
	records := make([]WeatherRecord, 0, 41)

	current, _, err := c.CurrentWeather(ctx, city)
	if err != nil {
		return nil, err
	}
	records = append(records, current)

	forecast, _, err := c.Forecast(ctx, city)
	if err != nil {
		return records, fmt.Errorf("прогноз для %s недоступен: %w", city, err)
	}

	return append(records, forecast...), nil
}

func firstWeatherID(blocks []weatherBlock) int {
	if len(blocks) == 0 {
		return 0
	}
	return blocks[0].ID
}

func firstWeatherMain(blocks []weatherBlock) string {
	if len(blocks) == 0 {
		return ""
	}
	return blocks[0].Main
}

func firstWeatherDesc(blocks []weatherBlock) string {
	if len(blocks) == 0 {
		return ""
	}
	return blocks[0].Description
}
