package owm

// Модели данных OpenWeatherMap и приведение ответов API к единому виду.
//
// Ответы разных эндпоинтов устроены по-разному: текущая погода отдаёт
// одно наблюдение, прогноз — список из 40 точек. Наружу отдаётся один тип
// WeatherRecord, чтобы конвейер в Python работал с однородной таблицей.

import (
	"time"
)

// WeatherRecord - одна нормализованная запись наблюдения.
//
// JSON-поля совпадают с теми, что читает Python на следующих шагах
// конвейера, поэтому переименование поля здесь требует правки там же.
type WeatherRecord struct {
	City        string  `json:"city"`
	Country     string  `json:"country"`
	Lat         float64 `json:"lat"`
	Lon         float64 `json:"lon"`
	Endpoint    string  `json:"endpoint"`
	ObservedAt  string  `json:"observed_at"`
	CollectedAt string  `json:"collected_at"`

	TempC       float64 `json:"temp_c"`
	FeelsLikeC  float64 `json:"feels_like_c"`
	TempMinC    float64 `json:"temp_min_c"`
	TempMaxC    float64 `json:"temp_max_c"`
	PressureHpa float64 `json:"pressure_hpa"`
	HumidityPct float64 `json:"humidity_pct"`
	DewPointC   float64 `json:"dew_point_c"`

	WindSpeedMs float64 `json:"wind_speed_ms"`
	WindDeg     float64 `json:"wind_deg"`
	WindGustMs  float64 `json:"wind_gust_ms"`
	CloudsPct   float64 `json:"clouds_pct"`
	VisibilityM float64 `json:"visibility_m"`
	PopProb     float64 `json:"pop_prob"`
	RainMm      float64 `json:"rain_mm"`
	SnowMm      float64 `json:"snow_mm"`
	WeatherID   int     `json:"weather_id"`
	WeatherMain string  `json:"weather_main"`
	WeatherDesc string  `json:"weather_desc"`
	IsDay       bool    `json:"is_day"`

	// RequestMs - время HTTP-запроса. Нужно для оценки быстродействия
	// источника и остаётся в данных вместе с метеоданными.
	RequestMs int64 `json:"request_ms"`
}

// Виды эндпоинтов: из них формируется поле Endpoint.
const (
	EndpointCurrent  = "current"
	EndpointForecast = "forecast"
)

// currentResponse - ответ /data/2.5/weather.
type currentResponse struct {
	Coord struct {
		Lat float64 `json:"lat"`
		Lon float64 `json:"lon"`
	} `json:"coord"`
	Weather []weatherBlock `json:"weather"`
	Main    struct {
		Temp      float64 `json:"temp"`
		FeelsLike float64 `json:"feels_like"`
		TempMin   float64 `json:"temp_min"`
		TempMax   float64 `json:"temp_max"`
		Pressure  float64 `json:"pressure"`
		Humidity  float64 `json:"humidity"`
		DewPoint  float64 `json:"dew_point"`
	} `json:"main"`
	Wind struct {
		Speed float64 `json:"speed"`
		Deg   float64 `json:"deg"`
		Gust  float64 `json:"gust"`
	} `json:"wind"`
	Clouds struct {
		All float64 `json:"all"`
	} `json:"clouds"`
	Rain struct {
		OneHour float64 `json:"1h"`
	} `json:"rain"`
	Snow struct {
		OneHour float64 `json:"1h"`
	} `json:"snow"`
	Visibility float64 `json:"visibility"`
	Dt         int64   `json:"dt"`
	Sys        struct {
		Country string `json:"country"`
		Pod     string `json:"pod"`
	} `json:"sys"`
	Timezone int64 `json:"timezone"`
}

// forecastResponse - ответ /data/2.5/forecast.
type forecastResponse struct {
	City struct {
		Name    string `json:"name"`
		Country string `json:"country"`
		Coord   struct {
			Lat float64 `json:"lat"`
			Lon float64 `json:"lon"`
		} `json:"coord"`
		Timezone int64 `json:"timezone"`
	} `json:"city"`
	List []forecastItem `json:"list"`
}

// forecastItem - одна точка прогноза.
type forecastItem struct {
	Dt      int64          `json:"dt"`
	Main    forecastMain   `json:"main"`
	Weather []weatherBlock `json:"weather"`
	Wind    struct {
		Speed float64 `json:"speed"`
		Deg   float64 `json:"deg"`
		Gust  float64 `json:"gust"`
	} `json:"wind"`
	Clouds struct {
		All float64 `json:"all"`
	} `json:"clouds"`
	Visibility float64 `json:"visibility"`
	Pop        float64 `json:"pop"`
	Rain       struct {
		ThreeHour float64 `json:"3h"`
	} `json:"rain"`
	Snow struct {
		ThreeHour float64 `json:"3h"`
	} `json:"snow"`
	Sys struct {
		Pod string `json:"pod"`
	} `json:"sys"`
}

// forecastMain - основные показатели точки прогноза.
type forecastMain struct {
	Temp      float64 `json:"temp"`
	FeelsLike float64 `json:"feels_like"`
	TempMin   float64 `json:"temp_min"`
	TempMax   float64 `json:"temp_max"`
	Pressure  float64 `json:"pressure"`
	Humidity  float64 `json:"humidity"`
	DewPoint  float64 `json:"dew_point"`
}

// weatherBlock - описание погодных условий.
type weatherBlock struct {
	ID          int    `json:"id"`
	Main        string `json:"main"`
	Description string `json:"description"`
	Icon        string `json:"icon"`
}

// isDay вычисляет признак «день» по подсистеме sys.pod: значение "d"
// означает дневное время. Отдельный эндпоинт для этого не нужен.
func isDay(pod string) bool {
	return pod == "d"
}

// formatTime переводит unix-время в RFC3339 в часовом поясе города.
//
// Время наблюдения отдаётся в часовом поясе города, а не сервера: без
// сдвига все города в отчёте выглядели бы как один и тот же момент.
func formatTime(unix int64, timezoneOffsetSec int64) string {
	location := time.FixedZone("local", int(timezoneOffsetSec))
	return time.Unix(unix, 0).In(location).Format(time.RFC3339)
}
