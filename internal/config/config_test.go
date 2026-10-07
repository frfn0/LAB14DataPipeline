package config

import (
	"strings"
	"testing"
	"time"
)

func TestParseDefaults(t *testing.T) {
	t.Setenv("OWM_API_KEY", "test-key")

	cfg, err := Parse(nil)
	if err != nil {
		t.Fatalf("настройки не разобраны: %v", err)
	}

	if len(cfg.Cities) != len(DefaultCities()) {
		t.Errorf("городов по умолчанию: получено %d, ожидалось %d",
			len(cfg.Cities), len(DefaultCities()))
	}
	if cfg.Output != "data/raw/weather.jsonl" {
		t.Errorf("файл по умолчанию: получен %q", cfg.Output)
	}
	if cfg.Concurrency != 5 {
		t.Errorf("параллелизм по умолчанию: получен %d, ожидалось 5", cfg.Concurrency)
	}
	if cfg.Timeout != 20*time.Second {
		t.Errorf("таймаут по умолчанию: получен %v, ожидалось 20s", cfg.Timeout)
	}
	if cfg.APIKey != "test-key" {
		t.Errorf("ключ не прочитан: получен %q", cfg.APIKey)
	}
}

func TestParseFlags(t *testing.T) {
	t.Setenv("OWM_API_KEY", "test-key")

	cfg, err := Parse([]string{
		"-cities", "Москва, Казань",
		"-out", "data/raw/custom.jsonl",
		"-concurrency", "3",
		"-timeout", "7s",
		"-log", "logs/collector.log",
	})
	if err != nil {
		t.Fatalf("настройки не разобраны: %v", err)
	}

	if len(cfg.Cities) != 2 || cfg.Cities[0] != "Москва" || cfg.Cities[1] != "Казань" {
		t.Errorf("города разобраны неверно: %v", cfg.Cities)
	}
	if cfg.Output != "data/raw/custom.jsonl" {
		t.Errorf("путь вывода: получен %q", cfg.Output)
	}
	if cfg.Concurrency != 3 {
		t.Errorf("параллелизм: получен %d, ожидалось 3", cfg.Concurrency)
	}
	if cfg.Timeout != 7*time.Second {
		t.Errorf("таймаут: получен %v, ожидалось 7s", cfg.Timeout)
	}
	if cfg.LogFile != "logs/collector.log" {
		t.Errorf("путь лога: получен %q", cfg.LogFile)
	}
}

func TestParseRequiresKey(t *testing.T) {
	t.Setenv("OWM_API_KEY", "")

	_, err := Parse(nil)
	if err == nil {
		t.Fatal("сборщик запущен без ключа API")
	}
	if !strings.Contains(err.Error(), "OWM_API_KEY") {
		t.Errorf("в ошибке не названа переменная с ключом: %v", err)
	}
}

func TestParseRejectsBadValues(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"нулевой параллелизм", []string{"-concurrency", "0"}, "concurrency"},
		{"отрицательный параллелизм", []string{"-concurrency", "-2"}, "concurrency"},
		{"пустые города", []string{"-cities", " , , "}, "город"},
		{"нулевой таймаут", []string{"-timeout", "0s"}, "timeout"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv("OWM_API_KEY", "test-key")

			_, err := Parse(testCase.args)
			if err == nil {
				t.Fatalf("настройки приняты: %v", testCase.args)
			}
			if !strings.Contains(err.Error(), testCase.want) {
				t.Errorf("в ошибке нет ожидаемой части %q: %v", testCase.want, err)
			}
		})
	}
}

func TestParseCustomKeyVariable(t *testing.T) {
	t.Setenv("OWM_API_KEY", "старый")
	t.Setenv("MY_WEATHER_KEY", "новый")

	cfg, err := Parse([]string{"-env-key", "MY_WEATHER_KEY"})
	if err != nil {
		t.Fatalf("настройки не разобраны: %v", err)
	}

	if cfg.APIKey != "новый" {
		t.Errorf("ключ прочитан не из своей переменной: получен %q", cfg.APIKey)
	}
	if cfg.EnvAPIKey != "MY_WEATHER_KEY" {
		t.Errorf("имя переменной не сохранено: %q", cfg.EnvAPIKey)
	}
}

func TestDefaultCitiesAreDistinct(t *testing.T) {
	seen := map[string]bool{}

	for _, city := range DefaultCities() {
		if seen[city] {
			t.Errorf("город повторяется в списке по умолчанию: %q", city)
		}
		seen[city] = true
	}
}

func TestRedactedKeyHidesSecret(t *testing.T) {
	// Короткие ключи скрываются целиком: в маске из звёздочек не видно,
	// сколько символов было, а длинный ключ показывается краями, чтобы по
	// логу можно было отличить один ключ от другого.
	cases := map[string]string{
		"":                      "(не задан)",
		"abc":                   "***",
		"12345678":              "********",
		"50b24c4b65476ce2092cb": "50b2...92cb",
	}

	for key, want := range cases {
		got := RedactedKey(key)
		if got != want {
			t.Errorf("RedactedKey(%q) = %q, ожидалось %q", key, got, want)
		}
		if key != "" && len(key) > 8 && strings.Contains(got, key[4:len(key)-4]) {
			t.Errorf("ключ %q виден в %q целиком", key, got)
		}
	}
}

func TestParsePositiveInt(t *testing.T) {
	if value, err := ParsePositiveInt(" 5 "); err != nil || value != 5 {
		t.Errorf("ParsePositiveInt(\" 5 \") = %d, %v", value, err)
	}

	for _, raw := range []string{"0", "-3", "abc"} {
		if _, err := ParsePositiveInt(raw); err == nil {
			t.Errorf("ParsePositiveInt(%q) принял значение", raw)
		}
	}
}
