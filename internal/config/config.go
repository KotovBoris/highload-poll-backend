// Package config — минималистичный хелпер для чтения конфигурации из
// переменных окружения с значениями по умолчанию.
package config

import (
	"os"
	"strconv"
	"time"
)

// String возвращает значение переменной окружения key или def, если она пуста.
func String(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Int возвращает целочисленное значение переменной окружения key или def.
func Int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// Bool возвращает булево значение переменной окружения key или def.
func Bool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

// Duration возвращает длительность из переменной окружения key или def.
// Значение парсится как duration-строка (например, "500ms", "10m").
func Duration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
