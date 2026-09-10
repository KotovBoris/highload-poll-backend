// Package results реализует сервис результатов: хранение метаданных опросов и
// итоговых результатов, админские и внутренние HTTP-ручки.
//
// См. specs/results.md — источник истины по контрактам.
package results

import (
	"context"
	"errors"

	"github.com/KotovBoris/highload-poll-backend/internal/model"
)

// ErrNotFound — опрос не найден в хранилище.
var ErrNotFound = errors.New("poll not found")

// Storage — абстракция хранилища опросов. Реализация поверх PostgreSQL —
// postgres.go; в тестах используется in-memory фейк.
type Storage interface {
	// CreatePoll сохраняет новый опрос.
	CreatePoll(ctx context.Context, poll model.Poll) error
	// GetPoll возвращает опрос по id или ErrNotFound.
	GetPoll(ctx context.Context, id string) (model.Poll, error)
	// FlushResults идемпотентно записывает результаты.
	// written=false, err=nil означает, что опрос уже был завершён ранее.
	FlushResults(ctx context.Context, id string, results model.Results) (written bool, err error)
	// Ping проверяет доступность хранилища.
	Ping(ctx context.Context) error
}
