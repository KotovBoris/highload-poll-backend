package results

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/KotovBoris/highload-poll-backend/internal/model"
)

// PostgresStorage — реализация Storage поверх PostgreSQL (pgx/v5).
type PostgresStorage struct {
	pool *pgxpool.Pool
}

// NewPostgresStorage создаёт пул соединений и проверяет подключение.
func NewPostgresStorage(ctx context.Context, dsn string, maxConns int) (*PostgresStorage, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = int32(maxConns)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &PostgresStorage{pool: pool}, nil
}

// Close закрывает пул соединений.
func (s *PostgresStorage) Close() {
	s.pool.Close()
}

// Ping проверяет доступность БД.
func (s *PostgresStorage) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// CreatePoll вставляет новый опрос.
func (s *PostgresStorage) CreatePoll(ctx context.Context, poll model.Poll) error {
	options, err := json.Marshal(poll.Options)
	if err != nil {
		return fmt.Errorf("marshal options: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO polls (id, question, options, status, created_at, ends_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		poll.ID, poll.Question, options, string(poll.Status), poll.CreatedAt, poll.EndsAt)
	if err != nil {
		return fmt.Errorf("insert poll: %w", err)
	}
	return nil
}

// GetPoll читает опрос по id.
func (s *PostgresStorage) GetPoll(ctx context.Context, id string) (model.Poll, error) {
	var (
		poll       model.Poll
		optionsRaw []byte
		resultsRaw []byte
		status     string
	)
	err := s.pool.QueryRow(ctx, `
		SELECT id, question, options, status, results, created_at, ends_at
		FROM polls WHERE id = $1`, id).
		Scan(&poll.ID, &poll.Question, &optionsRaw, &status, &resultsRaw, &poll.CreatedAt, &poll.EndsAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Poll{}, ErrNotFound
		}
		return model.Poll{}, fmt.Errorf("select poll: %w", err)
	}

	poll.Status = model.PollStatus(status)
	if err := json.Unmarshal(optionsRaw, &poll.Options); err != nil {
		return model.Poll{}, fmt.Errorf("unmarshal options: %w", err)
	}
	if len(resultsRaw) > 0 {
		var res model.Results
		if err := json.Unmarshal(resultsRaw, &res); err != nil {
			return model.Poll{}, fmt.Errorf("unmarshal results: %w", err)
		}
		poll.Results = &res
	}
	return poll, nil
}

// FlushResults идемпотентно записывает результаты.
//
// Ключевая гарантия: UPDATE ... WHERE status != 'completed'. Если опрос уже
// завершён — 0 строк затронуто, written=false. Атомарность обеспечивает СУБД,
// никакого read-modify-write в приложении нет.
func (s *PostgresStorage) FlushResults(ctx context.Context, id string, results model.Results) (bool, error) {
	payload, err := json.Marshal(results)
	if err != nil {
		return false, fmt.Errorf("marshal results: %w", err)
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE polls SET status = 'completed', results = $2
		WHERE id = $1 AND status != 'completed'`, id, payload)
	if err != nil {
		return false, fmt.Errorf("update results: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return true, nil
	}
	// Ни одна строка не обновлена: либо опрос уже завершён, либо его нет.
	// Различаем эти случаи отдельным запросом.
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT true FROM polls WHERE id = $1`, id).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrNotFound
		}
		return false, fmt.Errorf("check existence: %w", err)
	}
	return false, nil
}

// Migrate применяет SQL-миграции из каталога (в лексикографическом порядке).
// Файлы должны быть идемпотентными (CREATE TABLE IF NOT EXISTS и т.п.).
func (s *PostgresStorage) Migrate(ctx context.Context, dir string) error {
	files, err := migrationFiles(dir)
	if err != nil {
		return err
	}
	for _, f := range files {
		sqlBytes, err := readFile(f)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", f, err)
		}
		if _, err := s.pool.Exec(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("apply migration %s: %w", f, err)
		}
	}
	return nil
}
