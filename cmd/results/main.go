// Command results — сервис результатов: админка + внутренние ручки для
// API-воркера и consumer'а. Хранит опросы и итоги в PostgreSQL.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/KotovBoris/highload-poll-backend/internal/config"
	"github.com/KotovBoris/highload-poll-backend/internal/results"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	httpAddr := config.String("HTTP_ADDR", ":8081")
	dsn := config.String("DATABASE_URL", "")
	migrationsDir := config.String("MIGRATIONS_DIR", "migrations")
	maxConns := config.Int("DB_MAX_CONNS", 10)
	shutdownTimeout := config.Duration("SHUTDOWN_TIMEOUT", 10*time.Second)

	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	storage, err := results.NewPostgresStorage(ctx, dsn, maxConns)
	if err != nil {
		return err
	}
	defer storage.Close()

	// Применяем миграции (идемпотентно). Если каталога нет — не падаем:
	// это удобно в средах, где схема уже создана снаружи.
	if err := storage.Migrate(ctx, migrationsDir); err != nil {
		if os.IsNotExist(err) {
			logger.Warn("migrations dir not found, skipping", "dir", migrationsDir)
		} else {
			return err
		}
	}

	srv := results.NewServer(storage)
	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("results service listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server error", "err", err)
			cancel()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return err
	}
	logger.Info("stopped")
	return nil
}
