// Command consumer — consumer голосов: дедупликация, подсчёт и идемпотентный
// flush результатов в сервис результатов.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/KotovBoris/highload-poll-backend/internal/config"
	"github.com/KotovBoris/highload-poll-backend/internal/consumer"
	"github.com/KotovBoris/highload-poll-backend/internal/httpx"
	"github.com/KotovBoris/highload-poll-backend/internal/kafka"
	"github.com/KotovBoris/highload-poll-backend/internal/resultsclient"
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
	brokers := strings.Split(config.String("KAFKA_BROKERS", "kafka:9092"), ",")
	topic := config.String("KAFKA_TOPIC", "votes")
	groupID := config.String("KAFKA_GROUP_ID", "poll-consumers")
	resultsURL := config.String("RESULTS_URL", "http://results:8081")
	httpAddr := config.String("HTTP_ADDR", ":8082")
	checkInterval := config.Duration("CHECK_INTERVAL", time.Second)
	shutdownTimeout := config.Duration("SHUTDOWN_TIMEOUT", 10*time.Second)

	cfg := consumer.Config{
		DonePercent:   config.Int("COMMIT_DONE_PERCENT", 90),
		StaleMessages: config.Int("COMMIT_STALE_MESSAGES", 1000),
		CloseGrace:    config.Duration("CLOSE_GRACE", 30*time.Second),
		CloseHardTmo:  config.Duration("CLOSE_HARD_TIMEOUT", 5*time.Minute),
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	reader := kafka.NewGroupReader(kafka.ReaderConfig{
		Brokers:         brokers,
		Topic:           topic,
		GroupID:         groupID,
		MaxWait:         time.Second,
		MaxPollInterval: 10 * time.Minute,
		SessionTimeout:  60 * time.Second,
	})
	defer reader.Close()

	client := resultsclient.NewHTTPClient(resultsURL, 10*time.Second)
	cons := consumer.New(reader, client, cfg, logger)

	// Служебный HTTP: /healthz.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	httpServer := &http.Server{Addr: httpAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("consumer health server listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("health server error", "err", err)
		}
	}()

	logger.Info("consumer started", "topic", topic, "group", groupID)
	if err := cons.Run(ctx, checkInterval); err != nil {
		logger.Error("consumer run error", "err", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	logger.Info("stopped")
	return nil
}
