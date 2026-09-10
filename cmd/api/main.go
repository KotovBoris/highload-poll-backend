// Command api — API-воркер: приём голосов зрителей, буферизация и отправка
// батчей в Kafka.
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

	"github.com/KotovBoris/highload-poll-backend/internal/api"
	"github.com/KotovBoris/highload-poll-backend/internal/config"
	"github.com/KotovBoris/highload-poll-backend/internal/kafka"
	"github.com/KotovBoris/highload-poll-backend/internal/resultsclient"
	"github.com/KotovBoris/highload-poll-backend/internal/uuid"
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
	httpAddr := config.String("HTTP_ADDR", ":8080")
	brokers := strings.Split(config.String("KAFKA_BROKERS", "kafka:9092"), ",")
	topic := config.String("KAFKA_TOPIC", "votes")
	resultsURL := config.String("RESULTS_URL", "http://results:8081")
	workerID := config.String("WORKER_ID", uuid.New())
	batchSize := config.Int("VOTE_BATCH_SIZE", 10000)
	cacheTTL := config.Duration("POLL_CACHE_TTL", time.Minute)
	closeCheck := config.Duration("CLOSE_CHECK_INTERVAL", 500*time.Millisecond)
	queueSize := config.Int("PRODUCE_QUEUE_SIZE", 4096)
	produceWorkers := config.Int("PRODUCE_WORKERS", 4)
	shutdownTimeout := config.Duration("SHUTDOWN_TIMEOUT", 10*time.Second)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Топик создаём заранее (идемпотентно) — consumer-group не должна ждать
	// автосоздания. Ошибку не считаем фатальной: топик может уже существовать.
	if err := kafka.EnsureTopics(ctx, brokers, topic, 1); err != nil {
		logger.Warn("ensure topic failed (continuing)", "topic", topic, "err", err)
	}

	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        topic,
		WriteTimeout: 30 * time.Second,
	})
	defer producer.Close()

	sender := api.NewSender(producer, queueSize, produceWorkers, logger)
	sender.Start(ctx)

	client := resultsclient.NewHTTPClient(resultsURL, 5*time.Second)
	server := api.NewServer(client, sender, api.Config{
		WorkerID:  workerID,
		BatchSize: batchSize,
		CacheTTL:  cacheTTL,
		CloseTTL:  cacheTTL,
	}, logger)

	go server.StartWatcher(ctx, closeCheck)

	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("api worker listening", "addr", httpAddr, "worker_id", workerID)
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
		logger.Error("http shutdown error", "err", err)
	}
	// Даём фоновым продюсерам разослать остатки очереди.
	done := make(chan struct{})
	go func() { sender.Wait(); close(done) }()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		logger.Warn("shutdown timeout waiting for senders")
	}
	logger.Info("stopped")
	return nil
}
