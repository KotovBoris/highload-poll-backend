// Package kafka — тонкая обёртка над segmentio/kafka-go с интерфейсами
// Producer/Reader, чтобы сервисы зависели от абстракций, а не от клиента.
//
// Топик votes: key = poll_id (одна партиция на опрос), одно сообщение = батч
// голосов, компрессия LZ4 (см. docs/architecture/04-architecture.md).
package kafka

import (
	"context"

	"github.com/KotovBoris/highload-poll-backend/internal/model"
)

// Producer публикует батчи голосов в Kafka.
//
// Реализация обязана соблюдать маршрутизацию по key = poll_id, чтобы все
// сообщения одного опроса попадали в одну партицию (и, значит, к одному
// consumer'у) — это основа локальной дедупликации.
type Producer interface {
	// Produce отправляет один батч голосов. Возвращает ошибку, если запись
	// не подтверждена брокером.
	Produce(ctx context.Context, batch model.VoteBatch) error
	// Close освобождает ресурсы продюсера.
	Close() error
}

// Message — прочитанное из Kafka сообщение (облегчённое представление,
// достаточное для обработки и последующего коммита offset'а).
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
}

// Reader читает сообщения из Kafka в составе consumer-группы.
type Reader interface {
	// FetchMessage блокируется до получения следующего сообщения или
	// отмены контекста.
	FetchMessage(ctx context.Context) (Message, error)
	// CommitMessages фиксирует offset'ы указанных сообщений.
	CommitMessages(ctx context.Context, msgs ...Message) error
	// Close освобождает ресурсы читателя.
	Close() error
}
