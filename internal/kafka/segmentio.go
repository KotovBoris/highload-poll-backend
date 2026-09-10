package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/KotovBoris/highload-poll-backend/internal/model"
)

// WriterConfig — параметры продюсера.
type WriterConfig struct {
	Brokers []string
	Topic   string
	// WriteTimeout — таймаут одной записи в Kafka.
	WriteTimeout time.Duration
}

// Writer — реализация Producer поверх kafka.Writer.
type Writer struct {
	w            *kafka.Writer
	writeTimeout time.Duration
}

// NewWriter создаёт продюсера с маршрутизацией по ключу и компрессией LZ4.
func NewWriter(cfg WriterConfig) *Writer {
	timeout := cfg.WriteTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Writer{
		w: &kafka.Writer{
			Addr:  kafka.TCP(cfg.Brokers...),
			Topic: cfg.Topic,
			// Hash-балансировщик кладёт сообщения с одинаковым key
			// (poll_id) в одну партицию.
			Balancer: &kafka.Hash{},
			// Ждём подтверждения от всех синхронизированных реплик.
			RequiredAcks: kafka.RequireAll,
			Compression:  kafka.Lz4,
			// Небольшой батчинг внутри Writer снижает число сетевых вызовов.
			BatchTimeout: 10 * time.Millisecond,
		},
		writeTimeout: timeout,
	}
}

// Produce сериализует батч в JSON и пишет его в Kafka с key = poll_id.
func (p *Writer) Produce(ctx context.Context, batch model.VoteBatch) error {
	data, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, p.writeTimeout)
	defer cancel()
	err = p.w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(batch.PollID),
		Value: data,
	})
	if err != nil {
		return fmt.Errorf("write messages: %w", err)
	}
	return nil
}

// Close закрывает продюсера.
func (p *Writer) Close() error {
	return p.w.Close()
}

// ReaderConfig — параметры consumer-группы.
type ReaderConfig struct {
	Brokers []string
	Topic   string
	GroupID string
	// MaxWait — как долго ждать наполнения батча перед возвратом.
	MaxWait time.Duration
	// CommitInterval не используется при ручном коммите, но задаётся для
	// совместимости с kafka-go (0 = ручной коммит).
	// MaxPollInterval — верхняя граница времени обработки сообщения без
	// poll'а; по умолчанию 10 минут (см. архитектуру, п.10).
	MaxPollInterval time.Duration
	// SessionTimeout — таймаут heartbeat-сессии.
	SessionTimeout time.Duration
}

// GroupReader — реализация Reader поверх kafka.Reader (consumer-группа).
type GroupReader struct {
	r *kafka.Reader
}

// NewGroupReader создаёт читателя в consumer-группе.
func NewGroupReader(cfg ReaderConfig) *GroupReader {
	maxWait := cfg.MaxWait
	if maxWait <= 0 {
		maxWait = time.Second
	}
	maxPoll := cfg.MaxPollInterval
	if maxPoll <= 0 {
		maxPoll = 10 * time.Minute
	}
	session := cfg.SessionTimeout
	if session <= 0 {
		session = time.Minute
	}
	return &GroupReader{
		r: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        cfg.Brokers,
			Topic:          cfg.Topic,
			GroupID:        cfg.GroupID,
			MinBytes:       1,
			MaxBytes:       10 << 20, // 10 MiB
			MaxWait:        maxWait,
			CommitInterval: 0, // ручной коммит
			StartOffset:    kafka.FirstOffset,
		}),
	}
}

// FetchMessage читает следующее сообщение и приводит его к облегчённому виду.
func (g *GroupReader) FetchMessage(ctx context.Context) (Message, error) {
	m, err := g.r.FetchMessage(ctx)
	if err != nil {
		return Message{}, err
	}
	return Message{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
	}, nil
}

// CommitMessages фиксирует offset'ы по переданным сообщениям.
func (g *GroupReader) CommitMessages(ctx context.Context, msgs ...Message) error {
	km := make([]kafka.Message, 0, len(msgs))
	for _, m := range msgs {
		km = append(km, kafka.Message{
			Topic:     m.Topic,
			Partition: m.Partition,
			Offset:    m.Offset,
			Key:       m.Key,
			Value:     m.Value,
		})
	}
	return g.r.CommitMessages(ctx, km...)
}

// Close закрывает читателя.
func (g *GroupReader) Close() error {
	return g.r.Close()
}

// EnsureTopics создаёт топик, если его нет (идемпотентно). Полезно для
// локального запуска и e2e-тестов.
func EnsureTopics(ctx context.Context, brokers []string, topic string, partitions int) error {
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	return nil
}
