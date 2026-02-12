package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string, allowAutoTopicCreation bool, noAck bool) *Producer {
	requiredAcks := kafka.RequireOne // Default: wait for leader acknowledgment (reliable)
	if noAck {
		requiredAcks = kafka.RequireNone // No acknowledgment wait = maximum speed (less reliable)
	}
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			AllowAutoTopicCreation: allowAutoTopicCreation,
			// Optimized for maximum throughput
			// Based on Apache Kafka best practices and kafka-go documentation:
			// - Large batches reduce per-message overhead
			// - Longer timeout allows more accumulation before sending
			// - Snappy compression reduces network bandwidth with minimal CPU cost
			BatchSize:    10000,                 // Large batch size for high throughput
			BatchTimeout: 500 * time.Millisecond, // Wait up to 500ms to accumulate more messages
			BatchBytes:   50 * 1024 * 1024,      // Max 50MB per batch - allows larger batches
			WriteTimeout: 30 * time.Second,      // 30 second timeout for writes
			Async:        false,                  // Synchronous writes (Async=true can complicate error handling)
			RequiredAcks: requiredAcks,           // Configurable: RequireOne (default) or RequireNone (--no-ack)
			Compression:  kafka.Snappy,          // Snappy compression: fast, reduces network overhead
		},
	}
}

// WriteMessages writes multiple messages to Kafka
func (p *Producer) WriteMessages(ctx context.Context, messages ...kafka.Message) error {
	return p.writer.WriteMessages(ctx, messages...)
}

// Close closes the underlying writer
func (p *Producer) Close() error {
	if p.writer == nil {
		return nil
	}
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("failed to close producer: %w", err)
	}
	return nil
}
