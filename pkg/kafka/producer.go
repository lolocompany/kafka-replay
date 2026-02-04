package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string, allowAutoTopicCreation bool) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			AllowAutoTopicCreation: allowAutoTopicCreation,
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
