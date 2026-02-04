package kafka

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/segmentio/kafka-go"
)

// Consumer wraps a low-level kafka.Conn for reading messages from a specific partition.
// Unlike Producer which uses kafka.Writer (a high-level abstraction), Consumer needs
// direct connection access to:
//   - Seek to specific offsets (SetOffset)
//   - Read batches directly from a partition (ReadBatch)
//   - Maintain precise control over partition selection
//
// kafka.Writer handles connection pooling, routing, and retries automatically, while
// Consumer uses DialLeader which handles leader discovery but requires managing the
// initial broker connection. The low-level conn is needed for offset management and
// partition-specific operations.
type Consumer struct {
	conn  *kafka.Conn
	batch *kafka.Batch
	mu    sync.Mutex
}

// SetOffset sets the offset to a specific value
func (c *Consumer) SetOffset(offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.conn.Seek(offset, kafka.SeekStart)
	return err
}

func (c *Consumer) Close() error {
	if c.conn == nil {
		return nil
	}
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("failed to close consumer: %w", err)
	}
	return nil
}

// ReadNextMessage reads the next complete message from Kafka
// Returns the message value bytes, or an error if no message is available or context is canceled
func (c *Consumer) ReadNextMessage(ctx context.Context) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If we don't have a batch or it's exhausted, read a new one
	if c.batch == nil {
		// Set up batch reading with context cancellation support
		batchChan := make(chan *kafka.Batch, 1)
		errChan := make(chan error, 1)

		go func() {
			batch := c.conn.ReadBatch(1, 10*1024*1024) // minBytes=1, maxBytes=10MB
			select {
			case <-ctx.Done():
				batch.Close()
				errChan <- ctx.Err()
			case batchChan <- batch:
			}
		}()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-errChan:
			return nil, err
		case b := <-batchChan:
			c.batch = b
		}
	}

	// Read the next message from the batch
	msg, err := c.batch.ReadMessage()
	if err != nil {
		// If batch is exhausted, close it and return EOF
		// The next call will read a new batch
		if err == io.EOF {
			c.batch.Close()
			c.batch = nil
		}
		return nil, err
	}

	// Return a copy of the message value
	value := make([]byte, len(msg.Value))
	copy(value, msg.Value)
	return value, nil
}

func NewKafkaConsumer(ctx context.Context, brokers []string, topic string, partition int) (*Consumer, error) {
	// DialLeader expects a single broker address - it will discover the leader from metadata
	// Try each broker until one works
	var conn *kafka.Conn
	var err error
	for _, broker := range brokers {
		conn, err = kafka.DialLeader(ctx, "tcp", broker, topic, partition)
		if err == nil {
			break
		}
	}
	if err != nil {
		// The error may show a different broker address than the one provided because
		// Kafka returns the leader broker's advertised address in metadata, and the
		// client tries to connect to that address. If the advertised address is
		// unreachable, the error will show the leader's address, not the initial broker.
		return nil, fmt.Errorf("failed to connect to any broker (tried: %v). Note: Kafka may return a different broker address (leader) in metadata that must also be reachable: %w", brokers, err)
	}

	return &Consumer{
		conn: conn,
	}, nil
}
