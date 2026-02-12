package pkg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	kafkapkg "github.com/lolocompany/kafka-replay/v2/pkg/kafka"
	"github.com/lolocompany/kafka-replay/v2/pkg/transcoder"
	"github.com/segmentio/kafka-go"
)

const (
	// BatchSize is the number of messages to batch before writing to Kafka
	// Matches kafka-go Writer's BatchSize (10000) to maximize throughput
	BatchSize = 10000
	// BatchBytes is the maximum bytes to batch before writing
	// Matches kafka-go Writer's BatchBytes (50MB) to maximize throughput
	BatchBytes = 50 * 1024 * 1024 // 50MB
)

// ReplayConfig holds configuration for the Replay function
type ReplayConfig struct {
	Producer  *kafkapkg.Producer
	Decoder   *transcoder.DecodeReader
	Rate      int
	Loop      bool
	Partition *int // Optional partition to write to (nil for auto-assignment)
	LogWriter io.Writer
	DryRun    bool   // If true, validate messages without actually sending to Kafka
	FindBytes []byte // Optional byte sequence to search for in messages
}

func Replay(ctx context.Context, cfg ReplayConfig) (int64, error) {
	if cfg.Producer == nil {
		return 0, errors.New("producer is required")
	}
	if cfg.Decoder == nil {
		return 0, errors.New("decoder is required")
	}
	if cfg.LogWriter == nil {
		cfg.LogWriter = os.Stderr
	}

	// Rate limiting setup
	var rateLimiter *time.Ticker
	if cfg.Rate > 0 {
		interval := time.Second / time.Duration(cfg.Rate)
		rateLimiter = time.NewTicker(interval)
		defer rateLimiter.Stop()
	}

	var messageCount int64
	batch := make([]kafka.Message, 0, BatchSize)
	var batchBytes int64

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if cfg.DryRun {
			// In dry-run mode, skip actual writing but still validate
			// The fact that we got here means decoding succeeded, so validation passes
			batch = batch[:0]
			batchBytes = 0
			return nil
		}
		if err := cfg.Producer.WriteMessages(ctx, batch...); err != nil {
			return fmt.Errorf("failed to write batch to Kafka: %w", err)
		}
		batch = batch[:0]
		batchBytes = 0
		return nil
	}

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			if err := flushBatch(); err != nil {
				return messageCount, err
			}
			return messageCount, ctx.Err()
		default:
		}

		// Read next complete message
		entry, err := cfg.Decoder.Read()
		if err != nil {
			if err == io.EOF {
				// End of file reached - flush remaining batch
				if err := flushBatch(); err != nil {
					return messageCount, err
				}

				// Check if we should loop
				if cfg.Loop {
					if err := cfg.Decoder.Reset(); err != nil {
						return messageCount, err
					}
					continue
				}
				// No more looping, exit
				break
			}
			// Check if context was canceled
			if ctx.Err() != nil {
				if err := flushBatch(); err != nil {
					return messageCount, err
				}
				return messageCount, ctx.Err()
			}
			return messageCount, err
		}

		// Filter by find bytes if specified
		if cfg.FindBytes != nil && !bytes.Contains(entry.Data, cfg.FindBytes) {
			continue
		}

		// Rate limiting - if enabled, wait before adding to batch
		if rateLimiter != nil {
			select {
			case <-ctx.Done():
				if err := flushBatch(); err != nil {
					return messageCount, err
				}
				return messageCount, ctx.Err()
			case <-rateLimiter.C:
				// Rate limit tick received, proceed
			}
		}

		// Build Kafka message
		kafkaMsg := kafka.Message{
			Key:   entry.Key,
			Value: entry.Data,
			Time:  entry.Timestamp,
		}
		// Set partition if specified in config (nil means auto-assignment)
		if cfg.Partition != nil {
			kafkaMsg.Partition = *cfg.Partition
		}

		// Add to batch
		batch = append(batch, kafkaMsg)
		batchBytes += int64(len(entry.Data))
		messageCount++

		// Flush batch if it reaches size or byte limit
		// The kafka-go Writer will further batch these internally for optimal throughput
		if len(batch) >= BatchSize || batchBytes >= BatchBytes {
			if err := flushBatch(); err != nil {
				return messageCount, err
			}
		}
	}

	// Flush any remaining messages
	if err := flushBatch(); err != nil {
		return messageCount, err
	}

	return messageCount, nil
}
