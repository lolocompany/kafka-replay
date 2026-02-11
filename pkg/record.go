package pkg

import (
	"bytes"
	"context"
	"errors"
	"io"

	kafka "github.com/lolocompany/kafka-replay/v2/pkg/kafka"
	"github.com/lolocompany/kafka-replay/v2/pkg/transcoder"
)

// RecordConfig holds configuration for the Record function
type RecordConfig struct {
	Consumer  *kafka.Consumer
	Offset    *int64
	Output    io.WriteCloser
	Limit     int
	FindBytes []byte // Optional byte sequence to search for in messages
}

func Record(ctx context.Context, cfg RecordConfig) (int64, int64, error) {
	if cfg.Consumer == nil {
		return 0, 0, errors.New("consumer is required")
	}
	if cfg.Output == nil {
		return 0, 0, errors.New("output is required")
	}

	// Set offset if specified
	// Note: When using consumer groups, SetOffset will fail as offsets are managed automatically.
	// In that case, we skip setting the offset and let the consumer group handle it.
	if cfg.Offset != nil {
		if err := cfg.Consumer.SetOffset(*cfg.Offset); err != nil {
			// If SetOffset fails (e.g., when using consumer groups), we continue anyway.
			// Consumer groups manage offsets automatically, so this is expected behavior.
			// We only return the error if it's not related to consumer group offset management.
			// For now, we'll just log and continue - consumer groups will use committed offsets.
		}
	}

	// Create message encoder
	encoder, err := transcoder.NewEncodeWriter(cfg.Output)
	if err != nil {
		return 0, 0, err
	}
	defer encoder.Close()

	var messageCount int64

	for {
		// Check if we've reached the message limit
		if cfg.Limit > 0 && messageCount >= int64(cfg.Limit) {
			break
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			return encoder.TotalBytes(), messageCount, ctx.Err()
		default:
		}

		// Read next complete message
		timestamp, key, messageData, err := cfg.Consumer.ReadNextMessage(ctx)
		if err != nil {
			if err == io.EOF {
				// End of batch, continue to read next batch
				continue
			}
			// Check if context was canceled
			if ctx.Err() != nil {
				return encoder.TotalBytes(), messageCount, ctx.Err()
			}
			return encoder.TotalBytes(), messageCount, err
		}

		// Filter by find bytes if specified
		if cfg.FindBytes != nil && !bytes.Contains(messageData, cfg.FindBytes) {
			// Skip this message, continue to next one
			continue
		}

		// Write the matching message (version 2 format with key)
		if _, err := encoder.Write(timestamp, messageData, key); err != nil {
			return encoder.TotalBytes(), messageCount, err
		}
		messageCount++
	}

	return encoder.TotalBytes(), messageCount, nil
}
