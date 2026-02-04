package pkg

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	kafka "github.com/lolocompany/kafka-replay/pkg/kafka"
)

const (
	// TimestampFormat is a fixed-size ISO 8601 timestamp format
	// Format: "2006-01-02T15:04:05.000000Z" (27 bytes)
	TimestampFormat = "2006-01-02T15:04:05.000000Z"
	TimestampSize   = 27
	SizeFieldSize   = 8 // int64 = 8 bytes
)

// RecordConfig holds configuration for the Record function
type RecordConfig struct {
	Consumer     *kafka.Consumer
	Offset       *int64
	Output       io.WriteCloser
	Limit        int
	TimeProvider TimeProvider
}

func Record(ctx context.Context, cfg RecordConfig) (int64, int64, error) {
	if cfg.Consumer == nil {
		return 0, 0, errors.New("consumer is required")
	}
	if cfg.Output == nil {
		return 0, 0, errors.New("output is required")
	}
	if cfg.TimeProvider == nil {
		cfg.TimeProvider = RealTimeProvider{}
	}

	// Set offset if specified
	if cfg.Offset != nil {
		if err := cfg.Consumer.SetOffset(*cfg.Offset); err != nil {
			return 0, 0, err
		}
	}

	var totalBytes int64
	var messageCount int64
	timestampBuf := make([]byte, TimestampSize)
	sizeBuf := make([]byte, SizeFieldSize)

	for {
		// Check if we've reached the message limit
		if cfg.Limit > 0 && messageCount >= int64(cfg.Limit) {
			break
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			return totalBytes, messageCount, ctx.Err()
		default:
		}

		// Read next complete message
		messageData, err := cfg.Consumer.ReadNextMessage(ctx)
		if err != nil {
			if err == io.EOF {
				// End of batch, continue to read next batch
				continue
			}
			// Check if context was canceled
			if ctx.Err() != nil {
				return totalBytes, messageCount, ctx.Err()
			}
			return totalBytes, messageCount, err
		}

		bytesWritten, err := writeRecordedMessage(cfg.Output, messageData, cfg.TimeProvider, timestampBuf, sizeBuf)
		if err != nil {
			return totalBytes, messageCount, err
		}
		totalBytes += bytesWritten
		messageCount++
	}

	return totalBytes, messageCount, nil
}

// writeRecordedMessage writes a message to the output file in the binary format:
// timestamp (27 bytes) + size (8 bytes) + message data
func writeRecordedMessage(output io.Writer, messageData []byte, timeProvider TimeProvider, timestampBuf, sizeBuf []byte) (int64, error) {
	messageSize := int64(len(messageData))
	recordTime := timeProvider.Now().UTC()

	// Write timestamp (fixed size: 27 bytes)
	timestampStr := recordTime.Format(TimestampFormat)
	copy(timestampBuf, timestampStr)
	if _, err := output.Write(timestampBuf); err != nil {
		return 0, err
	}

	// Write message size (fixed size: 8 bytes, big-endian)
	binary.BigEndian.PutUint64(sizeBuf, uint64(messageSize))
	if _, err := output.Write(sizeBuf); err != nil {
		return TimestampSize, err
	}

	// Write message data
	if _, err := output.Write(messageData); err != nil {
		return TimestampSize + SizeFieldSize, err
	}

	return TimestampSize + SizeFieldSize + messageSize, nil
}
