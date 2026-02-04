package pkg

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// MessageFileReader reads recorded Kafka messages from a binary file
type MessageFileReader struct {
	reader             io.ReadSeeker
	timestampBuf       []byte
	sizeBuf            []byte
	preserveTimestamps bool
	timeProvider       TimeProvider
}

// RecordedMessage represents a message read from the recorded messages file
type RecordedMessage struct {
	Data      []byte    `json:"data"`
	Timestamp time.Time `json:"timestamp"`
}

// NewMessageFileReader creates a new reader for binary message files
func NewMessageFileReader(reader io.ReadSeeker, preserveTimestamps bool, timeProvider TimeProvider) *MessageFileReader {
	return &MessageFileReader{
		reader:             reader,
		timestampBuf:       make([]byte, TimestampSize),
		sizeBuf:            make([]byte, SizeFieldSize),
		preserveTimestamps: preserveTimestamps,
		timeProvider:       timeProvider,
	}
}

// ReadNextMessage reads the next complete message from the recorded messages file
// Returns the message data and timestamp, or an error if no message is available or EOF
func (r *MessageFileReader) ReadNextMessage(ctx context.Context) (*RecordedMessage, error) {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Read timestamp (27 bytes)
	if _, err := io.ReadFull(r.reader, r.timestampBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to read timestamp: %w", err)
	}

	// Read message size (8 bytes)
	if _, err := io.ReadFull(r.reader, r.sizeBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to read message size: %w", err)
	}

	messageSize := int64(binary.BigEndian.Uint64(r.sizeBuf))
	if messageSize < 0 || messageSize > 100*1024*1024 { // Sanity check: max 100MB
		return nil, fmt.Errorf("invalid message size: %d bytes", messageSize)
	}

	// Read message data
	messageData := make([]byte, messageSize)
	if _, err := io.ReadFull(r.reader, messageData); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to read message data: %w", err)
	}

	// Parse timestamp
	var msgTime time.Time
	if r.preserveTimestamps {
		timestampStr := string(r.timestampBuf)
		parsedTime, err := time.Parse(TimestampFormat, timestampStr)
		if err != nil {
			// If timestamp parsing fails, use current time
			msgTime = r.timeProvider.Now()
		} else {
			msgTime = parsedTime
		}
	} else {
		msgTime = r.timeProvider.Now()
	}

	return &RecordedMessage{
		Data:      messageData,
		Timestamp: msgTime,
	}, nil
}

// Close closes the underlying reader if it implements io.Closer
func (r *MessageFileReader) Close() error {
	if closer, ok := r.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// Reset seeks back to the beginning of the reader
func (r *MessageFileReader) Reset() error {
	_, err := r.reader.Seek(0, io.SeekStart)
	return err
}
