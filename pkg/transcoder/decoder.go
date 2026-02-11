package transcoder

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/lolocompany/kafka-replay/v2/pkg/transcoder/legacy"
)

// DecodeReader decodes messages from a binary file format
// Supports both version 1 (legacy, no keys) and version 2 (with keys)
type DecodeReader struct {
	reader             io.ReadSeeker
	timestampBuf       []byte
	keySizeBuf         []byte
	sizeBuf            []byte
	preserveTimestamps bool
	dataStartOffset    int64 // Offset after the header where message data starts
	protocolVersion    int32
}

type Entry struct {
	Timestamp time.Time
	Key       []byte
	Data      []byte
}

// NewDecodeReader creates a new decoder for binary message files
// It reads and validates the file header, then positions the reader at the start of message data
// Supports both version 1 (legacy) and version 2 formats
func NewDecodeReader(reader io.ReadSeeker, preserveTimestamps bool) (*DecodeReader, error) {
	d := &DecodeReader{
		reader:             reader,
		timestampBuf:       make([]byte, TimestampSize),
		keySizeBuf:         make([]byte, KeySizeFieldSize),
		sizeBuf:            make([]byte, SizeFieldSize),
		preserveTimestamps: preserveTimestamps,
	}

	// Read and validate file header
	if err := d.readFileHeader(); err != nil {
		return nil, fmt.Errorf("failed to read file header: %w", err)
	}

	// Store the offset after the header for reset operations
	d.dataStartOffset = HeaderSize

	return d, nil
}

// Read reads the next complete message from the binary file
// Returns the message timestamp, key, value, and error
// For version 1 files, key will be nil
func (d *DecodeReader) Read() (*Entry, error) {
	// Read timestamp (8 bytes Unix timestamp)
	if _, err := io.ReadFull(d.reader, d.timestampBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to read timestamp: %w", err)
	}

	var key []byte
	var messageData []byte
	var msgTime time.Time
	var err error

	if d.protocolVersion == ProtocolVersion1 {
		// Use legacy decoder for version 1 format
		msgTime, messageData, err = legacy.V1ReadMessage(d.reader, d.timestampBuf, d.sizeBuf, d.preserveTimestamps)
		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, err
		}
		// Version 1 has no key
		key = nil
		return &Entry{
			Timestamp: msgTime,
			Key:       key,
			Data:      messageData,
		}, nil
	} else {
		// Version 2 format: timestamp, key size, message size, key, message data
		// Read key size (8 bytes)
		if _, err := io.ReadFull(d.reader, d.keySizeBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("failed to read key size: %w", err)
		}

		keySize := int64(binary.BigEndian.Uint64(d.keySizeBuf))
		if keySize < 0 || keySize > 100*1024*1024 { // Sanity check: max 100MB
			return nil, fmt.Errorf("invalid key size: %d bytes", keySize)
		}

		// Read message size (8 bytes)
		if _, err := io.ReadFull(d.reader, d.sizeBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("failed to read message size: %w", err)
		}

		messageSize := int64(binary.BigEndian.Uint64(d.sizeBuf))
		if messageSize < 0 || messageSize > 100*1024*1024 { // Sanity check: max 100MB
			return nil, fmt.Errorf("invalid message size: %d bytes", messageSize)
		}

		// Read key data (if present)
		if keySize > 0 {
			key = make([]byte, keySize)
			if _, err := io.ReadFull(d.reader, key); err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					return nil, io.EOF
				}
				return nil, fmt.Errorf("failed to read key data: %w", err)
			}
		} else {
			key = nil
		}

		// Read message data
		messageData = make([]byte, messageSize)
		if _, err := io.ReadFull(d.reader, messageData); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("failed to read message data: %w", err)
		}
	}

	// Parse timestamp for version 2
	if d.preserveTimestamps {
		// Read Unix timestamp (int64, big-endian)
		unixTimestamp := int64(binary.BigEndian.Uint64(d.timestampBuf))
		msgTime = time.Unix(unixTimestamp, 0).UTC()
	} else {
		msgTime = time.Now().UTC()
	}

	return &Entry{
		Timestamp: msgTime,
		Key:       key,
		Data:      messageData,
	}, nil
}

// Close closes the underlying reader if it implements io.Closer
func (d *DecodeReader) Close() error {
	if closer, ok := d.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// Reset seeks back to the start of message data (after the header)
func (d *DecodeReader) Reset() error {
	_, err := d.reader.Seek(d.dataStartOffset, io.SeekStart)
	return err
}

// readFileHeader reads and validates the file header
func (d *DecodeReader) readFileHeader() error {
	headerBuf := make([]byte, HeaderSize)
	if _, err := io.ReadFull(d.reader, headerBuf); err != nil {
		return err
	}

	// Read protocol version (int32, big-endian)
	d.protocolVersion = int32(binary.BigEndian.Uint32(headerBuf[0:HeaderVersionSize]))

	// Validate protocol version (support version 1 and 2)
	if d.protocolVersion != ProtocolVersion1 && d.protocolVersion != ProtocolVersion {
		return fmt.Errorf("unsupported protocol version: %d (supported versions: %d, %d)", d.protocolVersion, ProtocolVersion1, ProtocolVersion)
	}

	// Reserved bytes are read but not used yet

	return nil
}
