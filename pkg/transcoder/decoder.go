package transcoder

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// DecodeReader decodes messages from a binary file format
type DecodeReader struct {
	reader             io.ReadSeeker
	timestampBuf       []byte
	sizeBuf            []byte
	preserveTimestamps bool
	dataStartOffset    int64 // Offset after the header where message data starts
	protocolVersion    int32
}

// NewDecodeReader creates a new decoder for binary message files
// It reads and validates the file header, then positions the reader at the start of message data
func NewDecodeReader(reader io.ReadSeeker, preserveTimestamps bool) (*DecodeReader, error) {
	d := &DecodeReader{
		reader:             reader,
		timestampBuf:       make([]byte, TimestampSize),
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
// Returns the message data and timestamp, or an error if no message is available or EOF
func (d *DecodeReader) Read() (time.Time, []byte, error) {
	// Read timestamp (8 bytes Unix timestamp)
	if _, err := io.ReadFull(d.reader, d.timestampBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return time.Time{}, nil, io.EOF
		}
		return time.Time{}, nil, fmt.Errorf("failed to read timestamp: %w", err)
	}

	// Read message size (8 bytes)
	if _, err := io.ReadFull(d.reader, d.sizeBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return time.Time{}, nil, io.EOF
		}
		return time.Time{}, nil, fmt.Errorf("failed to read message size: %w", err)
	}

	messageSize := int64(binary.BigEndian.Uint64(d.sizeBuf))
	if messageSize < 0 || messageSize > 100*1024*1024 { // Sanity check: max 100MB
		return time.Time{}, nil, fmt.Errorf("invalid message size: %d bytes", messageSize)
	}

	// Read message data
	messageData := make([]byte, messageSize)
	if _, err := io.ReadFull(d.reader, messageData); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return time.Time{}, nil, io.EOF
		}
		return time.Time{}, nil, fmt.Errorf("failed to read message data: %w", err)
	}

	// Parse timestamp
	var msgTime time.Time
	if d.preserveTimestamps {
		// Read Unix timestamp (int64, big-endian)
		unixTimestamp := int64(binary.BigEndian.Uint64(d.timestampBuf))
		msgTime = time.Unix(unixTimestamp, 0).UTC()
	} else {
		msgTime = time.Now().UTC()
	}

	return msgTime, messageData, nil
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

	// Validate protocol version
	if d.protocolVersion != ProtocolVersion {
		return fmt.Errorf("unsupported protocol version: %d (expected %d)", d.protocolVersion, ProtocolVersion)
	}

	// Reserved bytes are read but not used yet

	return nil
}
