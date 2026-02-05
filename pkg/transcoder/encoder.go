package transcoder

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// EncodeWriter encodes messages to a binary file format
type EncodeWriter struct {
	writer       io.Writer
	timestampBuf []byte
	keySizeBuf   []byte
	sizeBuf      []byte
	totalBytes   int64
}

// NewEncodeWriter creates a new encoder for binary message files
// It writes the file header and positions the writer ready for message data
// New files are written in version 2 format (with message keys)
func NewEncodeWriter(writer io.Writer) (*EncodeWriter, error) {
	e := &EncodeWriter{
		writer:       writer,
		timestampBuf: make([]byte, TimestampSize),
		keySizeBuf:   make([]byte, KeySizeFieldSize),
		sizeBuf:      make([]byte, SizeFieldSize),
	}

	// Write file header with version 2
	if err := e.writeFileHeader(); err != nil {
		return nil, fmt.Errorf("failed to write file header: %w", err)
	}

	e.totalBytes = HeaderSize

	return e, nil
}

// Write writes a message to the output in version 2 binary format:
// timestamp (8 bytes) + key size (8 bytes) + message size (8 bytes) + key (variable) + message data (variable)
// If key is nil or empty, key size is written as 0
func (e *EncodeWriter) Write(timestamp time.Time, messageData []byte, key []byte) (int64, error) {
	messageSize := int64(len(messageData))
	keySize := int64(len(key))
	if key == nil {
		keySize = 0
	}

	// Write timestamp (fixed size: 8 bytes Unix timestamp, big-endian)
	unixTimestamp := timestamp.Unix()
	binary.BigEndian.PutUint64(e.timestampBuf, uint64(unixTimestamp))
	if _, err := e.writer.Write(e.timestampBuf); err != nil {
		return 0, err
	}

	// Write key size (fixed size: 8 bytes, big-endian)
	binary.BigEndian.PutUint64(e.keySizeBuf, uint64(keySize))
	if _, err := e.writer.Write(e.keySizeBuf); err != nil {
		return TimestampSize, err
	}

	// Write message size (fixed size: 8 bytes, big-endian)
	binary.BigEndian.PutUint64(e.sizeBuf, uint64(messageSize))
	if _, err := e.writer.Write(e.sizeBuf); err != nil {
		return TimestampSize + KeySizeFieldSize, err
	}

	// Write key data (if present)
	if keySize > 0 {
		if _, err := e.writer.Write(key); err != nil {
			return TimestampSize + KeySizeFieldSize + SizeFieldSize, err
		}
	}

	// Write message data
	if _, err := e.writer.Write(messageData); err != nil {
		return TimestampSize + KeySizeFieldSize + SizeFieldSize + keySize, err
	}

	bytesWritten := TimestampSize + KeySizeFieldSize + SizeFieldSize + keySize + messageSize
	e.totalBytes += bytesWritten

	return bytesWritten, nil
}

// TotalBytes returns the total number of bytes written so far (including header)
func (e *EncodeWriter) TotalBytes() int64 {
	return e.totalBytes
}

// Close closes the underlying writer if it implements io.Closer
func (e *EncodeWriter) Close() error {
	if closer, ok := e.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// writeFileHeader writes the file header containing protocol version and reserved space
// Always writes version 2 (current version)
func (e *EncodeWriter) writeFileHeader() error {
	headerBuf := make([]byte, HeaderSize)

	// Write protocol version 2 (int32, big-endian)
	binary.BigEndian.PutUint32(headerBuf[0:HeaderVersionSize], uint32(ProtocolVersion))

	// Reserved bytes are already zero-initialized

	// Write header
	if _, err := e.writer.Write(headerBuf); err != nil {
		return err
	}

	return nil
}
