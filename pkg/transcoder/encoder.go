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
	sizeBuf      []byte
	totalBytes   int64
}

// NewEncodeWriter creates a new encoder for binary message files
// It writes the file header and positions the writer ready for message data
func NewEncodeWriter(writer io.Writer) (*EncodeWriter, error) {
	e := &EncodeWriter{
		writer:       writer,
		timestampBuf: make([]byte, TimestampSize),
		sizeBuf:      make([]byte, SizeFieldSize),
	}

	// Write file header
	if err := e.writeFileHeader(); err != nil {
		return nil, fmt.Errorf("failed to write file header: %w", err)
	}

	e.totalBytes = HeaderSize

	return e, nil
}

// Write writes a message to the output in the binary format:
// timestamp (8 bytes Unix timestamp) + size (8 bytes) + message data
func (e *EncodeWriter) Write(timestamp time.Time, messageData []byte) (int64, error) {
	messageSize := int64(len(messageData))

	// Write timestamp (fixed size: 8 bytes Unix timestamp, big-endian)
	unixTimestamp := timestamp.Unix()
	binary.BigEndian.PutUint64(e.timestampBuf, uint64(unixTimestamp))
	if _, err := e.writer.Write(e.timestampBuf); err != nil {
		return 0, err
	}

	// Write message size (fixed size: 8 bytes, big-endian)
	binary.BigEndian.PutUint64(e.sizeBuf, uint64(messageSize))
	if _, err := e.writer.Write(e.sizeBuf); err != nil {
		return TimestampSize, err
	}

	// Write message data
	if _, err := e.writer.Write(messageData); err != nil {
		return TimestampSize + SizeFieldSize, err
	}

	bytesWritten := TimestampSize + SizeFieldSize + messageSize
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
func (e *EncodeWriter) writeFileHeader() error {
	headerBuf := make([]byte, HeaderSize)

	// Write protocol version (int32, big-endian)
	binary.BigEndian.PutUint32(headerBuf[0:HeaderVersionSize], uint32(ProtocolVersion))

	// Reserved bytes are already zero-initialized

	// Write header
	if _, err := e.writer.Write(headerBuf); err != nil {
		return err
	}

	return nil
}
