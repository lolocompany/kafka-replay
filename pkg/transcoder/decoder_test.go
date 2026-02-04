package transcoder

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
	"time"
)

func TestNewDecodeReader(t *testing.T) {
	// Create a valid file with header
	buf := &bytes.Buffer{}
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:HeaderVersionSize], uint32(ProtocolVersion))
	buf.Write(header)

	reader := bytes.NewReader(buf.Bytes())
	decoder, err := NewDecodeReader(reader, true)
	if err != nil {
		t.Fatalf("NewDecodeReader failed: %v", err)
	}

	if decoder.dataStartOffset != HeaderSize {
		t.Errorf("Expected dataStartOffset %d, got %d", HeaderSize, decoder.dataStartOffset)
	}

	if decoder.protocolVersion != ProtocolVersion {
		t.Errorf("Expected protocol version %d, got %d", ProtocolVersion, decoder.protocolVersion)
	}
}

func TestNewDecodeReader_InvalidVersion(t *testing.T) {
	// Create a file with invalid protocol version
	buf := &bytes.Buffer{}
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:HeaderVersionSize], uint32(999)) // Invalid version
	buf.Write(header)

	reader := bytes.NewReader(buf.Bytes())
	_, err := NewDecodeReader(reader, true)
	if err == nil {
		t.Fatal("Expected error for invalid protocol version, got nil")
	}
}

func TestDecodeReader_Read(t *testing.T) {
	// Create a file with header and one message
	buf := &bytes.Buffer{}
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:HeaderVersionSize], uint32(ProtocolVersion))
	buf.Write(header)

	testTime := time.Date(2024, 2, 2, 10, 15, 30, 0, time.UTC)
	testData := []byte("Hello, World!")

	// Write message entry
	timestampBuf := make([]byte, TimestampSize)
	binary.BigEndian.PutUint64(timestampBuf, uint64(testTime.Unix()))
	buf.Write(timestampBuf)

	sizeBuf := make([]byte, SizeFieldSize)
	binary.BigEndian.PutUint64(sizeBuf, uint64(len(testData)))
	buf.Write(sizeBuf)

	buf.Write(testData)

	reader := bytes.NewReader(buf.Bytes())
	decoder, err := NewDecodeReader(reader, true)
	if err != nil {
		t.Fatalf("NewDecodeReader failed: %v", err)
	}

	// Read message
	timestamp, data, err := decoder.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !timestamp.Equal(testTime) {
		t.Errorf("Timestamp mismatch: expected %v, got %v", testTime, timestamp)
	}

	if !bytes.Equal(data, testData) {
		t.Errorf("Data mismatch: expected %q, got %q", testData, data)
	}

	// Should return EOF on next read
	_, _, err = decoder.Read()
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
}

func TestDecodeReader_ReadPreserveTimestamps(t *testing.T) {
	// Create a file with header and one message
	buf := &bytes.Buffer{}
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:HeaderVersionSize], uint32(ProtocolVersion))
	buf.Write(header)

	testTime := time.Date(2024, 2, 2, 10, 15, 30, 0, time.UTC)
	testData := []byte("Test message")

	// Write message entry
	timestampBuf := make([]byte, TimestampSize)
	binary.BigEndian.PutUint64(timestampBuf, uint64(testTime.Unix()))
	buf.Write(timestampBuf)

	sizeBuf := make([]byte, SizeFieldSize)
	binary.BigEndian.PutUint64(sizeBuf, uint64(len(testData)))
	buf.Write(sizeBuf)

	buf.Write(testData)

	reader := bytes.NewReader(buf.Bytes())
	decoder, err := NewDecodeReader(reader, true) // preserveTimestamps = true
	if err != nil {
		t.Fatalf("NewDecodeReader failed: %v", err)
	}

	timestamp, data, err := decoder.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// With preserveTimestamps=true, should get the original timestamp
	if !timestamp.Equal(testTime) {
		t.Errorf("Timestamp mismatch: expected %v, got %v", testTime, timestamp)
	}

	if !bytes.Equal(data, testData) {
		t.Errorf("Data mismatch: expected %q, got %q", testData, data)
	}
}

func TestDecodeReader_ReadWithoutPreserveTimestamps(t *testing.T) {
	// Create a file with header and one message
	buf := &bytes.Buffer{}
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:HeaderVersionSize], uint32(ProtocolVersion))
	buf.Write(header)

	testTime := time.Date(2024, 2, 2, 10, 15, 30, 0, time.UTC)
	testData := []byte("Test message")

	// Write message entry
	timestampBuf := make([]byte, TimestampSize)
	binary.BigEndian.PutUint64(timestampBuf, uint64(testTime.Unix()))
	buf.Write(timestampBuf)

	sizeBuf := make([]byte, SizeFieldSize)
	binary.BigEndian.PutUint64(sizeBuf, uint64(len(testData)))
	buf.Write(sizeBuf)

	buf.Write(testData)

	reader := bytes.NewReader(buf.Bytes())
	decoder, err := NewDecodeReader(reader, false) // preserveTimestamps = false
	if err != nil {
		t.Fatalf("NewDecodeReader failed: %v", err)
	}

	beforeRead := time.Now()
	timestamp, data, err := decoder.Read()
	afterRead := time.Now()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// With preserveTimestamps=false, should get current time
	if timestamp.Before(beforeRead) || timestamp.After(afterRead) {
		t.Errorf("Expected timestamp between %v and %v, got %v", beforeRead, afterRead, timestamp)
	}

	if !bytes.Equal(data, testData) {
		t.Errorf("Data mismatch: expected %q, got %q", testData, data)
	}
}

func TestDecodeReader_MultipleReads(t *testing.T) {
	// Create a file with header and multiple messages
	buf := &bytes.Buffer{}
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:HeaderVersionSize], uint32(ProtocolVersion))
	buf.Write(header)

	messages := []struct {
		timestamp time.Time
		data      []byte
	}{
		{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), []byte("First")},
		{time.Date(2024, 1, 1, 0, 0, 1, 0, time.UTC), []byte("Second")},
		{time.Date(2024, 1, 1, 0, 0, 2, 0, time.UTC), []byte("Third")},
	}

	// Write all messages
	for _, msg := range messages {
		timestampBuf := make([]byte, TimestampSize)
		binary.BigEndian.PutUint64(timestampBuf, uint64(msg.timestamp.Unix()))
		buf.Write(timestampBuf)

		sizeBuf := make([]byte, SizeFieldSize)
		binary.BigEndian.PutUint64(sizeBuf, uint64(len(msg.data)))
		buf.Write(sizeBuf)

		buf.Write(msg.data)
	}

	reader := bytes.NewReader(buf.Bytes())
	decoder, err := NewDecodeReader(reader, true)
	if err != nil {
		t.Fatalf("NewDecodeReader failed: %v", err)
	}

	// Read all messages
	for i, expectedMsg := range messages {
		timestamp, data, err := decoder.Read()
		if err != nil {
			t.Fatalf("Read %d failed: %v", i, err)
		}

		if !timestamp.Equal(expectedMsg.timestamp) {
			t.Errorf("Message %d timestamp mismatch: expected %v, got %v", i, expectedMsg.timestamp, timestamp)
		}

		if !bytes.Equal(data, expectedMsg.data) {
			t.Errorf("Message %d data mismatch: expected %q, got %q", i, expectedMsg.data, data)
		}
	}

	// Should return EOF
	_, _, err = decoder.Read()
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
}

func TestDecodeReader_Reset(t *testing.T) {
	// Create a file with header and multiple messages
	buf := &bytes.Buffer{}
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:HeaderVersionSize], uint32(ProtocolVersion))
	buf.Write(header)

	testTime := time.Date(2024, 2, 2, 10, 15, 30, 0, time.UTC)
	testData := []byte("Test message")

	// Write message entry
	timestampBuf := make([]byte, TimestampSize)
	binary.BigEndian.PutUint64(timestampBuf, uint64(testTime.Unix()))
	buf.Write(timestampBuf)

	sizeBuf := make([]byte, SizeFieldSize)
	binary.BigEndian.PutUint64(sizeBuf, uint64(len(testData)))
	buf.Write(sizeBuf)

	buf.Write(testData)

	reader := bytes.NewReader(buf.Bytes())
	decoder, err := NewDecodeReader(reader, true)
	if err != nil {
		t.Fatalf("NewDecodeReader failed: %v", err)
	}

	// Read first message
	timestamp1, data1, err := decoder.Read()
	if err != nil {
		t.Fatalf("First Read failed: %v", err)
	}

	// Reset
	if err := decoder.Reset(); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Read again - should get the same message
	timestamp2, data2, err := decoder.Read()
	if err != nil {
		t.Fatalf("Second Read failed: %v", err)
	}

	if !timestamp1.Equal(timestamp2) {
		t.Errorf("Timestamps don't match after reset: %v != %v", timestamp1, timestamp2)
	}

	if !bytes.Equal(data1, data2) {
		t.Errorf("Data doesn't match after reset: %q != %q", data1, data2)
	}
}

func TestDecodeReader_EmptyMessage(t *testing.T) {
	// Create a file with header and empty message
	buf := &bytes.Buffer{}
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:HeaderVersionSize], uint32(ProtocolVersion))
	buf.Write(header)

	testTime := time.Date(2024, 2, 2, 10, 15, 30, 0, time.UTC)

	// Write message entry with empty data
	timestampBuf := make([]byte, TimestampSize)
	binary.BigEndian.PutUint64(timestampBuf, uint64(testTime.Unix()))
	buf.Write(timestampBuf)

	sizeBuf := make([]byte, SizeFieldSize)
	binary.BigEndian.PutUint64(sizeBuf, 0) // Empty message
	buf.Write(sizeBuf)

	reader := bytes.NewReader(buf.Bytes())
	decoder, err := NewDecodeReader(reader, true)
	if err != nil {
		t.Fatalf("NewDecodeReader failed: %v", err)
	}

	timestamp, data, err := decoder.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !timestamp.Equal(testTime) {
		t.Errorf("Timestamp mismatch: expected %v, got %v", testTime, timestamp)
	}

	if len(data) != 0 {
		t.Errorf("Expected empty data, got %q", data)
	}
}

func TestDecodeReader_InvalidSize(t *testing.T) {
	// Create a file with invalid message size
	buf := &bytes.Buffer{}
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:HeaderVersionSize], uint32(ProtocolVersion))
	buf.Write(header)

	testTime := time.Date(2024, 2, 2, 10, 15, 30, 0, time.UTC)

	// Write message entry with invalid size (too large)
	timestampBuf := make([]byte, TimestampSize)
	binary.BigEndian.PutUint64(timestampBuf, uint64(testTime.Unix()))
	buf.Write(timestampBuf)

	sizeBuf := make([]byte, SizeFieldSize)
	binary.BigEndian.PutUint64(sizeBuf, uint64(200*1024*1024)) // 200MB - exceeds limit
	buf.Write(sizeBuf)

	reader := bytes.NewReader(buf.Bytes())
	decoder, err := NewDecodeReader(reader, true)
	if err != nil {
		t.Fatalf("NewDecodeReader failed: %v", err)
	}

	_, _, err = decoder.Read()
	if err == nil {
		t.Fatal("Expected error for invalid message size, got nil")
	}
}
