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

	// Write message entry (version 2 format: timestamp, key size, message size, key, message)
	timestampBuf := make([]byte, TimestampSize)
	binary.BigEndian.PutUint64(timestampBuf, uint64(testTime.Unix()))
	buf.Write(timestampBuf)

	keySizeBuf := make([]byte, KeySizeFieldSize)
	binary.BigEndian.PutUint64(keySizeBuf, 0) // No key
	buf.Write(keySizeBuf)

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
	entry, err := decoder.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !entry.Timestamp.Equal(testTime) {
		t.Errorf("Timestamp mismatch: expected %v, got %v", testTime, entry.Timestamp)
	}

	if len(entry.Key) > 0 {
		t.Errorf("Expected nil key, got %q", entry.Key)
	}

	if !bytes.Equal(entry.Data, testData) {
		t.Errorf("Data mismatch: expected %q, got %q", testData, entry.Data)
	}

	// Should return EOF on next read
	_, err = decoder.Read()
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

	// Write message entry (version 2 format: timestamp, key size, message size, key, message)
	timestampBuf := make([]byte, TimestampSize)
	binary.BigEndian.PutUint64(timestampBuf, uint64(testTime.Unix()))
	buf.Write(timestampBuf)

	keySizeBuf := make([]byte, KeySizeFieldSize)
	binary.BigEndian.PutUint64(keySizeBuf, 0) // No key
	buf.Write(keySizeBuf)

	sizeBuf := make([]byte, SizeFieldSize)
	binary.BigEndian.PutUint64(sizeBuf, uint64(len(testData)))
	buf.Write(sizeBuf)

	buf.Write(testData)

	reader := bytes.NewReader(buf.Bytes())
	decoder, err := NewDecodeReader(reader, true) // preserveTimestamps = true
	if err != nil {
		t.Fatalf("NewDecodeReader failed: %v", err)
	}

	entry, err := decoder.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// With preserveTimestamps=true, should get the original timestamp
	if !entry.Timestamp.Equal(testTime) {
		t.Errorf("Timestamp mismatch: expected %v, got %v", testTime, entry.Timestamp)
	}

	if len(entry.Key) > 0 {
		t.Errorf("Expected nil key, got %q", entry.Key)
	}

	if !bytes.Equal(entry.Data, testData) {
		t.Errorf("Data mismatch: expected %q, got %q", testData, entry.Data)
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

	// Write message entry (version 2 format: timestamp, key size, message size, key, message)
	timestampBuf := make([]byte, TimestampSize)
	binary.BigEndian.PutUint64(timestampBuf, uint64(testTime.Unix()))
	buf.Write(timestampBuf)

	keySizeBuf := make([]byte, KeySizeFieldSize)
	binary.BigEndian.PutUint64(keySizeBuf, 0) // No key
	buf.Write(keySizeBuf)

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
	entry, err := decoder.Read()
	afterRead := time.Now()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// With preserveTimestamps=false, should get current time
	if entry.Timestamp.Before(beforeRead) || entry.Timestamp.After(afterRead) {
		t.Errorf("Expected timestamp between %v and %v, got %v", beforeRead, afterRead, entry.Timestamp)
	}

	if len(entry.Key) > 0 {
		t.Errorf("Expected nil key, got %q", entry.Key)
	}

	if !bytes.Equal(entry.Data, testData) {
		t.Errorf("Data mismatch: expected %q, got %q", testData, entry.Data)
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

	// Write all messages (version 2 format)
	for _, msg := range messages {
		timestampBuf := make([]byte, TimestampSize)
		binary.BigEndian.PutUint64(timestampBuf, uint64(msg.timestamp.Unix()))
		buf.Write(timestampBuf)

		keySizeBuf := make([]byte, KeySizeFieldSize)
		binary.BigEndian.PutUint64(keySizeBuf, 0) // No key
		buf.Write(keySizeBuf)

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
		entry, err := decoder.Read()
		if err != nil {
			t.Fatalf("Read %d failed: %v", i, err)
		}

		if !entry.Timestamp.Equal(expectedMsg.timestamp) {
			t.Errorf("Message %d timestamp mismatch: expected %v, got %v", i, expectedMsg.timestamp, entry.Timestamp)
		}

		if len(entry.Key) > 0 {
			t.Errorf("Message %d: expected nil key, got %q", i, entry.Key)
		}

		if !bytes.Equal(entry.Data, expectedMsg.data) {
			t.Errorf("Message %d data mismatch: expected %q, got %q", i, expectedMsg.data, entry.Data)
		}
	}

	// Should return EOF
	_, err = decoder.Read()
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

	// Write message entry (version 2 format: timestamp, key size, message size, key, message)
	timestampBuf := make([]byte, TimestampSize)
	binary.BigEndian.PutUint64(timestampBuf, uint64(testTime.Unix()))
	buf.Write(timestampBuf)

	keySizeBuf := make([]byte, KeySizeFieldSize)
	binary.BigEndian.PutUint64(keySizeBuf, 0) // No key
	buf.Write(keySizeBuf)

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
	entry1, err := decoder.Read()
	if err != nil {
		t.Fatalf("First Read failed: %v", err)
	}

	// Reset
	if err := decoder.Reset(); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Read again - should get the same message
	entry2, err := decoder.Read()
	if err != nil {
		t.Fatalf("Second Read failed: %v", err)
	}

	if !entry1.Timestamp.Equal(entry2.Timestamp) {
		t.Errorf("Timestamps don't match after reset: %v != %v", entry1.Timestamp, entry2.Timestamp)
	}

	if !bytes.Equal(entry1.Key, entry2.Key) {
		t.Errorf("Keys don't match after reset: %q != %q", entry1.Key, entry2.Key)
	}

	if !bytes.Equal(entry1.Data, entry2.Data) {
		t.Errorf("Data doesn't match after reset: %q != %q", entry1.Data, entry2.Data)
	}
}

func TestDecodeReader_EmptyMessage(t *testing.T) {
	// Create a file with header and empty message
	buf := &bytes.Buffer{}
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:HeaderVersionSize], uint32(ProtocolVersion))
	buf.Write(header)

	testTime := time.Date(2024, 2, 2, 10, 15, 30, 0, time.UTC)

	// Write message entry with empty data (version 2 format)
	timestampBuf := make([]byte, TimestampSize)
	binary.BigEndian.PutUint64(timestampBuf, uint64(testTime.Unix()))
	buf.Write(timestampBuf)

	keySizeBuf := make([]byte, KeySizeFieldSize)
	binary.BigEndian.PutUint64(keySizeBuf, 0) // No key
	buf.Write(keySizeBuf)

	sizeBuf := make([]byte, SizeFieldSize)
	binary.BigEndian.PutUint64(sizeBuf, 0) // Empty message
	buf.Write(sizeBuf)

	reader := bytes.NewReader(buf.Bytes())
	decoder, err := NewDecodeReader(reader, true)
	if err != nil {
		t.Fatalf("NewDecodeReader failed: %v", err)
	}

	entry, err := decoder.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !entry.Timestamp.Equal(testTime) {
		t.Errorf("Timestamp mismatch: expected %v, got %v", testTime, entry.Timestamp)
	}

	if len(entry.Key) > 0 {
		t.Errorf("Expected nil key, got %q", entry.Key)
	}

	if len(entry.Data) != 0 {
		t.Errorf("Expected empty data, got %q", entry.Data)
	}
}

func TestDecodeReader_InvalidSize(t *testing.T) {
	// Create a file with invalid message size
	buf := &bytes.Buffer{}
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:HeaderVersionSize], uint32(ProtocolVersion))
	buf.Write(header)

	testTime := time.Date(2024, 2, 2, 10, 15, 30, 0, time.UTC)

	// Write message entry with invalid size (too large) - version 2 format
	timestampBuf := make([]byte, TimestampSize)
	binary.BigEndian.PutUint64(timestampBuf, uint64(testTime.Unix()))
	buf.Write(timestampBuf)

	keySizeBuf := make([]byte, KeySizeFieldSize)
	binary.BigEndian.PutUint64(keySizeBuf, 0) // No key
	buf.Write(keySizeBuf)

	sizeBuf := make([]byte, SizeFieldSize)
	binary.BigEndian.PutUint64(sizeBuf, uint64(200*1024*1024)) // 200MB - exceeds limit
	buf.Write(sizeBuf)

	reader := bytes.NewReader(buf.Bytes())
	decoder, err := NewDecodeReader(reader, true)
	if err != nil {
		t.Fatalf("NewDecodeReader failed: %v", err)
	}

	_, err = decoder.Read()
	if err == nil {
		t.Fatal("Expected error for invalid message size, got nil")
	}
}

// TestDecodeReader_Version1BackwardCompatibility tests reading version 1 files (legacy format without keys)
func TestDecodeReader_Version1BackwardCompatibility(t *testing.T) {
	// Create a version 1 file (legacy format: timestamp, message size, message data)
	buf := &bytes.Buffer{}
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:HeaderVersionSize], uint32(ProtocolVersion1))
	buf.Write(header)

	testTime := time.Date(2024, 2, 2, 10, 15, 30, 0, time.UTC)
	testData := []byte("Hello from version 1!")

	// Write message entry in version 1 format (no key size field)
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

	// Verify protocol version is detected correctly
	if decoder.protocolVersion != ProtocolVersion1 {
		t.Errorf("Expected protocol version %d, got %d", ProtocolVersion1, decoder.protocolVersion)
	}

	// Read message
	entry, err := decoder.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !entry.Timestamp.Equal(testTime) {
		t.Errorf("Timestamp mismatch: expected %v, got %v", testTime, entry.Timestamp)
	}

	// Version 1 files have no key
	if len(entry.Key) > 0 {
		t.Errorf("Expected nil key for version 1 file, got %q", entry.Key)
	}

	if !bytes.Equal(entry.Data, testData) {
		t.Errorf("Data mismatch: expected %q, got %q", testData, entry.Data)
	}
}

// TestDecodeReader_Version2WithKey tests reading version 2 files with message keys
func TestDecodeReader_Version2WithKey(t *testing.T) {
	// Create a version 2 file with a key
	buf := &bytes.Buffer{}
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:HeaderVersionSize], uint32(ProtocolVersion))
	buf.Write(header)

	testTime := time.Date(2024, 2, 2, 10, 15, 30, 0, time.UTC)
	testKey := []byte("message-key")
	testData := []byte("Hello with key!")

	// Write message entry in version 2 format
	timestampBuf := make([]byte, TimestampSize)
	binary.BigEndian.PutUint64(timestampBuf, uint64(testTime.Unix()))
	buf.Write(timestampBuf)

	keySizeBuf := make([]byte, KeySizeFieldSize)
	binary.BigEndian.PutUint64(keySizeBuf, uint64(len(testKey)))
	buf.Write(keySizeBuf)

	sizeBuf := make([]byte, SizeFieldSize)
	binary.BigEndian.PutUint64(sizeBuf, uint64(len(testData)))
	buf.Write(sizeBuf)

	buf.Write(testKey)
	buf.Write(testData)

	reader := bytes.NewReader(buf.Bytes())
	decoder, err := NewDecodeReader(reader, true)
	if err != nil {
		t.Fatalf("NewDecodeReader failed: %v", err)
	}

	// Read message
	entry, err := decoder.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !entry.Timestamp.Equal(testTime) {
		t.Errorf("Timestamp mismatch: expected %v, got %v", testTime, entry.Timestamp)
	}

	if !bytes.Equal(entry.Key, testKey) {
		t.Errorf("Key mismatch: expected %q, got %q", testKey, entry.Key)
	}

	if !bytes.Equal(entry.Data, testData) {
		t.Errorf("Data mismatch: expected %q, got %q", testData, entry.Data)
	}
}
