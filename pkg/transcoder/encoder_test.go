package transcoder

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestNewEncodeWriter(t *testing.T) {
	buf := &bytes.Buffer{}
	encoder, err := NewEncodeWriter(buf)
	if err != nil {
		t.Fatalf("NewEncodeWriter failed: %v", err)
	}

	// Verify header was written
	if encoder.TotalBytes() != HeaderSize {
		t.Errorf("Expected total bytes to be %d, got %d", HeaderSize, encoder.TotalBytes())
	}

	// Verify header content
	header := buf.Bytes()[:HeaderSize]
	if len(header) != HeaderSize {
		t.Fatalf("Header size mismatch: expected %d, got %d", HeaderSize, len(header))
	}

	// Check protocol version
	version := binary.BigEndian.Uint32(header[0:HeaderVersionSize])
	if version != ProtocolVersion {
		t.Errorf("Protocol version mismatch: expected %d, got %d", ProtocolVersion, version)
	}

	// Check reserved bytes are zero
	for i := HeaderVersionSize; i < HeaderSize; i++ {
		if header[i] != 0 {
			t.Errorf("Reserved byte at offset %d should be 0, got %d", i, header[i])
		}
	}
}

func TestEncodeWriter_Write(t *testing.T) {
	buf := &bytes.Buffer{}
	encoder, err := NewEncodeWriter(buf)
	if err != nil {
		t.Fatalf("NewEncodeWriter failed: %v", err)
	}

	testTime := time.Date(2024, 2, 2, 10, 15, 30, 0, time.UTC)
	testData := []byte("Hello, World!")

	bytesWritten, err := encoder.Write(testTime, testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	expectedBytes := int64(TimestampSize + SizeFieldSize + len(testData))
	if bytesWritten != expectedBytes {
		t.Errorf("Expected %d bytes written, got %d", expectedBytes, bytesWritten)
	}

	// Verify total bytes
	expectedTotal := HeaderSize + expectedBytes
	if encoder.TotalBytes() != expectedTotal {
		t.Errorf("Expected total bytes %d, got %d", expectedTotal, encoder.TotalBytes())
	}

	// Verify written data
	allData := buf.Bytes()
	offset := HeaderSize

	// Check timestamp
	timestampBytes := allData[offset : offset+TimestampSize]
	unixTimestamp := int64(binary.BigEndian.Uint64(timestampBytes))
	if unixTimestamp != testTime.Unix() {
		t.Errorf("Timestamp mismatch: expected %d, got %d", testTime.Unix(), unixTimestamp)
	}
	offset += TimestampSize

	// Check size
	sizeBytes := allData[offset : offset+SizeFieldSize]
	size := int64(binary.BigEndian.Uint64(sizeBytes))
	if size != int64(len(testData)) {
		t.Errorf("Size mismatch: expected %d, got %d", len(testData), size)
	}
	offset += SizeFieldSize

	// Check data
	dataBytes := allData[offset : offset+len(testData)]
	if !bytes.Equal(dataBytes, testData) {
		t.Errorf("Data mismatch: expected %q, got %q", testData, dataBytes)
	}
}

func TestEncodeWriter_MultipleWrites(t *testing.T) {
	buf := &bytes.Buffer{}
	encoder, err := NewEncodeWriter(buf)
	if err != nil {
		t.Fatalf("NewEncodeWriter failed: %v", err)
	}

	messages := []struct {
		timestamp time.Time
		data      []byte
	}{
		{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), []byte("First message")},
		{time.Date(2024, 1, 1, 0, 0, 1, 0, time.UTC), []byte("Second message")},
		{time.Date(2024, 1, 1, 0, 0, 2, 0, time.UTC), []byte("Third message")},
	}

	var totalWritten int64 = HeaderSize
	for i, msg := range messages {
		bytesWritten, err := encoder.Write(msg.timestamp, msg.data)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
		totalWritten += bytesWritten
	}

	if encoder.TotalBytes() != totalWritten {
		t.Errorf("Total bytes mismatch: expected %d, got %d", totalWritten, encoder.TotalBytes())
	}

	// Verify all messages are present
	allData := buf.Bytes()
	offset := HeaderSize

	for i, msg := range messages {
		// Read timestamp
		timestampBytes := allData[offset : offset+TimestampSize]
		unixTimestamp := int64(binary.BigEndian.Uint64(timestampBytes))
		if unixTimestamp != msg.timestamp.Unix() {
			t.Errorf("Message %d timestamp mismatch: expected %d, got %d", i, msg.timestamp.Unix(), unixTimestamp)
		}
		offset += TimestampSize

		// Read size
		sizeBytes := allData[offset : offset+SizeFieldSize]
		size := int64(binary.BigEndian.Uint64(sizeBytes))
		if size != int64(len(msg.data)) {
			t.Errorf("Message %d size mismatch: expected %d, got %d", i, len(msg.data), size)
		}
		offset += SizeFieldSize

		// Read data
		dataBytes := allData[offset : offset+len(msg.data)]
		if !bytes.Equal(dataBytes, msg.data) {
			t.Errorf("Message %d data mismatch: expected %q, got %q", i, msg.data, dataBytes)
		}
		offset += len(msg.data)
	}
}

func TestEncodeWriter_EmptyMessage(t *testing.T) {
	buf := &bytes.Buffer{}
	encoder, err := NewEncodeWriter(buf)
	if err != nil {
		t.Fatalf("NewEncodeWriter failed: %v", err)
	}

	testTime := time.Date(2024, 2, 2, 10, 15, 30, 0, time.UTC)
	emptyData := []byte{}

	bytesWritten, err := encoder.Write(testTime, emptyData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	expectedBytes := int64(TimestampSize + SizeFieldSize)
	if bytesWritten != expectedBytes {
		t.Errorf("Expected %d bytes written, got %d", expectedBytes, bytesWritten)
	}

	// Verify size field is 0
	allData := buf.Bytes()
	offset := HeaderSize + TimestampSize
	sizeBytes := allData[offset : offset+SizeFieldSize]
	size := int64(binary.BigEndian.Uint64(sizeBytes))
	if size != 0 {
		t.Errorf("Expected size 0, got %d", size)
	}
}

func TestEncodeWriter_LargeMessage(t *testing.T) {
	buf := &bytes.Buffer{}
	encoder, err := NewEncodeWriter(buf)
	if err != nil {
		t.Fatalf("NewEncodeWriter failed: %v", err)
	}

	testTime := time.Date(2024, 2, 2, 10, 15, 30, 0, time.UTC)
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	bytesWritten, err := encoder.Write(testTime, largeData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	expectedBytes := TimestampSize + SizeFieldSize + int64(len(largeData))
	if bytesWritten != expectedBytes {
		t.Errorf("Expected %d bytes written, got %d", expectedBytes, bytesWritten)
	}
}
