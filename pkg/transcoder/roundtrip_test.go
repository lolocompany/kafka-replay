package transcoder

import (
	"bytes"
	"io"
	"testing"
	"time"
)

// TestEncodeThenDecode tests encoding messages and then decoding them (forward sequence)
func TestEncodeThenDecode(t *testing.T) {
	buf := &bytes.Buffer{}

	// Encode messages
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

	for _, msg := range messages {
		_, err := encoder.Write(msg.timestamp, msg.data, nil)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if err := encoder.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Decode messages
	reader := bytes.NewReader(buf.Bytes())
	decoder, err := NewDecodeReader(reader, true)
	if err != nil {
		t.Fatalf("NewDecodeReader failed: %v", err)
	}

	for i, expectedMsg := range messages {
		entry, err := decoder.Read()
		if err != nil {
			t.Fatalf("Read %d failed: %v", i, err)
		}

		if !entry.Timestamp.Equal(expectedMsg.timestamp) {
			t.Errorf("Message %d timestamp mismatch: expected %v, got %v", i, expectedMsg.timestamp, entry.Timestamp)
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

	if err := decoder.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// TestDecodeThenEncode tests decoding messages and then encoding them again (reverse sequence)
func TestDecodeThenEncode(t *testing.T) {
	// First, create a file with encoded messages
	originalBuf := &bytes.Buffer{}
	encoder, err := NewEncodeWriter(originalBuf)
	if err != nil {
		t.Fatalf("NewEncodeWriter failed: %v", err)
	}

	originalMessages := []struct {
		timestamp time.Time
		data      []byte
	}{
		{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), []byte("Original first")},
		{time.Date(2024, 1, 1, 0, 0, 1, 0, time.UTC), []byte("Original second")},
		{time.Date(2024, 1, 1, 0, 0, 2, 0, time.UTC), []byte("Original third")},
	}

	for _, msg := range originalMessages {
		_, err := encoder.Write(msg.timestamp, msg.data, nil)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}
	encoder.Close()

	// Decode messages
	reader := bytes.NewReader(originalBuf.Bytes())
	decoder, err := NewDecodeReader(reader, true)
	if err != nil {
		t.Fatalf("NewDecodeReader failed: %v", err)
	}

	decodedMessages := []struct {
		timestamp time.Time
		data      []byte
	}{}

	for {
		entry, err := decoder.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		decodedMessages = append(decodedMessages, struct {
			timestamp time.Time
			data      []byte
		}{entry.Timestamp, entry.Data})
		// Note: key is discarded in this test
	}
	decoder.Close()

	// Encode decoded messages to a new buffer
	newBuf := &bytes.Buffer{}
	newEncoder, err := NewEncodeWriter(newBuf)
	if err != nil {
		t.Fatalf("NewEncodeWriter failed: %v", err)
	}

	for _, msg := range decodedMessages {
		_, err := newEncoder.Write(msg.timestamp, msg.data, nil)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}
	newEncoder.Close()

	// Verify the re-encoded data matches the original (excluding headers)
	originalData := originalBuf.Bytes()[HeaderSize:]
	newData := newBuf.Bytes()[HeaderSize:]

	if !bytes.Equal(originalData, newData) {
		t.Errorf("Re-encoded data doesn't match original")
		t.Errorf("Original length: %d, New length: %d", len(originalData), len(newData))
	}

	// Verify we can decode the re-encoded data
	newReader := bytes.NewReader(newBuf.Bytes())
	newDecoder, err := NewDecodeReader(newReader, true)
	if err != nil {
		t.Fatalf("NewDecodeReader failed: %v", err)
	}

	for i, expectedMsg := range originalMessages {
		entry, err := newDecoder.Read()
		if err != nil {
			t.Fatalf("Read %d failed: %v", i, err)
		}

		if !entry.Timestamp.Equal(expectedMsg.timestamp) {
			t.Errorf("Message %d timestamp mismatch: expected %v, got %v", i, expectedMsg.timestamp, entry.Timestamp)
		}

		if !bytes.Equal(entry.Data, expectedMsg.data) {
			t.Errorf("Message %d data mismatch: expected %q, got %q", i, expectedMsg.data, entry.Data)
		}
	}

	newDecoder.Close()
}

// TestRoundTripMultipleSequences tests multiple encode-decode cycles
func TestRoundTripMultipleSequences(t *testing.T) {
	messages := []struct {
		timestamp time.Time
		data      []byte
	}{
		{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), []byte("Message 1")},
		{time.Date(2024, 1, 1, 0, 0, 1, 0, time.UTC), []byte("Message 2")},
		{time.Date(2024, 1, 1, 0, 0, 2, 0, time.UTC), []byte("Message 3")},
	}

	buf := &bytes.Buffer{}

	// Multiple encode-decode cycles
	for cycle := 0; cycle < 3; cycle++ {
		// Encode
		encoder, err := NewEncodeWriter(buf)
		if err != nil {
			t.Fatalf("Cycle %d: NewEncodeWriter failed: %v", cycle, err)
		}

		for _, msg := range messages {
			_, err := encoder.Write(msg.timestamp, msg.data, nil)
			if err != nil {
				t.Fatalf("Cycle %d: Write failed: %v", cycle, err)
			}
		}
		encoder.Close()

		// Decode
		reader := bytes.NewReader(buf.Bytes())
		decoder, err := NewDecodeReader(reader, true)
		if err != nil {
			t.Fatalf("Cycle %d: NewDecodeReader failed: %v", cycle, err)
		}

		for i, expectedMsg := range messages {
			entry, err := decoder.Read()
			if err != nil {
				t.Fatalf("Cycle %d: Read %d failed: %v", cycle, i, err)
			}

			if !entry.Timestamp.Equal(expectedMsg.timestamp) {
				t.Errorf("Cycle %d: Message %d timestamp mismatch: expected %v, got %v", cycle, i, expectedMsg.timestamp, entry.Timestamp)
			}

			if !bytes.Equal(entry.Data, expectedMsg.data) {
				t.Errorf("Cycle %d: Message %d data mismatch: expected %q, got %q", cycle, i, expectedMsg.data, entry.Data)
			}
		}

		_, err = decoder.Read()
		if err != io.EOF {
			t.Errorf("Cycle %d: Expected EOF, got %v", cycle, err)
		}

		decoder.Close()

		// Reset buffer for next cycle
		buf.Reset()
	}
}

// TestRoundTripEmptyMessage tests round-trip with empty message
func TestRoundTripEmptyMessage(t *testing.T) {
	buf := &bytes.Buffer{}

	// Encode empty message
	encoder, err := NewEncodeWriter(buf)
	if err != nil {
		t.Fatalf("NewEncodeWriter failed: %v", err)
	}

	testTime := time.Date(2024, 2, 2, 10, 15, 30, 0, time.UTC)
	_, err = encoder.Write(testTime, []byte{}, nil)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	encoder.Close()

	// Decode empty message
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

	decoder.Close()
}

// TestRoundTripLargeMessage tests round-trip with large message
func TestRoundTripLargeMessage(t *testing.T) {
	buf := &bytes.Buffer{}

	// Encode large message
	encoder, err := NewEncodeWriter(buf)
	if err != nil {
		t.Fatalf("NewEncodeWriter failed: %v", err)
	}

	testTime := time.Date(2024, 2, 2, 10, 15, 30, 0, time.UTC)
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	_, err = encoder.Write(testTime, largeData, nil)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	encoder.Close()

	// Decode large message
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

	if len(entry.Data) != len(largeData) {
		t.Errorf("Data length mismatch: expected %d, got %d", len(largeData), len(entry.Data))
	}

	if !bytes.Equal(entry.Data, largeData) {
		t.Error("Data content mismatch")
	}

	decoder.Close()
}
