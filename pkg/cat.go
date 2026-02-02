package pkg

import (
	"context"
	"fmt"
	"io"
	"time"
)

func Cat(ctx context.Context, input string) error {
	// Create log file reader (preserve timestamps for display)
	reader, err := NewLogFileReader(input, true)
	if err != nil {
		return err
	}
	defer reader.Close()

	var messageCount int64

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read next complete message
		msg, err := reader.ReadNextMessage(ctx)
		if err != nil {
			if err == io.EOF {
				// End of file reached
				break
			}
			return err
		}

		// Display message
		fmt.Printf("[%s] [%d bytes] %s\n", msg.Timestamp.Format(time.RFC3339Nano), len(msg.Data), string(msg.Data))

		messageCount++
	}

	return nil
}
