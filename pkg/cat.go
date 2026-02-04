package pkg

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/lolocompany/kafka-replay/pkg/transcoder"
)

type CatConfig struct {
	Reader             io.ReadSeeker
	PreserveTimestamps bool
	Formatter          func(timestamp time.Time, data []byte) string
	Output             io.Writer
}

func Cat(ctx context.Context, cfg CatConfig) error {
	decoder, err := transcoder.NewDecodeReader(cfg.Reader, cfg.PreserveTimestamps)
	if err != nil {
		return err
	}
	defer decoder.Close()

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read next complete message
		timestamp, data, err := decoder.Read()
		if err != nil {
			if err == io.EOF {
				// End of file reached
				break
			}
			return err
		}

		// Display message
		formattedMessage := cfg.Formatter(timestamp, data)
		if cfg.Output != nil {
			fmt.Fprintf(cfg.Output, "%s\n", formattedMessage)
		}
	}

	return nil
}

// CatRaw reads messages from a reader and writes only the raw data bytes to the output
func CatRaw(ctx context.Context, reader io.ReadSeeker, output io.Writer) error {
	decoder, err := transcoder.NewDecodeReader(reader, false)
	if err != nil {
		return err
	}
	defer decoder.Close()

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read next complete message
		_, data, err := decoder.Read()
		if err != nil {
			if err == io.EOF {
				// End of file reached
				break
			}
			return err
		}

		// Write raw data directly
		if output != nil {
			if _, err := output.Write(data); err != nil {
				return err
			}
		}
	}

	return nil
}
