package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/lolocompany/kafka-replay/pkg"
	"github.com/urfave/cli/v3"
)

type catMessage struct {
	Timestamp string `json:"timestamp"`
	Data      string `json:"data"`
}

func CatCommand() *cli.Command {
	return &cli.Command{
		Name:        "cat",
		Usage:       "Display recorded messages from a message file",
		Description: "Read and display messages from a binary message file in human-readable format.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "input",
				Aliases:  []string{"i"},
				Usage:    "Input file path containing recorded messages",
				Required: true,
			},
			&cli.BoolFlag{
				Name:    "raw",
				Aliases: []string{"r"},
				Usage:   "Output only the raw message data, excluding timestamps and JSON formatting",
				Value:   false,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			input := cmd.String("input")
			raw := cmd.Bool("raw")

			// Open input file
			file, err := os.Open(input)
			if err != nil {
				return fmt.Errorf("failed to open input file: %w", err)
			}
			defer file.Close()

			fmt.Fprintf(os.Stderr, "Reading messages from: %s\n", input)

			if raw {
				// Raw mode: output only the data bytes directly
				if err := pkg.CatRaw(ctx, file, os.Stdout); err != nil {
					return err
				}
			} else {
				// JSON mode: format as JSON with timestamp
				formatter := func(timestamp time.Time, data []byte) string {
					catMessage := catMessage{
						Timestamp: timestamp.Format(time.RFC3339Nano),
						Data:      string(data),
					}
					jsonMessage, err := json.Marshal(catMessage)
					if err != nil {
						return fmt.Sprintf("{\"error\":\"%s\"}", err.Error())
					}
					return string(jsonMessage)
				}

				if err := pkg.Cat(ctx, pkg.CatConfig{
					Reader:    file,
					Formatter: formatter,
					Output:    os.Stdout,
				}); err != nil {
					return err
				}
			}
			return nil
		},
	}
}
