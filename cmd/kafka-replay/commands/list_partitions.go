package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/lolocompany/kafka-replay/pkg"
	"github.com/urfave/cli/v3"
)

func listPartitionsCommand() *cli.Command {
	return &cli.Command{
		Name:        "partitions",
		Aliases:     []string{"topics"},
		Usage:       "List partitions with their leaders",
		Description: "Display topic-partition pairs with their leader brokers as JSON objects (one per line).",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:     "broker",
				Aliases:  []string{"b"},
				Usage:    "Kafka broker address(es) (can be specified multiple times). Defaults to KAFKA_BROKERS env var if not provided.",
				Sources:  cli.EnvVars("KAFKA_BROKERS"),
			},
			&cli.BoolFlag{
				Name:  "offsets",
				Usage: "Include earliest and latest offsets for each partition",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "replicas",
				Usage: "Include replica assignment details (replicas and in-sync-replicas)",
				Value: false,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			brokers := cmd.StringSlice("broker")
			if len(brokers) == 0 {
				return fmt.Errorf("broker address(es) must be provided via --broker flag or KAFKA_BROKERS environment variable")
			}

			includeOffsets := cmd.Bool("offsets")
			includeReplicas := cmd.Bool("replicas")

			partitions, err := pkg.ListPartitions(ctx, brokers, includeOffsets, includeReplicas)
			if err != nil {
				return err
			}

			// Output one JSON object per partition (compact format)
			encoder := json.NewEncoder(os.Stdout)
			for _, partition := range partitions {
				if err := encoder.Encode(partition); err != nil {
					return fmt.Errorf("failed to encode partition JSON: %w", err)
				}
			}
			return nil
		},
	}
}
