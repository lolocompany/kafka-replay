package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/lolocompany/kafka-replay/pkg"
	"github.com/urfave/cli/v3"
)

func listBrokersCommand() *cli.Command {
	return &cli.Command{
		Name:        "brokers",
		Usage:       "List Kafka brokers with reachability status",
		Description: "Display broker addresses and their reachability status as JSON objects (one per line).",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:     "broker",
				Aliases:  []string{"b"},
				Usage:    "Kafka broker address(es) (can be specified multiple times). Defaults to KAFKA_BROKERS env var if not provided.",
				Sources:  cli.EnvVars("KAFKA_BROKERS"),
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			brokers := cmd.StringSlice("broker")
			if len(brokers) == 0 {
				return fmt.Errorf("broker address(es) must be provided via --broker flag or KAFKA_BROKERS environment variable")
			}

			brokerList, err := pkg.ListBrokers(ctx, brokers)
			if err != nil {
				return err
			}

			// Output one JSON object per line (compact format)
			encoder := json.NewEncoder(os.Stdout)
			for _, broker := range brokerList {
				if err := encoder.Encode(broker); err != nil {
					return fmt.Errorf("failed to encode broker JSON: %w", err)
				}
			}
			return nil
		},
	}
}
