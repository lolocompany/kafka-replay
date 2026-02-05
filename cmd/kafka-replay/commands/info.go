package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/lolocompany/kafka-replay/cmd/kafka-replay/util"
	"github.com/lolocompany/kafka-replay/pkg"
	"github.com/urfave/cli/v3"
)

func InfoCommand() *cli.Command {
	return &cli.Command{
		Name:        "info",
		Usage:       "Display information about Kafka brokers and topics",
		Description: "Collect and display information about Kafka brokers, topics, and partitions in JSON format.",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:    "broker",
				Aliases: []string{"b"},
				Usage:   "Kafka broker address(es) (can be specified multiple times). Defaults to KAFKA_BROKERS env var if not provided.",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			brokers, err := util.ResolveBrokers(cmd.StringSlice("broker"))
			if err != nil {
				return err
			}

			clusterInfo, err := pkg.CollectInfo(ctx, pkg.InfoConfig{
				Brokers: brokers,
			})
			if err != nil {
				return err
			}

			jsonBytes, err := json.MarshalIndent(clusterInfo, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal JSON: %w", err)
			}
			_, err = fmt.Fprintf(os.Stdout, "%s\n", jsonBytes)
			return err
		},
	}
}
