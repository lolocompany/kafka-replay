package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/lolocompany/kafka-replay/pkg"
	"github.com/urfave/cli/v3"
)

func listConsumerGroupsCommand() *cli.Command {
	return &cli.Command{
		Name:        "consumer-groups",
		Aliases:     []string{"groups"},
		Usage:       "List consumer groups",
		Description: "Display consumer groups as JSON objects (one per line).",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:    "broker",
				Aliases: []string{"b"},
				Usage:   "Kafka broker address(es) (can be specified multiple times). Defaults to KAFKA_BROKERS env var if not provided.",
				Sources: cli.EnvVars("KAFKA_BROKERS"),
			},
			&cli.BoolFlag{
				Name:  "offsets",
				Usage: "Include offset information for each partition",
				Value: false,
			},
			&cli.BoolFlag{
				Name:    "members",
				Aliases: []string{"m"},
				Usage:   "Include member information for each group",
				Value:   false,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			brokers := cmd.StringSlice("broker")
			if len(brokers) == 0 {
				return fmt.Errorf("broker address(es) must be provided via --broker flag or KAFKA_BROKERS environment variable")
			}

			includeOffsets := cmd.Bool("offsets")
			includeMembers := cmd.Bool("members")

			groups, err := pkg.ListConsumerGroups(ctx, brokers, includeOffsets, includeMembers)
			if err != nil {
				return err
			}

			// Output one JSON object per line (compact format)
			encoder := json.NewEncoder(os.Stdout)
			for _, group := range groups {
				if err := encoder.Encode(group); err != nil {
					return fmt.Errorf("failed to encode group JSON: %w", err)
				}
			}
			return nil
		},
	}
}
