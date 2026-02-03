package main

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"
)

var Version = "dev"

func versionCommand() *cli.Command {
	return &cli.Command{
		Name:        "version",
		Usage:       "Print version information",
		Description: "Display the current version of kafka-replay.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			_, err := fmt.Printf("kafka-replay version %s\n", Version)
			return err
		},
	}
}
