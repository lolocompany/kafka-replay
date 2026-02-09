package commands

import (
	"github.com/urfave/cli/v3"
)

func ListCommand() *cli.Command {
	return &cli.Command{
		Name:        "list",
		Aliases:     []string{"ls"},
		Usage:       "List Kafka resources",
		Description: "List Kafka resources with their details.",
		Commands: []*cli.Command{
			listBrokersCommand(),
			listPartitionsCommand(),
			listConsumerGroupsCommand(),
		},
	}
}
