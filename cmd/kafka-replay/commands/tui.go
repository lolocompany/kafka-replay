package commands

import (
	"context"

	"github.com/lolocompany/kafka-replay/v2/cmd/kafka-replay/tui"
	"github.com/lolocompany/kafka-replay/v2/cmd/kafka-replay/util"
	"github.com/urfave/cli/v3"
)

// TUICommand launches the interactive terminal UI (tview).
func TUICommand() *cli.Command {
	return &cli.Command{
		Name:        "tui",
		Usage:       "Start the interactive terminal UI",
		Description: "Launch an interactive terminal user interface for exploring and using kafka-replay functionality.",
		Flags:       util.GlobalFlags(),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			brokers, err := util.ResolveBrokers(cmd)
			if err != nil {
				return err
			}
			cfg := tui.Config{
				Brokers: brokers,
			}
			return tui.Run(ctx, cfg)
		},
	}
}
