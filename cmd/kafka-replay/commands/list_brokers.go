package commands

import (
	"context"
	"fmt"
	"os"

	"github.com/lolocompany/kafka-replay/v2/cmd/kafka-replay/util"
	"github.com/lolocompany/kafka-replay/v2/pkg"
	"github.com/lolocompany/kafka-replay/v2/cmd/kafka-replay/output"
	"github.com/urfave/cli/v3"
)

func listBrokersCommand() *cli.Command {
	return &cli.Command{
		Name:        "brokers",
		Aliases:     []string{"broker"},
		Usage:       "List Kafka brokers with reachability status",
		Description: "Display broker addresses and their reachability status (table or json).",
		Flags:       util.GlobalFlags(),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			brokers, err := util.ResolveBrokers(cmd)
			if err != nil {
				return err
			}

			brokerList, err := pkg.ListBrokers(ctx, brokers)
			if err != nil {
				return err
			}

			format, err := output.ParseFormat(util.GetFormat(cmd), output.IsTTY(os.Stdout))
			if err != nil {
				return err
			}
			if format == output.FormatRaw {
				return fmt.Errorf("format 'raw' is only supported by the 'cat' command")
			}
			enc := output.NewEncoder(format, os.Stdout)
			if format == output.FormatTable {
				rows := make([][]string, 0, len(brokerList))
				for _, b := range brokerList {
					rows = append(rows, []string{fmt.Sprintf("%d", b.ID), b.Address, fmt.Sprintf("%t", b.Reachable)})
				}
				return enc.EncodeTable([]string{"ID", "ADDRESS", "REACHABLE"}, rows)
			}
			return output.EncodeSlice(enc, brokerList)
		},
	}
}
