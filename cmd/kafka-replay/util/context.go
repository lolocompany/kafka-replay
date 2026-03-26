package util

import (
	"github.com/lolocompany/kafka-replay/v2/cmd/kafka-replay/config"
	"github.com/urfave/cli/v3"
)

// ResolveBrokers returns the broker list for the current invocation by reading
// --config, --profile, and --brokers from the command.
func ResolveBrokers(cmd *cli.Command) ([]string, error) {
	c, err := config.LoadConfig(cmd.String("config"))
	if err != nil {
		return nil, err
	}
	return config.ResolveBrokers(cmd.StringSlice("brokers"), cmd.String("profile"), c)
}
