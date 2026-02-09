package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"

	"github.com/urfave/cli/v3"
)

var Version = "unknown"

func getVersion() string {
	// If Version was set via -ldflags, use it (for CI/CD builds)
	if Version != "unknown" {
		return Version
	}

	// Try to get version from build info (works with go install @version)
	if info, ok := debug.ReadBuildInfo(); ok {
		// Check build settings for VCS tag first (most reliable for git tags)
		for _, setting := range info.Settings {
			if setting.Key == "vcs.tag" && setting.Value != "" {
				return setting.Value
			}
		}

		if info.Main.Version != "" {
			return info.Main.Version
		}

		// Also check if this module appears in dependencies (for some build scenarios)
		for _, dep := range info.Deps {
			if dep.Path == "github.com/lolocompany/kafka-replay" {
				if dep.Version != "" && dep.Version != "(devel)" {
					return dep.Version
				}
			}
		}
	}

	return "unknown"
}

func VersionCommand() *cli.Command {
	return &cli.Command{
		Name:        "version",
		Usage:       "Print version information",
		Description: "Display the current version of kafka-replay in JSON format with build information.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			version := getVersion()
			buildInfo, _ := debug.ReadBuildInfo()

			type VersionInfo struct {
				Version   string            `json:"version"`
				GoVersion string            `json:"go_version,omitempty"`
				GoOS      string            `json:"go_os,omitempty"`
				GoArch    string            `json:"go_arch,omitempty"`
				Build     map[string]string `json:"build,omitempty"`
			}

			versionInfo := VersionInfo{
				Version: version,
			}

			if buildInfo != nil {
				versionInfo.GoVersion = runtime.Version()
				versionInfo.GoOS = runtime.GOOS
				versionInfo.GoArch = runtime.GOARCH

				buildSettings := make(map[string]string)
				for _, setting := range buildInfo.Settings {
					if setting.Key == "vcs.revision" || setting.Key == "vcs.time" || setting.Key == "vcs.modified" {
						buildSettings[setting.Key] = setting.Value
					}
				}
				if len(buildSettings) > 0 {
					versionInfo.Build = buildSettings
				}
			}

			jsonBytes, err := json.MarshalIndent(versionInfo, "", "  ")
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(os.Stderr, "%s\n", jsonBytes)
			return err
		},
	}
}
