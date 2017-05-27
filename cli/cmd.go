package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

// NewCmd creates new root command.
func NewCmd(cmdName string, envPrefix string, appName string, appVersion string, appSubtitle string) (*cobra.Command, *viper.Viper) {
	title := fmt.Sprintf("%s %s - %s", appName, appVersion, appSubtitle)

	// Viper = Configuration store
	v := viper.New()
	v.Set("cmdName", cmdName)
	v.Set("envPrefix", envPrefix)
	v.Set("appName", appName)
	v.Set("appVersion", appVersion)
	v.Set("appSubtitle", appSubtitle)

	cmd := &cobra.Command{
		Use:           fmt.Sprintf("%s [host] database collection", cmdName),
		Short:         title,
		Long:          title,
		SilenceUsage:  true,
		SilenceErrors: true,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return PreRun(cmd, v, os.Args, args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			config, err := GetConfig(v)
			if err != nil {
				return err
			}

			return Run(cmd, config)
		},
	}

	InitFlags(cmd, v, envPrefix)

	return cmd, v
}
