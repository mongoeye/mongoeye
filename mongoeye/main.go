package main

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/mongoeye/mongoeye/cli"
	"os"
)

// CmdName - name of command.
const CmdName = "mongoeye"

// EnvPrefix - environment variables prefix.
// Environment variables can be used instead of flags.
const EnvPrefix = CmdName

// AppName - name of the application that appears in the help.
const AppName = "MongoEYE"

// AppVersion - version of application.
// This value is automatically set on release deploy (see .travis.yml).
const AppVersion = "DEV"

// AppSubtitle - application subtitle that appears in the help.
const AppSubtitle = "MongoDB exploration tool"

func main() {
	cmd, _ := cli.NewCmd(CmdName, EnvPrefix, AppName, AppVersion, AppSubtitle)
	cmd.SetOutput(color.Output)

	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
