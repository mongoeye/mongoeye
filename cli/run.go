package cli

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/mgo.v2"
	"io"
	"os"
)

var noOp = func(cmd *cobra.Command, args []string) error { return nil }

// Run command.
func Run(cmd *cobra.Command, config *Config) error {
	var out io.Writer = cmd.OutOrStdout()
	var outFile *os.File

	// Open output file
	if config.FilePath != "" {
		var err error
		outFile, err = os.Create(config.FilePath)
		if err != nil {
			return fmt.Errorf("Cannot open output file '%s'.\n", config.FilePath)
		}

		defer func() {
			if err := outFile.Close(); err != nil {
				cmd.OutOrStderr().Write([]byte(fmt.Sprintf("Cannot close output file '%s'.\n", config.FilePath)))
			}
		}()
	}

	// Print info messages?
	printInfo := outFile != nil || config.Format == "table"

	// Header
	if printInfo {
		fmt.Fprintf(out, "%s\n\n", cmd.Short)
	}

	// Connect to MongoDB
	info, collection, count, err := connect(out, printInfo, config)
	if err != nil {
		return err
	}

	// Generate possible plans of analysis
	allPlans := generateAnalysisPlans(info, count, config)

	// Run analysis
	result, err := runAnalysis(out, printInfo, allPlans, collection)
	if err != nil {
		return err
	}

	// Format results
	output, err := Format(result, config)
	if err != nil {
		return fmt.Errorf("Cannot format results: %s.\n", err)
	}

	// Write results
	if outFile == nil {
		out.Write(output)
	} else {
		outFile.Write(output)
	}

	// Footer
	if printInfo {
		if outFile != nil {
			fmt.Fprintf(
				out,
				"The analysis results were written to the file: %s.\n",
				outFile.Name(),
			)
		}

		// Print statistics
		plan := "analysis in database"
		if result.Plan == "local" {
			plan = "local analysis"
		}
		fmt.Fprintf(
			out,
			"\nOK  %.3fs (%s)\n    %d/%d docs (%.1f%%)\n    %d fields, depth %d\n",
			result.Duration.Seconds(),
			plan,
			result.DocsCount,
			result.AllDocsCount,
			(float64(result.DocsCount)/float64(result.AllDocsCount))*100.0,
			result.FieldsCount,
			config.Depth,
		)
	}

	return nil
}

// Connect to MongoDB, show spinner
func connect(out io.Writer, printInfo bool, config *Config) (info mgo.BuildInfo, collection *mgo.Collection, count int, err error) {
	task := func() {
		info, _, collection, count, err = Connect(config)
	}

	if printInfo {
		RunWithSpinner(out, "Connecting:", task)
		if err == nil {
			fmt.Fprint(out, "OK\n\n")
		} else {
			fmt.Fprint(out, "Error\n\n")
		}
	} else {
		task()
	}

	return
}

// Run analysis, show spinner
func runAnalysis(out io.Writer, printInfo bool, allPlans plans, collection *mgo.Collection) (result Result, err error) {
	task := func() {
		result = allPlans[0].Run(collection)
	}

	if printInfo {
		RunWithSpinner(out, "Analyzing:", task)
		fmt.Fprint(out, "OK\n\n")
	} else {
		task()
	}

	return
}

// PreRun prints help, version and validate arguments.
func PreRun(cmd *cobra.Command, v *viper.Viper, osArgs []string, args []string) error {
	out := cmd.OutOrStdout()

	// Help
	if len(osArgs) == 1 || v.GetBool("help") {
		cmd.Help()
		cmd.RunE = noOp
		return nil
	}

	// Version
	if v.GetBool("version") {
		fmt.Fprintf(out, "%s %s\n", v.GetString("appName"), v.GetString("appVersion"))
		cmd.RunE = noOp
		return nil
	}

	// Arguments
	if len(args) == 1 {
		v.Set("col", args[0])
	}
	if len(args) == 2 {
		v.Set("db", args[0])
		v.Set("col", args[1])
	}
	if len(args) == 3 {
		v.Set("host", args[0])
		v.Set("db", args[1])
		v.Set("col", args[2])
	}
	if len(args) > 3 {
		return errors.New("Too many arguments.\n")
	}

	// Validate arguments
	if v.GetString("db") == "" || v.GetString("col") == "" || v.GetString("host") == "" {
		return errors.New("Please specify the name of the database, collection, and host,\nusing arguments, flags, or environment variables.\n")
	}

	return nil
}
