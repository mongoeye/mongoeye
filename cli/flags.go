package cli

import (
	"fmt"
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Flags consist from multiple sections
type Flags struct {
	Sections []*FlagSection
}

// AddSection adds new section to flags
func (f *Flags) AddSection(name string) *FlagSection {
	section := &FlagSection{
		Name: name,
		Set:  &pflag.FlagSet{},
	}
	f.Sections = append(f.Sections, section)
	return section
}

// ExportAll exports flags from all sections
func (f *Flags) ExportAll(all *pflag.FlagSet) {
	for _, s := range f.Sections {
		s.Set.VisitAll(func(f *pflag.Flag) {
			all.AddFlag(f)
		})
	}
}

// FlagSection consists from section name and flags
type FlagSection struct {
	Name string
	Set  *pflag.FlagSet
}

// InitFlags initializes flags and their default values
func InitFlags(cmd *cobra.Command, v *viper.Viper, envPrefix string) {
	flags := &Flags{}

	var s *pflag.FlagSet

	// connection options
	s = flags.AddSection("connection options").Set
	s.String("host", "localhost:27017", "mongodb host")
	s.String("connection-mode", "SecondaryPreferred", "connection mode")
	s.Float64("connection-timeout", 5, "connection timeout")
	s.Float64("socket-timeout", 5*60, "socket timeout")
	s.Float64("sync-timeout", 5*60, "sync timeout")

	// authentication
	s = flags.AddSection("authentication").Set
	s.StringP("user", "u", "admin", "username for authentication")
	s.StringP("password", "p", "", "password for authentication")
	s.String("auth-db", "", "auth database (default: same as the working db)")
	s.String("auth-mech", "", "auth mechanism")

	// analysis options
	s = flags.AddSection("input options").Set
	s.String("db", "", "database for analysis")
	s.String("col", "", "collection for analysis")
	s.StringP("match", "", "", "filter documents before analysis (json, $match aggregation)")
	s.StringP("project", "", "", "filter/project fields before analysis (json, $project aggregation)")
	s.StringP("scope", "s", "random:1000", "all, first:N, last:N, random:N")
	s.UintP("depth", "d", 2, "max depth in nested documents")

	// statistics options
	s = flags.AddSection("output options").Set
	s.Bool("full", false, "all available analyzes")
	s.BoolP("value", "v", false, "get min, max, avg value")
	s.BoolP("length", "l", false, "get min, max, avg length")
	s.BoolP("value-hist", "V", false, "get value histogram")
	s.Uint("value-hist-steps", 100, "max steps of value histogram >=3")
	s.BoolP("length-hist", "L", false, "get length histogram")
	s.Uint("length-hist-steps", 100, "max steps of length histogram >=3")
	s.BoolP("weekday-hist", "W", false, "get weekday histogram for dates")
	s.BoolP("hour-hist", "H", false, "get hour histogram for dates")
	s.Bool("count-unique", false, "get count of unique values")
	s.Uint("most-freq", 0, "get the N most frequent values")
	s.Uint("least-freq", 0, "get the N least frequent values")
	s.StringP("format", "f", "table", "output format: table, json, yaml")
	s.StringP("file", "F", "", "path to the output file")

	// other options
	s = flags.AddSection("other options").Set
	s.StringP("timezone", "t", "local", "timezone, eg. UTC, Europe/Berlin")
	s.Bool("use-aggregation", false, fmt.Sprintf("analyze with aggregation framework (mongodb %s+)", analysis.AggregationMinVersionStr))
	s.Uint("string-max-length", 100, "max string length")
	s.Uint("array-max-length", 20, "analyze only first N array elements")
	s.Uint("concurrency", 0, "number of local processes (default 0 = auto)")
	s.Uint("buffer", 5000, "size of the buffer between local stages")
	s.Uint("batch", 500, "size of batch from database")
	s.Bool("no-color", false, "disable color output")
	s.Bool("version", false, "show version")
	s.BoolP("help", "h", false, "show this help")

	// Bind all flags to viper
	all := cmd.Flags()
	all.SortFlags = false
	flags.ExportAll(all)

	// Init Viper
	v.BindPFlags(all)
	v.SetEnvPrefix(envPrefix)
	v.AutomaticEnv()

	// Flags usage
	cmd.SetUsageFunc(usageFunc(flags))
}
