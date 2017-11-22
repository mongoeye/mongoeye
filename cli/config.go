package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/01sample"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/analysis/stages/04merge"
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/spf13/viper"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Config - app configuration.
type Config struct {
	// connection options
	ConnectionMode    mgo.Mode
	ConnectionTimeout time.Duration
	SyncTimeout       time.Duration
	SocketTimeout     time.Duration
	Host              string
	User              string
	Password          string
	AuthDatabase      string
	AuthMechanism     string

	// analysis options
	Database   string
	Collection string
	Match      bson.M
	Project    bson.M
	Sample     string
	Limit      uint64
	Depth      uint

	// statistics options
	MinMaxAvgValue       bool
	MinMaxAvgLength      bool
	ValueHistogram       bool
	ValueHistogramSteps  uint
	LengthHistogram      bool
	LengthHistogramSteps uint
	WeekdayHistogram     bool
	HourHistogram        bool
	CountUnique          bool
	MostFrequentValues   uint
	LeastFrequentValues  uint
	Format               string
	FilePath             string

	// other options
	Location        *time.Location
	UseAggregation  bool
	StringMaxLength uint
	ArrayMaxLength  uint
	Concurrency     uint
	BufferSize      uint
	BatchSize       uint
	NoColor         bool
}

// CreateAnalysisOptions generates analysis options from config.
func (c *Config) CreateAnalysisOptions() *analysis.Options {
	concurrency := int(c.Concurrency)
	if concurrency == 0 {
		concurrency = runtime.NumCPU()
	}

	return &analysis.Options{
		Location:    c.Location,
		Concurrency: concurrency,
		BufferSize:  int(c.BufferSize),
		BatchSize:   int(c.BatchSize),
	}
}

// CreateSampleStageOptions generates sample options from config.
func (c *Config) CreateSampleStageOptions() *sample.Options {
	var sampleMethod sample.SampleMethod
	switch c.Sample {
	case "all":
		sampleMethod = sample.AllDocuments
	case "first":
		sampleMethod = sample.FirstNDocuments
	case "last":
		sampleMethod = sample.LastNDocuments
	case "random":
		sampleMethod = sample.RandomNDocuments
	default:
		panic("Unexpected sample.")
	}

	return &sample.Options{
		Match:   c.Match,
		Project: c.Project,
		Method:  sampleMethod,
		Limit:   uint64(c.Limit),
	}
}

// CreateExpandStageOptions generates expand options from config.
func (c *Config) CreateExpandStageOptions() *expand.Options {
	return &expand.Options{
		StringMaxLength: c.StringMaxLength,
		ArrayMaxLength:  c.ArrayMaxLength,
		MaxDepth:        c.Depth,
		StoreValue: c.MinMaxAvgValue ||
			c.CountUnique ||
			c.MostFrequentValues > 0 ||
			c.LeastFrequentValues > 0 ||
			c.WeekdayHistogram ||
			c.HourHistogram ||
			c.ValueHistogram,
		StoreStringLength: c.MinMaxAvgLength || c.LengthHistogram,
		StoreArrayLength:  c.MinMaxAvgLength || c.LengthHistogram,
		StoreObjectLength: c.MinMaxAvgLength || c.LengthHistogram,
	}
}

// CreateGroupStageOptions generates group options from config.
func (c *Config) CreateGroupStageOptions() *group.Options {
	options := &group.Options{
		ProcessObjectIdAsDate: true,
		StoreMinMaxAvgValue:   c.MinMaxAvgValue,
		StoreMinMaxAvgLength:  c.MinMaxAvgLength,
		StoreCountOfUnique:    c.CountUnique,
		StoreMostFrequent:     c.MostFrequentValues,
		StoreLeastFrequent:    c.LeastFrequentValues,
		StoreWeekdayHistogram: c.WeekdayHistogram,
		StoreHourHistogram:    c.HourHistogram,
		ValueHistogramMaxRes:  0,
		LengthHistogramMaxRes: 0,
	}

	if c.ValueHistogram {
		options.ValueHistogramMaxRes = c.ValueHistogramSteps
	}

	if c.LengthHistogram {
		options.LengthHistogramMaxRes = c.LengthHistogramSteps
	}

	return options
}

// CreateMergeStageOptions generates merge options from config.
func (c *Config) CreateMergeStageOptions() *merge.Options {
	return &merge.Options{}
}

// GetConfig - returns configuration according Viper values.
func GetConfig(v *viper.Viper) (*Config, error) {
	// Connection mode
	connectionMode, err := parseConnectionMode(v)
	if err != nil {
		return nil, err
	}

	// Parse sample
	sample, limit, err := parseSample(v)
	if err != nil {
		return nil, err
	}

	// Parse location
	location, err := parseLocation(v)
	if err != nil {
		return nil, err
	}

	// Parse match
	match, err := parseJsonArgument(v, "match")
	if err != nil {
		return nil, err
	}

	// Parse project
	project, err := parseJsonArgument(v, "project")
	if err != nil {
		return nil, err
	}

	// Create config
	config := &Config{
		ConnectionMode:       connectionMode,
		ConnectionTimeout:    time.Duration(v.GetFloat64("connection-timeout") * float64(time.Second)),
		SocketTimeout:        time.Duration(v.GetFloat64("socket-timeout") * float64(time.Second)),
		SyncTimeout:          time.Duration(v.GetFloat64("sync-timeout") * float64(time.Second)),
		Host:                 v.GetString("host"),
		User:                 v.GetString("user"),
		Password:             v.GetString("password"),
		AuthDatabase:         v.GetString("auth-db"),
		AuthMechanism:        v.GetString("auth-mech"),
		Database:             v.GetString("db"),
		Collection:           v.GetString("col"),
		Match:                match,
		Project:              project,
		Sample:               sample,
		Limit:                limit,
		Depth:                uint(v.GetInt("depth")),
		MinMaxAvgValue:       v.GetBool("value"),
		MinMaxAvgLength:      v.GetBool("length"),
		ValueHistogram:       v.GetBool("value-hist"),
		ValueHistogramSteps:  uint(v.GetInt("value-hist-steps")),
		LengthHistogram:      v.GetBool("length-hist"),
		LengthHistogramSteps: uint(v.GetInt("length-hist-steps")),
		WeekdayHistogram:     v.GetBool("weekday-hist"),
		HourHistogram:        v.GetBool("hour-hist"),
		CountUnique:          v.GetBool("count-unique"),
		MostFrequentValues:   uint(v.GetInt("most-freq")),
		LeastFrequentValues:  uint(v.GetInt("least-freq")),
		Format:               v.GetString("format"),
		FilePath:             v.GetString("file"),
		Location:             location,
		UseAggregation:       v.GetBool("use-aggregation"),
		StringMaxLength:      uint(v.GetInt("string-max-length")),
		ArrayMaxLength:       uint(v.GetInt("array-max-length")),
		Concurrency:          uint(v.GetInt("concurrency")),
		BufferSize:           uint(v.GetInt("buffer")),
		BatchSize:            uint(v.GetInt("batch")),
		NoColor:              v.GetBool("no-color"),
	}

	// --full = perform all available analyzes
	if v.GetBool("full") {
		config.MinMaxAvgValue = true
		config.MinMaxAvgLength = true
		config.ValueHistogram = true
		config.LengthHistogram = true
		config.WeekdayHistogram = true
		config.HourHistogram = true
		config.CountUnique = true

		if config.MostFrequentValues == 0 {
			config.MostFrequentValues = 20
		}

		if config.LeastFrequentValues == 0 {
			config.LeastFrequentValues = 20
		}
	}

	// Default auth database to working database
	if config.AuthDatabase == "" {
		config.AuthDatabase = config.Database
	}

	err = config.validate()
	if err != nil {
		return nil, err
	}

	return config, nil
}

func parseConnectionMode(v *viper.Viper) (mode mgo.Mode, err error) {
	switch strings.ToLower(v.GetString("connection-mode")) {
	case "primary":
		mode = mgo.Primary
	case "primarypreferred":
		mode = mgo.PrimaryPreferred
	case "secondary":
		mode = mgo.Secondary
	case "secondarypreferred":
		mode = mgo.SecondaryPreferred
	case "nearest":
		mode = mgo.Nearest
	case "eventual":
		mode = mgo.Eventual
	case "monotonic":
		mode = mgo.Monotonic
	case "strong":
		mode = mgo.Strong
	default:
		err = errors.New(
			"Invalid value in 'connection-mode' option. Allowed values: 'Primary', 'PrimaryPreferred', 'Secondary', 'SecondaryPreferred', 'Nearest', 'Eventual', 'Monotonic', 'Strong'.",
		)
	}
	return
}

func parseSample(v *viper.Viper) (sample string, limit uint64, err error) {
	sampleParts := strings.SplitN(strings.ToLower(v.GetString("sample")), ":", 2)
	sample = sampleParts[0]
	if len(sampleParts) > 1 {
		i, e := strconv.ParseInt(sampleParts[1], 10, 64)
		if e != nil {
			err = errors.New(
				"Cannot parse a valid limit (integer) from 'sample' option.\nPlease enter a valid sample, eg. 'first:100'.",
			)
		}

		limit = uint64(i)
	}

	return
}

func parseLocation(v *viper.Viper) (location *time.Location, err error) {
	timezone := v.GetString("timezone")
	if timezone == "local" {
		location = time.Local
	} else if strings.ToLower(timezone) == "utc" {
		location = time.UTC
	} else {
		location, err = time.LoadLocation(timezone)
		if err != nil {
			err = fmt.Errorf("Cannot find timezone '%s' specified in 'timezone' option.", timezone)
		}
	}

	return
}

func parseJsonArgument(v *viper.Viper, argument string) (out bson.M, err error) {
	out = bson.M{}
	raw := v.GetString(argument)
	if len(raw) > 0 {
		err = json.Unmarshal([]byte(raw), &out)
		if err != nil {
			err = fmt.Errorf("Invalid JSON in '%s' option.\nPlease enter a valid query.", argument)
		}
	}

	return
}

func (c *Config) validate() error {
	if !helpers.InStringSlice(c.Sample, []string{"all", "first", "last", "random"}) {
		return errors.New(
			"Invalid value of 'sample' option.\nAllowed values are: 'all', 'first:N', 'last:N', 'random:N'.",
		)
	}

	if c.Sample != "all" && c.Limit < 1 {
		return errors.New(
			"Limit (N) in 'sample' option must be >= 1.",
		)
	}

	if c.ValueHistogramSteps < 3 {
		return errors.New(
			"Option 'value-histogram-steps' must be >= 3",
		)
	}

	if c.LengthHistogramSteps < 3 {
		return errors.New(
			"Option 'length-histogram-max-steps' must be >= 3",
		)
	}

	if c.BatchSize < 1 {
		return errors.New(
			"Option 'batch-size' must be >= 1",
		)
	}

	if !helpers.InStringSlice(c.Format, []string{"table", "json", "yaml"}) {
		return errors.New(
			"Invalid value of 'format' option.\nAllowed values are: 'table', 'json', 'yaml'.",
		)
	}

	return nil
}
