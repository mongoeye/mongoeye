package cli

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/01sample"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/analysis/stages/04merge"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"os"
	"runtime"
	"testing"
	"time"
)

func TestGetConfig_Default(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")
	c, err := GetConfig(v)
	assert.Equal(t, nil, err)

	assert.Equal(t, mgo.SecondaryPreferred, c.ConnectionMode)
	assert.Equal(t, 5*time.Second, c.ConnectionTimeout)
	assert.Equal(t, 5*time.Minute, c.SocketTimeout)
	assert.Equal(t, 5*time.Minute, c.SyncTimeout)
	assert.Equal(t, "localhost:27017", c.Host)
	assert.Equal(t, "admin", c.User)
	assert.Equal(t, "", c.Password)
	assert.Equal(t, "", c.AuthDatabase)
	assert.Equal(t, "", c.AuthMechanism)
	assert.Equal(t, "", c.Database)
	assert.Equal(t, "", c.Collection)
	assert.Equal(t, bson.M{}, c.Match)
	assert.Equal(t, bson.M{}, c.Project)
	assert.Equal(t, "random", c.Scope)
	assert.Equal(t, uint64(1000), c.Limit)
	assert.Equal(t, uint(2), c.Depth)
	assert.Equal(t, false, c.MinMaxAvgValue)
	assert.Equal(t, false, c.MinMaxAvgLength)
	assert.Equal(t, false, c.ValueHistogram)
	assert.Equal(t, uint(100), c.ValueHistogramSteps)
	assert.Equal(t, false, c.LengthHistogram)
	assert.Equal(t, uint(100), c.LengthHistogramSteps)
	assert.Equal(t, false, c.WeekdayHistogram)
	assert.Equal(t, false, c.HourHistogram)
	assert.Equal(t, false, c.CountUnique)
	assert.Equal(t, uint(0), c.MostFrequentValues)
	assert.Equal(t, uint(0), c.LeastFrequentValues)
	assert.Equal(t, "table", c.Format)
	assert.Equal(t, "", c.FilePath)
	assert.Equal(t, time.Local, c.Location)
	assert.Equal(t, false, c.UseAggregation)
	assert.Equal(t, uint(100), c.StringMaxLength)
	assert.Equal(t, uint(20), c.ArrayMaxLength)
	assert.Equal(t, uint(0), c.Concurrency)
	assert.Equal(t, uint(5000), c.BufferSize)
	assert.Equal(t, uint(500), c.BatchSize)
	assert.Equal(t, false, c.NoColor)
}

func TestGetConfig_Env(t *testing.T) {
	os.Clearenv()
	defer os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	os.Setenv("XYZ_CONNECTION-MODE", "PrimaryPreferred")
	os.Setenv("XYZ_CONNECTION-TIMEOUT", "11")
	os.Setenv("XYZ_SOCKET-TIMEOUT", "12")
	os.Setenv("XYZ_SYNC-TIMEOUT", "13")
	os.Setenv("XYZ_HOST", "testHost:12345")
	os.Setenv("XYZ_USER", "john")
	os.Setenv("XYZ_PASSWORD", "123***")
	os.Setenv("XYZ_AUTH-DB", "myDb")
	os.Setenv("XYZ_AUTH-MECH", "mech")
	os.Setenv("XYZ_DB", "dataDb")
	os.Setenv("XYZ_COL", "dataCol")
	os.Setenv("XYZ_MATCH", "{ \"user\": \"david\" }")
	os.Setenv("XYZ_PROJECT", "{ \"user\": 1 }")
	os.Setenv("XYZ_SCOPE", "first:456")
	os.Setenv("XYZ_DEPTH", "5")
	os.Setenv("XYZ_VALUE", "true")
	os.Setenv("XYZ_LENGTH", "true")
	os.Setenv("XYZ_VALUE-HIST", "true")
	os.Setenv("XYZ_VALUE-HIST-STEPS", "80")
	os.Setenv("XYZ_LENGTH-HIST", "true")
	os.Setenv("XYZ_LENGTH-HIST-STEPS", "120")
	os.Setenv("XYZ_WEEKDAY-HIST", "true")
	os.Setenv("XYZ_HOUR-HIST", "true")
	os.Setenv("XYZ_COUNT-UNIQUE", "true")
	os.Setenv("XYZ_MOST-FREQ", "40")
	os.Setenv("XYZ_LEAST-FREQ", "60")
	os.Setenv("XYZ_FORMAT", "yaml")
	os.Setenv("XYZ_FILE", "/tmp/abc")
	os.Setenv("XYZ_TIMEZONE", "America/New_York")
	os.Setenv("XYZ_USE-AGGREGATION", "true")
	os.Setenv("XYZ_STRING-MAX-LENGTH", "111")
	os.Setenv("XYZ_ARRAY-MAX-LENGTH", "222")
	os.Setenv("XYZ_CONCURRENCY", "15")
	os.Setenv("XYZ_BUFFER", "333")
	os.Setenv("XYZ_BATCH", "444")
	os.Setenv("XYZ_NO-COLOR", "true")

	c, err := GetConfig(v)
	assert.Equal(t, nil, err)

	assert.Equal(t, mgo.PrimaryPreferred, c.ConnectionMode)
	assert.Equal(t, 11*time.Second, c.ConnectionTimeout)
	assert.Equal(t, 12*time.Second, c.SocketTimeout)
	assert.Equal(t, 13*time.Second, c.SyncTimeout)
	assert.Equal(t, "testHost:12345", c.Host)
	assert.Equal(t, "john", c.User)
	assert.Equal(t, "123***", c.Password)
	assert.Equal(t, "myDb", c.AuthDatabase)
	assert.Equal(t, "mech", c.AuthMechanism)
	assert.Equal(t, "dataDb", c.Database)
	assert.Equal(t, "dataCol", c.Collection)
	assert.Equal(t, bson.M{"user": "david"}, c.Match)
	assert.Equal(t, bson.M{"user": float64(1)}, c.Project)
	assert.Equal(t, "first", c.Scope)
	assert.Equal(t, uint64(456), c.Limit)
	assert.Equal(t, uint(5), c.Depth)
	assert.Equal(t, true, c.MinMaxAvgValue)
	assert.Equal(t, true, c.MinMaxAvgLength)
	assert.Equal(t, true, c.ValueHistogram)
	assert.Equal(t, uint(80), c.ValueHistogramSteps)
	assert.Equal(t, true, c.LengthHistogram)
	assert.Equal(t, uint(120), c.LengthHistogramSteps)
	assert.Equal(t, true, c.WeekdayHistogram)
	assert.Equal(t, true, c.HourHistogram)
	assert.Equal(t, true, c.CountUnique)
	assert.Equal(t, uint(40), c.MostFrequentValues)
	assert.Equal(t, uint(60), c.LeastFrequentValues)
	assert.Equal(t, "yaml", c.Format)
	assert.Equal(t, "/tmp/abc", c.FilePath)
	loc, _ := time.LoadLocation("America/New_York")
	assert.Equal(t, loc, c.Location)
	assert.Equal(t, true, c.UseAggregation)
	assert.Equal(t, uint(111), c.StringMaxLength)
	assert.Equal(t, uint(222), c.ArrayMaxLength)
	assert.Equal(t, uint(15), c.Concurrency)
	assert.Equal(t, uint(333), c.BufferSize)
	assert.Equal(t, uint(444), c.BatchSize)
	assert.Equal(t, true, c.NoColor)
}

func TestGetConfig_Flags(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	err := cmd.ParseFlags([]string{
		"--connection-mode", "PrimaryPreferred",
		"--connection-timeout", "11",
		"--socket-timeout", "12",
		"--sync-timeout", "13",
		"--host", "testHost:12345",
		"--user", "john",
		"--password", "123***",
		"--auth-db", "myDb",
		"--auth-mech", "mech",
		"--db", "dataDb",
		"--col", "dataCol",
		"--match", "{ \"user\": \"david\" }",
		"--project", "{ \"user\": 1 }",
		"--scope", "first:123",
		"--depth", "5",
		"--value", "true",
		"--length", "true",
		"--value-hist", "true",
		"--value-hist-steps", "80",
		"--length-hist", "true",
		"--length-hist-steps", "120",
		"--weekday-hist", "true",
		"--hour-hist", "true",
		"--count-unique", "true",
		"--most-freq", "40",
		"--least-freq", "60",
		"--format", "yaml",
		"--file", "/tmp/abc",
		"--timezone", "America/New_York",
		"--use-aggregation", "true",
		"--string-max-length", "111",
		"--array-max-length", "222",
		"--concurrency", "15",
		"--buffer", "333",
		"--batch", "444",
		"--no-color", "true",
	})
	assert.Equal(t, err, nil)

	c, err := GetConfig(v)
	assert.Equal(t, nil, err)

	assert.Equal(t, mgo.PrimaryPreferred, c.ConnectionMode)
	assert.Equal(t, 11*time.Second, c.ConnectionTimeout)
	assert.Equal(t, 12*time.Second, c.SocketTimeout)
	assert.Equal(t, 13*time.Second, c.SyncTimeout)
	assert.Equal(t, "testHost:12345", c.Host)
	assert.Equal(t, "john", c.User)
	assert.Equal(t, "123***", c.Password)
	assert.Equal(t, "myDb", c.AuthDatabase)
	assert.Equal(t, "mech", c.AuthMechanism)
	assert.Equal(t, "dataDb", c.Database)
	assert.Equal(t, "dataCol", c.Collection)
	assert.Equal(t, bson.M{"user": "david"}, c.Match)
	assert.Equal(t, bson.M{"user": float64(1)}, c.Project)
	assert.Equal(t, "first", c.Scope)
	assert.Equal(t, uint64(123), c.Limit)
	assert.Equal(t, uint(5), c.Depth)
	assert.Equal(t, true, c.MinMaxAvgValue)
	assert.Equal(t, true, c.MinMaxAvgLength)
	assert.Equal(t, true, c.ValueHistogram)
	assert.Equal(t, uint(80), c.ValueHistogramSteps)
	assert.Equal(t, true, c.LengthHistogram)
	assert.Equal(t, uint(120), c.LengthHistogramSteps)
	assert.Equal(t, true, c.WeekdayHistogram)
	assert.Equal(t, true, c.HourHistogram)
	assert.Equal(t, true, c.CountUnique)
	assert.Equal(t, uint(40), c.MostFrequentValues)
	assert.Equal(t, uint(60), c.LeastFrequentValues)
	assert.Equal(t, "yaml", c.Format)
	assert.Equal(t, "/tmp/abc", c.FilePath)
	assert.Equal(t, true, c.MinMaxAvgValue)
	assert.Equal(t, true, c.MinMaxAvgLength)
	assert.Equal(t, true, c.ValueHistogram)
	assert.Equal(t, uint(80), c.ValueHistogramSteps)
	assert.Equal(t, true, c.LengthHistogram)
	assert.Equal(t, uint(120), c.LengthHistogramSteps)
	assert.Equal(t, true, c.WeekdayHistogram)
	assert.Equal(t, true, c.HourHistogram)
	assert.Equal(t, true, c.CountUnique)
	assert.Equal(t, uint(40), c.MostFrequentValues)
	assert.Equal(t, uint(60), c.LeastFrequentValues)
	assert.Equal(t, "yaml", c.Format)
	assert.Equal(t, "/tmp/abc", c.FilePath)
	loc, _ := time.LoadLocation("America/New_York")
	assert.Equal(t, loc, c.Location)
	assert.Equal(t, true, c.UseAggregation)
	assert.Equal(t, uint(111), c.StringMaxLength)
	assert.Equal(t, uint(222), c.ArrayMaxLength)
	assert.Equal(t, uint(15), c.Concurrency)
	assert.Equal(t, uint(333), c.BufferSize)
	assert.Equal(t, uint(444), c.BatchSize)
	assert.Equal(t, true, c.NoColor)
}

func TestGetConfig_ConnectionModes(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	cmd.ParseFlags([]string{"--connection-mode", "Primary"})
	config, _ := GetConfig(v)
	assert.Equal(t, config.ConnectionMode, mgo.Primary)

	cmd.ParseFlags([]string{"--connection-mode", "PrimaryPreferred"})
	config, _ = GetConfig(v)
	assert.Equal(t, config.ConnectionMode, mgo.PrimaryPreferred)

	cmd.ParseFlags([]string{"--connection-mode", "Secondary"})
	config, _ = GetConfig(v)
	assert.Equal(t, config.ConnectionMode, mgo.Secondary)

	cmd.ParseFlags([]string{"--connection-mode", "SecondaryPreferred"})
	config, _ = GetConfig(v)
	assert.Equal(t, config.ConnectionMode, mgo.SecondaryPreferred)

	cmd.ParseFlags([]string{"--connection-mode", "Nearest"})
	config, _ = GetConfig(v)
	assert.Equal(t, config.ConnectionMode, mgo.Nearest)

	cmd.ParseFlags([]string{"--connection-mode", "Eventual"})
	config, _ = GetConfig(v)
	assert.Equal(t, config.ConnectionMode, mgo.Eventual)

	cmd.ParseFlags([]string{"--connection-mode", "Monotonic"})
	config, _ = GetConfig(v)
	assert.Equal(t, config.ConnectionMode, mgo.Monotonic)

	cmd.ParseFlags([]string{"--connection-mode", "Strong"})
	config, _ = GetConfig(v)
	assert.Equal(t, config.ConnectionMode, mgo.Strong)

	cmd.ParseFlags([]string{"--connection-mode", "abc"})
	_, err := GetConfig(v)
	assert.NotEqual(t, nil, err)
}

func TestGetConfig_Full(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	err := cmd.ParseFlags([]string{
		"--host", "testHost:12345",
		"--user", "john",
		"--password", "123***",
		"--auth-db", "myDb",
		"--auth-mech", "mech",
		"--db", "dataDb",
		"--col", "dataCol",
		"--match", "{ \"user\": \"david\" }",
		"--project", "{ \"user\": 1 }",
		"--scope", "first:123",
		"--depth", "5",
		"--value-hist-steps", "80",
		"--length-hist-steps", "120",
		"--full",
	})
	assert.Equal(t, err, nil)

	c, err := GetConfig(v)
	assert.Equal(t, nil, err)

	assert.Equal(t, "testHost:12345", c.Host)
	assert.Equal(t, "john", c.User)
	assert.Equal(t, "123***", c.Password)
	assert.Equal(t, "myDb", c.AuthDatabase)
	assert.Equal(t, "mech", c.AuthMechanism)
	assert.Equal(t, "dataDb", c.Database)
	assert.Equal(t, "dataCol", c.Collection)
	assert.Equal(t, bson.M{"user": "david"}, c.Match)
	assert.Equal(t, bson.M{"user": float64(1)}, c.Project)
	assert.Equal(t, "first", c.Scope)
	assert.Equal(t, uint64(123), c.Limit)
	assert.Equal(t, uint(5), c.Depth)
	assert.Equal(t, true, c.MinMaxAvgValue)
	assert.Equal(t, true, c.MinMaxAvgLength)
	assert.Equal(t, true, c.ValueHistogram)
	assert.Equal(t, uint(80), c.ValueHistogramSteps)
	assert.Equal(t, true, c.LengthHistogram)
	assert.Equal(t, uint(120), c.LengthHistogramSteps)
	assert.Equal(t, true, c.WeekdayHistogram)
	assert.Equal(t, true, c.HourHistogram)
	assert.Equal(t, true, c.CountUnique)
	assert.Equal(t, uint(20), c.MostFrequentValues)
	assert.Equal(t, uint(20), c.LeastFrequentValues)
	assert.Equal(t, "table", c.Format)
	assert.Equal(t, "", c.FilePath)
}

func TestGetConfig_Full2(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	err := cmd.ParseFlags([]string{
		"--host", "testHost:12345",
		"--user", "john",
		"--password", "123***",
		"--auth-db", "myDb",
		"--auth-mech", "mech",
		"--db", "dataDb",
		"--col", "dataCol",
		"--match", "{ \"user\": \"david\" }",
		"--project", "{ \"user\": 1 }",
		"--scope", "first:123",
		"--depth", "5",
		"--value-hist-steps", "80",
		"--length-hist-steps", "120",
		"--most-freq", "40",
		"--least-freq", "60",
		"--full",
	})
	assert.Equal(t, err, nil)

	c, err := GetConfig(v)
	assert.Equal(t, nil, err)

	assert.Equal(t, "testHost:12345", c.Host)
	assert.Equal(t, "john", c.User)
	assert.Equal(t, "123***", c.Password)
	assert.Equal(t, "myDb", c.AuthDatabase)
	assert.Equal(t, "mech", c.AuthMechanism)
	assert.Equal(t, "dataDb", c.Database)
	assert.Equal(t, "dataCol", c.Collection)
	assert.Equal(t, bson.M{"user": "david"}, c.Match)
	assert.Equal(t, bson.M{"user": float64(1)}, c.Project)
	assert.Equal(t, "first", c.Scope)
	assert.Equal(t, uint64(123), c.Limit)
	assert.Equal(t, uint(5), c.Depth)
	assert.Equal(t, true, c.MinMaxAvgValue)
	assert.Equal(t, true, c.MinMaxAvgLength)
	assert.Equal(t, true, c.ValueHistogram)
	assert.Equal(t, uint(80), c.ValueHistogramSteps)
	assert.Equal(t, true, c.LengthHistogram)
	assert.Equal(t, uint(120), c.LengthHistogramSteps)
	assert.Equal(t, true, c.WeekdayHistogram)
	assert.Equal(t, true, c.HourHistogram)
	assert.Equal(t, true, c.CountUnique)
	assert.Equal(t, uint(40), c.MostFrequentValues)
	assert.Equal(t, uint(60), c.LeastFrequentValues)
	assert.Equal(t, "table", c.Format)
	assert.Equal(t, "", c.FilePath)
}

func TestGetConfig_TimezoneLocal(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	v.Set("timezone", "local")

	c, err := GetConfig(v)
	assert.Equal(t, time.Local, c.Location)
	assert.Equal(t, nil, err)
}

func TestGetConfig_TimezoneUTC(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	v.Set("timezone", "UTC")

	c, err := GetConfig(v)
	assert.Equal(t, time.UTC, c.Location)
	assert.Equal(t, nil, err)
}

func TestGetConfig_InvalidTimezone(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	v.Set("timezone", "abc")

	_, err := GetConfig(v)
	assert.NotEqual(t, nil, err)
}

func TestGetConfig_InvalidMatch(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	v.Set("match", "{ xyz")

	_, err := GetConfig(v)
	assert.NotEqual(t, nil, err)
}

func TestGetConfig_InvalidProject(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	v.Set("project", "{ xyz")

	_, err := GetConfig(v)
	assert.NotEqual(t, nil, err)
}

func TestGetConfig_InvalidScopeLimit(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	v.Set("scope", "first:abc")

	_, err := GetConfig(v)
	assert.NotEqual(t, nil, err)
}

func TestGetConfig_ValidateScope(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	v.Set("scope", "xyz")

	_, err := GetConfig(v)
	assert.NotEqual(t, nil, err)
}

func TestGetConfig_ValidateLimit(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	v.Set("scope", "first:0")

	_, err := GetConfig(v)
	assert.NotEqual(t, nil, err)
}

func TestGetConfig_ValidateValueHistSteps(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	v.Set("value-hist-steps", 2)

	_, err := GetConfig(v)
	assert.NotEqual(t, nil, err)
}

func TestGetConfig_ValidateLengthHistSteps(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	v.Set("length-hist-steps", 2)

	_, err := GetConfig(v)
	assert.NotEqual(t, nil, err)
}

func TestGetConfig_ValidateBatchSize(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	v.Set("batch", 0)

	_, err := GetConfig(v)
	assert.NotEqual(t, nil, err)
}

func TestGetConfig_ValidateFormat(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	v.Set("format", "abc")

	_, err := GetConfig(v)
	assert.NotEqual(t, nil, err)
}

func TestConfig_CreateAnalysisOptions(t *testing.T) {
	config := Config{
		Location:    time.Local,
		Concurrency: 12,
		BufferSize:  100,
		BatchSize:   200,
	}

	assert.Equal(t, &analysis.Options{
		Location:    time.Local,
		Concurrency: 12,
		BufferSize:  100,
		BatchSize:   200,
	}, config.CreateAnalysisOptions())
}

func TestConfig_CreateAnalysisOptions_ConcurrencyAuto(t *testing.T) {
	config := Config{
		Location:    time.Local,
		Concurrency: 0,
		BufferSize:  100,
		BatchSize:   200,
	}

	assert.Equal(t, &analysis.Options{
		Location:    time.Local,
		Concurrency: runtime.NumCPU(),
		BufferSize:  100,
		BatchSize:   200,
	}, config.CreateAnalysisOptions())
}

func TestConfig_CreateSampleStageOptions(t *testing.T) {
	c := Config{
		Match:   bson.M{"key": "value"},
		Project: bson.M{"key": 1},
		Scope:   "all",
		Limit:   0,
	}

	assert.Equal(t, &sample.Options{
		Match:   bson.M{"key": "value"},
		Project: bson.M{"key": 1},
		Scope:   sample.All,
		Limit:   0,
	}, c.CreateSampleStageOptions())

	// scope: first
	c.Scope = "first"
	c.Limit = 12345
	assert.Equal(t, &sample.Options{
		Match:   bson.M{"key": "value"},
		Project: bson.M{"key": 1},
		Scope:   sample.First,
		Limit:   12345,
	}, c.CreateSampleStageOptions())

	// scope: last
	c.Scope = "last"
	assert.Equal(t, &sample.Options{
		Match:   bson.M{"key": "value"},
		Project: bson.M{"key": 1},
		Scope:   sample.Last,
		Limit:   12345,
	}, c.CreateSampleStageOptions())

	// scope: random
	c.Scope = "random"
	assert.Equal(t, &sample.Options{
		Match:   bson.M{"key": "value"},
		Project: bson.M{"key": 1},
		Scope:   sample.Random,
		Limit:   12345,
	}, c.CreateSampleStageOptions())

	// invalid scope
	c.Scope = "abc"
	assert.Panics(t, func() {
		c.CreateSampleStageOptions()
	})
}

func TestConfig_CreateExpandStageOptions(t *testing.T) {
	newConfig := func() Config {
		return Config{
			StringMaxLength:     123,
			ArrayMaxLength:      456,
			Depth:               4,
			MinMaxAvgValue:      false,
			MinMaxAvgLength:     false,
			CountUnique:         false,
			MostFrequentValues:  0,
			LeastFrequentValues: 0,
			ValueHistogram:      false,
			LengthHistogram:     false,
			WeekdayHistogram:    false,
			HourHistogram:       false,
		}
	}

	// All off
	config := newConfig()
	assert.Equal(t, &expand.Options{
		StringMaxLength:   123,
		ArrayMaxLength:    456,
		MaxDepth:          4,
		StoreValue:        false,
		StoreStringLength: false,
		StoreArrayLength:  false,
		StoreObjectLength: false,
	}, config.CreateExpandStageOptions())

	// MinMaxAvgValue
	config = newConfig()
	config.MinMaxAvgValue = true
	assert.Equal(t, &expand.Options{
		StringMaxLength:   123,
		ArrayMaxLength:    456,
		MaxDepth:          4,
		StoreValue:        true,
		StoreStringLength: false,
		StoreArrayLength:  false,
		StoreObjectLength: false,
	}, config.CreateExpandStageOptions())

	// MinMaxAvgLength
	config = newConfig()
	config.MinMaxAvgLength = true
	assert.Equal(t, &expand.Options{
		StringMaxLength:   123,
		ArrayMaxLength:    456,
		MaxDepth:          4,
		StoreValue:        false,
		StoreStringLength: true,
		StoreArrayLength:  true,
		StoreObjectLength: true,
	}, config.CreateExpandStageOptions())

	// CountUnique
	config = newConfig()
	config.CountUnique = true
	assert.Equal(t, &expand.Options{
		StringMaxLength:   123,
		ArrayMaxLength:    456,
		MaxDepth:          4,
		StoreValue:        true,
		StoreStringLength: false,
		StoreArrayLength:  false,
		StoreObjectLength: false,
	}, config.CreateExpandStageOptions())

	// MostFrequentValues
	config = newConfig()
	config.MostFrequentValues = 20
	assert.Equal(t, &expand.Options{
		StringMaxLength:   123,
		ArrayMaxLength:    456,
		MaxDepth:          4,
		StoreValue:        true,
		StoreStringLength: false,
		StoreArrayLength:  false,
		StoreObjectLength: false,
	}, config.CreateExpandStageOptions())

	// LeastFrequentValues
	config = newConfig()
	config.LeastFrequentValues = 20
	assert.Equal(t, &expand.Options{
		StringMaxLength:   123,
		ArrayMaxLength:    456,
		MaxDepth:          4,
		StoreValue:        true,
		StoreStringLength: false,
		StoreArrayLength:  false,
		StoreObjectLength: false,
	}, config.CreateExpandStageOptions())

	// ValueHistogram
	config = newConfig()
	config.ValueHistogram = true
	assert.Equal(t, &expand.Options{
		StringMaxLength:   123,
		ArrayMaxLength:    456,
		MaxDepth:          4,
		StoreValue:        true,
		StoreStringLength: false,
		StoreArrayLength:  false,
		StoreObjectLength: false,
	}, config.CreateExpandStageOptions())

	// LengthHistogram
	config = newConfig()
	config.LengthHistogram = true
	assert.Equal(t, &expand.Options{
		StringMaxLength:   123,
		ArrayMaxLength:    456,
		MaxDepth:          4,
		StoreValue:        false,
		StoreStringLength: true,
		StoreArrayLength:  true,
		StoreObjectLength: true,
	}, config.CreateExpandStageOptions())

	// WeekdayHistogram
	config = newConfig()
	config.WeekdayHistogram = true
	assert.Equal(t, &expand.Options{
		StringMaxLength:   123,
		ArrayMaxLength:    456,
		MaxDepth:          4,
		StoreValue:        true,
		StoreStringLength: false,
		StoreArrayLength:  false,
		StoreObjectLength: false,
	}, config.CreateExpandStageOptions())

	// HourHistogram
	config = newConfig()
	config.HourHistogram = true
	assert.Equal(t, &expand.Options{
		StringMaxLength:   123,
		ArrayMaxLength:    456,
		MaxDepth:          4,
		StoreValue:        true,
		StoreStringLength: false,
		StoreArrayLength:  false,
		StoreObjectLength: false,
	}, config.CreateExpandStageOptions())
}

func TestConfig_CreateGroupStageOptions_HistogramsOn(t *testing.T) {
	config := Config{
		MinMaxAvgValue:       true,
		ValueHistogramSteps:  56,
		MinMaxAvgLength:      true,
		LengthHistogramSteps: 78,
		CountUnique:          true,
		MostFrequentValues:   12,
		LeastFrequentValues:  34,
		WeekdayHistogram:     true,
		HourHistogram:        true,
		ValueHistogram:       true,
		LengthHistogram:      true,
	}

	assert.Equal(t, &group.Options{
		ProcessObjectIdAsDate: true,
		StoreMinMaxAvgValue:   true,
		StoreMinMaxAvgLength:  true,
		StoreCountOfUnique:    true,
		StoreMostFrequent:     12,
		StoreLeastFrequent:    34,
		StoreWeekdayHistogram: true,
		StoreHourHistogram:    true,
		ValueHistogramMaxRes:  56,
		LengthHistogramMaxRes: 78,
	}, config.CreateGroupStageOptions())
}

func TestConfig_CreateGroupStageOptions_HistogramsOff(t *testing.T) {
	config := Config{
		MinMaxAvgValue:       true,
		ValueHistogramSteps:  56,
		MinMaxAvgLength:      true,
		LengthHistogramSteps: 78,
		CountUnique:          true,
		MostFrequentValues:   12,
		LeastFrequentValues:  34,
		WeekdayHistogram:     true,
		HourHistogram:        true,
		ValueHistogram:       false,
		LengthHistogram:      false,
	}

	assert.Equal(t, &group.Options{
		ProcessObjectIdAsDate: true,
		StoreMinMaxAvgValue:   true,
		StoreMinMaxAvgLength:  true,
		StoreCountOfUnique:    true,
		StoreMostFrequent:     12,
		StoreLeastFrequent:    34,
		StoreWeekdayHistogram: true,
		StoreHourHistogram:    true,
		ValueHistogramMaxRes:  0,
		LengthHistogramMaxRes: 0,
	}, config.CreateGroupStageOptions())
}

func TestConfig_CreateMergeStageOptions(t *testing.T) {
	config := Config{}

	assert.Equal(t, &merge.Options{}, config.CreateMergeStageOptions())
}
