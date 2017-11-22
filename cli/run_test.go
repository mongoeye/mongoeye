package cli

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/fatih/color"
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/mongoeye/mongoeye/tests"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestNoOp(t *testing.T) {
	cmd, _ := NewCmd("cmd", "env", "name", "version", "subtitle")
	assert.Equal(t, nil, noOp(cmd, []string{}))
}

func TestPreRun_HelpWhenNoArgs(t *testing.T) {
	os.Clearenv()

	out := bytes.NewBuffer(nil)
	cmd, v := NewCmd("cmd", "env", "name", "version", "subtitle")
	cmd.SetOutput(out)

	osArgs := []string{"cmd"}
	cmd.ParseFlags(osArgs)
	err := PreRun(cmd, v, osArgs, []string{})

	assert.Contains(t, out.String(), "Usage:")
	assert.Equal(t, nil, err)
}

func TestPreRun_HelpWhenHFlag(t *testing.T) {
	os.Clearenv()

	out := bytes.NewBuffer(nil)
	cmd, v := NewCmd("cmd", "env", "name", "version", "subtitle")
	cmd.SetOutput(out)

	osArgs := []string{"cmd -h"}
	cmd.ParseFlags(osArgs)
	err := PreRun(cmd, v, osArgs, []string{})

	assert.Contains(t, out.String(), "Usage:")
	assert.Equal(t, nil, err)
}

func TestPreRun_HelpWhenHelpFlag(t *testing.T) {
	os.Clearenv()

	out := bytes.NewBuffer(nil)
	cmd, v := NewCmd("cmd", "env", "name", "version", "subtitle")
	cmd.SetOutput(out)

	osArgs := []string{"cmd --help"}
	cmd.ParseFlags(osArgs)
	err := PreRun(cmd, v, osArgs, []string{})

	assert.Contains(t, out.String(), "Usage:")
	assert.Equal(t, nil, err)
}

func TestPreRun_Version(t *testing.T) {
	os.Clearenv()

	out := bytes.NewBuffer(nil)
	cmd, v := NewCmd("cmd", "env", "name", "version", "subtitle")
	cmd.SetOutput(out)

	osArgs := []string{"cmd", "--version"}
	cmd.ParseFlags(osArgs)
	err := PreRun(cmd, v, osArgs, []string{})

	assert.Contains(t, out.String(), "name version")
	assert.Equal(t, nil, err)
}

func TestPreRun_TooManyArguments(t *testing.T) {
	os.Clearenv()

	out := bytes.NewBuffer(nil)
	cmd, v := NewCmd("cmd", "env", "name", "version", "subtitle")
	cmd.SetOutput(out)

	osArgs := []string{"cmd", "A", "B", "C", "D"}
	cmd.ParseFlags(osArgs)
	err := PreRun(cmd, v, osArgs, []string{"A", "B", "C", "D"})

	assert.Equal(t, "", out.String())
	assert.Equal(t, errors.New("Too many arguments.\n"), err)
}

func TestPreRun_OneArgument(t *testing.T) {
	os.Clearenv()

	out := bytes.NewBuffer(nil)
	cmd, v := NewCmd("cmd", "env", "name", "version", "subtitle")
	cmd.SetOutput(out)

	osArgs := []string{"cmd", "A"}
	cmd.ParseFlags(osArgs)
	err := PreRun(cmd, v, osArgs, []string{"A"})

	assert.Equal(t, "A", v.Get("col"))
	assert.NotEqual(t, nil, err)
}

func TestPreRun_TwoArguments(t *testing.T) {
	os.Clearenv()

	out := bytes.NewBuffer(nil)

	cmd, v := NewCmd("cmd", "env", "name", "version", "subtitle")
	cmd.SetOutput(out)

	osArgs := []string{"cmd", "A", "B"}
	cmd.ParseFlags(osArgs)
	err := PreRun(cmd, v, osArgs, []string{"A", "B"})

	assert.Equal(t, "A", v.Get("db"))
	assert.Equal(t, "B", v.Get("col"))
	assert.Equal(t, nil, err)
}

func TestPreRun_ThreeArguments(t *testing.T) {
	os.Clearenv()

	out := bytes.NewBuffer(nil)

	cmd, v := NewCmd("cmd", "env", "name", "version", "subtitle")
	cmd.SetOutput(out)

	osArgs := []string{"cmd", "A", "B", "C"}
	cmd.ParseFlags(osArgs)
	err := PreRun(cmd, v, osArgs, []string{"A", "B", "C"})

	assert.Equal(t, "A", v.Get("host"))
	assert.Equal(t, "B", v.Get("db"))
	assert.Equal(t, "C", v.Get("col"))
	assert.Equal(t, nil, err)
}

func TestPreRun_ArgumentsByFlags(t *testing.T) {
	os.Clearenv()

	out := bytes.NewBuffer(nil)

	cmd, v := NewCmd("cmd", "env", "name", "version", "subtitle")
	cmd.SetOutput(out)

	osArgs := []string{"cmd", "--db", "A", "--col", "B", "--host", "C"}
	cmd.ParseFlags(osArgs)
	PreRun(cmd, v, osArgs, []string{})

	assert.Equal(t, "A", v.Get("db"))
	assert.Equal(t, "B", v.Get("col"))
	assert.Equal(t, "C", v.Get("host"))
}

func TestPreRun_ArgumentsByEnv(t *testing.T) {
	os.Clearenv()
	os.Setenv("TEST_ENV_DB", "A")
	os.Setenv("TEST_ENV_COL", "B")
	os.Setenv("TEST_ENV_HOST", "C")

	out := bytes.NewBuffer(nil)

	cmd, v := NewCmd("cmd", "test_env", "name", "version", "subtitle")
	cmd.SetOutput(out)

	osArgs := []string{"cmd"}
	cmd.ParseFlags(osArgs)
	PreRun(cmd, v, osArgs, []string{})

	assert.Equal(t, "A", v.Get("db"))
	assert.Equal(t, "B", v.Get("col"))
	assert.Equal(t, "C", v.Get("host"))
}

func TestPreRun_MissingCol(t *testing.T) {
	os.Clearenv()

	out := bytes.NewBuffer(nil)

	cmd, v := NewCmd("cmd", "env", "name", "version", "subtitle")
	cmd.SetOutput(out)

	osArgs := []string{"cmd", "A"}
	cmd.ParseFlags(osArgs)
	err := PreRun(cmd, v, osArgs, []string{"A"})

	assert.NotEqual(t, nil, err)
}

func TestPreRun_MissingDb(t *testing.T) {
	os.Clearenv()

	out := bytes.NewBuffer(nil)

	cmd, v := NewCmd("cmd", "env", "name", "version", "subtitle")
	cmd.SetOutput(out)

	osArgs := []string{"cmd", "--format", "yaml"}
	cmd.ParseFlags(osArgs)
	err := PreRun(cmd, v, osArgs, []string{})

	assert.NotEqual(t, nil, err)
}

func TestRun_ConnectError(t *testing.T) {
	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{
		"cmd",
		"--host", "invalidHost:12345",
		"--db", "db",
		"--col", "col",
		"--connection-timeout", "1",
	})

	config, _ := GetConfig(v)
	err := Run(cmd, config)
	assert.Equal(t, "Connection failed: no reachable servers.\n", err.Error())
}

func TestRun_Table(t *testing.T) {
	color.NoColor = true

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"str": "Abc",
	})

	stdout := bytes.NewBuffer(nil)

	cmd := &cobra.Command{}
	cmd.SetOutput(stdout)
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{
		"cmd",
		"--host", tests.TestDbUri,
		"--db", c.Database.Name,
		"--col", c.Name,
		"--sample", "all",
	})

	config, _ := GetConfig(v)
	err := Run(cmd, config)
	assert.Equal(t, nil, err)

	expected := []string{
		"         KEY         │ COUNT  │   %    ",
		"───────────────────────────────────────",
		"  all documents      │ 1      │        ",
		"  analyzed documents │ 1      │ 100.0  ",
		"                     │        │        ",
		"  _id ➜ objectId     │ 1      │ 100.0  ",
		"  str ➜ string       │ 1      │ 100.0  \n",
	}

	assert.Contains(t, stdout.String(), strings.Join(expected, "\n"))
	assert.Contains(t, stdout.String(), "OK")
}

func TestRun_Table_Color(t *testing.T) {
	color.NoColor = false

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"str": "Abc",
	})

	stdout := bytes.NewBuffer(nil)

	cmd := &cobra.Command{}
	cmd.SetOutput(stdout)
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{
		"cmd",
		"--host", tests.TestDbUri,
		"--db", c.Database.Name,
		"--col", c.Name,
		"--sample", "all",
	})

	config, _ := GetConfig(v)
	err := Run(cmd, config)
	assert.Equal(t, nil, err)

	assert.Contains(t, stdout.String(), "\u001b[0m") // contains color?
	assert.Contains(t, stdout.String(), "OK")
}

func TestRun_Table_NoColor(t *testing.T) {
	color.NoColor = false

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"str": "Abc",
	})

	stdout := bytes.NewBuffer(nil)

	cmd := &cobra.Command{}
	cmd.SetOutput(stdout)
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{
		"cmd",
		"--host", tests.TestDbUri,
		"--db", c.Database.Name,
		"--col", c.Name,
		"--sample", "all",
		"--no-color",
	})

	config, _ := GetConfig(v)
	err := Run(cmd, config)
	assert.Equal(t, nil, err)

	assert.NotContains(t, stdout.String(), "\u001b[0m") // not contains color?
	assert.Contains(t, stdout.String(), "OK")
}

func TestRun_Table_File(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	tmpFile, _ := ioutil.TempFile("", "mongoeye")
	defer os.Remove(tmpFile.Name())

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"str": "Abc",
	})

	stdout := bytes.NewBuffer(nil)

	cmd := &cobra.Command{}
	cmd.SetOutput(stdout)
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{
		"cmd",
		"--host", tests.TestDbUri,
		"--db", c.Database.Name,
		"--col", c.Name,
		"--sample", "all",
		"--file", tmpFile.Name(),
	})

	config, _ := GetConfig(v)
	err := Run(cmd, config)
	assert.Equal(t, nil, err)

	b, err := ioutil.ReadFile(tmpFile.Name())
	assert.Equal(t, nil, err)
	content := string(b)

	expected := []string{
		"         KEY         │ COUNT  │   %    ",
		"───────────────────────────────────────",
		"  all documents      │ 1      │        ",
		"  analyzed documents │ 1      │ 100.0  ",
		"                     │        │        ",
		"  _id ➜ objectId     │ 1      │ 100.0  ",
		"  str ➜ string       │ 1      │ 100.0  \n",
	}

	assert.Equal(t, strings.Join(expected, "\n"), content)
	assert.Contains(t, stdout.String(), fmt.Sprintf("The analysis results were written to the file: %s.", tmpFile.Name()))
	assert.Contains(t, stdout.String(), "OK")
}

func TestRun_Json(t *testing.T) {
	color.NoColor = true

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"str": "Abc",
	})

	stdout := bytes.NewBuffer(nil)

	cmd := &cobra.Command{}
	cmd.SetOutput(stdout)
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{
		"cmd",
		"--host", tests.TestDbUri,
		"--db", c.Database.Name,
		"--col", c.Name,
		"--sample", "all",
		"--format", "json",
	})

	config, _ := GetConfig(v)
	err := Run(cmd, config)
	assert.Equal(t, nil, err)

	expected := `{
	"database": "%s",
	"collection": "%s",
	"plan": "local",
	"duration": <DURATION>,
	"allDocs": 1,
	"analyzedDocs": 1,
	"fieldsCount": 2,
	"fields": [
		{
			"name": "_id",
			"level": 0,
			"count": 1,
			"types": [
				{
					"type": "objectId",
					"count": 1
				}
			]
		},
		{
			"name": "str",
			"level": 0,
			"count": 1,
			"types": [
				{
					"type": "string",
					"count": 1
				}
			]
		}
	]
}`

	assert.Equal(t, fmt.Sprintf(expected, c.Database.Name, c.Name), regexp.MustCompile("\"duration\": [0-9]+,").ReplaceAllString(stdout.String(), "\"duration\": <DURATION>,"))
	assert.NotContains(t, stdout.String(), "OK")
}

func TestRun_Json_File(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	tmpFile, _ := ioutil.TempFile("", "mongoeye")
	defer os.Remove(tmpFile.Name())

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"str": "Abc",
	})

	stdout := bytes.NewBuffer(nil)

	cmd := &cobra.Command{}
	cmd.SetOutput(stdout)
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{
		"cmd",
		"--host", tests.TestDbUri,
		"--db", c.Database.Name,
		"--col", c.Name,
		"--file", tmpFile.Name(),
		"--sample", "all",
		"--format", "json",
	})

	config, _ := GetConfig(v)
	err := Run(cmd, config)
	assert.Equal(t, nil, err)

	b, err := ioutil.ReadFile(tmpFile.Name())
	assert.Equal(t, nil, err)
	content := string(b)

	expected := `{
	"database": "%s",
	"collection": "%s",
	"plan": "local",
	"duration": <DURATION>,
	"allDocs": 1,
	"analyzedDocs": 1,
	"fieldsCount": 2,
	"fields": [
		{
			"name": "_id",
			"level": 0,
			"count": 1,
			"types": [
				{
					"type": "objectId",
					"count": 1
				}
			]
		},
		{
			"name": "str",
			"level": 0,
			"count": 1,
			"types": [
				{
					"type": "string",
					"count": 1
				}
			]
		}
	]
}`

	expected = strings.Replace(expected, "\n", "", -1)
	expected = strings.Replace(expected, "\t", "", -1)
	expected = strings.Replace(expected, ": ", ":", -1)

	assert.Equal(t, fmt.Sprintf(expected, c.Database.Name, c.Name), regexp.MustCompile("\"duration\":[0-9]+,").ReplaceAllString(content, "\"duration\":<DURATION>,"))
	assert.Contains(t, stdout.String(), fmt.Sprintf("The analysis results were written to the file: %s.", tmpFile.Name()))
	assert.Contains(t, stdout.String(), "OK")
}

func TestRun_Yaml(t *testing.T) {
	color.NoColor = true

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"str": "Abc",
	})

	stdout := bytes.NewBuffer(nil)

	cmd := &cobra.Command{}
	cmd.SetOutput(stdout)
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{
		"cmd",
		"--host", tests.TestDbUri,
		"--db", c.Database.Name,
		"--col", c.Name,
		"--sample", "all",
		"--format", "yaml",
	})

	config, _ := GetConfig(v)
	err := Run(cmd, config)
	assert.Equal(t, nil, err)

	expected := `
database: %s
collection: %s
plan: local
duration: <DURATION>
allDocs: 1
analyzedDocs: 1
fieldsCount: 2
fields:
- name: _id
  level: 0
  count: 1
  types:
  - type: objectId
    count: 1
- name: str
  level: 0
  count: 1
  types:
  - type: string
    count: 1
`

	assert.Equal(t, fmt.Sprintf(expected, c.Database.Name, c.Name), "\n"+regexp.MustCompile("\nduration: [0-9.]+.s").ReplaceAllString(stdout.String(), "\nduration: <DURATION>"))
	assert.NotContains(t, stdout.String(), "OK")
}

func TestRun_Yaml_File(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"str": "Abc",
	})

	tmpFile, _ := ioutil.TempFile("", "mongoeye")
	defer os.Remove(tmpFile.Name())

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"str": "Abc",
	})

	stdout := bytes.NewBuffer(nil)

	cmd := &cobra.Command{}
	cmd.SetOutput(stdout)
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{
		"cmd",
		"--host", tests.TestDbUri,
		"--db", c.Database.Name,
		"--col", c.Name,
		"--sample", "all",
		"--file", tmpFile.Name(),
		"--format", "yaml",
	})

	config, _ := GetConfig(v)
	err := Run(cmd, config)
	assert.Equal(t, nil, err)

	b, err := ioutil.ReadFile(tmpFile.Name())
	assert.Equal(t, nil, err)
	content := string(b)

	expected := `
database: %s
collection: %s
plan: local
duration: <DURATION>
allDocs: 1
analyzedDocs: 1
fieldsCount: 2
fields:
- name: _id
  level: 0
  count: 1
  types:
  - type: objectId
    count: 1
- name: str
  level: 0
  count: 1
  types:
  - type: string
    count: 1
`
	assert.Equal(t, fmt.Sprintf(expected, c.Database.Name, c.Name), "\n"+regexp.MustCompile("\nduration: [0-9.]+.s").ReplaceAllString(content, "\nduration: <DURATION>"))
	assert.Contains(t, stdout.String(), fmt.Sprintf("The analysis results were written to the file: %s.", tmpFile.Name()))
	assert.Contains(t, stdout.String(), "OK")
}

func TestRun_IncompatibleWithAggregationAlgorithm(t *testing.T) {
	if tests.TestDbInfo.VersionAtLeast(3, 5, 6) {
		t.Skip("Test for older MongoDB versions.")
	}

	os.Clearenv()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{})

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{
		"cmd",
		"--host", tests.TestDbUri,
		"--db", c.Database.Name,
		"--col", c.Name,
		"--sample", "all",
		"--use-aggregation",
	})

	config, _ := GetConfig(v)
	err := Run(cmd, config)

	assert.Equal(t,
		fmt.Sprintf("Option 'use-aggregation' require MongoDB version >= %s.\n", helpers.VersionToString(analysis.AggregationMinVersion...)),
		err.Error(),
	)
}

func TestRun_IncompatibleWithSampleStage(t *testing.T) {
	if tests.TestDbInfo.VersionAtLeast(3, 2, 0) {
		t.Skip("Test for older MongoDB versions.")
	}

	os.Clearenv()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{})

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{
		"cmd",
		"--host", tests.TestDbUri,
		"--db", c.Database.Name,
		"--col", c.Name,
		"--sample", "random:1000",
	})

	config, _ := GetConfig(v)
	err := Run(cmd, config)

	assert.Equal(t,
		fmt.Sprintf("Invalid value of '--sample' option.\nSample 'random' require MongoDB version >= %s.\nPlease, use 'all', 'first:N' or 'last:N' sample.\n", helpers.VersionToString(analysis.RandomSampleMinVersion...)),
		err.Error(),
	)
}

func Test_checkCompatibility(t *testing.T) {
	config := &Config{
		UseAggregation: true,
		Sample:         "random",
	}

	info := mgo.BuildInfo{
		Version:      analysis.AggregationMinVersionStr,
		VersionArray: analysis.AggregationMinVersion,
	}

	assert.Equal(t, nil, checkCompatibility(config, info))
}

func Test_checkCompatibility_UnsupportedAggregationAlgorithm(t *testing.T) {
	config := &Config{
		UseAggregation: true,
		Sample:         "all",
	}

	info := mgo.BuildInfo{
		Version:      "3.5.0",
		VersionArray: []int{3, 5, 0},
	}

	assert.NotEqual(t, nil, checkCompatibility(config, info))
}

func Test_checkCompatibility_UnsupportedSample(t *testing.T) {
	config := &Config{
		UseAggregation: false,
		Sample:         "random",
	}

	info := mgo.BuildInfo{
		Version:      "3.1.0",
		VersionArray: []int{3, 1, 0},
	}

	assert.NotEqual(t, nil, checkCompatibility(config, info))
}
