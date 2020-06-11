package cli

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

func TestFormat_JSON(t *testing.T) {
	result := Result{
		Database:     "db",
		Collection:   "col",
		Plan:         "local",
		Duration:     20 * time.Millisecond,
		AllDocsCount: 12345,
		DocsCount:    1000,
		FieldsCount:  1,
		Fields: analysis.Fields{
			{
				Name:  "_id",
				Level: 0,
				Count: 1000,
				Types: analysis.Types{
					{
						Name:  "objectId",
						Count: 1000,
						ValueStats: &analysis.ValueStats{
							Min: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9a1"),
							Max: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
						},
						ValueHistogram: &analysis.Histogram{
							Start:         helpers.ParseDate("2000-01-01T00:00:00+00:00"),
							End:           helpers.ParseDate("2000-01-10T00:00:00+00:00"),
							Range:         10 * 24 * 60 * 60,
							Step:          24 * 60 * 60,
							NumberOfSteps: 10,
							Intervals: analysis.Intervals{
								{
									Interval: 0,
									Count:    10,
								},
								{
									Interval: 5,
									Count:    4,
								},
								{
									Interval: 9,
									Count:    20,
								},
							},
						},
					},
				},
			},
		},
	}

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")

	cmd.ParseFlags([]string{"cmd", "--format", "json"})
	config, err := GetConfig(v)
	assert.Equal(t, nil, err)

	out, err := Format(result, config)
	assert.Equal(t, nil, err)

	expected := `{
	"database": "db",
	"collection": "col",
	"plan": "local",
	"duration": 20000000,
	"allDocs": 12345,
	"analyzedDocs": 1000,
	"fieldsCount": 1,
	"fields": [
		{
			"name": "_id",
			"level": 0,
			"count": 1000,
			"types": [
				{
					"type": "objectId",
					"count": 1000,
					"value": {
						"min": "58e20d849d3ae7e1f8eac9a1",
						"max": "58e20d849d3ae7e1f8eac9c1"
					},
					"valueHistogram": {
						"start": "2000-01-01T00:00:00Z",
						"end": "2000-01-10T00:00:00Z",
						"range": 864000,
						"step": 86400,
						"numOfSteps": 10,
						"intervals": [
							10,
							0,
							0,
							0,
							0,
							4,
							0,
							0,
							0,
							20
						]
					}
				}
			]
		}
	]
}`
	assert.Equal(t, expected, string(out))
}

func TestFormat_JSON_FileOutput(t *testing.T) {
	result := Result{
		Database:     "db",
		Collection:   "col",
		Plan:         "local",
		Duration:     20 * time.Millisecond,
		AllDocsCount: 10000,
		DocsCount:    1000,
		FieldsCount:  1,
		Fields: analysis.Fields{
			{
				Name:  "_id",
				Level: 0,
				Count: 1000,
			},
		},
	}

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")

	cmd.ParseFlags([]string{"cmd", "--format", "json", "--file", "abc"})
	config, err := GetConfig(v)
	assert.Equal(t, nil, err)

	out, err := Format(result, config)
	assert.Equal(t, nil, err)

	expected := `{"database":"db","collection":"col","plan":"local","duration":20000000,"allDocs":10000,"analyzedDocs":1000,"fieldsCount":1,"fields":[{"name":"_id","level":0,"count":1000,"types":null}]}`
	assert.Equal(t, expected, string(out))
}

func TestFormat_YAML(t *testing.T) {
	result := Result{
		Database:     "db",
		Collection:   "col",
		Plan:         "local",
		Duration:     20 * time.Millisecond,
		AllDocsCount: 12345,
		DocsCount:    1000,
		FieldsCount:  1,
		Fields: analysis.Fields{
			{
				Name:  "_id",
				Count: 1000,
				Level: 0,
				Types: analysis.Types{
					{
						Name:  "objectId",
						Count: 1000,
						ValueStats: &analysis.ValueStats{
							Min: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9a1"),
							Max: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
						},
						ValueHistogram: &analysis.Histogram{
							Start:         helpers.ParseDate("2000-01-01T00:00:00+00:00"),
							End:           helpers.ParseDate("2000-01-10T00:00:00+00:00"),
							Range:         10 * 24 * 60 * 60,
							Step:          24 * 60 * 60,
							NumberOfSteps: 10,
							Intervals: analysis.Intervals{
								{
									Interval: 0,
									Count:    10,
								},
								{
									Interval: 5,
									Count:    4,
								},
								{
									Interval: 9,
									Count:    20,
								},
							},
						},
					},
				},
			},
		},
	}

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")

	cmd.ParseFlags([]string{"cmd", "--format", "yaml"})
	config, err := GetConfig(v)
	assert.Equal(t, nil, err)

	out, err := Format(result, config)
	assert.Equal(t, nil, err)

	expected := `database: db
collection: col
plan: local
duration: 20ms
allDocs: 12345
analyzedDocs: 1000
fieldsCount: 1
fields:
- name: _id
  level: 0
  count: 1000
  types:
  - type: objectId
    count: 1000
    value:
      min: 58e20d849d3ae7e1f8eac9a1
      max: 58e20d849d3ae7e1f8eac9c1
    valueHistogram:
      start: 2000-01-01T00:00:00Z
      end: 2000-01-10T00:00:00Z
      range: 864000
      step: 86400
      numOfSteps: 10
      intervals:
      - 10
      - 0
      - 0
      - 0
      - 0
      - 4
      - 0
      - 0
      - 0
      - 20
`
	assert.Equal(t, expected, string(out))
}
