package cli

import (
	"github.com/fatih/color"
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"strings"
	"testing"
	"time"
)

// https://github.com/mongoeye/mongoeye/issues/11
func TestFormat_TABLE_OneItem(t *testing.T) {
	color.NoColor = true

	result := Result{
		Plan:         "local",
		Duration:     20 * time.Millisecond,
		AllDocsCount: 1,
		DocsCount:    1,
		FieldsCount:  3,
		Fields: analysis.Fields{
			{
				Name:  "_id",
				Count: 1,
				Level: 0,
				Types: analysis.Types{
					{
						Name:  "objectId",
						Count: 1,
					},
				},
			},
			{
				Name:  "hello",
				Count: 1,
				Level: 0,
				Types: analysis.Types{
					{
						Name:  "array",
						Count: 1,
					},
				},
			},
			{
				Name:  "hello.[]",
				Count: 5,
				Level: 1,
				Types: analysis.Types{
					{
						Name:  "int",
						Count: 2,
					},
					{
						Name:  "double",
						Count: 1,
					},
					{
						Name:  "string",
						Count: 2,
					},
				},
			},
		},
	}

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")

	cmd.ParseFlags([]string{"cmd", "--format", "table"})
	config, err := GetConfig(v)
	assert.Equal(t, nil, err)

	out, _ := Format(result, config)

	expected := []string{
		"         KEY         │ COUNT  │   %    ",
		"───────────────────────────────────────",
		"  all documents      │ 1      │        ",
		"  analyzed documents │ 1      │ 100.0  ",
		"                     │        │        ",
		"  _id ➜ objectId     │ 1      │ 100.0  ",
		"  hello ➜ array      │ 1      │ 100.0  ",
		"  ├╴[array item]     │ 5      │        ",
		"  │ │ ➜ int          │ 2      │  40.0  ",
		"  │ │ ➜ double       │ 1      │  20.0  ",
		"  └─┴╴➜ string       │ 2      │  40.0  \n",
	}

	assert.Equal(t, strings.Join(expected, "\n"), string(out))
}

func TestFormat_TABLE_Complex(t *testing.T) {
	color.NoColor = true

	result := Result{
		Plan:         "local",
		Duration:     20 * time.Millisecond,
		AllDocsCount: 12345,
		DocsCount:    1000,
		FieldsCount:  15,
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
			{
				Name:  "object",
				Count: 400,
				Level: 0,
				Types: analysis.Types{
					{
						Name:  "object",
						Count: 400,
					},
				},
			},
			{
				Name:  "root1",
				Count: 900,
				Level: 0,
				Types: analysis.Types{
					{
						Name:  "string",
						Count: 60,
					},
					{
						Name:  "object",
						Count: 840,
					},
				},
			},
			{
				Name:  "root1.sub1",
				Count: 900,
				Level: 1,
				Types: analysis.Types{
					{
						Name:  "long",
						Count: 840,
					},
					{
						Name:  "string",
						Count: 60,
					},
				},
			},
			{
				Name:  "root1.sub1.sub2",
				Count: 900,
				Level: 2,
				Types: analysis.Types{
					{
						Name:  "string",
						Count: 900,
					},
				},
			},
			{
				Name:  "root1.sub1.sub3",
				Count: 900,
				Level: 2,
				Types: analysis.Types{
					{
						Name:  "int",
						Count: 500,
					},
					{
						Name:  "string",
						Count: 400,
					},
				},
			},
			{
				Name:  "root2",
				Count: 900,
				Level: 0,
				Types: analysis.Types{
					{
						Name:  "string",
						Count: 60,
					},
					{
						Name:  "int",
						Count: 840,
					},
				},
			},
			{
				Name:  "array1",
				Count: 900,
				Level: 0,
				Types: analysis.Types{
					{
						Name:  "array",
						Count: 900,
					},
				},
			},
			{
				Name:  "array1.[]",
				Count: 900,
				Level: 1,
				Types: analysis.Types{
					{
						Name:  "object",
						Count: 900,
					},
				},
			},
			{
				Name:  "array2",
				Count: 900,
				Level: 0,
				Types: analysis.Types{
					{
						Name:  "int",
						Count: 100,
					},
					{
						Name:  "object",
						Count: 200,
					},
					{
						Name:  "array",
						Count: 600,
					},
				},
			},
			{
				Name:  "array2.[]",
				Count: 900,
				Level: 1,
				Types: analysis.Types{
					{
						Name:  "object",
						Count: 900,
					},
				},
			},
			{
				Name:  "array3",
				Count: 900,
				Level: 0,
				Types: analysis.Types{
					{
						Name:  "array",
						Count: 900,
					},
				},
			},
			{
				Name:  "array3.[]",
				Count: 900,
				Level: 1,
				Types: analysis.Types{
					{
						Name:  "array",
						Count: 900,
					},
				},
			},
			{
				Name:  "array3.[].[]",
				Count: 900,
				Level: 2,
				Types: analysis.Types{
					{
						Name:  "array",
						Count: 900,
					},
				},
			},
			{
				Name:  "array3.[].[].[]",
				Count: 900,
				Level: 3,
				Types: analysis.Types{
					{
						Name:  "int",
						Count: 900,
					},
				},
			},
		},
	}

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")

	cmd.ParseFlags([]string{"cmd", "--format", "table"})
	config, err := GetConfig(v)
	assert.Equal(t, nil, err)

	out, _ := Format(result, config)

	expected := []string{
		"            KEY            │ COUNT  │   %    ",
		"─────────────────────────────────────────────",
		"  all documents            │ 12345  │        ",
		"  analyzed documents       │  1000  │   8.1  ",
		"                           │        │        ",
		"  _id ➜ objectId           │  1000  │ 100.0  ",
		"  object ➜ object          │   400  │  40.0  ",
		"  root1                    │   900  │  90.0  ",
		"  │ ➜ string               │    60  │   6.7  ",
		"  │ ➜ object               │   840  │  93.3  ",
		"  ├╴sub1                   │   900  │ 107.1  ",
		"  │ │ ➜ long               │   840  │  93.3  ",
		"  │ │ ➜ string             │    60  │   6.7  ",
		"  │ ├╴sub2 ➜ string        │   900  │ 100.0  ",
		"  │ ├╴sub3                 │   900  │ 100.0  ",
		"  │ │ │ ➜ int              │   500  │  55.6  ",
		"  └─┴─┴╴➜ string           │   400  │  44.4  ",
		"  root2                    │   900  │  90.0  ",
		"  │ ➜ string               │    60  │   6.7  ",
		"  └╴➜ int                  │   840  │  93.3  ",
		"  array1 ➜ array           │   900  │  90.0  ",
		"  └╴[array item] ➜ object  │   900  │        ",
		"  array2                   │   900  │  90.0  ",
		"  │ ➜ int                  │   100  │  11.1  ",
		"  │ ➜ object               │   200  │  22.2  ",
		"  │ ➜ array                │   600  │  66.7  ",
		"  └╴[array item] ➜ object  │   900  │        ",
		"  array3 ➜ array           │   900  │  90.0  ",
		"  ├╴[array item] ➜ array   │   900  │        ",
		"  │ ├╴[array item] ➜ array │   900  │        ",
		"  └─┴─┴╴[array item] ➜ int │   900  │        \n",
	}

	assert.Equal(t, strings.Join(expected, "\n"), string(out))
}

func TestFormat_TABLE_FileOutput(t *testing.T) {
	color.NoColor = false

	result := Result{
		Plan:         "local",
		Duration:     20 * time.Millisecond,
		AllDocsCount: 12345,
		DocsCount:    1000,
		FieldsCount:  15,
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
			{
				Name:  "object",
				Count: 400,
				Level: 0,
				Types: analysis.Types{
					{
						Name:  "object",
						Count: 400,
					},
				},
			},
			{
				Name:  "root1",
				Count: 900,
				Level: 0,
				Types: analysis.Types{
					{
						Name:  "string",
						Count: 60,
					},
					{
						Name:  "object",
						Count: 840,
					},
				},
			},
			{
				Name:  "root1.sub1",
				Count: 900,
				Level: 1,
				Types: analysis.Types{
					{
						Name:  "long",
						Count: 840,
					},
					{
						Name:  "string",
						Count: 60,
					},
				},
			},
			{
				Name:  "root1.sub1.sub2",
				Count: 900,
				Level: 2,
				Types: analysis.Types{
					{
						Name:  "string",
						Count: 900,
					},
				},
			},
			{
				Name:  "root1.sub1.sub3",
				Count: 900,
				Level: 2,
				Types: analysis.Types{
					{
						Name:  "int",
						Count: 500,
					},
					{
						Name:  "string",
						Count: 400,
					},
				},
			},
			{
				Name:  "root2",
				Count: 900,
				Level: 0,
				Types: analysis.Types{
					{
						Name:  "string",
						Count: 60,
					},
					{
						Name:  "int",
						Count: 840,
					},
				},
			},
			{
				Name:  "array1",
				Count: 900,
				Level: 0,
				Types: analysis.Types{
					{
						Name:  "array",
						Count: 900,
					},
				},
			},
			{
				Name:  "array1.[]",
				Count: 900,
				Level: 1,
				Types: analysis.Types{
					{
						Name:  "object",
						Count: 900,
					},
				},
			},
			{
				Name:  "array2",
				Count: 900,
				Level: 0,
				Types: analysis.Types{
					{
						Name:  "int",
						Count: 100,
					},
					{
						Name:  "object",
						Count: 200,
					},
					{
						Name:  "array",
						Count: 600,
					},
				},
			},
			{
				Name:  "array2.[]",
				Count: 900,
				Level: 1,
				Types: analysis.Types{
					{
						Name:  "object",
						Count: 900,
					},
				},
			},
			{
				Name:  "array3",
				Count: 900,
				Level: 0,
				Types: analysis.Types{
					{
						Name:  "array",
						Count: 900,
					},
				},
			},
			{
				Name:  "array3.[]",
				Count: 900,
				Level: 1,
				Types: analysis.Types{
					{
						Name:  "array",
						Count: 900,
					},
				},
			},
			{
				Name:  "array3.[].[]",
				Count: 900,
				Level: 2,
				Types: analysis.Types{
					{
						Name:  "array",
						Count: 900,
					},
				},
			},
			{
				Name:  "array3.[].[].[]",
				Count: 900,
				Level: 3,
				Types: analysis.Types{
					{
						Name:  "int",
						Count: 900,
					},
				},
			},
		},
	}

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")

	cmd.ParseFlags([]string{"cmd", "--format", "table", "--file", "abc"})
	config, err := GetConfig(v)
	assert.Equal(t, nil, err)

	out, _ := Format(result, config)

	expected := []string{
		"            KEY            │ COUNT  │   %    ",
		"─────────────────────────────────────────────",
		"  all documents            │ 12345  │        ",
		"  analyzed documents       │  1000  │   8.1  ",
		"                           │        │        ",
		"  _id ➜ objectId           │  1000  │ 100.0  ",
		"  object ➜ object          │   400  │  40.0  ",
		"  root1                    │   900  │  90.0  ",
		"  │ ➜ string               │    60  │   6.7  ",
		"  │ ➜ object               │   840  │  93.3  ",
		"  ├╴sub1                   │   900  │ 107.1  ",
		"  │ │ ➜ long               │   840  │  93.3  ",
		"  │ │ ➜ string             │    60  │   6.7  ",
		"  │ ├╴sub2 ➜ string        │   900  │ 100.0  ",
		"  │ ├╴sub3                 │   900  │ 100.0  ",
		"  │ │ │ ➜ int              │   500  │  55.6  ",
		"  └─┴─┴╴➜ string           │   400  │  44.4  ",
		"  root2                    │   900  │  90.0  ",
		"  │ ➜ string               │    60  │   6.7  ",
		"  └╴➜ int                  │   840  │  93.3  ",
		"  array1 ➜ array           │   900  │  90.0  ",
		"  └╴[array item] ➜ object  │   900  │        ",
		"  array2                   │   900  │  90.0  ",
		"  │ ➜ int                  │   100  │  11.1  ",
		"  │ ➜ object               │   200  │  22.2  ",
		"  │ ➜ array                │   600  │  66.7  ",
		"  └╴[array item] ➜ object  │   900  │        ",
		"  array3 ➜ array           │   900  │  90.0  ",
		"  ├╴[array item] ➜ array   │   900  │        ",
		"  │ ├╴[array item] ➜ array │   900  │        ",
		"  └─┴─┴╴[array item] ➜ int │   900  │        \n",
	}

	assert.Equal(t, strings.Join(expected, "\n"), string(out))
}
