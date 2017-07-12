package groupTests

import (
	"github.com/jinzhu/copier"
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/mongoeye/mongoeye/tests"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

// RunTestValueMinMaxAvg tests group stage with StoreMinMaxAvgValue option.
func RunTestValueMinMaxAvg(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	// Decimal type is new in version 3.4
	decimalType := "decimal"
	var decimal1 interface{} = helpers.ParseDecimal("123.3")
	var decimal2 interface{} = helpers.ParseDecimal("456.9")
	var decimal3 interface{} = helpers.ParseDecimal("789.3")
	var decimalAvg interface{} = helpers.ParseDecimal("456.5")
	if !tests.HasMongoDBDecimalSupport() {
		decimalType = "string"
		decimal1 = "123.3"
		decimal2 = "456.9"
		decimal3 = "789.3"
		decimalAvg = nil
	}

	c.Insert(bson.M{
		"_id":                  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_double":              123.456,
		"_string":              "Šašo",
		"_binData":             []byte("Šašo"),
		"_undefined":           bson.Undefined,
		"_bool":                false,
		"_date":                time.Unix(1490959230, 0),
		"_null":                nil,
		"_regex":               bson.RegEx{Pattern: "a.*", Options: "i"},
		"_dbPointer":           bson.DBPointer{Namespace: "ns", Id: bson.ObjectIdHex("58de3f123d9654ba801bb30d")},
		"_javascript":          bson.JavaScript{Code: "var x = 1+1;", Scope: nil},
		"_symbol":              bson.Symbol("x"),
		"_javascriptWithScope": bson.JavaScript{Code: "var x = 1+y;", Scope: bson.M{"y": 5}},
		"_int":                 123,
		"_timestamp":           bson.MongoTimestamp(1490959230),
		"_long":                int64(456),
		"_decimal":             decimal1,
		"_minKey":              bson.MinKey,
		"_maxKey":              bson.MaxKey,
		"_object": bson.M{
			"f1": 10,
			"f2": "abc",
		},
	})
	c.Insert(bson.M{
		"_id":                  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"_double":              456.789,
		"_string":              "abc",
		"_binData":             []byte("abc"),
		"_undefined":           bson.Undefined,
		"_bool":                true,
		"_date":                time.Unix(1490959231, 0),
		"_null":                nil,
		"_regex":               bson.RegEx{Pattern: "b.*", Options: "i"},
		"_dbPointer":           bson.DBPointer{Namespace: "ns", Id: bson.ObjectIdHex("58de3f123d9654ba801bb31d")},
		"_javascript":          bson.JavaScript{Code: "var x = 2+2;", Scope: nil},
		"_symbol":              bson.Symbol("x"),
		"_javascriptWithScope": bson.JavaScript{Code: "var x = 2+y;", Scope: bson.M{"y": 5}},
		"_int":                 123,
		"_timestamp":           bson.MongoTimestamp(1490959230),
		"_long":                int64(456),
		"_decimal":             decimal2,
		"_minKey":              bson.MinKey,
		"_maxKey":              bson.MaxKey,
		"_object": bson.M{
			"f1": "cde",
			"f2": 20,
		},
		"_array": []interface{}{1, 2},
	})
	c.Insert(bson.M{
		"_id":                  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_double":              798.123,
		"_string":              "cde",
		"_binData":             []byte("cde"),
		"_undefined":           bson.Undefined,
		"_bool":                true,
		"_date":                time.Unix(1490959232, 0),
		"_null":                nil,
		"_regex":               bson.RegEx{Pattern: "c.*", Options: "i"},
		"_dbPointer":           bson.DBPointer{Namespace: "ns", Id: bson.ObjectIdHex("58de3f123d9654ba801bb32d")},
		"_javascript":          bson.JavaScript{Code: "var x = 3+3;", Scope: nil},
		"_symbol":              bson.Symbol("y"),
		"_javascriptWithScope": bson.JavaScript{Code: "var x = 3+y;", Scope: bson.M{"y": 5}},
		"_int":                 456,
		"_timestamp":           bson.MongoTimestamp(1490959231),
		"_long":                int64(123),
		"_decimal":             decimal3,
		"_minKey":              bson.MinKey,
		"_maxKey":              bson.MaxKey,
		"_array":               []interface{}{3, 4},
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreMinMaxAvgValue = true

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 3,
				ValueExtremes: &analysis.ValueExtremes{
					Min: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
					Max: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
				},
			},
		},
		group.Result{
			Name: "_double",
			Type: analysis.Type{
				Name:  "double",
				Count: 3,
				ValueExtremes: &analysis.ValueExtremes{
					Min: 123.456,
					Max: 798.123,
					Avg: 459.45599999999996,
				},
			},
		},
		group.Result{
			Name: "_string",
			Type: analysis.Type{
				Name:  "string",
				Count: 3,
				ValueExtremes: &analysis.ValueExtremes{
					Min: "abc",
					Max: "Šašo",
					Avg: nil,
				},
			},
		},
		group.Result{
			Name: "_binData",
			Type: analysis.Type{
				Name:  "binData",
				Count: 3,
			},
		},
		group.Result{
			Name: "_undefined",
			Type: analysis.Type{
				Name:  "undefined",
				Count: 3,
			},
		},
		group.Result{
			Name: "_bool",
			Type: analysis.Type{
				Name:  "bool",
				Count: 3,
				ValueExtremes: &analysis.ValueExtremes{
					Min: false,
					Max: true,
					Avg: 0.6666666666666666,
				},
			},
		},
		group.Result{
			Name: "_date",
			Type: analysis.Type{
				Name:  "date",
				Count: 3,
				ValueExtremes: &analysis.ValueExtremes{
					Min: time.Unix(1490959230, 0),
					Max: time.Unix(1490959232, 0),
				},
			},
		},
		group.Result{
			Name: "_null",
			Type: analysis.Type{
				Name:  "null",
				Count: 3,
			},
		},
		group.Result{
			Name: "_regex",
			Type: analysis.Type{
				Name:  "regex",
				Count: 3,
			},
		},
		group.Result{
			Name: "_dbPointer",
			Type: analysis.Type{
				Name:  "dbPointer",
				Count: 3,
			},
		},
		group.Result{
			Name: "_javascript",
			Type: analysis.Type{
				Name:  "javascript",
				Count: 3,
			},
		},
		group.Result{
			Name: "_symbol",
			Type: analysis.Type{
				Name:  "symbol",
				Count: 3,
			},
		},
		group.Result{
			Name: "_javascriptWithScope",
			Type: analysis.Type{
				Name:  "javascriptWithScope",
				Count: 3,
			},
		},
		group.Result{
			Name: "_int",
			Type: analysis.Type{
				Name:  "int",
				Count: 3,
				ValueExtremes: &analysis.ValueExtremes{
					Min: 123,
					Max: 456,
					Avg: 234,
				},
			},
		},
		group.Result{
			Name: "_timestamp",
			Type: analysis.Type{
				Name:  "timestamp",
				Count: 3,
				ValueExtremes: &analysis.ValueExtremes{
					Min: bson.MongoTimestamp(1490959230),
					Max: bson.MongoTimestamp(1490959231),
				},
			},
		},
		group.Result{
			Name: "_long",
			Type: analysis.Type{
				Name:  "long",
				Count: 3,
				ValueExtremes: &analysis.ValueExtremes{
					Min: 123,
					Max: 456,
					Avg: 345,
				},
			},
		},
		group.Result{
			Name: "_decimal",
			Type: analysis.Type{
				Name:  decimalType,
				Count: 3,
				ValueExtremes: &analysis.ValueExtremes{
					Min: decimal1,
					Max: decimal3,
					Avg: decimalAvg,
				},
			},
		},
		group.Result{
			Name: "_minKey",
			Type: analysis.Type{
				Name:  "minKey",
				Count: 3,
			},
		},
		group.Result{
			Name: "_maxKey",
			Type: analysis.Type{
				Name:  "maxKey",
				Count: 3,
			},
		},
		group.Result{
			Name: "_object",
			Type: analysis.Type{
				Name:  "object",
				Count: 2,
			},
		},
		group.Result{
			Name: "_object.f1",
			Type: analysis.Type{
				Name:  "int",
				Count: 1,
				ValueExtremes: &analysis.ValueExtremes{
					Min: 10,
					Max: 10,
					Avg: 10,
				},
			},
		},
		group.Result{
			Name: "_object.f1",
			Type: analysis.Type{
				Name:  "string",
				Count: 1,
				ValueExtremes: &analysis.ValueExtremes{
					Min: "cde",
					Max: "cde",
				},
			},
		},
		group.Result{
			Name: "_object.f2",
			Type: analysis.Type{
				Name:  "string",
				Count: 1,
				ValueExtremes: &analysis.ValueExtremes{
					Min: "abc",
					Max: "abc",
				},
			},
		},
		group.Result{
			Name: "_object.f2",
			Type: analysis.Type{
				Name:  "int",
				Count: 1,
				ValueExtremes: &analysis.ValueExtremes{
					Min: 20,
					Max: 20,
					Avg: 20,
				},
			},
		},
		group.Result{
			Name: "_array",
			Type: analysis.Type{
				Name:  "array",
				Count: 2,
			},
		},
		group.Result{
			Name: "_array.[]",
			Type: analysis.Type{
				Name:  "int",
				Count: 4,
				ValueExtremes: &analysis.ValueExtremes{
					Min: 1,
					Max: 4,
					Avg: 2.5,
				},
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}

// RunTestValueTopValues tests group stage with StoreTopNValues option.
func RunTestValueTopValues(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	// Decimal type is new in version 3.4
	decimalType := "decimal"
	var decimal1 interface{} = helpers.ParseDecimal("123.3")
	var decimal2 interface{} = helpers.ParseDecimal("45")
	var decimal3 interface{} = helpers.ParseDecimal("79")
	var decimal4 interface{} = helpers.ParseDecimal("91")
	if !tests.HasMongoDBDecimalSupport() {
		decimalType = "string"
		decimal1 = "123.3"
		decimal2 = "45"
		decimal3 = "79"
		decimal4 = "91"
	}

	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"_double":    123.456,
		"_string":    "Šašo",
		"_date":      time.Unix(1490959230, 0),
		"_int":       123,
		"_timestamp": bson.MongoTimestamp(1490959230),
		"_long":      int64(456),
		"_decimal":   decimal1,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_double":    123.456,
		"_string":    "Šašo",
		"_date":      time.Unix(1490959230, 0),
		"_int":       123,
		"_timestamp": bson.MongoTimestamp(1490959230),
		"_long":      int64(456),
		"_decimal":   decimal1,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
		"_double":    123.456,
		"_string":    "Šašo",
		"_date":      time.Unix(1490959230, 0),
		"_int":       123,
		"_timestamp": bson.MongoTimestamp(1490959230),
		"_long":      int64(456),
		"_decimal":   decimal1,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c4"),
		"_double":    16.0,
		"_string":    "fgh",
		"_date":      time.Unix(1290959230, 0),
		"_int":       12,
		"_timestamp": bson.MongoTimestamp(1290959230),
		"_long":      int64(111),
		"_decimal":   decimal2,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c5"),
		"_double":    16.0,
		"_string":    "fgh",
		"_date":      time.Unix(1290959230, 0),
		"_int":       12,
		"_timestamp": bson.MongoTimestamp(1290959230),
		"_long":      int64(111),
		"_decimal":   decimal2,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c6"),
		"_double":    87.0,
		"_string":    "xyz",
		"_date":      time.Unix(1390959230, 0),
		"_int":       35,
		"_timestamp": bson.MongoTimestamp(1470954230),
		"_long":      int64(106),
		"_decimal":   decimal3,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c7"),
		"_double":    80.0,
		"_string":    "xyzabc",
		"_date":      time.Unix(1590959230, 0),
		"_int":       31,
		"_timestamp": bson.MongoTimestamp(1471954230),
		"_long":      int64(105),
		"_decimal":   decimal4,
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreTopNValues = 2

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 7,
			},
		},
		group.Result{
			Name: "_double",
			Type: analysis.Type{
				Name:  "double",
				Count: 7,
				TopNValues: []analysis.ValueFreq{
					{
						Value: 123.456,
						Count: 3,
					},
					{
						Value: 16,
						Count: 2,
					},
				},
			},
		},
		group.Result{
			Name: "_string",
			Type: analysis.Type{
				Name:  "string",
				Count: 7,
				TopNValues: []analysis.ValueFreq{
					{
						Value: "Šašo",
						Count: 3,
					},
					{
						Value: "fgh",
						Count: 2,
					},
				},
			},
		},
		group.Result{
			Name: "_date",
			Type: analysis.Type{
				Name:  "date",
				Count: 7,
				TopNValues: []analysis.ValueFreq{
					{
						Value: helpers.ParseDate("2017-03-31T11:20:30+00:00"),
						Count: 3,
					},
					{
						Value: helpers.ParseDate("2010-11-28T15:47:10+00:00"),
						Count: 2,
					},
				},
			},
		},
		group.Result{
			Name: "_int",
			Type: analysis.Type{
				Name:  "int",
				Count: 7,
				TopNValues: []analysis.ValueFreq{
					{
						Value: 123,
						Count: 3,
					},
					{
						Value: 12,
						Count: 2,
					},
				},
			},
		},
		group.Result{
			Name: "_timestamp",
			Type: analysis.Type{
				Name:  "timestamp",
				Count: 7,
				TopNValues: []analysis.ValueFreq{
					{
						Value: 1490959230,
						Count: 3,
					},
					{
						Value: 1290959230,
						Count: 2,
					},
				},
			},
		},
		group.Result{
			Name: "_long",
			Type: analysis.Type{
				Name:  "long",
				Count: 7,
				TopNValues: []analysis.ValueFreq{
					{
						Value: 456,
						Count: 3,
					},
					{
						Value: 111,
						Count: 2,
					},
				},
			},
		},
		group.Result{
			Name: "_decimal",
			Type: analysis.Type{
				Name:  decimalType,
				Count: 7,
				TopNValues: []analysis.ValueFreq{
					{
						Value: decimal1,
						Count: 3,
					},
					{
						Value: decimal2,
						Count: 2,
					},
				},
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}

// RunTestValueTopValuesNGreaterThanNumberOfValues tests group stage with StoreTopNValues option + StoreTopNValues > number of values.
func RunTestValueTopValuesNGreaterThanNumberOfValues(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	// Decimal type is new in version 3.4
	decimalType := "decimal"
	var decimal interface{} = helpers.ParseDecimal("123.3")
	if !tests.HasMongoDBDecimalSupport() {
		decimalType = "string"
		decimal = "123.3"
	}

	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"_double":    123.456,
		"_string":    "Šašo",
		"_date":      time.Unix(1490959230, 0),
		"_int":       123,
		"_timestamp": bson.MongoTimestamp(1490959230),
		"_long":      int64(456),
		"_decimal":   decimal,
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreTopNValues = 200

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 1,
			},
		},
		group.Result{
			Name: "_double",
			Type: analysis.Type{
				Name:  "double",
				Count: 1,
				TopNValues: []analysis.ValueFreq{
					{
						Value: 123.456,
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_string",
			Type: analysis.Type{
				Name:  "string",
				Count: 1,
				TopNValues: []analysis.ValueFreq{
					{
						Value: "Šašo",
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_date",
			Type: analysis.Type{
				Name:  "date",
				Count: 1,
				TopNValues: []analysis.ValueFreq{
					{
						Value: helpers.ParseDate("2017-03-31T11:20:30+00:00"),
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_int",
			Type: analysis.Type{
				Name:  "int",
				Count: 1,
				TopNValues: []analysis.ValueFreq{
					{
						Value: 123,
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_timestamp",
			Type: analysis.Type{
				Name:  "timestamp",
				Count: 1,
				TopNValues: []analysis.ValueFreq{
					{
						Value: 1490959230,
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_long",
			Type: analysis.Type{
				Name:  "long",
				Count: 1,
				TopNValues: []analysis.ValueFreq{
					{
						Value: 456,
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_decimal",
			Type: analysis.Type{
				Name:  decimalType,
				Count: 1,
				TopNValues: []analysis.ValueFreq{
					{
						Value: decimal,
						Count: 1,
					},
				},
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}

// RunTestValueBottomValues tests group stage with StoreBottomNValues option.
func RunTestValueBottomValues(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	// Decimal type is new in version 3.4
	decimalType := "decimal"
	var decimal1 interface{} = helpers.ParseDecimal("123.3")
	var decimal2 interface{} = helpers.ParseDecimal("45")
	var decimal3 interface{} = helpers.ParseDecimal("79")
	var decimal4 interface{} = helpers.ParseDecimal("91")
	if !tests.HasMongoDBDecimalSupport() {
		decimalType = "string"
		decimal1 = "123.3"
		decimal2 = "45"
		decimal3 = "79"
		decimal4 = "91"
	}

	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"_double":    123.456,
		"_string":    "Šašo",
		"_date":      time.Unix(1490959230, 0),
		"_int":       123,
		"_timestamp": bson.MongoTimestamp(1490959230),
		"_long":      int64(456),
		"_decimal":   decimal1,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_double":    123.456,
		"_string":    "Šašo",
		"_date":      time.Unix(1490959230, 0),
		"_int":       123,
		"_timestamp": bson.MongoTimestamp(1490959230),
		"_long":      int64(456),
		"_decimal":   decimal1,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
		"_double":    123.456,
		"_string":    "Šašo",
		"_date":      time.Unix(1490959230, 0),
		"_int":       123,
		"_timestamp": bson.MongoTimestamp(1490959230),
		"_long":      int64(456),
		"_decimal":   decimal1,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c4"),
		"_double":    16.0,
		"_string":    "fgh",
		"_date":      time.Unix(1290959230, 0),
		"_int":       12,
		"_timestamp": bson.MongoTimestamp(1290959230),
		"_long":      int64(111),
		"_decimal":   decimal2,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c5"),
		"_double":    16.0,
		"_string":    "fgh",
		"_date":      time.Unix(1290959230, 0),
		"_int":       12,
		"_timestamp": bson.MongoTimestamp(1290959230),
		"_long":      int64(111),
		"_decimal":   decimal2,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c6"),
		"_double":    87.0,
		"_string":    "xyz",
		"_date":      time.Unix(1390959230, 0),
		"_int":       35,
		"_timestamp": bson.MongoTimestamp(1470954230),
		"_long":      int64(106),
		"_decimal":   decimal3,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c7"),
		"_double":    80.0,
		"_string":    "xyzabc",
		"_date":      time.Unix(1590959230, 0),
		"_int":       31,
		"_timestamp": bson.MongoTimestamp(1471954230),
		"_long":      int64(105),
		"_decimal":   decimal4,
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreBottomNValues = 2

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 7,
			},
		},
		group.Result{
			Name: "_double",
			Type: analysis.Type{
				Name:  "double",
				Count: 7,
				BottomNValues: []analysis.ValueFreq{
					{
						Value: 80.0,
						Count: 1,
					},
					{
						Value: 87.0,
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_string",
			Type: analysis.Type{
				Name:  "string",
				Count: 7,
				BottomNValues: []analysis.ValueFreq{
					{
						Value: "xyzabc",
						Count: 1,
					},
					{
						Value: "xyz",
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_date",
			Type: analysis.Type{
				Name:  "date",
				Count: 7,
				BottomNValues: []analysis.ValueFreq{
					{
						Value: helpers.ParseDate("2020-05-31T21:07:10+00:00"),
						Count: 1,
					},
					{
						Value: helpers.ParseDate("2014-01-29T01:33:50+00:00"),
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_int",
			Type: analysis.Type{
				Name:  "int",
				Count: 7,
				BottomNValues: []analysis.ValueFreq{
					{
						Value: 31,
						Count: 1,
					},
					{
						Value: 35,
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_timestamp",
			Type: analysis.Type{
				Name:  "timestamp",
				Count: 7,
				BottomNValues: []analysis.ValueFreq{
					{
						Value: 1471954230,
						Count: 1,
					},
					{
						Value: 1470954230,
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_long",
			Type: analysis.Type{
				Name:  "long",
				Count: 7,
				BottomNValues: []analysis.ValueFreq{
					{
						Value: 105,
						Count: 1,
					},
					{
						Value: 106,
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_decimal",
			Type: analysis.Type{
				Name:  decimalType,
				Count: 7,
				BottomNValues: []analysis.ValueFreq{
					{
						Value: decimal4,
						Count: 1,
					},
					{
						Value: decimal3,
						Count: 1,
					},
				},
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}

// RunTestValueBottomValuesNGreaterThanNumberOfValues tests group stage with StoreBottomNValues option + StoreBottomNValues > number of values.
func RunTestValueBottomValuesNGreaterThanNumberOfValues(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	// Decimal type is new in version 3.4
	decimalType := "decimal"
	var decimal interface{} = helpers.ParseDecimal("123.3")
	if !tests.HasMongoDBDecimalSupport() {
		decimalType = "string"
		decimal = "123.3"
	}

	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"_double":    123.456,
		"_string":    "Šašo",
		"_date":      time.Unix(1490959230, 0),
		"_int":       123,
		"_timestamp": bson.MongoTimestamp(1490959230),
		"_long":      int64(456),
		"_decimal":   decimal,
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreBottomNValues = 200

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 1,
			},
		},
		group.Result{
			Name: "_double",
			Type: analysis.Type{
				Name:  "double",
				Count: 1,
				BottomNValues: []analysis.ValueFreq{
					{
						Value: 123.456,
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_string",
			Type: analysis.Type{
				Name:  "string",
				Count: 1,
				BottomNValues: []analysis.ValueFreq{
					{
						Value: "Šašo",
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_date",
			Type: analysis.Type{
				Name:  "date",
				Count: 1,
				BottomNValues: []analysis.ValueFreq{
					{
						Value: helpers.ParseDate("2017-03-31T11:20:30+00:00"),
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_int",
			Type: analysis.Type{
				Name:  "int",
				Count: 1,
				BottomNValues: []analysis.ValueFreq{
					{
						Value: 123,
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_timestamp",
			Type: analysis.Type{
				Name:  "timestamp",
				Count: 1,
				BottomNValues: []analysis.ValueFreq{
					{
						Value: 1490959230,
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_long",
			Type: analysis.Type{
				Name:  "long",
				Count: 1,
				BottomNValues: []analysis.ValueFreq{
					{
						Value: 456,
						Count: 1,
					},
				},
			},
		},
		group.Result{
			Name: "_decimal",
			Type: analysis.Type{
				Name:  decimalType,
				Count: 1,
				BottomNValues: []analysis.ValueFreq{
					{
						Value: decimal,
						Count: 1,
					},
				},
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}

// RunTestCountOfUniqueValues tests group stage with StoreCountOfUnique option.
func RunTestCountOfUniqueValues(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	// Decimal type is new in version 3.4
	decimalType := "decimal"
	var decimal1 interface{} = helpers.ParseDecimal("123.3")
	var decimal2 interface{} = helpers.ParseDecimal("45")
	var decimal3 interface{} = helpers.ParseDecimal("79")
	var decimal4 interface{} = helpers.ParseDecimal("91")
	if !tests.HasMongoDBDecimalSupport() {
		decimalType = "string"
		decimal1 = "123.3"
		decimal2 = "45"
		decimal3 = "79"
		decimal4 = "91"
	}

	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"_double":    123.456,
		"_string":    "Šašo",
		"_date":      time.Unix(1490959230, 0),
		"_int":       123,
		"_timestamp": bson.MongoTimestamp(1490959230),
		"_long":      int64(456),
		"_decimal":   decimal1,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_double":    123.456,
		"_string":    "Šašo",
		"_date":      time.Unix(1490959230, 0),
		"_int":       123,
		"_timestamp": bson.MongoTimestamp(1490959230),
		"_long":      int64(456),
		"_decimal":   decimal1,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
		"_double":    123.456,
		"_string":    "Šašo",
		"_date":      time.Unix(1490959230, 0),
		"_int":       123,
		"_timestamp": bson.MongoTimestamp(1490959230),
		"_long":      int64(456),
		"_decimal":   decimal1,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c4"),
		"_double":    16.0,
		"_string":    "fgh",
		"_date":      time.Unix(1290959230, 0),
		"_int":       12,
		"_timestamp": bson.MongoTimestamp(1290959230),
		"_long":      int64(111),
		"_decimal":   decimal2,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c5"),
		"_double":    16.0,
		"_string":    "fgh",
		"_date":      time.Unix(1290959230, 0),
		"_int":       12,
		"_timestamp": bson.MongoTimestamp(1290959230),
		"_long":      int64(111),
		"_decimal":   decimal2,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c6"),
		"_double":    87.0,
		"_string":    "xyz",
		"_date":      time.Unix(1390959230, 0),
		"_int":       35,
		"_timestamp": bson.MongoTimestamp(1470954230),
		"_long":      int64(106),
		"_decimal":   decimal3,
	})
	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c7"),
		"_double":    80.0,
		"_string":    "xyzabc",
		"_date":      time.Unix(1590959230, 0),
		"_int":       31,
		"_timestamp": bson.MongoTimestamp(1471954230),
		"_long":      int64(105),
		"_decimal":   decimal4,
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreCountOfUnique = true

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 7,
			},
		},
		group.Result{
			Name: "_double",
			Type: analysis.Type{
				Name:        "double",
				Count:       7,
				CountUnique: 4,
			},
		},
		group.Result{
			Name: "_string",
			Type: analysis.Type{
				Name:        "string",
				Count:       7,
				CountUnique: 4,
			},
		},
		group.Result{
			Name: "_date",
			Type: analysis.Type{
				Name:        "date",
				Count:       7,
				CountUnique: 4,
			},
		},
		group.Result{
			Name: "_int",
			Type: analysis.Type{
				Name:        "int",
				Count:       7,
				CountUnique: 4,
			},
		},
		group.Result{
			Name: "_timestamp",
			Type: analysis.Type{
				Name:        "timestamp",
				Count:       7,
				CountUnique: 4,
			},
		},
		group.Result{
			Name: "_long",
			Type: analysis.Type{
				Name:        "long",
				Count:       7,
				CountUnique: 4,
			},
		},
		group.Result{
			Name: "_decimal",
			Type: analysis.Type{
				Name:        decimalType,
				Count:       7,
				CountUnique: 4,
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}

// RunTestObjectIdAsDate tests group stage with ProcessObjectIdAsDate option.
func RunTestObjectIdAsDate(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	c.Insert(bson.M{
		"_id": bson.NewObjectIdWithTime(helpers.ParseDate("2017-04-16T23:59:59+00:00")),
	})
	c.Insert(bson.M{
		"_id": bson.NewObjectIdWithTime(helpers.ParseDate("2017-04-17T00:00:01+00:00")),
	})
	c.Insert(bson.M{
		"_id": bson.NewObjectIdWithTime(helpers.ParseDate("2017-04-18T10:00:00+00:00")),
	})
	c.Insert(bson.M{
		"_id": bson.NewObjectIdWithTime(helpers.ParseDate("2017-04-19T14:30:00+00:00")),
	})
	c.Insert(bson.M{
		"_id": bson.NewObjectIdWithTime(helpers.ParseDate("2017-04-20T05:45:57+00:00")),
	})
	c.Insert(bson.M{
		"_id": bson.NewObjectIdWithTime(helpers.ParseDate("2017-04-24T12:54:57+00:00")),
	})
	c.Insert(bson.M{
		"_id": bson.NewObjectIdWithTime(helpers.ParseDate("2017-04-25T21:57:13+00:00")),
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.ProcessObjectIdAsDate = true
	options.StoreMinMaxAvgValue = true
	options.StoreWeekdayHistogram = true
	options.StoreHourHistogram = true
	options.StoreTopNValues = 3
	options.StoreBottomNValues = 3
	options.ValueHistogramMaxRes = 1000

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 7,
				ValueExtremes: &analysis.ValueExtremes{
					Min: helpers.ParseDate("2017-04-16T23:59:59+00:00"),
					Max: helpers.ParseDate("2017-04-25T21:57:13+00:00"),
				},
				TopNValues: analysis.ValueFreqSlice{
					{
						Count: 1,
						Value: helpers.ParseDate("2017-04-16T23:59:59+00:00"),
					},
					{
						Count: 1,
						Value: helpers.ParseDate("2017-04-24T12:54:57+00:00"),
					},
					{
						Count: 1,
						Value: helpers.ParseDate("2017-04-25T21:57:13+00:00"),
					},
				},
				BottomNValues: analysis.ValueFreqSlice{
					{
						Count: 1,
						Value: helpers.ParseDate("2017-04-20T05:45:57+00:00"),
					},
					{
						Count: 1,
						Value: helpers.ParseDate("2017-04-17T00:00:01+00:00"),
					},
					{
						Count: 1,
						Value: helpers.ParseDate("2017-04-18T10:00:00+00:00"),
					},
				},
				WeekdayHistogram: &analysis.WeekdayHistogram{
					1, // sunday
					2, // monday
					2, // ...
					1,
					1,
					0,
					0,
				},
				HourHistogram: &analysis.HourHistogram{
					1, // 00
					0, // 01
					0,
					0,
					0,
					1, // 05
					0, // 06
					0,
					0,
					0,
					1, // 10
					0, // 11
					1,
					0,
					1,
					0,
					0,
					0,
					0,
					0,
					0, // 20
					1, // 21
					0, // 22
					1, // 23
				},
				ValueHistogram: &analysis.Histogram{
					Start:         helpers.ParseDate("2017-04-16T23:30:00+00:00"),
					End:           helpers.ParseDate("2017-04-25T22:30:00+00:00"),
					Range:         774000,
					Step:          1800,
					NumberOfSteps: 430,
					Intervals: analysis.Intervals{
						{
							Interval: 0,
							Count:    1,
						},
						{
							Interval: 1,
							Count:    1,
						},
						{
							Interval: 69,
							Count:    1,
						},
						{
							Interval: 126,
							Count:    1,
						},
						{
							Interval: 156,
							Count:    1,
						},
						{
							Interval: 362,
							Count:    1,
						},
						{
							Interval: 428,
							Count:    1,
						},
					},
				},
			},
		},
	}

	stage := stageFactory(&options)

	testStage(t, c, time.UTC, stage, expected)
}
