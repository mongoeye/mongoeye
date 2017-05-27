package expandTests

import (
	"github.com/jinzhu/copier"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/mongoeye/mongoeye/tests"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

// RunTestScalarTypesMinimal tests expand stage with scalar types and minimal configuration.
func RunTestScalarTypesMinimal(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	// Decimal type is new in version 3.4
	decimalType := "decimal"
	var decimal interface{} = helpers.ParseDecimal("456")
	if !tests.HasMongoDBDecimalSupport() {
		decimalType = "string"
		decimal = "456"
	}

	c.Insert(bson.M{
		"_id":        bson.ObjectIdHex("58de3f123d9654ba801bb36d"),
		"_double":    123.456,
		"_string":    "Šašo",
		"_binData":   []byte("abc"),
		"_undefined": bson.Undefined,
		"_bool":      true,
		"_date":      time.Unix(1490959237, 0),
		"_null":      nil,
	})
	c.Insert(bson.M{
		"_id":                  bson.ObjectIdHex("58de3f123d9654ba801bb30e"),
		"_regex":               bson.RegEx{Pattern: ".*", Options: "i"},
		"_dbPointer":           bson.DBPointer{Namespace: "ns", Id: bson.ObjectIdHex("58de3f123d9654ba801bb31d")},
		"_javascript":          bson.JavaScript{Code: "var x = 1+1;", Scope: nil},
		"_symbol":              bson.Symbol("x"),
		"_javascriptWithScope": bson.JavaScript{Code: "var x = 1+y;", Scope: bson.M{"y": 5}},
		"_int":                 123,
		"_timestamp":           bson.MongoTimestamp(1490959237),
		"_long":                int64(456),
		"_decimal":             decimal,
		"_minKey":              bson.MinKey,
		"_maxKey":              bson.MaxKey,
	})

	options := expand.Options{}
	copier.Copy(&options, &testOptions)

	expected := []interface{}{
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
		},
		expand.Value{
			Level: 0,
			Name:  "_double",
			Type:  "double",
		},
		expand.Value{
			Level: 0,
			Name:  "_string",
			Type:  "string",
		},
		expand.Value{
			Level: 0,
			Name:  "_binData",
			Type:  "binData",
		},
		expand.Value{
			Level: 0,
			Name:  "_undefined",
			Type:  "undefined",
		},
		expand.Value{
			Level: 0,
			Name:  "_bool",
			Type:  "bool",
		},
		expand.Value{
			Level: 0,
			Name:  "_date",
			Type:  "date",
		},
		expand.Value{
			Level: 0,
			Name:  "_null",
			Type:  "null",
		},
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
		},
		expand.Value{
			Level: 0,
			Name:  "_regex",
			Type:  "regex",
		},
		expand.Value{
			Level: 0,
			Name:  "_dbPointer",
			Type:  "dbPointer",
		},
		expand.Value{
			Level: 0,
			Name:  "_javascript",
			Type:  "javascript",
		},
		expand.Value{
			Level: 0,
			Name:  "_symbol",
			Type:  "symbol",
		},
		expand.Value{
			Level: 0,
			Name:  "_javascriptWithScope",
			Type:  "javascriptWithScope",
		},
		expand.Value{
			Level: 0,
			Name:  "_int",
			Type:  "int",
		},
		expand.Value{
			Level: 0,
			Name:  "_timestamp",
			Type:  "timestamp",
		},
		expand.Value{
			Level: 0,
			Name:  "_long",
			Type:  "long",
		},
		expand.Value{
			Level: 0,
			Name:  "_decimal",
			Type:  decimalType,
		},
		expand.Value{
			Level: 0,
			Name:  "_maxKey",
			Type:  "maxKey",
		},
		expand.Value{
			Level: 0,
			Name:  "_minKey",
			Type:  "minKey",
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}

// RunTestScalarTypesValue tests expand stage with scalar types and StoreValue option.
func RunTestScalarTypesValue(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	// Decimal type is new in version 3.4
	decimalType := "decimal"
	var decimal interface{} = helpers.ParseDecimal("456")
	if !tests.HasMongoDBDecimalSupport() {
		decimalType = "string"
		decimal = "456"
	}

	c.Insert(bson.M{
		"_id":           bson.ObjectIdHex("58de3f123d9654ba801bb32d"),
		"_double":       123.456,
		"_string":       "Šašo",
		"_binData":      []byte("abc"),
		"_binData_kind": bson.Binary{Kind: 0x05, Data: []byte("abc")},
		"_undefined":    bson.Undefined,
		"_bool":         true,
		"_date":         time.Unix(1490959237, 0),
		"_date_min":     time.Time{},
		"_null":         nil,
	})
	c.Insert(bson.M{
		"_id":                  bson.ObjectIdHex("58de3f123d9654ba801bb30e"),
		"_regex":               bson.RegEx{Pattern: ".*", Options: "i"},
		"_dbPointer":           bson.DBPointer{Namespace: "ns", Id: bson.ObjectIdHex("58de3f123d9654ba801bb20f")},
		"_javascript":          bson.JavaScript{Code: "var x = 1+1;", Scope: nil},
		"_symbol":              bson.Symbol("x"),
		"_javascriptWithScope": bson.JavaScript{Code: "var x = 1+y;", Scope: bson.M{"y": 5}},
		"_int":                 123,
		"_timestamp":           bson.MongoTimestamp(1490959237),
		"_long":                int64(456),
		"_decimal":             decimal,
		"_minKey":              bson.MinKey,
		"_maxKey":              bson.MaxKey,
	})

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.StoreValue = true

	expected := []interface{}{
		expand.Value{
			Level: 0,
			Name:  "_id",
			Value: bson.ObjectIdHex("58de3f123d9654ba801bb32d"),
			Type:  "objectId",
		},
		expand.Value{
			Level: 0,
			Name:  "_double",
			Value: 123.456,
			Type:  "double",
		},
		expand.Value{
			Level: 0,
			Name:  "_string",
			Value: "Šašo",
			Type:  "string",
		},
		expand.Value{
			Level: 0,
			Name:  "_binData",
			Value: []byte("abc"),
			Type:  "binData",
		},
		expand.Value{
			Level: 0,
			Name:  "_binData_kind",
			Value: bson.Binary{Kind: 0x05, Data: []byte("abc")},
			Type:  "binData",
		},
		expand.Value{
			Level: 0,
			Name:  "_undefined",
			Value: bson.Undefined,
			Type:  "undefined",
		},
		expand.Value{
			Level: 0,
			Name:  "_bool",
			Value: true,
			Type:  "bool",
		},
		expand.Value{
			Level: 0,
			Name:  "_date",
			Value: time.Unix(1490959237, 0),
			Type:  "date",
		},
		expand.Value{
			Level: 0,
			Name:  "_date_min",
			Value: time.Time{},
			Type:  "date",
		},
		expand.Value{
			Level: 0,
			Name:  "_null",
			Value: nil,
			Type:  "null",
		},
		expand.Value{
			Level: 0,
			Name:  "_id",
			Value: bson.ObjectIdHex("58de3f123d9654ba801bb30e"),
			Type:  "objectId",
		},
		expand.Value{
			Level: 0,
			Name:  "_regex",
			Value: bson.RegEx{Pattern: ".*", Options: "i"},
			Type:  "regex",
		},
		expand.Value{
			Level: 0,
			Name:  "_dbPointer",
			Value: bson.DBPointer{Namespace: "ns", Id: bson.ObjectIdHex("58de3f123d9654ba801bb20f")},
			Type:  "dbPointer",
		},
		expand.Value{
			Level: 0,
			Name:  "_javascript",
			Value: bson.JavaScript{Code: "var x = 1+1;", Scope: nil},
			Type:  "javascript",
		},
		expand.Value{
			Level: 0,
			Name:  "_symbol",
			Value: "x",
			Type:  "symbol",
		},
		expand.Value{
			Level: 0,
			Name:  "_javascriptWithScope",
			Value: bson.JavaScript{Code: "var x = 1+y;", Scope: bson.M{"y": 5}},
			Type:  "javascriptWithScope",
		},
		expand.Value{
			Level: 0,
			Name:  "_int",
			Value: 123,
			Type:  "int",
		},
		expand.Value{
			Level: 0,
			Name:  "_timestamp",
			Value: bson.MongoTimestamp(1490959237),
			Type:  "timestamp",
		},
		expand.Value{
			Level: 0,
			Name:  "_long",
			Value: int64(456),
			Type:  "long",
		},
		expand.Value{
			Level: 0,
			Name:  "_decimal",
			Value: decimal,
			Type:  decimalType,
		},
		expand.Value{
			Level: 0,
			Name:  "_maxKey",
			Value: bson.MaxKey,
			Type:  "maxKey",
		},
		expand.Value{
			Level: 0,
			Name:  "_minKey",
			Value: bson.MinKey,
			Type:  "minKey",
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}
