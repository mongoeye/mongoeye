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

// RunBenchmarkDepth0Min tests speed of expand stage with depth 0 and minimal configuration.
func RunBenchmarkDepth0Min(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 0
	options.StoreValue = false
	options.StoreStringLength = false
	options.StoreArrayLength = false
	options.StoreObjectLength = false

	benchmarkStage(b, c, stageFactory(&options), false)
}

// RunBenchmarkDepth0Full tests speed of expand stage with depth 0 and full configuration.
func RunBenchmarkDepth0Full(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 0
	options.StoreValue = true
	options.StoreStringLength = true
	options.StoreArrayLength = true
	options.StoreObjectLength = true

	benchmarkStage(b, c, stageFactory(&options), false)
}

// RunBenchmarkDepth5Full tests speed of expand stage with depth 5 and full configuration.
func RunBenchmarkDepth5Full(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 5
	options.StoreValue = true
	options.StoreStringLength = true
	options.StoreArrayLength = true
	options.StoreObjectLength = true

	benchmarkStage(b, c, stageFactory(&options), false)
}

// RunBenchmarkDoubleField tests speed of expand stage with double field.
func RunBenchmarkDoubleField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": 10.10,
		"f2": 20.20,
		"f3": 30.30,
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkStringField tests speed of expand stage with string field.
func RunBenchmarkStringField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": "abc",
		"f2": "CDE",
		"f3": "šaŠo",
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkBinDataField tests speed of expand stage with bin field.
func RunBenchmarkBinDataField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": []byte("abc"),
		"f2": []byte("CDE"),
		"f3": []byte("šaŠo"),
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkUndefinedField tests speed of expand stage with undefined field.
func RunBenchmarkUndefinedField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": bson.Undefined,
		"f2": bson.Undefined,
		"f3": bson.Undefined,
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkBoolField tests speed of expand stage with bool field.
func RunBenchmarkBoolField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": true,
		"f2": false,
		"f3": true,
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkDateField tests speed of expand stage with date field.
func RunBenchmarkDateField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": time.Unix(1490959231, 0),
		"f2": time.Unix(1490959232, 0),
		"f3": time.Unix(1490959233, 0),
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkNullField tests speed of expand stage with null field.
func RunBenchmarkNullField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": nil,
		"f2": nil,
		"f3": nil,
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkObjectIdField tests speed of expand stage with ObjectId field.
func RunBenchmarkObjectIdField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": bson.ObjectIdHex("58de3f123d9654ba801bb30e"),
		"f2": bson.ObjectIdHex("58de3f123d9654ba801bb31e"),
		"f3": bson.ObjectIdHex("58de3f123d9654ba801bb32e"),
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkRegexField tests speed of expand stage with regexp field.
func RunBenchmarkRegexField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": bson.RegEx{Pattern: "abc.*", Options: "i"},
		"f2": bson.RegEx{Pattern: "CDE.*", Options: "i"},
		"f3": bson.RegEx{Pattern: "šaŠo.*", Options: "i"},
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkDbPointerField tests speed of expand stage with dbPointer field.
func RunBenchmarkDbPointerField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": bson.DBPointer{Namespace: "ns", Id: bson.ObjectIdHex("58de3f123d9654ba801bb30d")},
		"f2": bson.DBPointer{Namespace: "ns", Id: bson.ObjectIdHex("58de3f123d9654ba801bb31d")},
		"f3": bson.DBPointer{Namespace: "ns", Id: bson.ObjectIdHex("58de3f123d9654ba801bb32d")},
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkJavascriptField tests speed of expand stage with Javascript field.
func RunBenchmarkJavascriptField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": bson.JavaScript{Code: "var x = 1+1;", Scope: nil},
		"f2": bson.JavaScript{Code: "var x = 1+2;", Scope: nil},
		"f3": bson.JavaScript{Code: "var x = 1+3;", Scope: nil},
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkSymbolField tests speed of expand stage with Symbol field.
func RunBenchmarkSymbolField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": bson.Symbol("x"),
		"f2": bson.Symbol("y"),
		"f3": bson.Symbol("z"),
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkJavascriptWithField tests speed of expand stage with Javascript field.
func RunBenchmarkJavascriptWithField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": bson.JavaScript{Code: "var x = 3+y;", Scope: bson.M{"y": 1}},
		"f2": bson.JavaScript{Code: "var x = 2+y;", Scope: bson.M{"y": 2}},
		"f3": bson.JavaScript{Code: "var x = 1+y;", Scope: bson.M{"y": 3}},
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkIntField tests speed of expand stage with int field.
func RunBenchmarkIntField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": 10,
		"f2": 20,
		"f3": 30,
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkTimestampField tests speed of expand stage with timestamp field.
func RunBenchmarkTimestampField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": bson.MongoTimestamp(1490959230),
		"f2": bson.MongoTimestamp(1490959231),
		"f3": bson.MongoTimestamp(1490959232),
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkLongField tests speed of expand stage with long field.
func RunBenchmarkLongField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": int64(10),
		"f2": int64(20),
		"f3": int64(30),
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkDecimalField tests speed of expand stage with decimal field.
func RunBenchmarkDecimalField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": helpers.ParseDecimal("123"),
		"f2": helpers.ParseDecimal("456"),
		"f3": helpers.ParseDecimal("789"),
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkMinKeyField tests speed of expand stage with minKey field.
func RunBenchmarkMinKeyField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": bson.MinKey,
		"f2": bson.MinKey,
		"f3": bson.MinKey,
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkMaxKeyField tests speed of expand stage with maxKey field.
func RunBenchmarkMaxKeyField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": bson.MaxKey,
		"f2": bson.MaxKey,
		"f3": bson.MaxKey,
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkObjectField tests speed of expand stage with object field.
func RunBenchmarkObjectField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": bson.M{
			"f1": "string1",
			"f2": "string2",
			"f3": "string3",
		},
		"f2": bson.M{
			"f1": "string1",
			"f2": "string2",
			"f3": "string3",
		},
		"f3": bson.M{
			"f1": "string1",
			"f2": "string2",
			"f3": "string3",
		},
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}

// RunBenchmarkArrayField tests speed of expand stage with array field.
func RunBenchmarkArrayField(b *testing.B, stageFactory expand.StageFactory) {
	b.StopTimer()

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.MaxDepth = 2
	options.StoreValue = true

	c.Insert(bson.M{
		"f1": []interface{}{
			"string1",
			"string2",
			"string3",
		},
		"f2": []interface{}{
			"string1",
			"string2",
			"string3",
		},
		"f3": []interface{}{
			"string1",
			"string2",
			"string3",
		},
	})

	benchmarkStage(b, c, stageFactory(&options), true)
}
