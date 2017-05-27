package expr

import (
	"github.com/mongoeye/mongoeye/tests"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

func TestField(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"field": "data1",
		"sub": bson.M{
			"field": "data2",
		},
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":     0,
		"result1": Field("field"),
		"result2": Field("sub", "field"),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, "data1", out["result1"])
	assert.Equal(t, "data2", out["result2"])
}

func TestVar(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"field": "data",
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":    0,
		"result": Let(bson.M{"localVar": "data"}, Var("localVar")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, "data", out["result"])
}

func TestEq(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"a1": "a",
		"a2": "a",
		"b":  "b",
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":      0,
		"A1SameA2": Eq(Field("a1"), Field("a2")),
		"A1SameB":  Eq(Field("a1"), Field("b")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, true, out["A1SameA2"])
	assert.Equal(t, false, out["A1SameB"])
}

func TestNe(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"a1": "a",
		"a2": "a",
		"b":  "b",
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":         0,
		"A1NotSameA2": Ne(Field("a1"), Field("a2")),
		"A1NotSameB":  Ne(Field("a1"), Field("b")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, false, out["A1NotSameA2"])
	assert.Equal(t, true, out["A1NotSameB"])
}

func TestSwitch(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"a": "a",
		"b": "b",
		"c": "c",
	})

	sw := Switch()
	sw.AddBranch(
		Eq(Var("input"), "a"),
		"=a",
	)
	sw.AddBranch(
		Eq(Var("input"), "b"),
		"=b",
	)
	sw.SetDefault("unexpected")

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"a": Let(
			bson.M{"input": Field("a")},
			sw.Bson(),
		),
		"b": Let(
			bson.M{"input": Field("b")},
			sw.Bson(),
		),
		"c": Let(
			bson.M{"input": Field("c")},
			sw.Bson(),
		),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, "=a", out["a"])
	assert.Equal(t, "=b", out["b"])
	assert.Equal(t, "unexpected", out["c"])
}

func TestLet(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"a": Let(
			bson.M{"input": "A"},
			Eq(Var("input"), "A"),
		),
		"b": Let(
			bson.M{"input": "B"},
			Eq(Var("input"), "B"),
		),
		"c": Let(
			bson.M{"input": "D"},
			Eq(Var("input"), "C"),
		),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, true, out["a"])
	assert.Equal(t, true, out["b"])
	assert.Equal(t, false, out["c"])
}

func TestCond(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"true":  true,
		"false": false,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"trueInt": Cond(
			Eq(Field("true"), true),
			1,
			0,
		),
		"falseInt": Cond(
			Eq(Field("false"), true),
			1,
			0,
		),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 1, out["trueInt"])
	assert.Equal(t, 0, out["falseInt"])
}

func TestType(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_string":    "Abc",
		"_int":       123,
		"_undefined": bson.Undefined,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":       0,
		"string":    Type(Field("_string")),
		"int":       Type(Field("_int")),
		"undefined": Type(Field("_undefined")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, "string", out["string"])
	assert.Equal(t, "int", out["int"])
	assert.Equal(t, "undefined", out["undefined"])
}

func TestFacet(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"f1": 1,
		"f2": 2,
	})

	f := Facet()

	f1 := f.AddField("sum", nil)
	f1.AddStage("project", bson.M{
		"_id": 0,
		"sum": Add(Field("f1"), Field("f2")),
	})

	f2 := f.AddField("sub", nil)
	f2.AddStage("project", bson.M{
		"_id": 0,
		"sub": Subtract(Field("f1"), Field("f2")),
	})

	p := NewPipeline()
	p.AddStage("facet", f.GetMap())
	p.AddStage("replaceRoot", bson.M{
		"newRoot": bson.M{
			"data": ConcatArrays(Field("sum"), Field("sub")),
		},
	})
	p.AddStage("unwind", Field("data"))
	p.AddStage("replaceRoot", bson.M{
		"newRoot": Field("data"),
	})

	out := []bson.M{}
	p.GetPipe(c).All(&out)

	assert.Equal(t, []bson.M{{"sum": 3}, {"sub": -1}}, out)
}

func TestFacet_GetField(t *testing.T) {
	p1 := NewPipeline()

	f := Facet()
	f.AddField("sum", p1)

	assert.Equal(t, p1, f.GetField("sum"))
}

func TestFacet_GetKeys(t *testing.T) {
	p1 := NewPipeline()
	p2 := NewPipeline()

	f := Facet()
	f.AddField("f1", p1)
	f.AddField("f2", p2)

	assert.Equal(t, []string{"f1", "f2"}, f.GetKeys())
}

func TestFacet_GetKeysAsFields(t *testing.T) {
	p1 := NewPipeline()
	p2 := NewPipeline()

	f := Facet()
	f.AddField("f1", p1)
	f.AddField("f2", p2)

	tests.AssertEqualSet(t, []interface{}{"$f1", "$f2"}, f.GetKeysAsFields())
}

func TestMergeObjects(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"a": bson.M{
			"f1": "1",
			"f2": "2",
		},
		"b": bson.M{
			"f3": "3",
			"f4": "4",
		},
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"m":   MergeObjects([]interface{}{Field("a"), Field("b")}),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	expected := bson.M{
		"f1": "1",
		"f2": "2",
		"f3": "3",
		"f4": "4",
	}

	assert.Equal(t, expected, out["m"])
}
