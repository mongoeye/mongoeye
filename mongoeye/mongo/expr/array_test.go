package expr

import (
	"github.com/mongoeye/mongoeye/tests"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

func TestMap(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"array": []interface{}{1, 2, 3, 4, 5},
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":    0,
		"mapped": Map(Field("array"), "i", Multiply(Var("i"), 10)),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, []interface{}{10, 20, 30, 40, 50}, out["mapped"])
}

func TestSlice(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"array": []interface{}{1, 2, 3, 4, 5},
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":   0,
		"slice": Slice(Field("array"), 2),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, []interface{}{1, 2}, out["slice"])
}

func TestIn(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"array": []interface{}{1, 2, 3, 4, 5},
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":    0,
		"in1":    In(1, Field("array")),
		"in2":    In("abc", []string{"abc", "cde"}),
		"notIn1": In(10, Field("array")),
		"notIn2": In("xyz", []string{"abc", "cde"}),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, true, out["in1"])
	assert.Equal(t, true, out["in2"])
	assert.Equal(t, false, out["notIn1"])
	assert.Equal(t, false, out["notIn2"])
}

func TestSize(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"array1": []interface{}{1, 2, 3},
		"array2": []interface{}{1, 2, 3, 4, 5, 6},
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":   0,
		"size1": Size(Field("array1")),
		"size2": Size(Field("array2")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 3, out["size1"])
	assert.Equal(t, 6, out["size2"])
}

func TestConcatArrays(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"array1": []interface{}{1, 2, 3},
		"array2": []interface{}{1, 2, 3, 4, 5, 6},
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":    0,
		"concat": ConcatArrays(Field("array1"), Field("array2")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, []interface{}{1, 2, 3, 1, 2, 3, 4, 5, 6}, out["concat"])
}

func TestObjectToArray(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58ed0817344c64f7fca5847b"),
		"key1": "value1",
		"key2": 100,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":   0,
		"array": ObjectToArray(Var("ROOT")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	expected := []interface{}{
		bson.M{"k": "_id", "v": bson.ObjectIdHex("58ed0817344c64f7fca5847b")},
		bson.M{"k": "key1", "v": "value1"},
		bson.M{"k": "key2", "v": 100},
	}

	tests.AssertEqualSet(t, []interface{}{expected}, []interface{}{out["array"]})
}
