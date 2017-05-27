package expr

import (
	"github.com/mongoeye/mongoeye/tests"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

func TestNewPipeline(t *testing.T) {
	p := NewPipeline()
	assert.IsType(t, p, &Pipeline{})
}

func TestPipeline_Add(t *testing.T) {
	p := NewPipeline()
	stage1 := bson.M{"key": "value"}
	stage2 := bson.M{"key2": "value2"}
	stage3 := bson.M{"key3": "value3"}
	p.add(stage1)
	p.add(stage2)
	p.add(stage3)
	assert.Equal(t, []bson.M{stage1, stage2, stage3}, p.GetStages())
}

func TestPipeline_AddStage(t *testing.T) {
	p := NewPipeline()
	p.AddStage("stage1", bson.M{
		"key1": "value1",
	})
	p.AddStage("stage2", bson.M{
		"key2": "value2",
	})

	assert.Equal(t, p.GetStages(), []bson.M{
		{"$stage1": bson.M{"key1": "value1"}},
		{"$stage2": bson.M{"key2": "value2"}},
	})
}

func TestPipeline_AddStages(t *testing.T) {
	p := NewPipeline()
	p.AddStages(
		bson.M{
			"$stage1": bson.M{
				"key1": "value1",
			},
		},
		bson.M{
			"$stage2": bson.M{
				"key2": "value2",
			},
		},
	)

	assert.Equal(t, p.GetStages(), []bson.M{
		{"$stage1": bson.M{"key1": "value1"}},
		{"$stage2": bson.M{"key2": "value2"}},
	})
}

func TestPipeline_GetPipe(t *testing.T) {
	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	p := NewPipeline()

	pipe := p.GetPipe(c)
	assert.IsType(t, &mgo.Pipe{}, pipe)
}

func TestPipeline_Iter(t *testing.T) {
	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	bulk := c.Bulk()
	for i := 1; i < 50; i++ {
		bulk.Insert(bson.M{
			"_id": i,
			"i":   i * 10,
		})
	}
	bulk.Run()

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"i": "$i",
	})

	iter := p.Iter(c, 2)
	assert.IsType(t, &mgo.Iter{}, iter)

	out := bson.M{}
	ok := iter.Next(&out)
	assert.Equal(t, true, ok)

	if !ok {
		panic(iter.Err())
	}

	assert.Equal(t, bson.M{"_id": 1, "i": 10}, out)
}

func TestPipeline_Iter_InvalidBatchSize(t *testing.T) {
	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	p := NewPipeline()

	assert.Panics(t, func() {
		p.Iter(c, -1)
	})
}

func TestPipeline_ToRawChannel(t *testing.T) {
	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	for i := 0; i < 50; i++ {
		c.Insert(bson.M{
			"i": i,
		})
	}

	p := NewPipeline()
	p.AddStage("sort", bson.M{"i": 1})
	p.AddStage("project", bson.M{
		"_id": 0,
		"i":   1,
	})

	ch := make(chan []byte)
	p.ToRawChannel(c, ch, 1, 10, 10)

	i := 0
	for r := range ch {
		assert.Equal(t, []byte{0xc, 0x0, 0x0, 0x0, 0x10, 0x69, 0x0, byte(i), 0x0, 0x0, 0x0, 0x0}, r)
		i++
	}
}

func TestPipeline_ToRawChannel_InvalidConcurrencyParam(t *testing.T) {
	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	p := NewPipeline()

	ch := make(chan []byte)

	assert.Panics(t, func() {
		p.ToRawChannel(c, ch, 0, 1, 1)
	})
}

func TestPipeline_ToRawChannel_InvalidBufferParam(t *testing.T) {
	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	p := NewPipeline()

	ch := make(chan []byte)

	assert.Panics(t, func() {
		p.ToRawChannel(c, ch, 1, -1, 1)
	})
}

func TestPipeline_ToRawChannel_InvalidBatchSizeParam(t *testing.T) {
	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	p := NewPipeline()

	ch := make(chan []byte)

	assert.Panics(t, func() {
		p.ToRawChannel(c, ch, 1, 1, -1)
	})
}

func TestPipeline_ToBsonChannel(t *testing.T) {
	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	for i := 0; i < 50; i++ {
		c.Insert(bson.M{
			"i": i,
		})
	}

	p := NewPipeline()
	p.AddStage("sort", bson.M{"i": 1})
	p.AddStage("project", bson.M{
		"_id": 0,
		"i":   1,
	})

	ch := make(chan bson.M)
	p.ToBsonChannel(c, ch, 1, 10, 10)

	i := 0
	for r := range ch {
		assert.Equal(t, i, r["i"])
		i++
	}
}

func TestPipeline_ToBsonChannel_InvalidConcurrencyParam(t *testing.T) {
	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	p := NewPipeline()

	ch := make(chan bson.M)

	assert.Panics(t, func() {
		p.ToBsonChannel(c, ch, 0, 1, 1)
	})
}

func TestPipeline_ToBsonChannel_InvalidBufferParam(t *testing.T) {
	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	p := NewPipeline()

	ch := make(chan bson.M)

	assert.Panics(t, func() {
		p.ToBsonChannel(c, ch, 1, -1, 1)
	})
}

func TestPipeline_ToBsonChannel_InvalidBatchSizeParam(t *testing.T) {
	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	p := NewPipeline()

	ch := make(chan bson.M)

	assert.Panics(t, func() {
		p.ToBsonChannel(c, ch, 1, 1, -1)
	})
}
