package expr

import (
	"github.com/mongoeye/mongoeye/tests"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

func TestConcat(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"str1": "ABC",
		"str2": "def",
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":    0,
		"concat": Concat(Field("str1"), Field("str2")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, "ABCdef", out["concat"])
}
