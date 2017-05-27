package mongo

import (
	"github.com/mongoeye/mongoeye/tests"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
	"testing"
)

func TestConnect(t *testing.T) {
	session, err := Connect(tests.TestDbUri, mgo.Secondary)
	if err != nil {
		t.Error(err)
	}

	assert.IsType(t, &mgo.Session{}, session)
}

func TestCollection(t *testing.T) {
	c := Collection(tests.TestDbSession, "_test", "_test")
	assert.IsType(t, &mgo.Collection{}, c)
	c.DropCollection()
}
