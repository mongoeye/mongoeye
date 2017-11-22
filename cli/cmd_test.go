package cli

import (
	"bytes"
	"github.com/mongoeye/mongoeye/tests"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

func TestCmdRun_Analysis(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{})

	out := bytes.NewBuffer(nil)
	cmd, _ := NewCmd("cmd", "env", "name", "version", "subtitle")
	cmd.SetOutput(out)

	cmd.ParseFlags([]string{
		"cmd",
		"--host", tests.TestDbUri,
		"--db", c.Database.Name,
		"--col", c.Name,
		"--sample", "all",
	})

	err := cmd.Execute()
	assert.Equal(t, nil, err)
}
