package cli

import (
	"bytes"
	"testing"

	"github.com/mongoeye/mongoeye/tests"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
)

func TestCmdRun_Analysis(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{})

	out := bytes.NewBuffer(nil)
	cmd, _ := NewCmd("cmd", "env", "name", "version", "subtitle")
	cmd.SetOutput(out)

	//fmt.Println("database", c.Database.Name)
	cmd.ParseFlags([]string{
		"cmd",
		"--host", "localhost:27017",
		"--db", "company",
		"--col", "company",
		"--sample", "all",
		"-u", "admin",
		"-p", "12345",
	})

	err := cmd.Execute()
	assert.Equal(t, nil, err)
}
