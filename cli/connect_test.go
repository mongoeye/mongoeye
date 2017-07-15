package cli

import (
	"github.com/mongoeye/mongoeye/tests"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

func TestConnect(t *testing.T) {
	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	c.Insert(bson.M{})

	config := &Config{
		Host:       tests.TestDbUri,
		Database:   c.Database.Name,
		Collection: c.Name,
	}

	info, session, collection, err := Connect(config)
	assert.NotEqual(t, nil, info)
	assert.NotEqual(t, nil, session)
	assert.NotEqual(t, nil, collection)
	assert.Equal(t, nil, err)

}

func TestConnect_InvalidHost(t *testing.T) {
	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")
	v.Set("host", "invalidHost:12345")
	v.Set("db", "db")
	v.Set("col", "col")
	v.Set("connection-timeout", 1)

	config, _ := GetConfig(v)

	_, _, _, err := Connect(config)
	assert.Equal(t, "Connection failed: no reachable servers.\n", err.Error())
}

func TestConnect_InvalidDb(t *testing.T) {
	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")
	v.Set("host", tests.TestDbUri)
	v.Set("db", "invalidDb")
	v.Set("col", "invalidCol")
	v.Set("connection-timeout", 1)

	config, _ := GetConfig(v)

	_, _, _, err := Connect(config)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, "The database 'db' does not exist.\nPlease enter the name of the existing database.\n", err.Error())
}

func TestConnect_InvalidCol(t *testing.T) {
	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	c.Insert(bson.M{})

	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")
	v.Set("host", tests.TestDbUri)
	v.Set("db", c.Database.Name)
	v.Set("col", "INVALID")
	v.Set("connection-timeout", 1)

	config, _ := GetConfig(v)

	_, _, _, err := Connect(config)
	assert.Equal(t, "The collection '_test.INVALID' does not exist.\nPlease enter the name of the existing collection.\n", err.Error())
}
