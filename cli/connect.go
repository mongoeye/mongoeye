package cli

import (
	"errors"
	"fmt"
	"github.com/mongoeye/mongoeye/helpers"
	"gopkg.in/mgo.v2"
	"strings"
)

// Connect to MongoDB database and returns server info and session.
func Connect(config *Config) (info mgo.BuildInfo, session *mgo.Session, collection *mgo.Collection, err error) {
	// SESSION
	session, err = mgo.DialWithTimeout(config.Host, config.ConnectionTimeout)
	if err != nil {
		err = fmt.Errorf("Connection failed: %s.\n", err)
		return
	}
	session.SetMode(config.ConnectionMode, true)
	session.SetSyncTimeout(config.SyncTimeout)
	session.SetSocketTimeout(config.SocketTimeout)

	// Get server info
	info, err = session.BuildInfo()
	if err != nil {
		err = fmt.Errorf("Failed to get server version: %s\n", err)
		return
	}

	// Login
	if config.User != "" && config.Password != "" {
		err = session.Login(&mgo.Credential{
			Username:  config.User,
			Password:  config.Password,
			Source:    config.AuthDatabase,
			Mechanism: config.AuthMechanism,
		})
		if err != nil {
			err = errors.New("Failed to authenticate.\nPlease make sure you have entered the correct credentials.\n")
			return
		}
	}

	// Test if database require authentication and load database names
	dbNames, err := session.DatabaseNames()
	if err != nil {
		if strings.Contains(err.Error(), "not authorized") {
			err = errors.New("Database requires authentication.\nPlease enter credentials using flags or environment variables.\n")
		} else {
			err = fmt.Errorf("Failed to load database names: %s\n", err)
		}
		return
	}

	// Check if database exists
	if !helpers.InStringSlice(config.Database, dbNames) {
		err = fmt.Errorf("The database '%s' does not exist.\nPlease enter the name of the existing database.\n", config.Database)
		return
	}

	// DATABASE
	database := session.DB(config.Database)

	// Load collections names in database
	colNames, err := database.CollectionNames()
	if err != nil {
		if strings.Contains(err.Error(), "not authorized") {
			err = fmt.Errorf("User '%s' is not authorized to access database '%s'.\nPlease make sure you have entered the correct credentials.\n", config.User, config.Database)
		} else {
			err = fmt.Errorf("Failed to load collection names: %s\n", err)
		}
		return
	}

	// Check if collection exists.
	if !helpers.InStringSlice(config.Collection, colNames) {
		err = fmt.Errorf("The collection '%s.%s' does not exist.\nPlease enter the name of the existing collection.\n", config.Database, config.Collection)
		return
	}

	// COLLECTION
	collection = database.C(config.Collection)

	return info, session, collection, nil
}
