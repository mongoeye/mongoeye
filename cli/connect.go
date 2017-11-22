package cli

import (
	"errors"
	"fmt"
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/helpers"
	"gopkg.in/mgo.v2"
	"strings"
)

// Connect to MongoDB database and returns server info and session.
func Connect(config *Config) (info mgo.BuildInfo, session *mgo.Session, collection *mgo.Collection, count int, err error) {
	// Session
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
	useAuth := config.User != "" && config.Password != ""
	if useAuth {
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

	// Database and collection
	database := session.DB(config.Database)
	collection = database.C(config.Collection)

	// Check compatibility
	err = checkCompatibility(config, info)
	if err != nil {
		return
	}

	// Count documents in collection
	count, err = collection.Count()
	if err != nil {
		if useAuth && strings.Contains(err.Error(), "not authorized") {
			err = fmt.Errorf("User '%s' is not authorized to access database '%s'.\nPlease make sure you have entered the correct credentials.\n", config.User, config.Database)
		} else {
			err = fmt.Errorf("Cannot count documents in collection: %s.\n", err)
		}
		return
	}

	// Check number of documents
	if count == 0 {
		err = fmt.Errorf("Collection '%s.%s' does not exist or is empty.\n", database.Name, collection.Name)
		return
	}

	return info, session, collection, count, nil
}

// Check compatibility between given configuration and MongoDB version
func checkCompatibility(config *Config, info mgo.BuildInfo) error {
	// Aggregation framework require MongoDB 3.5.6+
	if config.UseAggregation && !info.VersionAtLeast(analysis.AggregationMinVersion...) {
		version := helpers.VersionToString(analysis.AggregationMinVersion...)
		return fmt.Errorf("Option 'use-aggregation' require MongoDB version >= %s.\n", version)

	}

	// Random sample sample require MongoDB 3.2+
	if config.Sample == "random" && !info.VersionAtLeast(analysis.RandomSampleMinVersion...) {
		version := helpers.VersionToString(analysis.RandomSampleMinVersion...)
		return fmt.Errorf("Invalid value of '--sample' option.\nSample '%s' require MongoDB version >= %s.\nPlease, use 'all', 'first:N' or 'last:N' sample.\n", config.Sample, version)

	}

	return nil
}
