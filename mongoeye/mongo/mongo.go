// Package mongo realizes connection to database.
package mongo

import (
	"gopkg.in/mgo.v2"
	"time"
)

// Connect to MongoDB host with given connection mode
func Connect(host string, mode mgo.Mode) (*mgo.Session, error) {
	session, err := mgo.DialWithTimeout(host, 5*time.Second)
	if err == nil {
		session.SetMode(mode, true)
		session.SetSyncTimeout(5 * time.Minute)
		session.SetSocketTimeout(5 * time.Minute)
	}
	return session, err
}

// Collection returns a value representing the named collection.
func Collection(session *mgo.Session, db string, collection string) *mgo.Collection {
	return session.DB(db).C(collection)
}
