package expr

import "gopkg.in/mgo.v2/bson"

// Concat encapsulates MongoDB operation $concat.
func Concat(parts ...interface{}) bson.M {
	return bson.M{"$concat": parts}
}
