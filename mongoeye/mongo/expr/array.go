package expr

import "gopkg.in/mgo.v2/bson"

// Map encapsulates MongoDB operation $map.
func Map(input interface{}, as interface{}, in interface{}) bson.M {
	return bson.M{
		"$map": bson.M{
			"input": input,
			"as":    as,
			"in":    in,
		},
	}
}

// Slice encapsulates MongoDB operation $slice.
func Slice(array interface{}, length interface{}) bson.M {
	return bson.M{
		"$slice": []interface{}{
			array,
			length,
		},
	}
}

// In encapsulates MongoDB operation $in.
func In(el interface{}, array interface{}) bson.M {
	return bson.M{"$in": []interface{}{el, array}}
}

// Size encapsulates MongoDB operation $size.
func Size(array interface{}) bson.M {
	return bson.M{"$size": array}
}

// ConcatArrays encapsulates MongoDB operation $concatArrays.
func ConcatArrays(arrays ...interface{}) bson.M {
	return bson.M{"$concatArrays": arrays}

}

// ObjectToArray encapsulates MongoDB operation $objectToArray.
func ObjectToArray(obj interface{}) bson.M {
	return bson.M{"$objectToArray": obj}
}
