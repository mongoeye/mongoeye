package helpers

import (
	"gopkg.in/mgo.v2/bson"
	"time"
)

// MinT gets minimum of two values.
func MinT(t string, a interface{}, b interface{}) interface{} {
	switch t {
	case "objectId":
		{
			return MinObjectId(SafeToObjectId(a), SafeToObjectId(b))
		}
	case "double":
		{
			return MinDouble(SafeToDouble(a), SafeToDouble(b))
		}
	case "string":
		{
			return MinString(SafeToString(a), SafeToString(b))
		}
	case "bool":
		{
			return MinBool(SafeToBool(a), SafeToBool(b))
		}
	case "date":
		{
			return MinDate(SafeToDate(a), SafeToDate(b))
		}
	case "int":
		{
			return MinInt(SafeToInt(a), SafeToInt(b))
		}
	case "timestamp":
		{
			return MinTimestamp(SafeToTimestamp(a), SafeToTimestamp(b))
		}
	case "long":
		{
			return MinLong(SafeToLong(a), SafeToLong(b))
		}
	case "decimal":
		{
			return MinDecimal(SafeToDecimal(a), SafeToDecimal(b))
		}
	}

	panic("Unexpected type: " + t)
}

// MaxT gets maximum of two values.
func MaxT(t string, a interface{}, b interface{}) interface{} {
	switch t {
	case "objectId":
		{
			return MaxObjectId(SafeToObjectId(a), SafeToObjectId(b))
		}
	case "double":
		{
			return MaxDouble(SafeToDouble(a), SafeToDouble(b))
		}
	case "string":
		{
			return MaxString(SafeToString(a), SafeToString(b))
		}
	case "bool":
		{
			return MaxBool(SafeToBool(a), SafeToBool(b))
		}
	case "date":
		{
			return MaxDate(SafeToDate(a), SafeToDate(b))
		}
	case "int":
		{
			return MaxInt(SafeToInt(a), SafeToInt(b))
		}
	case "timestamp":
		{
			return MaxTimestamp(SafeToTimestamp(a), SafeToTimestamp(b))
		}
	case "long":
		{
			return MaxLong(SafeToLong(a), SafeToLong(b))
		}
	case "decimal":
		{
			return MaxDecimal(SafeToDecimal(a), SafeToDecimal(b))
		}
	}

	panic("Unexpected type: " + t)
}

// MinObjectId gets minimum of two ObjectId values.
func MinObjectId(a, b bson.ObjectId) bson.ObjectId {
	if a < b {
		return a
	}
	return b
}

// MaxObjectId gets maximum of two ObjectId values.
func MaxObjectId(a, b bson.ObjectId) bson.ObjectId {
	if a > b {
		return a
	}
	return b
}

// MinString gets minimum of two string values.
func MinString(a, b string) string {
	if a < b {
		return a
	}
	return b
}

// MaxString gets maximum of two string values.
func MaxString(a, b string) string {
	if a > b {
		return a
	}
	return b
}

//MinDouble gets minimum of two double values.
func MinDouble(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// MaxDouble gets maximum of two double values.
func MaxDouble(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// MinBool gets maximum of two bool values.
func MinBool(a, b bool) bool {
	if a == false || b == false {
		return false
	}
	return true
}

// MaxBool gets maximum of two bool values.
func MaxBool(a, b bool) bool {
	if a == true || b == true {
		return true
	}
	return false
}

// MinDate gets minimum of two time.Time values.
func MinDate(a, b time.Time) time.Time {
	if a.Unix() < b.Unix() {
		return a
	}
	return b
}

// MaxDate gets maximum of time.Time values.
func MaxDate(a, b time.Time) time.Time {
	if a.Unix() > b.Unix() {
		return a
	}
	return b
}

// MinTimestamp gets minimum of two bson.MongoTimestamp values.
func MinTimestamp(a, b bson.MongoTimestamp) bson.MongoTimestamp {
	if a < b {
		return a
	}
	return b
}

// MaxTimestamp gets maximum of two bson.MongoTimestamp values.
func MaxTimestamp(a, b bson.MongoTimestamp) bson.MongoTimestamp {
	if a > b {
		return a
	}
	return b
}

// MinLong gets minimum of two int64 values.
func MinLong(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// MaxLong gets maximum of two int64 values.
func MaxLong(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// MinInt gets minimum of two int values.
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MaxInt gets maximum of two int values.
func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// MinInt32 gets minimum of two int32 values.
func MinInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

// MaxInt32 gets minimum of two int32 values.
func MaxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

// MinUInt gets minimum of two uint values.
func MinUInt(a, b uint) uint {
	if a < b {
		return a
	}
	return b
}

// MaxUInt gets maximum of two uint values.
func MaxUInt(a, b uint) uint {
	if a > b {
		return a
	}
	return b
}

// MinDecimal gets minimum of two bson.Decimal128 values.
func MinDecimal(a, b bson.Decimal128) bson.Decimal128 {
	if CmpDecimal(a, b) < 0 {
		return a
	}
	return b
}

// MaxDecimal gets maximum of two bson.Decimal128 values.
func MaxDecimal(a, b bson.Decimal128) bson.Decimal128 {
	if CmpDecimal(a, b) > 0 {
		return a
	}
	return b
}

// InStringSlice return true if value is in slice.
func InStringSlice(v string, s []string) bool {
	for _, i := range s {
		if i == v {
			return true
		}
	}
	return false
}
