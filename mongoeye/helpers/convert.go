package helpers

import (
	"bytes"
	"gopkg.in/mgo.v2/bson"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// ParseDate or panic.
func ParseDate(input string) time.Time {
	out, err := time.Parse("2006-01-02T15:04:05-07:00", input)
	if err != nil {
		panic(err)
	}
	return out
}

// ParseDecimal or panic.
func ParseDecimal(input string) bson.Decimal128 {
	out, err := bson.ParseDecimal128(input)
	if err != nil {
		panic(err)
	}
	return out
}

// DecimalToDouble or panic.
func DecimalToDouble(input bson.Decimal128) float64 {
	out, err := strconv.ParseFloat(input.String(), 64)
	if err != nil {
		panic(err)
	}
	return out
}

// ToDouble converts some selected types to float64 or panic.
func ToDouble(input interface{}) float64 {
	switch v := input.(type) {
	case float64:
		return v
	case time.Time:
		return float64(v.Unix())
	case int:
		return float64(v)
	case int32:
		return float64(v)
	case uint:
		return float64(v)
	case int64:
		return float64(v)
	case bson.Decimal128:
		return DecimalToDouble(v)
	}

	if input == nil {
		panic("Unexpected type: <nil>")
	}

	panic("Unexpected type: " + reflect.TypeOf(input).String())
}

// FromDoubleTo converts float64 to some selected types or panic.
func FromDoubleTo(t string, v float64, location *time.Location) interface{} {
	switch t {
	case "double":
		return v
	case "date":
		return time.Unix(int64(v), 0).In(location)
	case "int":
		return int(v)
	case "int32":
		return int32(v)
	case "long":
		return int64(v)
	case "decimal":
		return DoubleToDecimal(v)
	}

	panic("Unexpected type: " + t)
}

// ToUInt converts some selected types to uint or panic.
func ToUInt(input interface{}) uint {
	switch v := input.(type) {
	case uint:
		return v
	case uint32:
		return uint(v)
	case int:
		return uint(v)
	case int32:
		return uint(v)
	case int64:
		return uint(v)
	case float64:
		return uint(v)
	case bson.Decimal128:
		return uint(DecimalToDouble(v))
	}

	if input == nil {
		panic("Unexpected type: <nil>")
	}

	panic("Unexpected type: " + reflect.TypeOf(input).String())
}

// DoubleToDecimal or panic.
func DoubleToDecimal(input float64) bson.Decimal128 {
	str := strconv.FormatFloat(input, 'f', 3, 64)
	str = strings.TrimRight(strings.TrimRight(str, "0"), ".")

	v, err := bson.ParseDecimal128(str)
	if err != nil {
		panic(err)
	}

	return v
}

// DecimalProxy serves to circumvent non-exported attributes of bson.Decimal type.
type DecimalProxy struct {
	H, L uint64
}

// Positive gets number sign.
func (d *DecimalProxy) Positive() int {
	if d.H>>63&1 == 0 {
		return 1
	}
	return 0
}

// Exponent gets number exponent.
func (d *DecimalProxy) Exponent() int {
	if d.H>>61&3 == 3 {
		// Bits: 1*sign 2*ignored 14*exponent 111*significand.
		// Implicit 0b100 prefix in significand.
		return int(d.H>>47&(1<<14-1)) - 6176
	}

	// Bits: 1*sign 14*exponent 113*significand
	return int(d.H>>49&(1<<14-1)) - 6176
}

// Parts gets number parts.
func (d *DecimalProxy) Parts() (uint64, uint64) {
	if d.H>>61&3 == 3 {
		return 0, 0
	}
	return d.H & (1<<49 - 1), d.L
}

// ToDecimalProxy converts bson.Decimal128 to DecimalProxy.
func ToDecimalProxy(value bson.Decimal128) DecimalProxy {
	return *(*DecimalProxy)(unsafe.Pointer(&value))
}

// CmpDecimalProxy compares DecimalProxy.
func CmpDecimalProxy(a DecimalProxy, b DecimalProxy) int {
	sA := a.Positive()
	sB := b.Positive()
	sDiff := sA - sB
	if sDiff != 0 {
		return sDiff
	}

	eA := a.Exponent()
	eB := b.Exponent()
	eDiff := eA - eB
	if eDiff != 0 {
		return eDiff
	}

	hA, lA := a.Parts()
	hB, lB := b.Parts()
	if hA > hB {
		return 1
	} else if hA < hB {
		return -1
	}

	if lA > lB {
		return 1
	} else if lA < lB {
		return -1
	}

	return 0
}

// CmpDecimal compares Decimal.
func CmpDecimal(a bson.Decimal128, b bson.Decimal128) int {
	return CmpDecimalProxy(ToDecimalProxy(a), ToDecimalProxy(b))
}

// ReadChannelToNull reads values from channel to null. Useful for testing.
func ReadChannelToNull(ch interface{}) int {
	count := 0
	if x, ok := ch.(chan []uint8); ok {
		for range x {
			count++
		}
		return count
	}

	panic("Unexpected type. Expected 'chan []uint8'. Given: " + reflect.TypeOf(ch).String())
}

// VersionToString converts MongoDB version to string
func VersionToString(version ...int) string {
	b := bytes.NewBuffer(nil)
	for i := 0; i < len(version); i++ {
		if i != 0 {
			b.WriteString(".")
		}
		b.WriteString(strconv.Itoa(version[i]))
	}

	return b.String()
}
