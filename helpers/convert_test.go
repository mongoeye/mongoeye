package helpers

import (
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

func TestParseDate(t *testing.T) {
	date, _ := time.Parse(time.RFC822Z, "13 Feb 17 12:04 +0200")
	assert.Equal(t, date, ParseDate("2017-02-13T12:04:00+02:00"))
}

func TestParseDate_Invalid(t *testing.T) {
	assert.Panics(t, func() {
		ParseDate("xyz")
	})
}

func TestDoubleToDecimal(t *testing.T) {
	assert.Equal(t, ParseDecimal("123"), DoubleToDecimal(123.0))
	assert.Equal(t, ParseDecimal("456.789"), DoubleToDecimal(456.789))
}

func TestToDouble(t *testing.T) {
	assert.Equal(t, float64(456.678), ToDouble(float64(456.678)))
	assert.Equal(t, float64(1234567), ToDouble(time.Unix(1234567, 0)))
	assert.Equal(t, float64(1234567), ToDouble(int(1234567)))
	assert.Equal(t, float64(1234567), ToDouble(int32(1234567)))
	assert.Equal(t, float64(1234567), ToDouble(int64(1234567)))
	assert.Equal(t, float64(1234567), ToDouble(uint(1234567)))
	assert.Equal(t, float64(789.123), ToDouble(ParseDecimal("789.123")))
	assert.Panics(t, func() {
		ToDouble(nil)
	})
	assert.Panics(t, func() {
		ToDouble("xyz")
	})
}

func TestFromDoubleTo(t *testing.T) {
	loc, _ := time.LoadLocation("America/New_York")
	assert.Equal(t, float64(456.678), FromDoubleTo("double", float64(456.678), loc))
	assert.Equal(t, time.Unix(1234567, 0).In(loc), FromDoubleTo("date", float64(1234567), loc))
	assert.Equal(t, int(124), FromDoubleTo("int", float64(124.75), loc))
	assert.Equal(t, int32(134), FromDoubleTo("int32", float64(134.15), loc))
	assert.Equal(t, int64(522), FromDoubleTo("long", float64(522), loc))
	assert.Equal(t, ParseDecimal("234.456"), FromDoubleTo("decimal", float64(234.456), loc))
	assert.Panics(t, func() {
		FromDoubleTo("xyz", float64(456.678), loc)
	})
}

func TestToUInt(t *testing.T) {
	assert.Equal(t, uint(101), ToUInt(uint(101)))
	assert.Equal(t, uint(67), ToUInt(uint32(67)))
	assert.Equal(t, uint(647), ToUInt(int(647)))
	assert.Equal(t, uint(782), ToUInt(int32(782)))
	assert.Equal(t, uint(61247), ToUInt(int64(61247)))
	assert.Equal(t, uint(123), ToUInt(float64(123.567)))
	assert.Equal(t, uint(423), ToUInt(ParseDecimal("423.567")))
	assert.Panics(t, func() {
		ToUInt(nil)
	})
	assert.Panics(t, func() {
		ToUInt("abc")
	})
}

func TestParseDecimal(t *testing.T) {
	d, _ := bson.ParseDecimal128("123.456")
	assert.Equal(t, d, ParseDecimal("123.456"))
}

func TestParseDecimal_Panic(t *testing.T) {
	assert.Panics(t, func() {
		ParseDecimal("x.y")
	})
}

func TestDecimalToDouble(t *testing.T) {
	d, _ := bson.ParseDecimal128("1234567891012345678910.1234")
	assert.Equal(t, 1234567891012345678910.1234, DecimalToDouble(d))
}

func TestDecimalProxy_Positive(t *testing.T) {
	d1 := ParseDecimal("123.456")
	dp1 := ToDecimalProxy(d1)
	assert.Equal(t, 1, dp1.Positive())

	d2 := ParseDecimal("-123.456")
	dp2 := ToDecimalProxy(d2)
	assert.Equal(t, 0, dp2.Positive())
}

func TestDecimalProxy_Exponent(t *testing.T) {
	d1 := ParseDecimal("1234.456")
	dp1 := ToDecimalProxy(d1)
	assert.Equal(t, -3, dp1.Exponent())

	d2 := ParseDecimal("-12.456789")
	dp2 := ToDecimalProxy(d2)
	assert.Equal(t, -6, dp2.Exponent())
}

func TestDecimalProxy_Parts(t *testing.T) {
	d := ParseDecimal("1023456789102345678910234567891234")
	dp := ToDecimalProxy(d)
	h, l := dp.Parts()
	assert.Equal(t, uint64(0x3275D73DBB32), h)
	assert.Equal(t, uint64(0x18A22E2A644FD922), l)
}

func TestToDecimalProxy(t *testing.T) {
	d := ParseDecimal("123.456")
	dp := ToDecimalProxy(d)
	assert.Equal(t, 1, dp.Positive())
	assert.Equal(t, -3, dp.Exponent())
	h, l := dp.Parts()
	assert.Equal(t, uint64(0), h)
	assert.Equal(t, uint64(123456), l)
}

func TestCmpDecimal(t *testing.T) {
	assert.Equal(t, 0, CmpDecimal(ParseDecimal("123.456"), ParseDecimal("123.456")))
	assert.Equal(t, -1, CmpDecimal(ParseDecimal("-123.456"), ParseDecimal("123.456")))
	assert.Equal(t, 1, CmpDecimal(ParseDecimal("123.456"), ParseDecimal("-123.456")))
	assert.Equal(t, 1, CmpDecimal(ParseDecimal("1234.56"), ParseDecimal("123.456")))
	assert.Equal(t, -1, CmpDecimal(ParseDecimal("12.3456"), ParseDecimal("123.456")))
	assert.Equal(t, 1, CmpDecimal(ParseDecimal("123.44"), ParseDecimal("123.43")))
	assert.Equal(t, -1, CmpDecimal(ParseDecimal("123.44"), ParseDecimal("123.45")))
	assert.Equal(t, 1, CmpDecimal(ParseDecimal("2.123456789112345678911234567891234"), ParseDecimal("1.123456789112345678911234567891234")))
	assert.Equal(t, -1, CmpDecimal(ParseDecimal("2.123456789112345678911234567891234"), ParseDecimal("3.123456789112345678911234567891234")))
}

func TestReadChannelToNull(t *testing.T) {
	ch := make(chan []byte, 10)
	ch <- []byte{0x00}
	ch <- []byte{0x01}
	ch <- []byte{0x02}
	close(ch)

	ReadChannelToNull(ch)

	_, ok := <-ch
	assert.Equal(t, false, ok)
}

func TestVersionToString(t *testing.T) {
	assert.Equal(t, "1", VersionToString(1))
	assert.Equal(t, "1.2.3", VersionToString(1, 2, 3))
}
