package decoder

import (
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

func TestDecoder_ReadRegEx(t *testing.T) {
	bytes, _ := bson.Marshal(bson.M{
		"_regex": bson.RegEx{Pattern: "^abc$", Options: "i"},
	})
	d := NewDecoder(bytes)

	d.ReadLength()
	assert.Equal(t, byte(0x0B), d.ReadByte())
	assert.Equal(t, "_regex", d.ReadCStr())
	assert.Equal(t, bson.RegEx{Pattern: "^abc$", Options: "i"}, d.ReadRegEx())
}

func TestDecoder_ReadBinary(t *testing.T) {
	bytes, _ := bson.Marshal(bson.M{
		"_bin": []byte{0x01, 0x02, 0x03},
	})
	d := NewDecoder(bytes)

	d.ReadLength()
	assert.Equal(t, byte(0x05), d.ReadByte())
	assert.Equal(t, "_bin", d.ReadCStr())
	assert.Equal(t, bson.Binary{Kind: 0x00, Data: []byte{0x01, 0x02, 0x03}}, d.ReadBinary())
}

func TestDecoder_ReadBinary_Old(t *testing.T) {
	bytes, _ := bson.Marshal(bson.M{
		"_bin": bson.Binary{Kind: 0x02, Data: []byte{0x01, 0x02, 0x03}},
	})
	d := NewDecoder(bytes)

	d.ReadLength()
	assert.Equal(t, byte(0x05), d.ReadByte())
	assert.Equal(t, "_bin", d.ReadCStr())
	assert.Equal(t, bson.Binary{Kind: 0x02, Data: []byte{0x01, 0x02, 0x03}}, d.ReadBinary())
}

func TestDecoder_ReadStr(t *testing.T) {
	bytes, _ := bson.Marshal(bson.M{
		"_str": "Šašo",
	})
	d := NewDecoder(bytes)

	d.ReadLength()
	assert.Equal(t, byte(0x02), d.ReadByte())
	assert.Equal(t, "_str", d.ReadCStr())
	assert.Equal(t, "Šašo", d.ReadStr())
}

func TestDecoder_ReadStr_InvalidString(t *testing.T) {
	d := NewDecoder([]byte{4, 0, 0, 0, '1', '2', '3', '4', '5', 0x00})
	assert.Panics(t, func() {
		d.ReadStr()
	})
}

func TestDecoder_ReadCStr(t *testing.T) {
	bytes, _ := bson.Marshal(bson.M{
		"Šžčde_x": 123,
	})
	d := NewDecoder(bytes)

	d.ReadLength()
	assert.Equal(t, byte(0x10), d.ReadByte())
	assert.Equal(t, "Šžčde_x", d.ReadCStr())
}

func TestDecoder_ReadCStr_InvalidString(t *testing.T) {
	d := NewDecoder([]byte{'1', '2', '3', '4', '5'})
	assert.Panics(t, func() {
		d.ReadCStr()
	})
}

func TestDecoder_ReadBool_True(t *testing.T) {
	bytes, _ := bson.Marshal(bson.M{
		"_bool": true,
	})
	d := NewDecoder(bytes)

	d.ReadLength()
	assert.Equal(t, byte(0x08), d.ReadByte())
	assert.Equal(t, "_bool", d.ReadCStr())
	assert.Equal(t, true, d.ReadBool())
}

func TestDecoder_ReadBool_False(t *testing.T) {
	bytes, _ := bson.Marshal(bson.M{
		"_bool": false,
	})
	d := NewDecoder(bytes)

	d.ReadLength()
	assert.Equal(t, byte(0x08), d.ReadByte())
	assert.Equal(t, "_bool", d.ReadCStr())
	assert.Equal(t, false, d.ReadBool())
}

func TestDecoder_ReadBool_Invalid(t *testing.T) {
	d := NewDecoder([]byte{0x02})
	assert.Panics(t, func() {
		d.ReadBool()
	})
}

func TestDecoder_ReadFloat64(t *testing.T) {
	bytes, _ := bson.Marshal(bson.M{
		"_double": float64(123.45),
	})
	d := NewDecoder(bytes)

	d.ReadLength()
	assert.Equal(t, byte(0x01), d.ReadByte())
	assert.Equal(t, "_double", d.ReadCStr())
	assert.Equal(t, float64(123.45), d.ReadFloat64())
}

func TestDecoder_ReadInt32(t *testing.T) {
	bytes, _ := bson.Marshal(bson.M{
		"_int": int32(54),
	})
	d := NewDecoder(bytes)

	d.ReadLength()
	assert.Equal(t, byte(0x10), d.ReadByte())
	assert.Equal(t, "_int", d.ReadCStr())
	assert.Equal(t, int32(54), d.ReadInt32())
}

func TestDecoder_ReadInt64(t *testing.T) {
	bytes, _ := bson.Marshal(bson.M{
		"_long": int64(54),
	})
	d := NewDecoder(bytes)

	d.ReadLength()
	assert.Equal(t, byte(0x12), d.ReadByte())
	assert.Equal(t, "_long", d.ReadCStr())
	assert.Equal(t, int64(54), d.ReadInt64())
}

func TestDecoder_ReadByte(t *testing.T) {
	d := NewDecoder([]byte{0x01, 0x02, 0x03, 0x04, 0x05})
	assert.Equal(t, byte(0x01), d.ReadByte())
}

func TestDecoder_ReadByte_Invalid(t *testing.T) {
	d := NewDecoder([]byte{})
	assert.Panics(t, func() {
		d.ReadByte()
	})
}

func TestDecoder_ReadBytes(t *testing.T) {
	d := NewDecoder([]byte{0x01, 0x02, 0x03, 0x04, 0x05})
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, d.ReadBytes(3))
}

func TestDecoder_ReadBytes_Invalid(t *testing.T) {
	d := NewDecoder([]byte{0x01, 0x02})
	assert.Panics(t, func() {
		d.ReadBytes(3)
	})
}

func TestDecoder_ReadBytes_InvalidLength(t *testing.T) {
	d := NewDecoder([]byte{0x01, 0x02})
	assert.Panics(t, func() {
		d.ReadBytes(-2)
	})
}

func TestDecoder_ReadLength(t *testing.T) {
	d := NewDecoder([]byte{0, 9, 0, 0, 0, 1, 2, 3, 4, 0x00})
	assert.Equal(t, byte(0), d.ReadByte())

	l, end := d.ReadLength()
	assert.Equal(t, 9, l)
	assert.Equal(t, 10, end)
	assert.Equal(t, 5, d.Position())
}

func TestDecoder_ReadLength_ShortDocument(t *testing.T) {
	d := NewDecoder([]byte{0, 9, 0, 0, 0, 1, 2, 3, 0x00})
	assert.Equal(t, byte(0), d.ReadByte())
	assert.Panics(t, func() {
		d.ReadLength()
	})
}

func TestDecoder_ReadLength_LongDocument(t *testing.T) {
	d := NewDecoder([]byte{0, 9, 0, 0, 0, 1, 2, 3, 4, 5, 0x00})
	assert.Equal(t, byte(0), d.ReadByte())
	assert.Panics(t, func() {
		d.ReadLength()
	})
}

func TestDecoder_CurrentByte(t *testing.T) {
	d := NewDecoder([]byte{0x00, 0x01, 0x02})
	d.Skip(1)
	assert.Equal(t, byte(0x01), d.CurrentByte())
}

func TestDecoder_AssertBefore(t *testing.T) {
	d := NewDecoder([]byte{0x00, 0x01, 0x02})
	d.Skip(1)
	d.AssertBefore(2)
}

func TestDecoder_AssertBefore_False(t *testing.T) {
	d := NewDecoder([]byte{0x00, 0x01, 0x02})
	d.Skip(2)
	assert.Panics(t, func() {
		d.AssertBefore(2)
	})
}

func TestDecoder_AssertEnd(t *testing.T) {
	d := NewDecoder([]byte{0x00, 0x01, 0x02, 0x00})
	d.Skip(3)
	d.AssertEnd(4)
}

func TestDecoder_AssertEnd_InvalidEndByte(t *testing.T) {
	d := NewDecoder([]byte{0x00, 0x01, 0x00, 0x00})
	d.Skip(2)
	assert.Panics(t, func() {
		d.AssertEnd(4)
	})
}

func TestDecoder_AssertEnd_InvalidPosition(t *testing.T) {
	d := NewDecoder([]byte{0x00, 0x01, 0x02, 0x00})
	d.Skip(1)
	assert.Panics(t, func() {
		d.AssertEnd(4)
	})
}

func TestDecoder_Skip(t *testing.T) {
	d := NewDecoder([]byte{0x00, 0x01, 0x00})
	d.Skip(2)
	assert.Equal(t, 2, d.Position())
}

func TestDecoder_Skip_Invalid(t *testing.T) {
	d := NewDecoder([]byte{0x00, 0x01, 0x00})
	assert.Panics(t, func() {
		d.Skip(4)
	})
}

func TestDecoder_Rewind(t *testing.T) {
	d := NewDecoder([]byte{0x00, 0x01, 0x00})
	d.Skip(3)
	d.Rewind(2)
	assert.Equal(t, 1, d.Position())
}

func TestDecoder_Rewind_Invalid(t *testing.T) {
	d := NewDecoder([]byte{0x00, 0x01, 0x00})
	d.Skip(3)
	assert.Panics(t, func() {
		d.Rewind(4)
	})
}

func TestDecoder_Position(t *testing.T) {
	d := NewDecoder([]byte{0x00, 0x01, 0x00})
	d.Skip(1)
	assert.Equal(t, 1, d.Position())
}
