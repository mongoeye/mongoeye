// Package decoder contains decoder of BSON data.
// Modified version of gopkg.In/mgo.v2/bson decoder
// original: https://github.com/go-mgo/mgo/blob/v2-unstable/bson/decode.go
// This modified version contains some added features
// that could not be externally added to the original version.
package decoder

import (
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"math"
	"reflect"
)

// Decoder consists of input data and current position.
type Decoder struct {
	In      []byte
	I       int
	docType reflect.Type
}

var typeM = reflect.TypeOf(bson.M{})

// NewDecoder - decoder factory.
func NewDecoder(in []byte) *Decoder {
	return &Decoder{in, 0, typeM}
}

// --------------------------------------------------------------------------
// Some helper functions.

func corrupted() {
	panic("Document is corrupted")
}

// --------------------------------------------------------------------------
// Parsers of basic types.

// ReadRegEx - read regular expression.
func (d *Decoder) ReadRegEx() bson.RegEx {
	re := bson.RegEx{}
	re.Pattern = d.ReadCStr()
	re.Options = d.ReadCStr()
	return re
}

// ReadBinary - read binary data.
func (d *Decoder) ReadBinary() bson.Binary {
	l := d.ReadInt32()
	b := bson.Binary{}
	b.Kind = d.ReadByte()
	b.Data = d.ReadBytes(l)
	if b.Kind == 0x02 && len(b.Data) >= 4 {
		// Weird obsolete format with redundant length.
		b.Data = b.Data[4:]
	}
	return b
}

// ReadStr - read string.
func (d *Decoder) ReadStr() string {
	l := d.ReadInt32()
	b := d.ReadBytes(l - 1)
	if d.ReadByte() != '\x00' {
		corrupted()
	}
	return string(b)
}

// ReadCStr - read string of unknown length.
func (d *Decoder) ReadCStr() string {
	start := d.I
	end := start
	l := len(d.In)
	for ; end != l; end++ {
		if d.In[end] == '\x00' {
			break
		}
	}
	d.I = end + 1
	if d.I > l {
		corrupted()
	}
	return string(d.In[start:end])
}

// ReadBool - read boolean value.
func (d *Decoder) ReadBool() bool {
	b := d.ReadByte()
	if b == 0 {
		return false
	}
	if b == 1 {
		return true
	}
	panic(fmt.Sprintf("encoded boolean must be 1 or 0, found %d", b))
}

// ReadFloat64 - read double.
func (d *Decoder) ReadFloat64() float64 {
	return math.Float64frombits(uint64(d.ReadInt64()))
}

// ReadInt32 - read int32.
func (d *Decoder) ReadInt32() int32 {
	b := d.ReadBytes(4)
	return int32((uint32(b[0]) << 0) |
		(uint32(b[1]) << 8) |
		(uint32(b[2]) << 16) |
		(uint32(b[3]) << 24))
}

// ReadInt64 - read long.
func (d *Decoder) ReadInt64() int64 {
	b := d.ReadBytes(8)
	return int64((uint64(b[0]) << 0) |
		(uint64(b[1]) << 8) |
		(uint64(b[2]) << 16) |
		(uint64(b[3]) << 24) |
		(uint64(b[4]) << 32) |
		(uint64(b[5]) << 40) |
		(uint64(b[6]) << 48) |
		(uint64(b[7]) << 56))
}

// ReadByte - read byte.
func (d *Decoder) ReadByte() byte {
	i := d.I
	d.I++
	if d.I > len(d.In) {
		corrupted()
	}
	return d.In[i]
}

// ReadBytes - read N bytes.
func (d *Decoder) ReadBytes(length int32) []byte {
	if length < 0 {
		corrupted()
	}
	start := d.I
	d.I += int(length)
	if d.I < start || d.I > len(d.In) {
		corrupted()
	}
	return d.In[start : start+int(length)]
}

// --------------------------------------------------------------------------
// Custom functions.

// ReadLength - read length of document.
func (d *Decoder) ReadLength() (int, int) {
	length := int(d.ReadInt32())
	end := length + d.I - 4
	if end <= d.I || end > len(d.In) || d.In[end-1] != '\x00' {
		corrupted()
	}

	return length, end
}

// CurrentByte - get current byte.
func (d *Decoder) CurrentByte() byte {
	return d.In[d.I]
}

// AssertBefore - assert if current decoder position is before specified.
func (d *Decoder) AssertBefore(position int) {
	if d.I >= position {
		corrupted()
	}
}

// AssertEnd - assert that end has been reached.
func (d *Decoder) AssertEnd(end int) {
	if d.ReadByte() != '\x00' {
		corrupted()
	}

	if d.I != end {
		corrupted()
	}
}

// Skip N bytes.
func (d *Decoder) Skip(skip int) {
	d.I += skip
	if d.I > len(d.In) {
		corrupted()
	}
}

// Rewind N bytes.
func (d *Decoder) Rewind(rewind int) {
	if rewind > d.I {
		corrupted()
	}
	d.I -= rewind
}

// Position gets current position.
func (d *Decoder) Position() int {
	return d.I
}
