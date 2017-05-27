// Package expandLocally is the implementation of the expand stage that runs locally.
package expandLocally

import (
	"fmt"
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/decoder"
	"gopkg.in/mgo.v2/bson"
	"reflect"
	"sync"
	"time"
	"unicode/utf8"
	"unsafe"
)

// NewStage - ExpandLocally stage factory.
//
// This implementation of expand stage
// processing binary data from the database to achieve maximum speed.
func NewStage(expandOptions *expand.Options) *analysis.Stage {
	return &analysis.Stage{
		Processor: func(_input interface{}, analysisOptions *analysis.Options) interface{} {
			// Assert type of input channel
			var input <-chan []byte
			if _input, ok := _input.(chan []byte); ok {
				input = _input
			} else {
				panic("Unexpected input. Expected 'chan []byte'. Given: " + reflect.TypeOf(_input).String())
			}

			// Create output channel and wait group
			output := make(chan expand.Value, analysisOptions.BufferSize)
			wg := &sync.WaitGroup{}
			wg.Add(analysisOptions.Concurrency)

			// Run workers
			for i := 0; i < analysisOptions.Concurrency; i++ {
				go expandWorker(input, output, expandOptions, wg)
			}

			// Close the channel after work
			go func() {
				wg.Wait()
				close(output)
			}()

			return output
		},
	}
}

// Expand worker is gradually processing binary documents from input channel.
func expandWorker(input <-chan []byte, output chan<- expand.Value, options *expand.Options, wg *sync.WaitGroup) {
	defer wg.Done()

	for bin := range input {
		d := decoder.NewDecoder(bin)
		processDocument(d, "", 0, options, output, true)
	}
}

// Process binary document.
func processDocument(d *decoder.Decoder, prefix string, level uint, options *expand.Options, output chan<- expand.Value, send bool) bson.M {
	_, end := d.ReadLength()

	m := bson.M{}

	for d.CurrentByte() != '\x00' {
		d.AssertBefore(end)

		kind := d.ReadByte()
		name := d.ReadCStr()

		var subName string
		if prefix != "" {
			subName = prefix + analysis.NameSeparator + name
		} else {
			subName = name
		}

		v := processField(subName, kind, d, level, options, output, send)

		m[name] = v

		d.AssertBefore(end)
	}

	d.AssertEnd(end)

	return m
}

// Process one field of binary document.
func processField(name string, kind byte, d *decoder.Decoder, level uint, options *expand.Options, output chan<- expand.Value, send bool) interface{} {
	value := expand.Value{
		Name:  name,
		Level: level,
	}

	switch kind {
	case 0x01: // Float64
		value.Type = "double"
		v := d.ReadFloat64()

		if options.StoreValue {
			value.Value = v
		}
	case 0x02: // UTF-8 string
		value.Type = "string"
		v := d.ReadStr()

		length := uint(utf8.RuneCountInString(v))

		if options.StoreValue {
			if length > options.StringMaxLength {
				value.Value = string([]rune(v)[:options.StringMaxLength])
			} else {
				value.Value = v
			}
		}

		if options.StoreStringLength {
			value.Length = length
		}
	case 0x03: // Document
		value.Type = "object"

		subSend := send
		if level >= options.MaxDepth {
			subSend = false
		}

		m := processDocument(d, name, level+1, options, output, subSend)

		if options.StoreValue {
			value.Value = m
		}

		if options.StoreObjectLength {
			value.Length = uint(len(m))
		}
	case 0x04: // Array
		value.Type = "array"
		subName := name + analysis.NameSeparator + analysis.ArrayItemMark

		_, arrayEnd := d.ReadLength()

		length := uint(0)
		values := []interface{}{}
		for d.CurrentByte() != '\x00' {
			d.AssertBefore(arrayEnd)

			subKind := d.ReadByte()
			d.ReadCStr() // read array key

			// The value of item must be always read, but not always continue to next stages
			subSend := send
			if length >= options.ArrayMaxLength || level >= options.MaxDepth {
				subSend = false
			}

			v := processField(subName, subKind, d, level+1, options, output, subSend)

			if options.StoreValue && length < options.ArrayMaxLength {
				values = append(values, v)
			}

			d.AssertBefore(arrayEnd)

			length++
		}

		d.AssertEnd(arrayEnd)

		if options.StoreValue {
			value.Value = values
		}

		if options.StoreArrayLength {
			value.Length = length
		}
	case 0x05: // Binary
		value.Type = "binData"
		b := d.ReadBinary()

		if options.StoreValue {
			if b.Kind == 0x00 || b.Kind == 0x02 {
				value.Value = b.Data
			} else {
				value.Value = b
			}
		}
	case 0x06: // Undefined (obsolete, but still seen in the wild)
		value.Type = "undefined"

		if options.StoreValue {
			value.Value = bson.Undefined
		}
	case 0x07: // ObjectId
		value.Type = "objectId"
		v := bson.ObjectId(d.ReadBytes(12))

		if options.StoreValue {
			value.Value = v
		}
	case 0x08: // Bool
		value.Type = "bool"
		v := d.ReadBool()

		if options.StoreValue {
			value.Value = v
		}
	case 0x09: // Date
		value.Type = "date"
		i := d.ReadInt64()

		if options.StoreValue {
			var v time.Time
			// MongoDB handles timestamps as milliseconds.
			if i == -62135596800000 {
				v = time.Time{} // In UTC for convenience.
			} else {
				v = time.Unix(i/1e3, i%1e3*1e6)
			}
			value.Value = v
		}
	case 0x0A: // Nil
		value.Type = "null"

		if options.StoreValue {
			value.Value = nil
		}
	case 0x0B: // RegEx
		value.Type = "regex"
		v := d.ReadRegEx()

		if options.StoreValue {
			value.Value = v
		}
	case 0x0C: // dbPointer
		value.Type = "dbPointer"
		v := bson.DBPointer{Namespace: d.ReadStr(), Id: bson.ObjectId(d.ReadBytes(12))}

		if options.StoreValue {
			value.Value = v
		}
	case 0x0D: // JavaScript without scope
		value.Type = "javascript"
		v := bson.JavaScript{Code: d.ReadStr()}

		if options.StoreValue {
			value.Value = v
		}
	case 0x0E: // Symbol
		value.Type = "symbol"
		v := bson.Symbol(d.ReadStr())

		if options.StoreValue {
			value.Value = v
		}
	case 0x0F: // JavaScript with scope
		value.Type = "javascriptWithScope"

		d.Skip(4) // Skip length
		js := bson.JavaScript{Code: d.ReadStr(), Scope: bson.M{}}

		scopeLen := d.ReadInt32()
		d.Rewind(4)

		bson.Unmarshal(d.ReadBytes(scopeLen), &js.Scope)

		if options.StoreValue {
			value.Value = js
		}
	case 0x10: // Int32
		value.Type = "int"
		v := int(d.ReadInt32())

		if options.StoreValue {
			value.Value = v
		}
	case 0x11: // Timestamp
		value.Type = "timestamp"
		v := bson.MongoTimestamp(d.ReadInt64())

		if options.StoreValue {
			value.Value = v
		}
	case 0x12: // Int64
		value.Type = "long"
		v := d.ReadInt64()

		if options.StoreValue {
			value.Value = v
		}
	case 0x13: // Decimal128
		value.Type = "decimal"

		type DecimalHack struct{ h, l uint64 }
		_v := DecimalHack{
			l: uint64(d.ReadInt64()),
			h: uint64(d.ReadInt64()),
		}

		if options.StoreValue {
			value.Value = *(*bson.Decimal128)(unsafe.Pointer(&_v))
		}
	case 0x7F: // Max key
		value.Type = "maxKey"

		if options.StoreValue {
			value.Value = bson.MaxKey
		}
	case 0xFF: // Min key
		value.Type = "minKey"

		if options.StoreValue {
			value.Value = bson.MinKey
		}
	default:
		panic(fmt.Sprintf("Unknown element kind (0x%02X)", kind))
	}

	if send {
		output <- value
	}

	return value.Value
}
