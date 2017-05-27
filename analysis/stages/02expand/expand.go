// Package expand represents expand stage of analysis.
// Expand stage expands documents to value (result: [name, type] => value).
// Individual fields in this stage lose their link to the original document
// that allows analyzing all values of some field in next stages.
package expand

import "github.com/mongoeye/mongoeye/analysis"

// Options for expand stage.
type Options struct {
	StringMaxLength   uint // strings will be truncated to this length, this setting does not affect the processing of length
	ArrayMaxLength    uint // array will be truncated to the first N items, this setting does not affect the processing of length
	MaxDepth          uint // analysis will be proceed to the desired depth, zero means that only root fields will be processed
	StoreValue        bool // storing of value enables detailed analysis in next stages
	StoreStringLength bool // store string length (before truncation)
	StoreArrayLength  bool // store array length (before shortening)
	StoreObjectLength bool // store number of object fields
}

// Value of field with given name and type
type Value struct {
	Name   string      `bson:"n"` // name of field
	Type   string      `bson:"t"` // type of field
	Level  uint        `bson:"e"` // level of nested field, root level is zero
	Length uint        `bson:"l"` // length of value, it is available for some types (if enabled in options)
	Value  interface{} `bson:"v"` // value of field (if enabled in options)
}

// StageFactory prototype.
type StageFactory func(expandOptions *Options) *analysis.Stage
