// Package group represents group stage of analysis.
// Group stage group values from expand stage.
// The values are grouped under the same name and type (result: [name, type] => value aggregation).
// This stage counts all statistics above the data.
package group

import "github.com/mongoeye/mongoeye/analysis"

// Options for group stage.
type Options struct {
	ProcessObjectIdAsDate bool // objectId will be converted to date and analysis
	//MaxItemsForFreqAnalysis   uint // for the frequency analysis (unique, top, bottom), the first N samples will be used, zero = all items
	StoreMinMaxAvgValue   bool // store minimum, maximum and average value if possible
	StoreMinMaxAvgLength  bool // store minimum, maximum and average length
	StoreCountOfUnique    bool // store number of unique values
	StoreMostFrequent     uint // saves the N values that most occur, zero = disabled
	StoreLeastFrequent    uint // saves the N values that least occur, zero = disabled
	StoreWeekdayHistogram bool
	StoreHourHistogram    bool
	ValueHistogramMaxRes  uint // create histogram from values, zero = disabled
	LengthHistogramMaxRes uint // create histogram from length of values, zero = disabled
}

// IsNecessaryToCalcValueFreq - will be value frequency distribution needed for further calculations?
func (options *Options) IsNecessaryToCalcValueFreq() bool {
	return options.StoreCountOfUnique ||
		options.StoreMostFrequent > 0 ||
		options.StoreLeastFrequent > 0 ||
		options.ValueHistogramMaxRes > 0
}

// IsNecessaryToCalcLengthFreq - will be length frequency distribution needed for further calculations?
func (options *Options) IsNecessaryToCalcLengthFreq() bool {
	return options.LengthHistogramMaxRes > 0
}

// Result - values from expand stage are grouped by name and type.
type Result struct {
	Name string        `bson:"n"`
	Type analysis.Type `bson:",inline"`
}

// StoreMinMaxValueTypes - Types for which the minimum and maximum values are calculated and stored,
// if options.StoreMinMaxAvgValue == true
var StoreMinMaxValueTypes = []string{
	"objectId",
	"double",
	"string",
	"bool",
	"date",
	"int",
	"timestamp",
	"long",
	"decimal",
}

// StoreAvgValueTypes - types for which the average value is calculated and stored,
// if options.StoreMinMaxAvgValue == true
var StoreAvgValueTypes = []string{
	"double",
	"bool",
	"int",
	"long",
	"decimal",
}

// StoreLengthTypes - types for which the minimum, maximum, and average values are stored,
// if options.StoreMinMaxAvgLength == true
var StoreLengthTypes = []string{
	"string",
	"array",
	"object",
}

// StoreTopValuesTypes - types for which are saved most occurring value,
// if options.StoreMostFrequent > 0
var StoreTopValuesTypes = []string{
	"double",
	"string",
	"date",
	"int",
	"timestamp",
	"long",
	"decimal",
}

// StoreBottomValuesTypes - types for which are saved least occurring value,
// if options.StoreLeastFrequent > 0
var StoreBottomValuesTypes = []string{
	"double",
	"string",
	"date",
	"int",
	"timestamp",
	"long",
	"decimal",
}

// StoreCountOfUniqueTypes - types for which is stored number of unique values
// if options.StoreCountOfUnique == true
var StoreCountOfUniqueTypes = []string{
	"double",
	"string",
	"date",
	"int",
	"timestamp",
	"long",
	"decimal",
}

// ValueHistogramTypes - types for which a histogram is created from the values.
// if ValueHistogramResolution > 0
var ValueHistogramTypes = []string{
	"double",
	"date",
	"int",
	"long",
	"decimal",
}

// LengthHistogramTypes - types for which a histogram is created from the lengths.
// if LengthHistogramResolution > 0
var LengthHistogramTypes = []string{
	"string",
	"array",
	"object",
}

// TopBottomValuesTypes = STORE_TOP_VALUES_TYPES + STORE_BOTTOM_VALUES_TYPES
var TopBottomValuesTypes []string

// StoreValueTypes - types for which is calculated frequency distribution of value.
var StoreValueTypes []string

func init() {
	// Compute VALUE_FREQ_TYPES
	types := make(map[string]bool)
	for _, t := range StoreTopValuesTypes {
		types[t] = true
	}
	for _, t := range StoreBottomValuesTypes {
		types[t] = true
	}
	for _, t := range ValueHistogramTypes {
		types[t] = true
	}
	StoreValueTypes = make([]string, len(types))
	for t := range types {
		StoreValueTypes = append(StoreValueTypes, t)
	}
	StoreValueTypes = append(StoreValueTypes, "bool", "objectId")

	// Compute TOP_BOTTOM_VALUES_TYPES
	types = make(map[string]bool)
	for _, t := range StoreTopValuesTypes {
		types[t] = true
	}
	for _, t := range StoreBottomValuesTypes {
		types[t] = true
	}
	TopBottomValuesTypes = make([]string, len(types))
	for t := range types {
		TopBottomValuesTypes = append(TopBottomValuesTypes, t)
	}
}

// StageFactory prototype.
type StageFactory func(options *Options) *analysis.Stage
