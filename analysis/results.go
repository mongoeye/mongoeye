package analysis

import (
	"fmt"
	"github.com/mongoeye/mongoeye/helpers"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/yaml.v2"
	"strings"
)

// Fields result of the analysis.
type Fields []*Field

func (r Fields) Len() int           { return len(r) }
func (r Fields) Less(i, j int) bool { return strings.ToLower(r[i].Name) < strings.ToLower(r[j].Name) }
func (r Fields) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

// Field - analysis results for one document field.
type Field struct {
	Name  string `json:"name"   yaml:"name"    bson:"n"`
	Level uint   `json:"level"  yaml:"level"`
	Count uint64 `json:"count"  yaml:"count"   bson:"c"`
	Types Types  `json:"types"  yaml:"types"   bson:"T"`
}

// Types of the one document field.
type Types []*Type

// TypesSort represents sort of types in final results.
var TypesSort = map[string]int{
	"null":                1,
	"undefined":           2,
	"bool":                3,
	"int":                 4,
	"long":                5,
	"double":              6,
	"decimal":             7,
	"objectId":            8,
	"dbPointer":           9,
	"symbol":              10,
	"string":              11,
	"regex":               12,
	"javascript":          13,
	"javascriptWithScope": 14,
	"binData":             15,
	"date":                16,
	"timestamp":           17,
	"minKey":              18,
	"maxKey":              19,
	"array":               20,
	"object":              21,
}

func (s Types) Len() int           { return len(s) }
func (s Types) Less(i, j int) bool { return TypesSort[s[i].Name] < TypesSort[s[j].Name] }
func (s Types) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Type - analysis results for one type of the one document field.
type Type struct {
	Name             string            `json:"type"                       yaml:"type"                        bson:"t"`
	Count            uint64            `json:"count"                      yaml:"count"                       bson:"c"`
	CountUnique      uint64            `json:"unique,omitempty"           yaml:"unique,omitempty"            bson:"cu,omitempty"`
	ValueExtremes    *ValueExtremes    `json:"value,omitempty"            yaml:"value,omitempty"             bson:"ve,omitempty"`
	LengthExtremes   *LengthExtremes   `json:"length,omitempty"           yaml:"length,omitempty"            bson:"le,omitempty"`
	TopNValues       ValueFreqSlice    `json:"top,omitempty"              yaml:"top,omitempty"               bson:"tv,omitempty"`
	BottomNValues    ValueFreqSlice    `json:"bottom,omitempty"           yaml:"bottom,omitempty"            bson:"bv,omitempty"`
	ValueHistogram   *Histogram        `json:"valueHistogram,omitempty"   yaml:"valueHistogram,omitempty"    bson:"vH,omitempty"`
	LengthHistogram  *Histogram        `json:"lengthHistogram,omitempty"  yaml:"lengthHistogram,omitempty"   bson:"lH,omitempty"`
	WeekdayHistogram *WeekdayHistogram `json:"weekdayHistogram,omitempty" yaml:"weekdayHistogram,omitempty"  bson:"wH,omitempty"`
	HourHistogram    *HourHistogram    `json:"hourHistogram,omitempty"    yaml:"hourHistogram,omitempty"     bson:"hH,omitempty"`
}

// ValueExtremes - min, Max, Avg value.
type ValueExtremes struct {
	Min interface{} `json:"min"           yaml:"min"             bson:"i"`
	Max interface{} `json:"max"           yaml:"max"             bson:"a"`
	Avg interface{} `json:"avg,omitempty" yaml:"avg,omitempty"   bson:"g"`
}

// LengthExtremes - min, Max, Avg length.
type LengthExtremes struct {
	Min uint    `json:"min"           yaml:"min"             bson:"il"`
	Max uint    `json:"max"           yaml:"max"             bson:"al"`
	Avg float64 `json:"avg,omitempty" yaml:"avg,omitempty"   bson:"gl"`
}

// ValueFreqSlice - frequency of values occurrence.
type ValueFreqSlice []ValueFreq

// ValueFreq - - frequency of one value occurrence.
type ValueFreq struct {
	Value interface{} `json:"value"        yaml:"value"           bson:"v"`
	Count Count       `json:"count"        yaml:"count"           bson:"c"`
}

// Count of intervals or values.
type Count uint

// Histogram of values from one specific type of specific document field.
type Histogram struct {
	Start         interface{} `json:"start"      yaml:"start"       bson:"sta"` // minimal value rounded down with Step
	End           interface{} `json:"end"        yaml:"end"         bson:"end"` // maximal value rounded up with Step
	Range         float64     `json:"range"      yaml:"range"       bson:"r"`   // (end-start) converted to float64
	Step          float64     `json:"step"       yaml:"step"        bson:"s"`   // size of one interval, rounded to 1, 0.5, 0.25, 0.2, 0.1, 0.05, 0.025, ...
	NumberOfSteps uint        `json:"numOfSteps" yaml:"numOfSteps"  bson:"ns"`  // total number of steps
	Intervals     Intervals   `json:"intervals"  yaml:"intervals"   bson:"it"`  // values, interval => count
}

// SetBSON - handle specific types, such as bson.Decimal, ...
func (h *Histogram) SetBSON(raw bson.Raw) error {

	decoded := new(struct {
		Start         interface{} `bson:"sta"`
		End           interface{} `bson:"end"`
		Range         interface{} `bson:"r"`
		Step          interface{} `bson:"s"`
		NumberOfSteps interface{} `bson:"ns"`
		Intervals     Intervals   `bson:"it"`
	})

	err := raw.Unmarshal(decoded)
	if err != nil {
		return err
	}

	h.Start = decoded.Start
	h.End = decoded.End
	h.Range = helpers.ToDouble(decoded.Range)
	h.Step = helpers.ToDouble(decoded.Step)
	h.NumberOfSteps = uint(helpers.ToDouble(decoded.NumberOfSteps) + 0.5)
	h.Intervals = decoded.Intervals

	return nil
}

// MarshalJSON - convert intervals to []Count.
// Key is a interval and empty intervals are added.
func (h Histogram) MarshalJSON() ([]byte, error) {
	intervals := make([]Count, h.NumberOfSteps)

	i := 0
	for _, interval := range h.Intervals {
		for ; i < int(interval.Interval); i++ {
			intervals[i] = Count(0)
		}

		if i != int(interval.Interval) {
			return nil, fmt.Errorf("Invalid interval order. Unexpected interval: %d, expected %d", interval.Interval, i)
		}

		intervals[i] = interval.Count

		i++
	}

	return []byte(fmt.Sprintf("{\"%s\": %s,\"%s\": %s,\"%s\": %s,\"%s\": %s,\"%s\": %s,\"%s\": %s}",
		JsonHistogramStart, helpers.MarshalToJSON(h.Start),
		JsonHistogramEnd, helpers.MarshalToJSON(h.End),
		JsonHistogramRange, helpers.MarshalToJSON(h.Range),
		JsonHistogramStep, helpers.MarshalToJSON(h.Step),
		JsonHistogramNumOfSteps, helpers.MarshalToJSON(h.NumberOfSteps),
		JsonHistogramIntervals, helpers.MarshalToJSON(intervals),
	)), nil
}

// MarshalYAML - convert intervals to []Count.
// Key is a interval and empty intervals are added.
func (h Histogram) MarshalYAML() (interface{}, error) {
	intervals := make([]Count, h.NumberOfSteps)

	i := 0
	for _, interval := range h.Intervals {
		// Add empty intervals
		for ; i < int(interval.Interval); i++ {
			intervals[i] = Count(0)
		}

		if i != int(interval.Interval) {
			return nil, fmt.Errorf("Invalid interval order. Unexpected interval: %d, expected %d", interval.Interval, i)
		}

		intervals[i] = interval.Count

		i++
	}

	m := yaml.MapSlice{
		{
			Key:   YamlHistogramStart,
			Value: h.Start,
		},
		{
			Key:   YamlHistogramEnd,
			Value: h.End,
		},
		{
			Key:   YamlHistogramRange,
			Value: h.Range,
		},
		{
			Key:   YamlHistogramStep,
			Value: h.Step,
		},
		{
			Key:   YamlHistogramNumOfSteps,
			Value: h.NumberOfSteps,
		},
		{
			Key:   YamlHistogramIntervals,
			Value: intervals,
		},
	}

	return m, nil
}

// Intervals in histogram.
type Intervals []*Interval

func (hv Intervals) Len() int           { return len(hv) }
func (hv Intervals) Less(i, j int) bool { return hv[i].Interval < hv[j].Interval }
func (hv Intervals) Swap(i, j int)      { hv[i], hv[j] = hv[j], hv[i] }

// SetBSON - handle specific types, such as bson.Decimal, ...
func (i *Interval) SetBSON(raw bson.Raw) error {

	decoded := new(struct {
		Interval interface{} `bson:"i"`
		Count    Count       `bson:"c"`
	})

	err := raw.Unmarshal(decoded)
	if err != nil {
		return err
	}

	i.Interval = helpers.ToUInt(decoded.Interval)
	i.Count = decoded.Count

	return nil
}

// Interval and count of values that belong to it.
type Interval struct {
	Interval uint  `bson:"i"` // Interval in histogram. From 0 to (NumberOfSteps - 1)
	Count    Count `bson:"c"` // Number of items belonging to the interval
}

// WeekdayHistogram (0=Sunday ... 6=Saturday).
type WeekdayHistogram [7]Count

// SetBSON - parse histogram to array
func (wh *WeekdayHistogram) SetBSON(raw bson.Raw) error {

	decoded := new(struct {
		Intervals Intervals `bson:"it"`
	})

	err := raw.Unmarshal(decoded)
	if err != nil {
		return err
	}

	for _, i := range decoded.Intervals {
		wh[i.Interval] = i.Count
	}

	return nil
}

// MarshalYAML - convert array to slice.
// yaml.Marshal from unknown reason can not handle array.
func (wh WeekdayHistogram) MarshalYAML() (interface{}, error) {
	return wh[:], nil
}

// HourHistogram (0 - 23).
type HourHistogram [24]Count

// SetBSON - parse histogram to array
func (hh *HourHistogram) SetBSON(raw bson.Raw) error {

	decoded := new(struct {
		Intervals Intervals `bson:"it"`
	})

	err := raw.Unmarshal(decoded)
	if err != nil {
		return err
	}

	for _, i := range decoded.Intervals {
		hh[i.Interval] = i.Count
	}

	return nil
}

// MarshalYAML - convert array to slice.
// yaml.Marshal from unknown reason can not handle array.
func (hh HourHistogram) MarshalYAML() (interface{}, error) {
	return hh[:], nil
}
