package groupLocally

import (
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/helpers"
	"math"
)

// GroupId  - values are grouped by field name and type.
type GroupId struct {
	Name string
	Type string
}

// Accumulator represents aggregation in one group worker.
type Accumulator struct {
	Count uint64

	ConvertObjectIdToDate bool

	StoreMinMaxValue bool
	MinValue         interface{}
	MaxValue         interface{}

	StoreAvgValue bool
	ValuesSum     float64

	StoreMinMaxAvgLength bool
	MinLength            uint
	MaxLength            uint
	LengthSum            uint64

	StoreValueDistribution       bool
	StoreLengthDistribution      bool
	StoreDateWeekdayDistribution bool
	StoreDateHourDistribution    bool
}

// Create accumulator. Accumulator represents aggregation in one group worker.
func createAccumulator(id GroupId, options *group.Options) *Accumulator {
	acc := &Accumulator{}

	t := id.Type

	if t == "objectId" && options.ProcessObjectIdAsDate && (options.StoreMinMaxAvgValue ||
		options.StoreWeekdayHistogram ||
		options.StoreHourHistogram ||
		options.ValueHistogramMaxRes > 0) {

		acc.ConvertObjectIdToDate = true
		t = "date"
	}

	if (options.StoreCountOfUnique && helpers.InStringSlice(t, group.StoreCountOfUniqueTypes)) ||
		(options.StoreMostFrequent > 0 && helpers.InStringSlice(t, group.StoreTopValuesTypes)) ||
		(options.StoreLeastFrequent > 0 && helpers.InStringSlice(t, group.StoreBottomValuesTypes)) ||
		(options.ValueHistogramMaxRes > 0 && helpers.InStringSlice(t, group.ValueHistogramTypes)) {
		acc.StoreValueDistribution = true
	}

	if options.LengthHistogramMaxRes > 0 && helpers.InStringSlice(t, group.LengthHistogramTypes) {
		acc.StoreLengthDistribution = true
	}

	if options.StoreMinMaxAvgValue || acc.StoreValueDistribution {
		acc.StoreMinMaxValue = helpers.InStringSlice(t, group.StoreMinMaxValueTypes)
		acc.StoreAvgValue = helpers.InStringSlice(t, group.StoreAvgValueTypes)
	}

	if options.StoreMinMaxAvgLength || acc.StoreLengthDistribution {
		acc.StoreMinMaxAvgLength = helpers.InStringSlice(t, group.StoreLengthTypes)
		if acc.StoreMinMaxAvgLength {
			acc.MinLength = math.MaxUint32
			acc.MaxLength = 0
		}
	}

	if options.StoreWeekdayHistogram && t == "date" {
		acc.StoreDateWeekdayDistribution = true
	}

	if options.StoreHourHistogram && t == "date" {
		acc.StoreDateHourDistribution = true
	}

	return acc
}
