package groupLocally

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/helpers"
	"math"
	"sort"
)

func calculateHistogram(t string, min float64, max float64, _resolution float64, freqTable commonFreqTable, analysisOptions *analysis.Options) *analysis.Histogram {
	_range := max - min
	if _range == 0 {
		return nil
	}

	density := (_resolution - 1) / _range
	shift := math.Pow10(int(math.Floor(math.Log10(density))))
	normDensity := density / shift

	divisor := calcDivisorFromNormDensity(normDensity)

	histogram := analysis.Histogram{}
	histogram.Step = 1 / (divisor * shift)

	if t == "int" || t == "long" {
		// Decimal steps do not make sense for numbers
		histogram.Step = math.Ceil(histogram.Step)
	} else if t == "date" {
		// 1,2,5,10,15,30,60 seconds/minutes ... , 1-24 hours ..., x days
		histogram.Step = helpers.CeilDateSeconds(histogram.Step)
	}

	start := helpers.FloorWithStep(min, histogram.Step)
	end := helpers.CeilWithStep(max, histogram.Step) + histogram.Step

	histogram.Start = helpers.FromDoubleTo(t, start, analysisOptions.Location)
	histogram.End = helpers.FromDoubleTo(t, end, analysisOptions.Location)
	histogram.Range = end - start
	histogram.NumberOfSteps = uint((histogram.Range / histogram.Step) + 0.5)

	intervalTable := make(map[uint]analysis.Count)
	for value, count := range freqTable {
		interval := uint(math.Floor((helpers.ToDouble(value) - start) / histogram.Step))
		intervalTable[interval] += analysis.Count(count)
	}

	i := 0
	histogram.Intervals = make(analysis.Intervals, len(intervalTable))
	for interval, count := range intervalTable {
		histogram.Intervals[i] = &analysis.Interval{
			Interval: interval,
			Count:    count,
		}
		i++
	}

	sort.Sort(histogram.Intervals)

	return &histogram
}

func calcDivisorFromNormDensity(normDensity float64) float64 {
	switch {
	case normDensity < 2:
		return 1
	case normDensity < 4:
		return 2
	default:
		return 4
	}
}
