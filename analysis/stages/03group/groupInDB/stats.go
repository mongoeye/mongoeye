package groupInDB

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
)

// ComputeStats generates pipeline that compute stats of values and lengths.
// Example results:
//	[
//		{
//			ID: {
//				FIELD_NAME: "name"
//				FIELD_TYPE: "string",
//				STAT_TYPE: BASE_STATS,
//			}
//			COUNT: 5
//			...
//		},
//		...
//	]
func ComputeStats(p *expr.Pipeline, groupOptions *group.Options, analysisOptions *analysis.Options) {
	// Facet allows you to perform various operations over the same data
	f := expr.Facet()

	// Base computations
	f.AddField(baseStats, BaseComputation(groupOptions))

	// Value extremes
	if groupOptions.StoreMinMaxAvgValue {
		f.AddField(valueStats, ValueStatsComputation(groupOptions))
	}

	// Length extremes
	if groupOptions.StoreMinMaxAvgLength {
		f.AddField(lengthStats, LengthStatsComputation(groupOptions))
	}

	// The most and least frequent values
	if groupOptions.StoreCountOfUnique ||
		groupOptions.StoreMostFrequent > 0 ||
		groupOptions.StoreLeastFrequent > 0 {

		f.AddField(valueFreqStats, ValueFreqComputation(groupOptions))
	}

	if groupOptions.ValueHistogramMaxRes > 0 {
		f.AddField(valueHistogram, ValuesHistogram(groupOptions))
	}

	if groupOptions.LengthHistogramMaxRes > 0 {
		f.AddField(lengthHistogram, LengthsHistogram(groupOptions))
	}

	if groupOptions.StoreWeekdayHistogram {
		f.AddField(weekdayHistogram, DateWeekdayHistogram(analysisOptions.Location, groupOptions))
	}

	if groupOptions.StoreHourHistogram {
		f.AddField(hourHistogram, DateHourHistogram(analysisOptions.Location, groupOptions))
	}

	p.AddStage("facet", f.GetMap())

	// Merge facet results
	p.AddStage("project", bson.M{"data": expr.ConcatArrays(f.GetKeysAsFields()...)})
	p.AddStage("unwind", expr.Field("data"))
	p.AddStage("replaceRoot", bson.M{"newRoot": expr.Field("data")})
}
