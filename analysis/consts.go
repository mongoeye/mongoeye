package analysis

import "github.com/mongoeye/mongoeye/helpers"

// Abbreviations for aggregation pipeline.
var (
	BsonId                  string
	BsonFieldType           string
	BsonCount               string
	BsonCountUnique         string
	BsonValueStats          string
	BsonMinValue            string
	BsonMaxValue            string
	BsonAvgValue            string
	BsonLengthStats         string
	BsonMinLength           string
	BsonMaxLength           string
	BsonAvgLength           string
	BsonMostFrequent        string
	BsonLeastFrequent       string
	BsonValueFreqValue      string
	BsonValueFreqCount      string
	BsonValueHistogram      string
	BsonLengthHistogram     string
	BsonWeekdayHistogram    string
	BsonHourHistogram       string
	BsonHistogramStart      string
	BsonHistogramEnd        string
	BsonHistogramRange      string
	BsonHistogramStep       string
	BsonHistogramNumOfSteps string
	BsonHistogramIntervals  string
	BsonIntervalValue       string
	BsonIntervalCount       string

	JsonHistogramStart      string
	JsonHistogramEnd        string
	JsonHistogramRange      string
	JsonHistogramStep       string
	JsonHistogramNumOfSteps string
	JsonHistogramIntervals  string

	YamlHistogramStart      string
	YamlHistogramEnd        string
	YamlHistogramRange      string
	YamlHistogramStep       string
	YamlHistogramNumOfSteps string
	YamlHistogramIntervals  string
)

func init() {
	t := Type{}
	v := ValueStats{}
	l := LengthStats{}
	f := ValueFreq{}
	h := Histogram{}
	i := Interval{}

	BsonId = "_id"
	BsonFieldType = helpers.GetBSONFieldName(t, "Name")
	BsonCount = helpers.GetBSONFieldName(t, "Count")
	BsonCountUnique = helpers.GetBSONFieldName(t, "CountUnique")
	BsonValueStats = helpers.GetBSONFieldName(t, "ValueStats")
	BsonMinValue = helpers.GetBSONFieldName(v, "Min")
	BsonMaxValue = helpers.GetBSONFieldName(v, "Max")
	BsonAvgValue = helpers.GetBSONFieldName(v, "Avg")
	BsonLengthStats = helpers.GetBSONFieldName(t, "LengthStats")
	BsonMinLength = helpers.GetBSONFieldName(l, "Min")
	BsonMaxLength = helpers.GetBSONFieldName(l, "Max")
	BsonAvgLength = helpers.GetBSONFieldName(l, "Avg")
	BsonMostFrequent = helpers.GetBSONFieldName(t, "MostFrequent")
	BsonLeastFrequent = helpers.GetBSONFieldName(t, "LeastFrequent")
	BsonValueFreqValue = helpers.GetBSONFieldName(f, "Value")
	BsonValueFreqCount = helpers.GetBSONFieldName(f, "Count")
	BsonValueHistogram = helpers.GetBSONFieldName(t, "ValueHistogram")
	BsonLengthHistogram = helpers.GetBSONFieldName(t, "LengthHistogram")
	BsonHistogramStart = helpers.GetBSONFieldName(h, "Start")
	BsonHistogramEnd = helpers.GetBSONFieldName(h, "End")
	BsonHistogramRange = helpers.GetBSONFieldName(h, "Range")
	BsonHistogramStep = helpers.GetBSONFieldName(h, "Step")
	BsonHistogramNumOfSteps = helpers.GetBSONFieldName(h, "NumberOfSteps")
	BsonHistogramIntervals = helpers.GetBSONFieldName(h, "Intervals")
	BsonIntervalValue = helpers.GetBSONFieldName(i, "Interval")
	BsonIntervalCount = helpers.GetBSONFieldName(i, "Count")
	BsonWeekdayHistogram = helpers.GetBSONFieldName(t, "WeekdayHistogram")
	BsonHourHistogram = helpers.GetBSONFieldName(t, "HourHistogram")

	JsonHistogramStart = helpers.GetJSONFieldName(h, "Start")
	JsonHistogramEnd = helpers.GetJSONFieldName(h, "End")
	JsonHistogramRange = helpers.GetJSONFieldName(h, "Range")
	JsonHistogramStep = helpers.GetJSONFieldName(h, "Step")
	JsonHistogramNumOfSteps = helpers.GetJSONFieldName(h, "NumberOfSteps")
	JsonHistogramIntervals = helpers.GetJSONFieldName(h, "Intervals")

	YamlHistogramStart = helpers.GetYAMLFieldName(h, "Start")
	YamlHistogramEnd = helpers.GetYAMLFieldName(h, "End")
	YamlHistogramRange = helpers.GetYAMLFieldName(h, "Range")
	YamlHistogramStep = helpers.GetYAMLFieldName(h, "Step")
	YamlHistogramNumOfSteps = helpers.GetYAMLFieldName(h, "NumberOfSteps")
	YamlHistogramIntervals = helpers.GetYAMLFieldName(h, "Intervals")
}
