package analysis

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/yaml.v2"
	"sort"
	"testing"
)

func TestResults_Sort(t *testing.T) {
	results := Fields{
		{Name: "URL"},
		{Name: "_id"},
		{Name: "def"},
		{Name: "abc"},
		{Name: "xyz"},
	}

	sort.Sort(results)

	assert.Equal(t, 5, len(results))
	assert.Equal(t, Field{Name: "_id"}, *results[0])
	assert.Equal(t, Field{Name: "abc"}, *results[1])
	assert.Equal(t, Field{Name: "def"}, *results[2])
	assert.Equal(t, Field{Name: "URL"}, *results[3])
	assert.Equal(t, Field{Name: "xyz"}, *results[4])
}

func TestTypes_Sort(t *testing.T) {
	types := Types{
		{Name: "object"},
		{Name: "string"},
		{Name: "long"},
		{Name: "null"},
	}

	sort.Sort(types)

	assert.Equal(t, 4, len(types))
	assert.Equal(t, Type{Name: "null"}, *types[0])
	assert.Equal(t, Type{Name: "long"}, *types[1])
	assert.Equal(t, Type{Name: "string"}, *types[2])
	assert.Equal(t, Type{Name: "object"}, *types[3])
}

func TestIntervals_Sort(t *testing.T) {
	intervals := Intervals{
		{Interval: 2, Count: 100},
		{Interval: 0, Count: 200},
		{Interval: 1, Count: 300},
	}

	sort.Sort(intervals)

	assert.Equal(t, 3, len(intervals))
	assert.Equal(t, Interval{Interval: 0, Count: 200}, *intervals[0])
	assert.Equal(t, Interval{Interval: 1, Count: 300}, *intervals[1])
	assert.Equal(t, Interval{Interval: 2, Count: 100}, *intervals[2])
}

func TestInterval_SetBSON(t *testing.T) {
	b, _ := bson.Marshal(bson.M{
		"i": 10,
		"c": 123,
	})

	out := Interval{}
	bson.Unmarshal(b, &out)

	assert.Equal(t, Interval{Interval: 10, Count: 123}, out)
}

func TestInterval_SetBSON_Invalid(t *testing.T) {
	i := Interval{}
	err := bson.Unmarshal([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, &i)
	assert.NotEqual(t, nil, err)
}

func TestHistogram_SetBSON(t *testing.T) {
	m := bson.M{
		"sta": 100,
		"end": 200,
		"r":   100,
		"s":   1,
		"ns":  100,
		"it": []bson.M{
			{
				"i": 10,
				"c": 123,
			},
		},
	}

	b, _ := bson.Marshal(m)

	h := Histogram{}
	err := bson.Unmarshal(b, &h)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, Histogram{
		Start:         100,
		End:           200,
		Range:         100,
		Step:          1,
		NumberOfSteps: 100,
		Intervals: Intervals{
			{
				Interval: 10,
				Count:    123,
			},
		},
	}, h)
}

func TestHistogram_MarshalJSON(t *testing.T) {
	h := Histogram{
		Start:         100,
		End:           105,
		Step:          1,
		NumberOfSteps: 6,
		Intervals: Intervals{
			{
				Interval: 1,
				Count:    25,
			},
			{
				Interval: 4,
				Count:    10,
			},
		},
	}

	j, _ := json.Marshal(h)
	assert.Equal(t, "{\"start\":100,\"end\":105,\"range\":0,\"step\":1,\"numOfSteps\":6,\"intervals\":[0,25,0,0,10,0]}", string(j))
}

func TestHistogram_MarshalJSON_InvalidIntervalSort(t *testing.T) {
	h := Histogram{
		Start:         100,
		End:           109,
		Step:          1,
		NumberOfSteps: 10,
		Intervals: Intervals{
			{
				Interval: 8,
				Count:    25,
			},
			{
				Interval: 9,
				Count:    10,
			},
			{
				Interval: 5,
				Count:    80,
			},
		},
	}

	_, err := json.Marshal(h)
	assert.NotEqual(t, nil, err)
}

func TestHistogram_MarshalYAML(t *testing.T) {
	h := Histogram{
		Start:         100,
		End:           105,
		Step:          1,
		NumberOfSteps: 6,
		Intervals: Intervals{
			{
				Interval: 1,
				Count:    25,
			},
			{
				Interval: 4,
				Count:    10,
			},
		},
	}

	j, _ := yaml.Marshal(h)
	assert.Equal(t, "start: 100\nend: 105\nrange: 0\nstep: 1\nnumOfSteps: 6\nintervals:\n- 0\n- 25\n- 0\n- 0\n- 10\n- 0\n", string(j))
}

func TestHistogram_MarshalYAML_InvalidIntervalOrder(t *testing.T) {
	h := Histogram{
		Start:         100,
		End:           109,
		Step:          1,
		NumberOfSteps: 10,
		Intervals: Intervals{
			{
				Interval: 8,
				Count:    25,
			},
			{
				Interval: 9,
				Count:    10,
			},
			{
				Interval: 5,
				Count:    80,
			},
		},
	}

	_, err := yaml.Marshal(h)
	assert.NotEqual(t, nil, err)
}

func TestWeekdayHistogram_SetBSON(t *testing.T) {
	m := bson.M{
		"it": []bson.M{
			{
				"i": 2,
				"c": 5,
			},
			{
				"i": 4,
				"c": 10,
			},
		},
	}

	b, _ := bson.Marshal(m)

	h := WeekdayHistogram{}
	err := bson.Unmarshal(b, &h)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, WeekdayHistogram{0, 0, 5, 0, 10, 0, 0}, h)
}

func TestHourHistogram_SetBSON(t *testing.T) {
	m := bson.M{
		"it": []bson.M{
			{
				"i": 2,
				"c": 5,
			},
			{
				"i": 4,
				"c": 10,
			},
			{
				"i": 23,
				"c": 2,
			},
		},
	}

	b, _ := bson.Marshal(m)

	h := HourHistogram{}
	err := bson.Unmarshal(b, &h)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, HourHistogram{0, 0, 5, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}, h)
}

func TestWeekdayHistogram_MarshalJSON(t *testing.T) {
	h := WeekdayHistogram{0, 1, 2, 3, 4, 5, 6}
	out, err := json.Marshal(h)

	assert.Equal(t, nil, err)
	assert.Equal(t, "[0,1,2,3,4,5,6]", string(out))
}

func TestWeekdayHistogram_MarshalYAML(t *testing.T) {
	h := WeekdayHistogram{0, 1, 2, 3, 4, 5, 6}
	out, err := yaml.Marshal(h)

	assert.Equal(t, nil, err)
	assert.Equal(t, "- 0\n- 1\n- 2\n- 3\n- 4\n- 5\n- 6\n", string(out))
}

func TestHourHistogram_MarshalJSON(t *testing.T) {
	h := HourHistogram{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23}
	out, err := json.Marshal(h)

	assert.Equal(t, nil, err)
	assert.Equal(t, "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]", string(out))
}

func TestHourHistogram_MarshalYAML(t *testing.T) {
	h := HourHistogram{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23}
	out, err := yaml.Marshal(h)

	assert.Equal(t, nil, err)
	assert.Equal(t, "- 0\n- 1\n- 2\n- 3\n- 4\n- 5\n- 6\n- 7\n- 8\n- 9\n- 10\n- 11\n- 12\n- 13\n- 14\n- 15\n- 16\n- 17\n- 18\n- 19\n- 20\n- 21\n- 22\n- 23\n", string(out))
}
