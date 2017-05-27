package group

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/helpers"
	"gopkg.in/mgo.v2/bson"
	"reflect"
	"sync"
	"time"
)

// ToResultChannel converts input channel to Result channel.
// The input may by raw []byte channel or Result channel (no conversion).
func ToResultChannel(inCh interface{}, location *time.Location, concurrency int, bufferSize int) <-chan Result {
	if ch, ok := inCh.(chan Result); ok {
		// local results
		return ch
	} else if inCh, ok := inCh.(chan []byte); ok {
		// database results, raw -> Result
		outCh := make(chan Result, bufferSize)

		wg := &sync.WaitGroup{}
		wg.Add(concurrency)

		// Run workers
		for i := 0; i < concurrency; i++ {
			go convertWorker(inCh, outCh, location, wg)
		}

		// Wait for workers
		go func() {
			wg.Wait()
			close(outCh)
		}()

		return outCh
	}

	panic("Invalid input. Expected 'chan expand.Value' or 'chan []byte'. Given: " + reflect.TypeOf(inCh).String())
}

func convertWorker(inCh <-chan []byte, outCh chan<- Result, location *time.Location, wg *sync.WaitGroup) {
	defer wg.Done()

	for bin := range inCh {
		r := Result{}
		err := bson.Unmarshal(bin, &r)
		if err != nil {
			panic(err)
		}

		NormalizeType(&r.Type, location)

		outCh <- r
	}
}

// NormalizeType normalizes data from the database.
func NormalizeType(t *analysis.Type, location *time.Location) {
	if t.ValueHistogram != nil {
		switch t.Name {
		case "int":
			{
				t.ValueHistogram.Start = int(helpers.ToDouble(t.ValueHistogram.Start))
				t.ValueHistogram.End = int(helpers.ToDouble(t.ValueHistogram.End))
			}
		case "long":
			{
				t.ValueHistogram.Start = int64(helpers.ToDouble(t.ValueHistogram.Start))
				t.ValueHistogram.End = int64(helpers.ToDouble(t.ValueHistogram.End))
			}
		case "date":
			{
				t.ValueHistogram.Start = helpers.SafeToDate(t.ValueHistogram.Start).In(location)
				t.ValueHistogram.End = helpers.SafeToDate(t.ValueHistogram.End).In(location)
			}
		case "decimal":
			{
				t.ValueHistogram.Start = helpers.DoubleToDecimal(helpers.ToDouble(t.ValueHistogram.Start))
				t.ValueHistogram.End = helpers.DoubleToDecimal(helpers.ToDouble(t.ValueHistogram.End))
			}
		}
	}

	if t.LengthHistogram != nil {
		t.LengthHistogram.Start = int(helpers.ToDouble(t.LengthHistogram.Start))
		t.LengthHistogram.End = int(helpers.ToDouble(t.LengthHistogram.End))
	}

	if t.Name == "date" {
		if t.ValueExtremes != nil {
			t.ValueExtremes.Min = helpers.SafeToDate(t.ValueExtremes.Min).In(location)
			t.ValueExtremes.Max = helpers.SafeToDate(t.ValueExtremes.Max).In(location)
		}

		if t.TopNValues != nil {
			for i, v := range t.TopNValues {
				t.TopNValues[i].Value = helpers.SafeToDate(v.Value).In(location)
			}
		}

		if t.BottomNValues != nil {
			for i, v := range t.BottomNValues {
				t.BottomNValues[i].Value = helpers.SafeToDate(v.Value).In(location)
			}
		}
	}
}

// ResultChannelToSlice reads Result channel into slice.
func ResultChannelToSlice(ch <-chan Result) []interface{} {
	out := []interface{}{}
	for v := range ch {
		out = append(out, v)
	}
	return out
}
