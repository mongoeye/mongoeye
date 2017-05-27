package groupLocally

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/helpers"
	"sync"
)

func runStatsWorkers(dataProcesses *dataProcesses, mergeProcess *mergeProcess, output chan group.Result, groupOptions *group.Options, analysisOptions *analysis.Options) *statsProcess {
	wg := &sync.WaitGroup{}

	wg.Add(analysisOptions.Concurrency)
	for i := 0; i < analysisOptions.Concurrency; i++ {
		go statsWorker(dataProcesses, mergeProcess, groupOptions, analysisOptions, output, wg)
	}

	return &statsProcess{
		wg: wg,
	}
}

func statsWorker(dataProcesses *dataProcesses, mergeProcess *mergeProcess, groupOptions *group.Options, analysisOptions *analysis.Options, output chan group.Result, wg *sync.WaitGroup) {
	defer wg.Done()

	// Wait for completion of frequency distribution calculations
	dataProcesses.wait()

	for field := range mergeProcess.Output {
		processField(&field, dataProcesses, groupOptions, analysisOptions)
		output <- field
	}
}

func processField(field *group.Result, dataProcesses *dataProcesses, groupOptions *group.Options, analysisOptions *analysis.Options) {
	id := GroupId{
		Name: field.Name,
		Type: field.Type.Name,
	}

	// Frequency distribution of value, length, weekday and hour
	freq := dataProcesses.getFreqTables(id)

	// Field type
	t := field.Type.Name
	if groupOptions.ProcessObjectIdAsDate && t == "objectId" {
		t = "date"
	}

	wg := &sync.WaitGroup{}

	// Statistics workers
	valueHistogram(field, t, freq, groupOptions, analysisOptions, wg)
	lengthHistogram(field, t, freq, groupOptions, analysisOptions, wg)
	weekdayHistogram(field, freq, groupOptions, wg)
	hourHistogram(field, freq, groupOptions, wg)
	topBottomNValues(field, t, freq, groupOptions, wg)

	// Number of unique values
	if groupOptions.StoreCountOfUnique {
		field.Type.CountUnique = uint64(len(freq.Value))
	}

	// Wait for statistics to be calculated
	wg.Wait()

	// Remove value extremes, if not required
	if !groupOptions.StoreMinMaxAvgValue {
		field.Type.ValueExtremes = nil
	}

	// Remove length extremes, if not required
	if !groupOptions.StoreMinMaxAvgLength {
		field.Type.LengthExtremes = nil
	}
}

func valueHistogram(field *group.Result, t string, freq *freqTables, groupOptions *group.Options, analysisOptions *analysis.Options, wg *sync.WaitGroup) {
	if freq.Value != nil && groupOptions.ValueHistogramMaxRes > 0 && helpers.InStringSlice(t, group.ValueHistogramTypes) {
		wg.Add(1)
		go func() {
			defer wg.Done()

			field.Type.ValueHistogram = calculateHistogram(
				t,
				helpers.ToDouble(field.Type.ValueExtremes.Min),
				helpers.ToDouble(field.Type.ValueExtremes.Max),
				float64(groupOptions.ValueHistogramMaxRes),
				freq.Value,
				analysisOptions,
			)
		}()
	}
}

func weekdayHistogram(field *group.Result, freq *freqTables, groupOptions *group.Options, wg *sync.WaitGroup) {
	if groupOptions.StoreWeekdayHistogram && freq.Weekday != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()

			histogram := analysis.WeekdayHistogram{}
			for day, count := range freq.Weekday {
				histogram[day] = analysis.Count(count)
			}

			field.Type.WeekdayHistogram = &histogram
		}()
	}
}

func hourHistogram(field *group.Result, freq *freqTables, groupOptions *group.Options, wg *sync.WaitGroup) {
	if groupOptions.StoreHourHistogram && freq.Hour != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()

			histogram := analysis.HourHistogram{}
			for hour, count := range freq.Hour {
				histogram[hour] = analysis.Count(count)
			}

			field.Type.HourHistogram = &histogram
		}()
	}
}

func lengthHistogram(field *group.Result, t string, freq *freqTables, groupOptions *group.Options, analysisOptions *analysis.Options, wg *sync.WaitGroup) {
	if freq.Length != nil && groupOptions.LengthHistogramMaxRes > 0 && helpers.InStringSlice(t, group.LengthHistogramTypes) {
		wg.Add(1)
		go func() {
			defer wg.Done()

			field.Type.LengthHistogram = calculateHistogram(
				"int",
				helpers.ToDouble(field.Type.LengthExtremes.Min),
				helpers.ToDouble(field.Type.LengthExtremes.Max),
				float64(groupOptions.LengthHistogramMaxRes),
				freq.Length,
				analysisOptions,
			)
		}()
	}
}

func topBottomNValues(field *group.Result, t string, freq *freqTables, groupOptions *group.Options, wg *sync.WaitGroup) {
	if freq.Value != nil && ((groupOptions.StoreTopNValues > 0 && helpers.InStringSlice(t, group.StoreTopValuesTypes)) || (groupOptions.StoreBottomNValues > 0 && helpers.InStringSlice(t, group.StoreBottomValuesTypes))) {
		wg.Add(1)
		go func() {
			defer wg.Done()

			table := newSortedFreqTable(freq.Value)
			keys := table.Keys()

			// Top N values
			if groupOptions.StoreTopNValues > 0 {
				n := int(groupOptions.StoreTopNValues)
				if n > len(keys) {
					n = len(keys)
				}
				field.Type.TopNValues = make([]analysis.ValueFreq, n)

				i := 0
				for _, v := range keys[0:n] {
					field.Type.TopNValues[i] = analysis.ValueFreq{Value: v, Count: analysis.Count(table.freqTable[v])}
					i++
				}
			}

			// Bottom N values
			if groupOptions.StoreBottomNValues > 0 {
				n := int(groupOptions.StoreBottomNValues)
				if n > len(keys) {
					n = len(keys)
				}
				field.Type.BottomNValues = make([]analysis.ValueFreq, n)

				i := 0
				for _, v := range keys[len(keys)-n:] {
					field.Type.BottomNValues[i] = analysis.ValueFreq{Value: v, Count: analysis.Count(table.freqTable[v])}
					i++
				}
			}
		}()
	}
}
