package groupLocally

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/helpers"
	"reflect"
	"sync"
)

func runGroupWorkers(input <-chan expand.Value, dataProcesses *dataProcesses, groupOptions *group.Options, analysisOptions *analysis.Options) *groupProcess {
	wg := &sync.WaitGroup{}
	wg.Add(analysisOptions.Concurrency)
	results := make([]GroupResults, analysisOptions.Concurrency)
	for i := 0; i < analysisOptions.Concurrency; i++ {
		results[i] = make(GroupResults)
		go groupWorker(input, results[i], dataProcesses, groupOptions, analysisOptions, wg)
	}

	go func() {
		wg.Wait()

		// Close input channels for frequency distribution calculations
		dataProcesses.closeAllInputs()
	}()

	return &groupProcess{
		Results: results,
		wg:      wg,
	}
}

func groupWorker(input <-chan expand.Value, results GroupResults, dataProcesses *dataProcesses, groupOptions *group.Options, analysisOptions *analysis.Options, wg *sync.WaitGroup) {
	defer wg.Done()

	for fieldValue := range input {
		// Group by id
		id := GroupId{
			Name: fieldValue.Name,
			Type: fieldValue.Type,
		}

		t := fieldValue.Type

		// Load or create
		acc := results[id]
		if acc == nil {
			acc = createAccumulator(id, groupOptions)
			results[id] = acc
		}

		// Convert ObjectId to date
		if acc.ConvertObjectIdToDate && fieldValue.Value != nil {
			fieldValue.Value = helpers.SafeToObjectId(fieldValue.Value).Time()
			t = "date"
		}

		// Set date timezone
		if t == "date" && fieldValue.Value != nil {
			fieldValue.Value = helpers.SafeToDate(fieldValue.Value).In(analysisOptions.Location)
		}

		// Count
		acc.Count++

		// Value extremes
		if acc.StoreMinMaxValue {
			storeMinMaxSum(acc, t, fieldValue.Value)
		}

		// Value freq
		if acc.StoreValueDistribution {
			dataProcesses.valueFreq.Input <- value{
				Id:    id,
				Value: fieldValue.Value,
			}
		}

		// Length extremes
		if acc.StoreMinMaxAvgLength {
			if fieldValue.Length < acc.MinLength {
				acc.MinLength = fieldValue.Length
			}
			if fieldValue.Length > acc.MaxLength {
				acc.MaxLength = fieldValue.Length
			}
			acc.LengthSum += uint64(fieldValue.Length)
		}

		// Length freq
		if acc.StoreLengthDistribution {
			dataProcesses.lengthFreq.Input <- length{
				Id:     id,
				Length: fieldValue.Length,
			}
		}

		// Date weekday freq
		if acc.StoreDateWeekdayDistribution {
			dataProcesses.dateWeekdayFreq.Input <- value{
				Id:    id,
				Value: fieldValue.Value,
			}
		}

		// Date weekday hour
		if acc.StoreDateHourDistribution {
			dataProcesses.dateHourFreq.Input <- value{
				Id:    id,
				Value: fieldValue.Value,
			}
		}
	}
}

func storeMinMaxSum(acc *Accumulator, t string, value interface{}) {
	var valueDouble float64

	// same as MIN_MAX_TYPES

	switch t {
	case "objectId":
		{
			value := helpers.SafeToObjectId(value)
			if acc.MinValue == nil {
				acc.MinValue = value
			} else {
				min := helpers.SafeToObjectId(acc.MinValue)
				if min > value {
					acc.MinValue = value
				}
			}

			if acc.MaxValue == nil {
				acc.MaxValue = value
			} else {
				max := helpers.SafeToObjectId(acc.MaxValue)
				if max < value {
					acc.MaxValue = value
				}
			}
		}
	case "double":
		{
			value := helpers.SafeToDouble(value)
			if acc.MinValue == nil {
				acc.MinValue = value
			} else {
				min := helpers.SafeToDouble(acc.MinValue)
				if min > value {
					acc.MinValue = value
				}
			}

			if acc.MaxValue == nil {
				acc.MaxValue = value
			} else {
				max := helpers.SafeToDouble(acc.MaxValue)
				if max < value {
					acc.MaxValue = value
				}
			}
			valueDouble = value
		}
	case "string":
		{
			value := helpers.SafeToString(value)
			if acc.MinValue == nil {
				acc.MinValue = value
			} else {
				min := helpers.SafeToString(acc.MinValue)
				if min > value {
					acc.MinValue = value
				}
			}

			if acc.MaxValue == nil {
				acc.MaxValue = value
			} else {
				max := helpers.SafeToString(acc.MaxValue)
				if max < value {
					acc.MaxValue = value
				}
			}

		}
	case "bool":
		{
			value := helpers.SafeToBool(value)
			if acc.MinValue == nil {
				acc.MinValue = value
			} else {
				min := helpers.SafeToBool(acc.MinValue)
				if min == true {
					acc.MinValue = value
				}
			}

			if acc.MaxValue == nil {
				acc.MaxValue = value
			} else {
				max := helpers.SafeToBool(acc.MaxValue)
				if max == false {
					acc.MaxValue = value
				}
			}

			if value {
				valueDouble = 1
			} else {
				valueDouble = 0
			}
		}
	case "date":
		{
			value := helpers.SafeToDate(value)
			if acc.MinValue == nil {
				acc.MinValue = value
			} else {
				min := helpers.SafeToDate(acc.MinValue)
				if min.Unix() > value.Unix() {
					acc.MinValue = value
				}
			}

			if acc.MaxValue == nil {
				acc.MaxValue = value
			} else {
				max := helpers.SafeToDate(acc.MaxValue)
				if max.Unix() < value.Unix() {
					acc.MaxValue = value
				}
			}
		}
	case "int":
		{
			value := helpers.SafeToInt(value)
			if acc.MinValue == nil {
				acc.MinValue = value
			} else {
				min := helpers.SafeToInt(acc.MinValue)
				if min > value {
					acc.MinValue = value
				}
			}

			if acc.MaxValue == nil {
				acc.MaxValue = value
			} else {
				max := helpers.SafeToInt(acc.MaxValue)
				if max < value {
					acc.MaxValue = value
				}
			}

			valueDouble = float64(value)
		}
	case "timestamp":
		{
			value := helpers.SafeToTimestamp(value)
			if acc.MinValue == nil {
				acc.MinValue = value
			} else {
				min := helpers.SafeToTimestamp(acc.MinValue)
				if min > value {
					acc.MinValue = value
				}
			}

			if acc.MaxValue == nil {
				acc.MaxValue = value
			} else {
				max := helpers.SafeToTimestamp(acc.MaxValue)
				if max < value {
					acc.MaxValue = value
				}
			}
		}
	case "long":
		{
			value := helpers.SafeToLong(value)
			if acc.MinValue == nil {
				acc.MinValue = value
			} else {
				min := helpers.SafeToLong(acc.MinValue)
				if min > value {
					acc.MinValue = value
				}
			}

			if acc.MaxValue == nil {
				acc.MaxValue = value
			} else {
				max := helpers.SafeToLong(acc.MaxValue)
				if max < value {
					acc.MaxValue = value
				}
			}

			valueDouble = float64(value)
		}
	case "decimal":
		{
			value := helpers.SafeToDecimal(value)
			if acc.MinValue == nil {
				acc.MinValue = value
			} else {
				min := helpers.SafeToDecimal(acc.MinValue)
				if helpers.CmpDecimal(min, value) > 0 {
					acc.MinValue = value
				}
			}

			if acc.MaxValue == nil {
				acc.MaxValue = value
			} else {
				max := helpers.SafeToDecimal(acc.MaxValue)
				if helpers.CmpDecimal(max, value) < 0 {
					acc.MaxValue = value
				}
			}

			valueDouble = helpers.DecimalToDouble(value)
		}
	default:
		panic("Unexpected type: " + reflect.TypeOf(value).String())
	}

	if acc.StoreAvgValue {
		acc.ValuesSum += valueDouble
	}
}
