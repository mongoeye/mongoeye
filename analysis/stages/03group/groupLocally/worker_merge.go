package groupLocally

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/helpers"
)

func runMergeWorker(groupProcess *groupProcess, groupOptions *group.Options, analysisOptions *analysis.Options) *mergeProcess {
	ch := make(chan group.Result, analysisOptions.BufferSize)

	go func() {
		// Wait for completion of the group process
		groupProcess.Wait()

		// Merge results
		finalResults := mergeResults(groupProcess.Results, groupOptions)

		// Pass results
		for id, acc := range finalResults {
			t := analysis.Type{
				Name:  id.Type,
				Count: acc.Count,
			}

			// Value extremes
			if acc.StoreMinMaxValue {
				t.ValueExtremes = &analysis.ValueExtremes{
					Min: acc.MinValue,
					Max: acc.MaxValue,
				}

				if acc.StoreAvgValue {
					avg := acc.ValuesSum / float64(acc.Count)
					if id.Type == "decimal" {
						t.ValueExtremes.Avg = helpers.DoubleToDecimal(avg)
					} else {
						t.ValueExtremes.Avg = avg
					}
				}
			}

			// Length extremes
			if acc.StoreMinMaxAvgLength {
				t.LengthExtremes = &analysis.LengthExtremes{
					Min: acc.MinLength,
					Max: acc.MaxLength,
					Avg: float64(acc.LengthSum) / float64(acc.Count),
				}
			}

			ch <- group.Result{
				Name: id.Name,
				Type: t,
			}
		}

		close(ch)
	}()

	return &mergeProcess{
		Output: ch,
	}
}

func mergeResults(partialResults []GroupResults, options *group.Options) GroupResults {
	finalResults := make(GroupResults)
	for _, result := range partialResults {
		for id, acc := range result {
			// Load or create
			final := finalResults[id]
			if final == nil {
				final = createAccumulator(id, options)
				finalResults[id] = final
			}

			// Count
			final.Count += acc.Count

			// Value extremes
			if final.StoreMinMaxValue {
				t := id.Type
				if final.ConvertObjectIdToDate {
					t = "date"
				}

				if final.MinValue == nil {
					final.MinValue = acc.MinValue
				} else {
					final.MinValue = helpers.MinT(t, final.MinValue, acc.MinValue)
				}

				if final.MaxValue == nil {
					final.MaxValue = acc.MaxValue
				} else {
					final.MaxValue = helpers.MaxT(t, final.MaxValue, acc.MaxValue)
				}

				if final.StoreAvgValue {
					final.ValuesSum += acc.ValuesSum
				}
			}

			// Length extremes
			if final.StoreMinMaxAvgLength {
				final.MinLength = helpers.MinUInt(final.MinLength, acc.MinLength)
				final.MaxLength = helpers.MaxUInt(final.MaxLength, acc.MaxLength)
				final.LengthSum += acc.LengthSum
			}
		}
	}

	return finalResults
}
