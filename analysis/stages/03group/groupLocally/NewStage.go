// Package groupLocally is the implementation of the group stage that runs locally.
package groupLocally

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
)

// NewStage - GroupLocally stage factory.
func NewStage(groupOptions *group.Options) *analysis.Stage {
	return &analysis.Stage{
		Processor: func(_input interface{}, analysisOptions *analysis.Options) interface{} {
			// Input channel
			input := expand.ToValueChannel(_input, analysisOptions.Concurrency, analysisOptions.BufferSize)

			// Create output channel and wait group
			output := make(chan group.Result, analysisOptions.BufferSize)

			dataProcesses := &dataProcesses{
				valueFreq:       runValueFreqWorkers(groupOptions, analysisOptions),
				lengthFreq:      runLengthFreqWorkers(groupOptions, analysisOptions),
				dateWeekdayFreq: runDateWeekdayFreqWorkers(groupOptions, analysisOptions),
				dateHourFreq:    runDateHourFreqWorkers(groupOptions, analysisOptions),
			}

			groupProcess := runGroupWorkers(input, dataProcesses, groupOptions, analysisOptions)
			mergeProcess := runMergeWorker(groupProcess, groupOptions, analysisOptions)
			statsProcess := runStatsWorkers(dataProcesses, mergeProcess, output, groupOptions, analysisOptions)

			// Close output channel
			go func() {
				statsProcess.Wait()
				close(output)
			}()

			return output
		},
	}
}
