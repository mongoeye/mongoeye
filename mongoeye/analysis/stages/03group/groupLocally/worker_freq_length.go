package groupLocally

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"sync"
)

func runLengthFreqWorkers(groupOptions *group.Options, analysisOptions *analysis.Options) *lengthFreqProcess {
	ch := make(chan length, analysisOptions.BufferSize)
	wg := &sync.WaitGroup{}
	m := make(lengthFreqMap)

	if groupOptions.IsNecessaryToCalcLengthFreq() {
		wg.Add(1)
		go lengthFreqWorker(ch, m, groupOptions, wg)
	}

	return &lengthFreqProcess{
		Input:  ch,
		Output: m,
		wg:     wg,
	}
}

func lengthFreqWorker(ch <-chan length, m lengthFreqMap, options *group.Options, wg *sync.WaitGroup) {
	defer wg.Done()

	var itemN uint = 1

	for v := range ch {
		// Load or create frequency distribution table
		table := m[v.Id]
		if table == nil {
			table = make(commonFreqTable)
			m[v.Id] = table
		}

		table[v.Length]++

		//if itemN == options.MaxItemsForFreqAnalysis {
		//	break
		//}

		itemN++
	}

	for range ch {
	}
}
