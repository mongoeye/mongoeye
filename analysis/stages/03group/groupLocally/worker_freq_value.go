package groupLocally

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"sync"
)

func runValueFreqWorkers(groupOptions *group.Options, analysisOptions *analysis.Options) *valueFreqProcess {
	ch := make(chan value, analysisOptions.BufferSize)
	wg := &sync.WaitGroup{}
	m := make(valueFreqMap)

	if groupOptions.IsNecessaryToCalcValueFreq() {
		wg.Add(1)
		go valueFreqWorker(ch, m, wg)
	}

	return &valueFreqProcess{
		Input:  ch,
		Output: m,
		wg:     wg,
	}
}

func valueFreqWorker(ch <-chan value, m valueFreqMap, wg *sync.WaitGroup) {
	defer wg.Done()

	for v := range ch {
		// Load or create frequency distribution table
		table := m[v.Id]
		if table == nil {
			table = make(commonFreqTable)
			m[v.Id] = table
		}

		table[v.Value]++
	}
}
