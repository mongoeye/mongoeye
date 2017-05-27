package groupLocally

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/helpers"
	"sync"
)

func runDateHourFreqWorkers(groupOptions *group.Options, analysisOptions *analysis.Options) *dateHourFreqProcess {
	ch := make(chan value, analysisOptions.BufferSize)
	wg := &sync.WaitGroup{}
	m := make(dateHourFreqMap)

	if groupOptions.StoreHourHistogram {
		wg.Add(1)
		go dateHourFreqWorker(ch, m, wg)
	}

	return &dateHourFreqProcess{
		Input:  ch,
		Output: m,
		wg:     wg,
	}
}

func dateHourFreqWorker(ch <-chan value, m dateHourFreqMap, wg *sync.WaitGroup) {
	defer wg.Done()

	for v := range ch {
		// Load or create frequency distribution table
		table := m[v.Id]
		if table == nil {
			table = make(uIntFreqTable)
			m[v.Id] = table
		}

		table[uint(helpers.SafeToDate(v.Value).Hour())]++
	}
}
