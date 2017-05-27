package groupLocally

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/helpers"
	"sync"
)

func runDateWeekdayFreqWorkers(groupOptions *group.Options, analysisOptions *analysis.Options) *dateWeekdayFreqProcess {
	ch := make(chan value, analysisOptions.BufferSize)
	wg := &sync.WaitGroup{}
	m := make(dateWeekdayFreqMap)

	if groupOptions.StoreWeekdayHistogram {
		wg.Add(1)
		go dateWeekdayFreqWorker(ch, m, wg)
	}

	return &dateWeekdayFreqProcess{
		Input:  ch,
		Output: m,
		wg:     wg,
	}
}

func dateWeekdayFreqWorker(ch <-chan value, m dateWeekdayFreqMap, wg *sync.WaitGroup) {
	defer wg.Done()

	for v := range ch {
		// Load or create frequency distribution table
		table := m[v.Id]
		if table == nil {
			table = make(uIntFreqTable)
			m[v.Id] = table
		}

		table[uint(helpers.SafeToDate(v.Value).Weekday())]++
	}
}
