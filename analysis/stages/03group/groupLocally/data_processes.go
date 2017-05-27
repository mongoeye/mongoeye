package groupLocally

import (
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"sync"
)

type dataProcesses struct {
	valueFreq       *valueFreqProcess
	lengthFreq      *lengthFreqProcess
	dateWeekdayFreq *dateWeekdayFreqProcess
	dateHourFreq    *dateHourFreqProcess
}

func (dp *dataProcesses) closeAllInputs() {
	dp.valueFreq.closeInput()
	dp.lengthFreq.closeInput()
	dp.dateWeekdayFreq.closeInput()
	dp.dateHourFreq.closeInput()
}

func (dp *dataProcesses) wait() {
	dp.valueFreq.wait()
	dp.lengthFreq.wait()
	dp.dateWeekdayFreq.wait()
	dp.dateHourFreq.wait()
}

func (dp *dataProcesses) getFreqTables(id GroupId) *freqTables {
	return &freqTables{
		Value:   dp.valueFreq.Output[id],
		Length:  dp.lengthFreq.Output[id],
		Weekday: dp.dateWeekdayFreq.Output[id],
		Hour:    dp.dateHourFreq.Output[id],
	}
}

type dateHourFreqProcess struct {
	Input  chan value
	Output dateHourFreqMap
	wg     *sync.WaitGroup
}

func (p *dateHourFreqProcess) wait() {
	p.wg.Wait()
}

func (p *dateHourFreqProcess) closeInput() {
	close(p.Input)
}

type dateWeekdayFreqProcess struct {
	Input  chan value
	Output dateWeekdayFreqMap
	wg     *sync.WaitGroup
}

func (p *dateWeekdayFreqProcess) wait() {
	p.wg.Wait()
}

func (p *dateWeekdayFreqProcess) closeInput() {
	close(p.Input)
}

type lengthFreqProcess struct {
	Input  chan length
	Output lengthFreqMap
	wg     *sync.WaitGroup
}

func (p *lengthFreqProcess) wait() {
	p.wg.Wait()
}

func (p *lengthFreqProcess) closeInput() {
	close(p.Input)
}

type valueFreqProcess struct {
	Input  chan value
	Output valueFreqMap
	wg     *sync.WaitGroup
}

func (p *valueFreqProcess) wait() {
	p.wg.Wait()
}

func (p *valueFreqProcess) closeInput() {
	close(p.Input)
}

type groupProcess struct {
	Results []GroupResults
	wg      *sync.WaitGroup
}

func (p *groupProcess) Wait() {
	p.wg.Wait()
}

type mergeProcess struct {
	Output chan group.Result
}

type statsProcess struct {
	wg *sync.WaitGroup
}

func (p *statsProcess) Wait() {
	p.wg.Wait()
}
