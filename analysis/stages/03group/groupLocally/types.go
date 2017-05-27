package groupLocally

import (
	"fmt"
	"github.com/mongoeye/mongoeye/helpers"
	"sort"
)

// GroupResults are partial results from one go routine.
type GroupResults map[GroupId]*Accumulator

// Value os structure for value frequency distribution calculation.
type value struct {
	Id    GroupId
	Value interface{}
}

// Length is structure for length frequency distribution calculation.
type length struct {
	Id     GroupId
	Length uint
}

// Table of frequency distribution: KEY represents examined value and VALUE represents number of occurrences.
type commonFreqTable map[interface{}]count
type uIntFreqTable map[uint]count
type count uint

// Frequency distribution tables.
type freqTables struct {
	Value   commonFreqTable
	Length  commonFreqTable
	Weekday uIntFreqTable
	Hour    uIntFreqTable
}

// Frequency distribution maps.
type valueFreqMap map[GroupId]commonFreqTable
type lengthFreqMap map[GroupId]commonFreqTable
type dateWeekdayFreqMap map[GroupId]uIntFreqTable
type dateHourFreqMap map[GroupId]uIntFreqTable

// SortedFreqTable allows sorting of CommonFreqTable by count.
// Items with the same count are sorted by key.
type sortedFreqTable struct {
	freqTable commonFreqTable
	keySort   []interface{}
}

func (table *sortedFreqTable) Keys() []interface{} { return table.keySort }
func (table *sortedFreqTable) Len() int            { return len(table.freqTable) }
func (table *sortedFreqTable) Less(i, j int) (out bool) {
	defer func() {
		// If it failed to sort items as double,
		// then are sorted as a strings.
		if recover() != nil {
			strI := fmt.Sprint(table.keySort[i])
			strJ := fmt.Sprint(table.keySort[j])
			out = strI < strJ
		}
	}()

	countI := table.freqTable[table.keySort[i]]
	countJ := table.freqTable[table.keySort[j]]

	// If same count then are items sorted by key
	if countI == countJ {
		return helpers.ToDouble(table.keySort[i]) < helpers.ToDouble(table.keySort[j])
	}

	return countI > countJ
}
func (table *sortedFreqTable) Swap(i, j int) {
	table.keySort[i], table.keySort[j] = table.keySort[j], table.keySort[i]
}

func newSortedFreqTable(m commonFreqTable) *sortedFreqTable {
	table := new(sortedFreqTable)
	table.freqTable = m
	table.keySort = make([]interface{}, len(m))

	i := 0
	for key := range m {
		table.keySort[i] = key
		i++
	}

	sort.Sort(table)

	return table
}
