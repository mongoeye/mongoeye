package tests

import (
	"github.com/deckarep/golang-set"
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/stretchr/testify/assert"
	"testing"
)

// AssertEqualSet is helper function that compare two slices  regardless of order.
func AssertEqualSet(t *testing.T, expected []interface{}, actual []interface{}) {
	setE := mapset.NewSet()
	setA := mapset.NewSet()

	expectedMap := make(map[uint32]interface{})
	actualMap := make(map[uint32]interface{})

	for _, e := range expected {
		hash := helpers.HashData(e)
		for {
			if _, ok := expectedMap[hash]; !ok {
				break
			}
			hash++
		}
		expectedMap[hash] = e
		setE.Add(hash)
	}

	for _, a := range actual {
		hash := helpers.HashData(a)
		for {
			if _, ok := actualMap[hash]; !ok {
				break
			}
			hash++
		}
		actualMap[hash] = a
		setA.Add(hash)
	}

	diffE := setE.Difference(setA)
	for _, h := range diffE.ToSlice() {
		if hash, ok := h.(uint32); ok {
			assert.Fail(t, "Missing:", helpers.DataToString(expectedMap[hash]))
		} else {
			panic("Unexpected type.")
		}
	}

	diffA := setA.Difference(setE)
	for _, h := range diffA.ToSlice() {
		if hash, ok := h.(uint32); ok {
			assert.Fail(t, "Unexpected:", helpers.DataToString(actualMap[hash]))
		} else {
			panic("Unexpected type.")
		}
	}

	if diffE.Cardinality() != 0 || diffA.Cardinality() != 0 {
		assert.Fail(t, "Not equal. Given: ", helpers.DataToString(actual))
	}
}
