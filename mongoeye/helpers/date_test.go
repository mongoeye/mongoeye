package helpers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetTimezone(t *testing.T) {
	Hawaii, _ := time.LoadLocation("US/Hawaii")
	timezoneHawaii := GetTimezone(Hawaii)
	assert.Equal(t, false, timezoneHawaii.TimeZoneChanging)
	assert.Equal(t, -36000000, timezoneHawaii.WinterTimeOffset)
	assert.Equal(t, 0, timezoneHawaii.WinterTimeStartMonth)
	assert.Equal(t, 0, timezoneHawaii.WinterTimeStartSunday)
	assert.Equal(t, 0, timezoneHawaii.SummerTimeOffset)
	assert.Equal(t, 0, timezoneHawaii.SummerTimeStartMonth)
	assert.Equal(t, 0, timezoneHawaii.SummerTimeStartSunday)

	NewYork, _ := time.LoadLocation("America/New_York")
	timezoneNewYork := GetTimezone(NewYork)
	assert.Equal(t, true, timezoneNewYork.TimeZoneChanging)
	assert.Equal(t, -18000000, timezoneNewYork.WinterTimeOffset)
	assert.Equal(t, 11, timezoneNewYork.WinterTimeStartMonth)
	assert.Equal(t, 0, timezoneNewYork.WinterTimeStartSunday)
	assert.Equal(t, -14400000, timezoneNewYork.SummerTimeOffset)
	assert.Equal(t, 3, timezoneNewYork.SummerTimeStartMonth)
	assert.Equal(t, 1, timezoneNewYork.SummerTimeStartSunday)

	Bratislava, _ := time.LoadLocation("Europe/Bratislava")
	timezoneBratislava := GetTimezone(Bratislava)
	assert.Equal(t, true, timezoneBratislava.TimeZoneChanging)
	assert.Equal(t, 3600000, timezoneBratislava.WinterTimeOffset)
	assert.Equal(t, 10, timezoneBratislava.WinterTimeStartMonth)
	assert.Equal(t, LAST_SUNDAY, timezoneBratislava.WinterTimeStartSunday)
	assert.Equal(t, 7200000, timezoneBratislava.SummerTimeOffset)
	assert.Equal(t, 3, timezoneBratislava.SummerTimeStartMonth)
	assert.Equal(t, -1, timezoneBratislava.SummerTimeStartSunday)
}
