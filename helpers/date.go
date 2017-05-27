package helpers

import (
	"fmt"
	"strconv"
	"time"
)

// LAST_SUNDAY represents last sunday in month.
const LAST_SUNDAY = -1

// Timezone struct represents information about timezone.
type Timezone struct {
	TimeZoneChanging         bool
	WinterTimeOffset         int
	WinterTimeStartDate      time.Time
	WinterTimeStartMonth     int
	WinterTimeStartMonthDays int
	WinterTimeStartSunday    int // -1 means last sunday of month
	SummerTimeOffset         int
	SummerTimeStartDate      time.Time
	SummerTimeStartMonth     int
	SummerTimeStartMonthDays int
	SummerTimeStartSunday    int // -1 means last sunday of month
}

// GetTimezone returns information about given timezone.
func GetTimezone(location *time.Location) *Timezone {
	tz := &Timezone{
		TimeZoneChanging: false,
	}

	yearInt := time.Now().Year()
	year := strconv.Itoa(yearInt)
	start := ParseDate(fmt.Sprintf("%s-01-01T00:00:01+00:00", year)).In(location)
	end := start.AddDate(1, 0, 0)

	_, tz.WinterTimeOffset = start.Zone()

	var d time.Time = start

	// Find day of change winter time => summer time
	for ; d.Before(end); d = d.AddDate(0, 0, 1) {
		_, offset := d.Zone()
		if offset != tz.WinterTimeOffset {
			tz.TimeZoneChanging = true
			tz.SummerTimeOffset = offset
			tz.SummerTimeStartMonth = int(d.Month())

			monthStart := d.AddDate(0, 0, -(d.Day() - 1))
			tz.SummerTimeStartDate = monthStart
			tz.SummerTimeStartSunday = (d.YearDay() - monthStart.YearDay() + int(monthStart.Weekday())) / 7
			if tz.SummerTimeStartSunday > 2 {
				tz.SummerTimeStartSunday = LAST_SUNDAY // last sunday
			}

			break
		}
	}

	// Find day of change summer time => winter time
	for ; d.Before(end); d = d.AddDate(0, 0, 1) {
		_, offset := d.Zone()
		if offset != tz.SummerTimeOffset {
			tz.WinterTimeStartMonth = int(d.Month())

			monthStart := d.AddDate(0, 0, -(d.Day() - 1))
			tz.WinterTimeStartDate = monthStart
			tz.WinterTimeStartSunday = (d.YearDay() - monthStart.YearDay() + int(monthStart.Weekday())) / 7
			if tz.WinterTimeStartSunday > 2 {
				tz.WinterTimeStartSunday = LAST_SUNDAY // last sunday
			}

			break
		}
	}

	tz.WinterTimeStartMonthDays = int(time.Date(yearInt, time.Month(tz.WinterTimeStartMonth)+1, 0, 0, 0, 0, 0, location).Day())
	tz.WinterTimeOffset *= 1000
	tz.SummerTimeStartMonthDays = int(time.Date(yearInt, time.Month(tz.SummerTimeStartMonth)+1, 0, 0, 0, 0, 0, location).Day())
	tz.SummerTimeOffset *= 1000

	return tz
}
