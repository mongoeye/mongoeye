package expr

import (
	"github.com/mongoeye/mongoeye/helpers"
	"gopkg.in/mgo.v2/bson"
	"time"
)

const dayDuration = 24 * 60 * 60 * 1000

// Year encapsulates MongoDB operation $year.
func Year(el interface{}) bson.M {
	return bson.M{"$year": el}
}

// Month encapsulates MongoDB operation $month.
func Month(el interface{}) bson.M {
	return bson.M{"$month": el}
}

// DayOfMonth encapsulates MongoDB operation $dayOfMonth.
func DayOfMonth(el interface{}) bson.M {
	return bson.M{"$dayOfMonth": el}
}

// DayOfWeek encapsulates MongoDB operation $dayOfWeek.
func DayOfWeek(el interface{}) bson.M {
	return Subtract(bson.M{"$dayOfWeek": el}, 1)
}

// DayOfYear encapsulates MongoDB operation $dayOfYear.
func DayOfYear(el interface{}) bson.M {
	return bson.M{"$dayOfYear": el}
}

// Hour encapsulates MongoDB operation $hour.
func Hour(el interface{}) bson.M {
	return bson.M{"$hour": el}
}

// DateToTimestamp converts date to timestamp in DB.
func DateToTimestamp(date interface{}) bson.M {
	return Divide(
		Subtract(
			date,
			time.Unix(0, 0),
		),
		1000,
	)
}

// TimestampToDate converts timestamp to date in DB.
func TimestampToDate(timestamp interface{}) bson.M {
	return Add(
		time.Unix(0, 0),
		Multiply(
			timestamp,
			1000,
		),
	)
}

// ResetMonthDay converts date to date of the fist day of month
func ResetMonthDay(date interface{}) bson.M {
	return Subtract(
		date,
		Multiply(
			Subtract(DayOfMonth(date), 1),
			dayDuration,
		),
	)
}

// FirstDayOfMonth converts date to first day of given month. The year remains unchanged.
func FirstDayOfMonth(yearDate interface{}, month int) interface{} {
	return ResetMonthDay(
		Subtract(
			yearDate,
			Multiply(
				Subtract(
					DayOfYear(yearDate),
					month*29,
				),
				dayDuration,
			),
		),
	)
}

// NthSundayOfMonth get date of N-th sunday of month
func NthSundayOfMonth(date interface{}, n int, numberOfDays int) bson.M {
	if n > 0 {
		return Add(
			Mod(
				Subtract(
					8,
					DayOfWeek(ResetMonthDay(date)),
				),
				7,
			),
			Multiply(
				7,
				Subtract(
					n,
					1,
				),
			),
		)
	}

	// Last sunday
	return Subtract(
		numberOfDays+1,
		Mod(
			Add(
				numberOfDays,
				DayOfWeek(ResetMonthDay(date)),
			),
			7,
		),
	)
}

// CeilDateSeconds rounds up seconds to whole seconds, minutes, hours, days.
func CeilDateSeconds(step interface{}) bson.M {
	sw := Switch()

	sw.AddBranch(
		Lte(step, 1),
		1,
	)

	// seconds
	sw.AddBranch(
		Lte(step, 60),
		CeilIn60System(step),
	)

	// minutes
	sw.AddBranch(
		Lte(step, 3600),
		Multiply(
			CeilIn60System(
				Divide(
					step,
					60,
				),
			),
			60,
		),
	)

	// hours
	sw.AddBranch(
		Lte(step, 86400),
		Multiply(
			CeilIn24System(
				Divide(
					step,
					3600,
				),
			),
			3600,
		),
	)

	// days
	sw.SetDefault(CeilWithStep(step, 86400))

	return sw.Bson()
}

// DateInTimezone converts date in UTC timezone to desired timezone
func DateInTimezone(date interface{}, timezone *helpers.Timezone) interface{} {
	var offset interface{}

	// Calculate offset winter/summer time
	if timezone.TimeZoneChanging == false {
		if timezone.WinterTimeOffset == 0 {
			return date
		}
		offset = timezone.WinterTimeOffset
	} else {
		sw := Switch()
		// Winter months
		sw.AddBranch(
			Or(
				Lt("$$m", timezone.SummerTimeStartMonth),
				Gt("$$m", timezone.WinterTimeStartMonth),
			),
			timezone.WinterTimeOffset,
		)
		// Summer months
		sw.AddBranch(
			And(
				Gt("$$m", timezone.SummerTimeStartMonth),
				Lt("$$m", timezone.WinterTimeStartMonth),
			),
			timezone.SummerTimeOffset,
		)
		// Winter / summer time month
		sw.AddBranch(
			Eq("$$m", timezone.SummerTimeStartMonth),
			Let(
				bson.M{"border": NthSundayOfMonth(
					FirstDayOfMonth(date, timezone.SummerTimeStartMonth),
					timezone.SummerTimeStartSunday,
					timezone.SummerTimeStartMonthDays,
				),
				},
				Cond(
					Lt(DayOfMonth(date), "$$border"),
					timezone.WinterTimeOffset,
					timezone.SummerTimeOffset,
				),
			),
		)
		// Summer / winter time month
		sw.AddBranch(
			Eq("$$m", timezone.WinterTimeStartMonth),
			Let(
				bson.M{"border": NthSundayOfMonth(
					FirstDayOfMonth(date, timezone.WinterTimeStartMonth),
					timezone.WinterTimeStartSunday,
					timezone.WinterTimeStartMonthDays,
				),
				},
				Cond(
					Lt(DayOfMonth(date), "$$border"),
					timezone.SummerTimeOffset,
					timezone.WinterTimeOffset,
				),
			),
		)

		offset = sw.Bson()
	}

	return Let(
		bson.M{
			"m": Month(date),
		},
		Add(
			date,
			offset,
		),
	)
}
