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

// Minute encapsulates MongoDB operation $minute.
func Minute(el interface{}) bson.M {
	return bson.M{"$minute": el}
}

// Second encapsulates MongoDB operation $second.
func Second(el interface{}) bson.M {
	return bson.M{"$second": el}
}

// Millisecond encapsulates MongoDB operation $millisecond.
func Millisecond(el interface{}) bson.M {
	return bson.M{"$millisecond": el}
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

// ObjectIdToDate converts objectId value to date
func ObjectIdToDate(id interface{}) interface{} {
	timestamp := Let(
		bson.M{
			"id": id,
		},
		Let(
			bson.M{
				"y":    Year(Var("id")),
				"m":    Month(Var("id")),
				"d":    DayOfMonth(Var("id")),
				"hour": Hour(Var("id")),
				"min":  Minute(Var("id")),
				"sec":  Second(Var("id")),
			},
			// Algorithm: http://howardhinnant.github.io/date_algorithms.html#days_from_civil
			// STEP 1: y -= m <= 2;
			// STEP 2: const Int era = (y >= 0 ? y : y-399) / 400;
			// STEP 3: const unsigned yoe = static_cast<unsigned>(y - era * 400);      // [0, 399]
			// STEP 3: const unsigned doy = (153*(m + (m > 2 ? -3 : 9)) + 2)/5 + d-1;  // [0, 365]
			// STEP 4: const unsigned doe = (yoe * 365) + (yoe/4 - yoe/100) + doy;         // [0, 146096]
			// STEP 5: days = (era * 146097) + (static_cast<Int>(doe) - 719468)
			// STEP 5 : (days * 86400 )+ (hour*3600) + (min*60) + sec;
			Let(
				// STEP 1
				bson.M{
					// y = m <= 2 ? (y-1) : y;
					"y": Cond(Lte(Var("m"), 2), Subtract(Var("y"), 1), Var("y")),
				},
				Let(
					// STEP 2
					bson.M{
						// era =     (        y >= 0 ?            y :                y-399    )/ 400;
						"era": DivideInt(Cond(Gte(Var("y"), 0), Var("y"), Subtract(Var("y"), 399)), 400),
					},
					Let(
						// STEP 3
						bson.M{
							// yoe =       (y -                     era  * 400 )
							"yoe": Subtract(Var("y"), Multiply(Var("era"), 400)),
							// doy = (153*(m + (m > 2 ? -3 : 9)) + 2)/5 + d-1;
							"doy": Add(DivideInt(Add(Multiply(153, Add(Var("m"), Cond(Gt(Var("m"), 2), -3, 9))), 2), 5), Subtract(Var("d"), 1)),
						},
						Let(
							// STEP 4
							bson.M{
								// doe = (yoe * 365) + (yoe/4 - yoe/100) + doy;
								"doe": bson.M{
									"$add": []interface{}{
										// (yoe * 365)
										Multiply(Var("yoe"), 365),
										// (yoe/4 - yoe/100)
										Subtract(DivideInt(Var("yoe"), 4), DivideInt(Var("yoe"), 100)),
										// doy
										Var("doy"),
									},
								},
							},
							// STEP 5
							bson.M{
								"$add": []interface{}{
									// ((era * 146097) + (doe - 719468)) * 86400
									Multiply(Add(Multiply(Var("era"), 146097), Subtract(Var("doe"), 719468)), 86400),
									// (hour*3600)
									Multiply(Var("hour"), 3600),
									// (min*60)
									Multiply(Var("min"), 60),
									// sec
									Var("sec"),
								},
							},
						),
					),
				),
			),
		),
	)

	return TimestampToDate(timestamp)
}
