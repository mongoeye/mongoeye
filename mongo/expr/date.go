package expr

import (
	"gopkg.in/mgo.v2/bson"
	"time"
)

func formatDateAndLocation(date interface{}, location *time.Location) interface{} {
	if location == nil {
		return date
	}
	return bson.M{"date": date, "timezone": location.String()}
}

// Year encapsulates MongoDB operation $year.
func Year(el interface{}, location *time.Location) bson.M {
	return bson.M{"$year": formatDateAndLocation(el, location)}
}

// Month encapsulates MongoDB operation $month.
func Month(el interface{}, location *time.Location) bson.M {
	return bson.M{"$month": formatDateAndLocation(el, location)}
}

// DayOfMonth encapsulates MongoDB operation $dayOfMonth.
func DayOfMonth(el interface{}, location *time.Location) bson.M {
	return bson.M{"$dayOfMonth": formatDateAndLocation(el, location)}
}

// DayOfWeek encapsulates MongoDB operation $dayOfWeek.
func DayOfWeek(el interface{}, location *time.Location) bson.M {
	return Subtract(bson.M{"$dayOfWeek": formatDateAndLocation(el, location)}, 1)
}

// DayOfYear encapsulates MongoDB operation $dayOfYear.
func DayOfYear(el interface{}, location *time.Location) bson.M {
	return bson.M{"$dayOfYear": formatDateAndLocation(el, location)}
}

// Hour encapsulates MongoDB operation $hour.
func Hour(el interface{}, location *time.Location) bson.M {
	return bson.M{"$hour": formatDateAndLocation(el, location)}
}

// Minute encapsulates MongoDB operation $minute.
func Minute(el interface{}, location *time.Location) bson.M {
	return bson.M{"$minute": formatDateAndLocation(el, location)}
}

// Second encapsulates MongoDB operation $second.
func Second(el interface{}, location *time.Location) bson.M {
	return bson.M{"$second": formatDateAndLocation(el, location)}
}

// Millisecond encapsulates MongoDB operation $millisecond.
func Millisecond(el interface{}, location *time.Location) bson.M {
	return bson.M{"$millisecond": formatDateAndLocation(el, location)}
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

// ObjectIdToDate converts objectId value to date
func ObjectIdToDate(id interface{}) interface{} {
	timestamp := Let(
		bson.M{
			"id": id,
		},
		Let(
			bson.M{
				"y":    Year(Var("id"), nil),
				"m":    Month(Var("id"), nil),
				"d":    DayOfMonth(Var("id"), nil),
				"hour": Hour(Var("id"), nil),
				"min":  Minute(Var("id"), nil),
				"sec":  Second(Var("id"), nil),
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
