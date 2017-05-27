package groupInDB

import (
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"time"
)

// ConvertDateToTimezone converts date to desired timezone
func ConvertDateToTimezone(date interface{}, location *time.Location) interface{} {
	// Convert dates to specified timezone
	if location != time.UTC {
		timezone := helpers.GetTimezone(location)
		if !timezone.TimeZoneChanging {
			return expr.Add(
				date,
				timezone.WinterTimeOffset,
			)
		}
		return expr.DateInTimezone(date, timezone)
	}

	return date
}
