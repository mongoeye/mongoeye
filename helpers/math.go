package helpers

import (
	"math"
)

// FloorWithStep - floor value with given step.
func FloorWithStep(input float64, step float64) float64 {
	return (math.Floor(input / step)) * step
}

// CeilWithStep - ceil value with given step.
func CeilWithStep(input float64, step float64) float64 {
	return (math.Ceil(input / step)) * step
}

// CeilIn60System - 1, 2, 5, 10, 15, 30
func CeilIn60System(input float64) float64 {
	if input <= 1 {
		return 1
	} else if input <= 2 {
		return 2
	} else if input <= 5 {
		return 5
	} else if input <= 10 {
		return 10
	} else if input <= 15 {
		return 15
	} else if input <= 30 {
		return 30
	}
	return 60
}

// CeilIn24System - 1, 2, 3, 6, 12, 24
func CeilIn24System(input float64) float64 {
	if input <= 1 {
		return 1
	} else if input <= 2 {
		return 2
	} else if input <= 3 {
		return 3
	} else if input <= 6 {
		return 6
	} else if input <= 12 {
		return 12
	}
	return 24
}

// CeilDateSeconds rounds up seconds to whole seconds, minutes, hours, days.
func CeilDateSeconds(step float64) float64 {
	if step <= 1 {
		return 1
	} else if step <= 60 {
		// seconds
		return CeilIn60System(step)
	} else if step <= 3600 {
		// minutes
		return CeilIn60System(step/60) * 60
	} else if step <= 86400 {
		// hours
		return CeilIn24System(step/3600) * 3600
	}

	// days
	return CeilWithStep(step, 86400)
}
