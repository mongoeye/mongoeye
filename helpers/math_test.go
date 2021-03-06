package helpers

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFloorWithStep(t *testing.T) {
	assert.Equal(t, 0.0, FloorWithStep(0.4, 0.5))
	assert.Equal(t, 0.5, FloorWithStep(0.5, 0.5))
	assert.Equal(t, 0.5, FloorWithStep(0.7, 0.5))
	assert.Equal(t, 1.0, FloorWithStep(1.2, 0.5))
	assert.Equal(t, 1236.625, FloorWithStep(1236.7, 0.125))
	assert.Equal(t, 1236.75, FloorWithStep(1236.75, 0.125))
	assert.Equal(t, 1000.0, FloorWithStep(1236.7, 1000))
	assert.Equal(t, 0.0, FloorWithStep(1236.7, 1500))
	assert.Equal(t, 0.0, FloorWithStep(1236.7, 1500))
	assert.Equal(t, 1500.0, FloorWithStep(1500, 1500))
	assert.Equal(t, 1500.0, FloorWithStep(1500.0001, 1500))
}

func TestCeilWithStep(t *testing.T) {
	assert.Equal(t, 0.5, CeilWithStep(0.4, 0.5))
	assert.Equal(t, 0.5, CeilWithStep(0.5, 0.5))
	assert.Equal(t, 1.0, CeilWithStep(0.7, 0.5))
	assert.Equal(t, 1236.75, CeilWithStep(1236.7, 0.125))
	assert.Equal(t, 1236.75, CeilWithStep(1236.75, 0.125))
	assert.Equal(t, 2000.0, CeilWithStep(1236.7, 1000))
	assert.Equal(t, 1500.0, CeilWithStep(1236.7, 1500))
	assert.Equal(t, 1500.0, CeilWithStep(1236.7, 1500))
	assert.Equal(t, 1500.0, CeilWithStep(1500, 1500))
	assert.Equal(t, 3000.0, CeilWithStep(1500.0001, 1500))
}

func TestCeilIn60System(t *testing.T) {
	assert.Equal(t, 1.0, CeilIn60System(0.5))
	assert.Equal(t, 1.0, CeilIn60System(1.0))
	assert.Equal(t, 2.0, CeilIn60System(1.1))
	assert.Equal(t, 2.0, CeilIn60System(1.9))
	assert.Equal(t, 2.0, CeilIn60System(2.0))
	assert.Equal(t, 5.0, CeilIn60System(2.1))
	assert.Equal(t, 5.0, CeilIn60System(4.9))
	assert.Equal(t, 5.0, CeilIn60System(5.0))
	assert.Equal(t, 10.0, CeilIn60System(5.1))
	assert.Equal(t, 10.0, CeilIn60System(9.9))
	assert.Equal(t, 10.0, CeilIn60System(10.0))
	assert.Equal(t, 15.0, CeilIn60System(10.1))
	assert.Equal(t, 15.0, CeilIn60System(14.9))
	assert.Equal(t, 15.0, CeilIn60System(15.0))
	assert.Equal(t, 30.0, CeilIn60System(15.1))
	assert.Equal(t, 30.0, CeilIn60System(29.9))
	assert.Equal(t, 30.0, CeilIn60System(30.0))
	assert.Equal(t, 60.0, CeilIn60System(30.1))
	assert.Equal(t, 60.0, CeilIn60System(59.9))
	assert.Equal(t, 60.0, CeilIn60System(60.0))
	assert.Equal(t, 60.0, CeilIn60System(112.7))
}

func TestCeilIn24System(t *testing.T) {
	assert.Equal(t, 1.0, CeilIn24System(0.5))
	assert.Equal(t, 1.0, CeilIn24System(1.0))
	assert.Equal(t, 2.0, CeilIn24System(1.1))
	assert.Equal(t, 2.0, CeilIn24System(1.9))
	assert.Equal(t, 2.0, CeilIn24System(2.0))
	assert.Equal(t, 3.0, CeilIn24System(2.1))
	assert.Equal(t, 3.0, CeilIn24System(2.9))
	assert.Equal(t, 3.0, CeilIn24System(3.0))
	assert.Equal(t, 6.0, CeilIn24System(3.1))
	assert.Equal(t, 6.0, CeilIn24System(5.9))
	assert.Equal(t, 6.0, CeilIn24System(6.0))
	assert.Equal(t, 12.0, CeilIn24System(6.1))
	assert.Equal(t, 12.0, CeilIn24System(11.9))
	assert.Equal(t, 12.0, CeilIn24System(12.0))
	assert.Equal(t, 24.0, CeilIn24System(12.1))
	assert.Equal(t, 24.0, CeilIn24System(23.9))
	assert.Equal(t, 24.0, CeilIn24System(24.0))
	assert.Equal(t, 24.0, CeilIn24System(123.45))
}

func TestCeilDateInSeconds(t *testing.T) {
	// Seconds
	assert.Equal(t, float64(1), CeilDateSeconds(0.1))
	assert.Equal(t, float64(1), CeilDateSeconds(1))
	assert.Equal(t, float64(2), CeilDateSeconds(1.1))
	assert.Equal(t, float64(2), CeilDateSeconds(2))
	assert.Equal(t, float64(5), CeilDateSeconds(2.1))
	assert.Equal(t, float64(5), CeilDateSeconds(3))
	assert.Equal(t, float64(5), CeilDateSeconds(5))
	assert.Equal(t, float64(10), CeilDateSeconds(5.1))
	assert.Equal(t, float64(10), CeilDateSeconds(7.5))
	assert.Equal(t, float64(10), CeilDateSeconds(10))
	assert.Equal(t, float64(15), CeilDateSeconds(10.1))
	assert.Equal(t, float64(15), CeilDateSeconds(13.4))
	assert.Equal(t, float64(15), CeilDateSeconds(15))
	assert.Equal(t, float64(30), CeilDateSeconds(15.1))
	assert.Equal(t, float64(30), CeilDateSeconds(20))
	assert.Equal(t, float64(30), CeilDateSeconds(29.9))
	assert.Equal(t, float64(30), CeilDateSeconds(30))
	assert.Equal(t, float64(60), CeilDateSeconds(30.1))
	assert.Equal(t, float64(60), CeilDateSeconds(33))
	assert.Equal(t, float64(60), CeilDateSeconds(45))
	assert.Equal(t, float64(60), CeilDateSeconds(60))
	// Minutes
	assert.Equal(t, float64(2*60), CeilDateSeconds(60.1))
	assert.Equal(t, float64(2*60), CeilDateSeconds(100))
	assert.Equal(t, float64(2*60), CeilDateSeconds(120))
	assert.Equal(t, float64(5*60), CeilDateSeconds(120.1))
	assert.Equal(t, float64(5*60), CeilDateSeconds(200.5))
	assert.Equal(t, float64(5*60), CeilDateSeconds(300))
	assert.Equal(t, float64(10*60), CeilDateSeconds(300.1))
	assert.Equal(t, float64(10*60), CeilDateSeconds(500))
	assert.Equal(t, float64(10*60), CeilDateSeconds(600))
	assert.Equal(t, float64(15*60), CeilDateSeconds(600.1))
	assert.Equal(t, float64(15*60), CeilDateSeconds(700))
	assert.Equal(t, float64(15*60), CeilDateSeconds(900))
	assert.Equal(t, float64(30*60), CeilDateSeconds(900.1))
	assert.Equal(t, float64(30*60), CeilDateSeconds(1200))
	assert.Equal(t, float64(30*60), CeilDateSeconds(1800))
	assert.Equal(t, float64(60*60), CeilDateSeconds(1800.1))
	assert.Equal(t, float64(60*60), CeilDateSeconds(3000))
	assert.Equal(t, float64(60*60), CeilDateSeconds(3600))
	// Hours
	assert.Equal(t, float64(2*60*60), CeilDateSeconds(3600.1))
	assert.Equal(t, float64(2*60*60), CeilDateSeconds(5000))
	assert.Equal(t, float64(2*60*60), CeilDateSeconds(7200))
	assert.Equal(t, float64(3*60*60), CeilDateSeconds(7200.1))
	assert.Equal(t, float64(3*60*60), CeilDateSeconds(10000))
	assert.Equal(t, float64(3*60*60), CeilDateSeconds(10800))
	assert.Equal(t, float64(6*60*60), CeilDateSeconds(10800.1))
	assert.Equal(t, float64(6*60*60), CeilDateSeconds(20000))
	assert.Equal(t, float64(6*60*60), CeilDateSeconds(21600))
	assert.Equal(t, float64(12*60*60), CeilDateSeconds(21600.1))
	assert.Equal(t, float64(12*60*60), CeilDateSeconds(40000))
	assert.Equal(t, float64(12*60*60), CeilDateSeconds(43200))
	assert.Equal(t, float64(24*60*60), CeilDateSeconds(43200.1))
	assert.Equal(t, float64(24*60*60), CeilDateSeconds(80000))
	assert.Equal(t, float64(24*60*60), CeilDateSeconds(86400))
	// Days
	assert.Equal(t, float64(2*24*60*60), CeilDateSeconds(86400.1))
	for d := float64(2); d < float64(100); d++ {
		assert.Equal(t, float64(d*24*60*60), CeilDateSeconds((d-1)*24*60*60+43200))
	}
}
