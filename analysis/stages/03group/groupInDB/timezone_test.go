package groupInDB

import (
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"github.com/mongoeye/mongoeye/tests"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

func Test_ConvertDateToTimezone_UTCTimezone(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	start := helpers.ParseDate("2017-01-01T00:00:00+00:00")
	end := start.AddDate(1, 0, 0)

	loc := time.UTC

	bulk := c.Bulk()

	for d := start; d.Before(end); d = d.Add(time.Hour / 2) {
		// Skip 2017-03-26, border hours are not yet processed as well
		if d.Year() == 2017 && d.Month() == 3 && d.Day() == 26 {
			continue
		}

		// Skip 2017-11-05, border hours are not yet processed as well
		if d.Year() == 2017 && d.Month() == 10 && d.Day() == 29 {
			continue
		}

		bulk.Insert(bson.M{
			"date": d,
		})
	}

	bulk.Run()

	p := expr.NewPipeline()

	p.AddStage("project", bson.M{
		"date":    1,
		"convert": ConvertDateToTimezone("$date", loc),
	})

	results := []bson.M{}

	pipe := p.GetPipe(c)
	err := pipe.All(&results)
	if err != nil {
		panic(err)
	}

	for _, r := range results {
		date := helpers.SafeToDate(r["date"])
		local := helpers.SafeToDate(r["convert"])
		offset2 := local.Sub(date).Seconds()
		_, offset1 := date.In(loc).Zone()
		assert.Equal(t, float64(offset1), offset2, date.String()+" - "+local.String())
	}
}

func Test_ConvertDateToTimezone_EuropeTimezone(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	start := helpers.ParseDate("2017-01-01T00:00:00+00:00")
	end := start.AddDate(1, 0, 0)

	loc, _ := time.LoadLocation("Europe/Bratislava")

	bulk := c.Bulk()

	for d := start; d.Before(end); d = d.Add(time.Hour / 2) {
		// Skip 2017-03-26, border hours are not yet processed as well
		if d.Year() == 2017 && d.Month() == 3 && d.Day() == 26 {
			continue
		}

		// Skip 2017-11-05, border hours are not yet processed as well
		if d.Year() == 2017 && d.Month() == 10 && d.Day() == 29 {
			continue
		}

		bulk.Insert(bson.M{
			"date": d,
		})
	}

	bulk.Run()

	p := expr.NewPipeline()

	p.AddStage("project", bson.M{
		"date":    1,
		"convert": ConvertDateToTimezone("$date", loc),
	})

	results := []bson.M{}

	pipe := p.GetPipe(c)
	err := pipe.All(&results)
	if err != nil {
		panic(err)
	}

	for _, r := range results {
		date := helpers.SafeToDate(r["date"])
		local := helpers.SafeToDate(r["convert"])
		offset2 := local.Sub(date).Seconds()
		_, offset1 := date.In(loc).Zone()
		assert.Equal(t, float64(offset1), offset2, date.String()+" - "+local.String())
	}
}

func Test_ConvertDateToTimezone_NewYorkTimezone(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	start := helpers.ParseDate("2017-01-01T00:00:00+00:00")
	end := start.AddDate(1, 0, 0)

	loc, _ := time.LoadLocation("America/New_York")

	bulk := c.Bulk()

	for d := start; d.Before(end); d = d.Add(time.Hour / 2) {
		// Skip 2017-03-12, border hours are not yet processed as well
		if d.Year() == 2017 && d.Month() == 3 && d.Day() == 12 {
			continue
		}

		// Skip 2017-11-05, border hours are not yet processed as well
		if d.Year() == 2017 && d.Month() == 11 && d.Day() == 5 {
			continue
		}

		bulk.Insert(bson.M{
			"date": d,
		})
	}

	bulk.Run()

	p := expr.NewPipeline()

	p.AddStage("project", bson.M{
		"date":    1,
		"convert": ConvertDateToTimezone("$date", loc),
	})

	results := []bson.M{}

	pipe := p.GetPipe(c)
	err := pipe.All(&results)
	if err != nil {
		panic(err)
	}

	for _, r := range results {
		date := helpers.SafeToDate(r["date"])
		local := helpers.SafeToDate(r["convert"])
		offset2 := local.Sub(date).Seconds()
		_, offset1 := date.In(loc).Zone()
		assert.Equal(t, float64(offset1), offset2, date.String()+" - "+local.String())
	}
}

// Europe/Istanbul: same time in winter and summer
func Test_ConvertDateToTimezone_IstanbulTimezone(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	start := helpers.ParseDate("2017-01-01T00:00:00+00:00")
	end := start.AddDate(1, 0, 0)

	loc, _ := time.LoadLocation("Europe/Istanbul")

	bulk := c.Bulk()

	for d := start; d.Before(end); d = d.Add(time.Hour / 2) {
		// Skip 2017-03-12, border hours are not yet processed as well
		if d.Year() == 2017 && d.Month() == 3 && d.Day() == 12 {
			continue
		}

		// Skip 2017-11-05, border hours are not yet processed as well
		if d.Year() == 2017 && d.Month() == 11 && d.Day() == 5 {
			continue
		}

		bulk.Insert(bson.M{
			"date": d,
		})
	}

	bulk.Run()

	p := expr.NewPipeline()

	p.AddStage("project", bson.M{
		"date":    1,
		"convert": ConvertDateToTimezone("$date", loc),
	})

	results := []bson.M{}

	pipe := p.GetPipe(c)
	err := pipe.All(&results)
	if err != nil {
		panic(err)
	}

	for _, r := range results {
		date := helpers.SafeToDate(r["date"])
		local := helpers.SafeToDate(r["convert"])
		offset2 := local.Sub(date).Seconds()
		_, offset1 := date.In(loc).Zone()
		assert.Equal(t, float64(offset1), offset2, date.String()+" - "+local.String())
	}
}
