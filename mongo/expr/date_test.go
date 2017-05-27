package expr

import (
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/mongoeye/mongoeye/tests"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

func TestYear(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"date": helpers.ParseDate("2006-01-02T15:04:05-07:00"),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":  0,
		"year": Year(Field("date")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 2006, out["year"])
}

func TestMonth(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"date": helpers.ParseDate("2012-05-29T15:04:05-07:00"),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":   0,
		"month": Month(Field("date")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 5, out["month"])
}

func TestDayOfWeek(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"sun":  helpers.ParseDate("2017-01-01T00:00:00+00:00"),
		"mon":  helpers.ParseDate("2017-01-02T00:00:00+00:00"),
		"tue":  helpers.ParseDate("2017-01-03T00:00:00+00:00"),
		"wed":  helpers.ParseDate("2017-01-04T00:00:00+00:00"),
		"thu":  helpers.ParseDate("2017-01-05T00:00:00+00:00"),
		"fri":  helpers.ParseDate("2017-01-06T00:00:00+00:00"),
		"sat":  helpers.ParseDate("2017-01-07T00:00:00+00:00"),
		"sun2": helpers.ParseDate("2017-01-01T00:00:00+05:00"), // day of week work with UTC time
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":  0,
		"sun":  DayOfWeek(Field("sun")),
		"mon":  DayOfWeek(Field("mon")),
		"tue":  DayOfWeek(Field("tue")),
		"wed":  DayOfWeek(Field("wed")),
		"thu":  DayOfWeek(Field("thu")),
		"fri":  DayOfWeek(Field("fri")),
		"sat":  DayOfWeek(Field("sat")),
		"sat2": DayOfWeek(Field("sun2")), // day of week work with UTC time
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 0, out["sun"])
	assert.Equal(t, 1, out["mon"])
	assert.Equal(t, 2, out["tue"])
	assert.Equal(t, 3, out["wed"])
	assert.Equal(t, 4, out["thu"])
	assert.Equal(t, 5, out["fri"])
	assert.Equal(t, 6, out["sat"])
	assert.Equal(t, 6, out["sat2"]) // day of week work with UTC time
}

func TestDayOfYear(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"1a": helpers.ParseDate("2017-01-01T00:00:00+00:00"),
		"1b": helpers.ParseDate("2017-01-02T00:00:00+05:00"), // day of year work with UTC time
		"2":  helpers.ParseDate("2017-01-02T00:00:00+00:00"),
		"3":  helpers.ParseDate("2017-01-03T00:00:00+00:00"),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"1a":  DayOfYear(Field("1a")),
		"1b":  DayOfYear(Field("1b")), // day of year work with UTC time
		"2":   DayOfYear(Field("2")),
		"3":   DayOfYear(Field("3")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 1, out["1a"])
	assert.Equal(t, 1, out["1b"])
	assert.Equal(t, 2, out["2"])
	assert.Equal(t, 3, out["3"])
}

func TestHour(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"00": helpers.ParseDate("2017-01-01T00:00:00+00:00"),
		"19": helpers.ParseDate("2017-01-02T00:00:00+05:00"),
		"01": helpers.ParseDate("2017-01-02T01:00:00+00:00"),
		"14": helpers.ParseDate("2017-01-03T14:00:00+00:00"),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"00":  Hour(Field("00")),
		"19":  Hour(Field("19")),
		"01":  Hour(Field("01")),
		"14":  Hour(Field("14")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 0, out["00"])
	assert.Equal(t, 19, out["19"])
	assert.Equal(t, 1, out["01"])
	assert.Equal(t, 14, out["14"])
}

func TestDateToTimestamp(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	c.Insert(bson.M{
		"date": time.Unix(1234567, 0),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":       0,
		"timestamp": DateToTimestamp(Field("date")),
	})

	result := bson.M{}
	pipe := p.GetPipe(c)
	err := pipe.One(&result)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, float64(1234567), result["timestamp"])
}

func TestTimestampToDate(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	c.Insert(bson.M{
		"timestamp": 1234567,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":  0,
		"date": TimestampToDate(Field("timestamp")),
	})

	result := bson.M{}
	pipe := p.GetPipe(c)
	err := pipe.One(&result)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, time.Unix(1234567, 0), result["date"])
}

func TestResetMonthDay(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	c.Insert(bson.M{
		"date": helpers.ParseDate("2017-01-20T05:00:01+01:00"),
	})
	c.Insert(bson.M{
		"date": helpers.ParseDate("2000-04-15T07:00:01+01:00"),
	})
	c.Insert(bson.M{
		"date": helpers.ParseDate("2010-08-15T07:00:01+01:00"),
	})
	c.Insert(bson.M{
		"date": helpers.ParseDate("2014-10-01T07:00:01+01:00"),
	})
	c.Insert(bson.M{
		"date": helpers.ParseDate("2005-11-30T07:00:01+01:00"),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"date":     1,
		"firstDay": ResetMonthDay("$date"),
	})

	results := []bson.M{}
	pipe := p.GetPipe(c)
	err := pipe.All(&results)
	if err != nil {
		panic(err)
	}

	for _, r := range results {
		date := helpers.SafeToDate(r["date"])
		day := helpers.SafeToDate(r["firstDay"])
		assert.Equal(t, date.Month(), day.Month())
		assert.Equal(t, 1, day.Day())
	}
}

func TestFirstDayOfMonth(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	c.Insert(bson.M{
		"date": helpers.ParseDate("2017-03-20T05:00:01+01:00"),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"date": 1,
		"1":    FirstDayOfMonth("$date", 1),
		"2":    FirstDayOfMonth("$date", 2),
		"3":    FirstDayOfMonth("$date", 3),
		"4":    FirstDayOfMonth("$date", 4),
		"10":   FirstDayOfMonth("$date", 10),
		"11":   FirstDayOfMonth("$date", 11),
		"12":   FirstDayOfMonth("$date", 12),
	})

	result := bson.M{}
	pipe := p.GetPipe(c)
	err := pipe.One(&result)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, 1, int(helpers.SafeToDate(result["1"]).Month()))
	assert.Equal(t, 2, int(helpers.SafeToDate(result["2"]).Month()))
	assert.Equal(t, 3, int(helpers.SafeToDate(result["3"]).Month()))
	assert.Equal(t, 4, int(helpers.SafeToDate(result["4"]).Month()))
	assert.Equal(t, 10, int(helpers.SafeToDate(result["10"]).Month()))
	assert.Equal(t, 11, int(helpers.SafeToDate(result["11"]).Month()))
	assert.Equal(t, 12, int(helpers.SafeToDate(result["12"]).Month()))
}

func TestGetNthSundayOfMonth1(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	c.Insert(bson.M{
		"date": helpers.ParseDate("2017-03-20T05:00:01+01:00"),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"date": 1,
		"1":    NthSundayOfMonth("$date", 1, 31),
		"2":    NthSundayOfMonth("$date", 2, 31),
		"3":    NthSundayOfMonth("$date", 3, 31),
		"4":    NthSundayOfMonth("$date", 4, 31),
		"last": NthSundayOfMonth("$date", -1, 31),
	})

	result := bson.M{}
	pipe := p.GetPipe(c)
	err := pipe.One(&result)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, 5, result["1"])
	assert.Equal(t, 12, result["2"])
	assert.Equal(t, 19, result["3"])
	assert.Equal(t, 26, result["4"])
	assert.Equal(t, 26, result["last"])
}

func TestGetNthSundayOfMonth2(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	c.Insert(bson.M{
		"date": helpers.ParseDate("2017-01-20T05:00:01+01:00"),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"date": 1,
		"1":    NthSundayOfMonth("$date", 1, 31),
		"2":    NthSundayOfMonth("$date", 2, 31),
		"3":    NthSundayOfMonth("$date", 3, 31),
		"4":    NthSundayOfMonth("$date", 4, 31),
		"last": NthSundayOfMonth("$date", -1, 31),
	})

	result := bson.M{}
	pipe := p.GetPipe(c)
	err := pipe.One(&result)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, 1, result["1"])
	assert.Equal(t, 8, result["2"])
	assert.Equal(t, 15, result["3"])
	assert.Equal(t, 22, result["4"])
	assert.Equal(t, 29, result["last"])
}

func Test_DateInTimezone_EuropeTimezone(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	start := helpers.ParseDate("2017-01-01T00:00:00+00:00")
	end := start.AddDate(1, 0, 0)

	loc, _ := time.LoadLocation("Europe/Bratislava")
	tz := helpers.GetTimezone(loc)

	for d := start; d.Before(end); d = d.Add(time.Hour / 2) {
		// Skip 2017-03-26, border hours are not yet processed as well
		if d.Year() == 2017 && d.Month() == 3 && d.Day() == 26 {
			continue
		}

		// Skip 2017-11-05, border hours are not yet processed as well
		if d.Year() == 2017 && d.Month() == 10 && d.Day() == 29 {
			continue
		}

		c.Insert(bson.M{
			"date": d,
		})
	}

	p := NewPipeline()

	p.AddStage("project", bson.M{
		"date":  1,
		"local": DateInTimezone("$date", tz),
	})

	results := []bson.M{}

	pipe := p.GetPipe(c)
	err := pipe.All(&results)
	if err != nil {
		panic(err)
	}

	for _, r := range results {
		date := helpers.SafeToDate(r["date"])
		local := helpers.SafeToDate(r["local"])
		offset2 := local.Sub(date).Seconds()
		_, offset1 := date.In(loc).Zone()
		assert.Equal(t, float64(offset1), offset2, date.String()+" - "+local.String())
	}
}

func Test_DateInTimezone_NewYorkTimezone(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	start := helpers.ParseDate("2017-01-01T00:00:00+00:00")
	end := start.AddDate(1, 0, 0)

	loc, _ := time.LoadLocation("America/New_York")
	tz := helpers.GetTimezone(loc)

	for d := start; d.Before(end); d = d.Add(time.Hour / 2) {
		// Skip 2017-03-12, border hours are not yet processed as well
		if d.Year() == 2017 && d.Month() == 3 && d.Day() == 12 {
			continue
		}

		// Skip 2017-11-05, border hours are not yet processed as well
		if d.Year() == 2017 && d.Month() == 11 && d.Day() == 5 {
			continue
		}

		c.Insert(bson.M{
			"date": d,
		})
	}

	p := NewPipeline()

	p.AddStage("project", bson.M{
		"date":  1,
		"local": DateInTimezone("$date", tz),
	})

	results := []bson.M{}

	pipe := p.GetPipe(c)
	err := pipe.All(&results)
	if err != nil {
		panic(err)
	}

	for _, r := range results {
		date := helpers.SafeToDate(r["date"])
		local := helpers.SafeToDate(r["local"])
		offset2 := local.Sub(date).Seconds()
		_, offset1 := date.In(loc).Zone()
		assert.Equal(t, float64(offset1), offset2, date.String()+" - "+local.String())
	}
}
