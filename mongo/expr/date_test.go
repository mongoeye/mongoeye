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
		"year": Year(Field("date"), nil),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 2006, out["year"])
}

func TestYear_ObjectId(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.NewObjectIdWithTime(helpers.ParseDate("2006-01-02T15:04:05-07:00")),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"year": Year(Field("_id"), nil),
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
		"month": Month(Field("date"), nil),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 5, out["month"])
}

func TestMonth_ObjectId(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.NewObjectIdWithTime(helpers.ParseDate("2012-05-29T15:04:05-07:00")),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"month": Month(Field("_id"), nil),
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
		"sun2": helpers.ParseDate("2017-01-01T00:00:00+05:00"),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":  0,
		"sun":  DayOfWeek(Field("sun"), nil),
		"mon":  DayOfWeek(Field("mon"), nil),
		"tue":  DayOfWeek(Field("tue"), nil),
		"wed":  DayOfWeek(Field("wed"), nil),
		"thu":  DayOfWeek(Field("thu"), nil),
		"fri":  DayOfWeek(Field("fri"), nil),
		"sat":  DayOfWeek(Field("sat"), nil),
		"sat2": DayOfWeek(Field("sun2"), nil),
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
	assert.Equal(t, 6, out["sat2"])
}

func TestDayOfWeekLocation(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	locationNY, err := time.LoadLocation("America/New_York")
	if err != nil {
		panic(err)
	}

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"sun":  helpers.ParseDate("2017-01-01T05:00:00+00:00"),
		"mon":  helpers.ParseDate("2017-01-02T00:00:00+00:00"),
		"tue":  helpers.ParseDate("2017-01-03T10:00:00+00:00"),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":  0,
		"sun":   DayOfWeek(Field("sun"), locationNY), // - 05:00
		"sun2":  DayOfWeek(Field("mon"), locationNY), // - 05:00
		"tue":   DayOfWeek(Field("tue"), locationNY), // - 05:00
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 0, out["sun"])
	assert.Equal(t, 0, out["sun2"])
	assert.Equal(t, 2, out["tue"])
}

func TestDayOfWeek_ObjectId(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"sun":  bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-01T00:00:00+00:00")),
		"mon":  bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-02T00:00:00+00:00")),
		"tue":  bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-03T00:00:00+00:00")),
		"wed":  bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-04T00:00:00+00:00")),
		"thu":  bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-05T00:00:00+00:00")),
		"fri":  bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-06T00:00:00+00:00")),
		"sat":  bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-07T00:00:00+00:00")),
		"sun2": bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-01T00:00:00+05:00")), // day of week work with UTC time
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":  0,
		"sun":  DayOfWeek(Field("sun"), nil),
		"mon":  DayOfWeek(Field("mon"), nil),
		"tue":  DayOfWeek(Field("tue"), nil),
		"wed":  DayOfWeek(Field("wed"), nil),
		"thu":  DayOfWeek(Field("thu"), nil),
		"fri":  DayOfWeek(Field("fri"), nil),
		"sat":  DayOfWeek(Field("sat"), nil),
		"sat2": DayOfWeek(Field("sun2"), nil), // day of week work with UTC time
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
		"1a":  DayOfYear(Field("1a"), nil),
		"1b":  DayOfYear(Field("1b"), nil), // day of year work with UTC time
		"2":   DayOfYear(Field("2"), nil),
		"3":   DayOfYear(Field("3"), nil),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 1, out["1a"])
	assert.Equal(t, 1, out["1b"])
	assert.Equal(t, 2, out["2"])
	assert.Equal(t, 3, out["3"])
}

func TestDayOfYear_ObjectId(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"1a": bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-01T00:00:00+00:00")),
		"1b": bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-02T00:00:00+05:00")), // day of year work with UTC time
		"2":  bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-02T00:00:00+00:00")),
		"3":  bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-03T00:00:00+00:00")),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"1a":  DayOfYear(Field("1a"), nil),
		"1b":  DayOfYear(Field("1b"), nil), // day of year work with UTC time
		"2":   DayOfYear(Field("2"), nil),
		"3":   DayOfYear(Field("3"), nil),
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
		"00":  Hour(Field("00"), nil),
		"19":  Hour(Field("19"), nil),
		"01":  Hour(Field("01"), nil),
		"14":  Hour(Field("14"), nil),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 0, out["00"])
	assert.Equal(t, 19, out["19"])
	assert.Equal(t, 1, out["01"])
	assert.Equal(t, 14, out["14"])
}

func TestHourLocation(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	locationNY, err := time.LoadLocation("America/New_York")
	if err != nil {
		panic(err)
	}

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"00": helpers.ParseDate("2017-01-01T00:00:00+00:00"),
		"19": helpers.ParseDate("2017-01-02T00:00:00+05:00"),
		"01": helpers.ParseDate("2017-01-02T01:00:00+00:00"),
		"14": helpers.ParseDate("2017-01-03T14:00:00+00:00"),
		"10": helpers.ParseDate("2017-07-10T10:00:00+00:00"),
		"12": helpers.ParseDate("2017-07-15T12:00:00+00:00"),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"19":  Hour(Field("00"), locationNY), // - 05:00
		"14":  Hour(Field("19"), locationNY), // - 05:00
		"20":  Hour(Field("01"), locationNY), // - 05:00
		"9":   Hour(Field("14"), locationNY), // - 05:00
		"6":   Hour(Field("10"), locationNY), // - 04:00
		"8":   Hour(Field("12"), locationNY), // - 04:00
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 19, out["19"])
	assert.Equal(t, 14, out["14"])
	assert.Equal(t, 20, out["20"])
	assert.Equal(t, 9,  out["9"])
	assert.Equal(t, 6,  out["6"])
	assert.Equal(t, 8,  out["8"])
}

func TestHour_ObjectId(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"00": bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-01T00:00:00+00:00")),
		"19": bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-02T00:00:00+05:00")),
		"01": bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-02T01:00:00+00:00")),
		"14": bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-03T14:00:00+00:00")),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"00":  Hour(Field("00"), nil),
		"19":  Hour(Field("19"), nil),
		"01":  Hour(Field("01"), nil),
		"14":  Hour(Field("14"), nil),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 0, out["00"])
	assert.Equal(t, 19, out["19"])
	assert.Equal(t, 1, out["01"])
	assert.Equal(t, 14, out["14"])
}

func TestMinute(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"12": helpers.ParseDate("2017-01-01T00:12:00+00:00"),
		"48": helpers.ParseDate("2017-01-02T00:48:00+05:00"),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"12":  Minute(Field("12"), nil),
		"48":  Minute(Field("48"), nil),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 12, out["12"])
	assert.Equal(t, 48, out["48"])
}

func TestMinute_ObjectId(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"12": bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-01T00:12:00+00:00")),
		"48": bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-02T00:48:00+05:00")),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"12":  Minute(Field("12"), nil),
		"48":  Minute(Field("48"), nil),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 12, out["12"])
	assert.Equal(t, 48, out["48"])
}

func TestSecond(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"17": helpers.ParseDate("2017-01-01T00:00:17+00:00"),
		"51": helpers.ParseDate("2017-01-02T00:00:51+05:00"),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"17":  Second(Field("17"), nil),
		"51":  Second(Field("51"), nil),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 17, out["17"])
	assert.Equal(t, 51, out["51"])
}

func TestSecond_ObjectId(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"17": bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-01T00:00:17+00:00")),
		"51": bson.NewObjectIdWithTime(helpers.ParseDate("2017-01-02T00:00:51+05:00")),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"17":  Second(Field("17"), nil),
		"51":  Second(Field("51"), nil),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 17, out["17"])
	assert.Equal(t, 51, out["51"])
}

func TestMillisecond(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	date1 := helpers.ParseDate("2017-01-01T00:00:17+00:00")
	date1 = date1.Add(time.Millisecond * 123)
	date2 := helpers.ParseDate("2017-01-01T00:00:17+00:00")
	date2 = date2.Add(time.Millisecond * 456)

	c.Insert(bson.M{
		"123": date1,
		"456": date2,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"123": Millisecond(Field("123"), nil),
		"456": Millisecond(Field("456"), nil),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 123, out["123"])
	assert.Equal(t, 456, out["456"])
}

func TestMillisecond_ObjectId(t *testing.T) {
	t.Skip("ObjectId doesn't support milliseconds.")

	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	date1 := helpers.ParseDate("2017-01-01T00:00:17+00:00")
	date1 = date1.Add(time.Millisecond * 123)
	date2 := helpers.ParseDate("2017-01-01T00:00:17+00:00")
	date2 = date2.Add(time.Millisecond * 456)

	c.Insert(bson.M{
		"123": bson.NewObjectIdWithTime(date1),
		"456": bson.NewObjectIdWithTime(date2),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"123": Millisecond(Field("123"), nil),
		"456": Millisecond(Field("456"), nil),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 123, out["123"])
	assert.Equal(t, 456, out["456"])
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

func TestObjectIdToDate(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	date1 := helpers.ParseDate("2006-01-02T15:04:05-07:00")
	date2 := helpers.ParseDate("1971-10-25T02:14:25-04:00")

	c.Insert(bson.M{
		"id1":   bson.NewObjectIdWithTime(date1),
		"date1": date1,
		"id2":   bson.NewObjectIdWithTime(date2),
		"date2": date2,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"id1date": ObjectIdToDate(Field("id1")),
		"date1":   Field("date1"),
		"id2date": ObjectIdToDate(Field("id2")),
		"date2":   Field("date2"),
	})

	out := bson.M{}
	err := p.GetPipe(c).One(&out)

	assert.Equal(t, nil, err)
	assert.Equal(t, date1.Unix(), helpers.SafeToDate(out["date1"]).Unix())
	assert.Equal(t, date1.Unix(), helpers.SafeToDate(out["id1date"]).Unix())
	assert.Equal(t, date2.Unix(), helpers.SafeToDate(out["date2"]).Unix())
	assert.Equal(t, date2.Unix(), helpers.SafeToDate(out["id2date"]).Unix())
}
