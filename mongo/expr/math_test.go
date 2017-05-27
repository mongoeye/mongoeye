package expr

import (
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/mongoeye/mongoeye/tests"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

func TestAdd(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"f1": 10,
		"f2": 123.45,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"sum": Add(Field("f1"), Field("f2")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 133.45, out["sum"])
}

func TestSubtract(t *testing.T) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"f1": 10,
		"f2": 123.45,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":  0,
		"diff": Subtract(Field("f1"), Field("f2")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, -113.45, out["diff"])
}

func TestSubtract_Decimal(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"f1": helpers.ParseDecimal("790.00000000000054"),
		"f2": helpers.ParseDecimal("123.00000000000000"),
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":  0,
		"diff": Subtract(Field("f1"), Field("f2")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, helpers.ParseDecimal("667.00000000000054"), out["diff"])
}

func TestMultiply(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"f1": 10,
		"f2": 123.45,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":    0,
		"result": Multiply(Field("f1"), Field("f2")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 1234.5, out["result"])
}

func TestDivide(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"f1": 123.45,
		"f2": 10,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":    0,
		"result": Divide(Field("f1"), Field("f2")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 12.345, out["result"])
}

func TestLog10(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"f1": 1000,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":    0,
		"result": Log10(Field("f1")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 3.0, out["result"])
}

func TestPow(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"b": 24,
		"e": 3,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":    0,
		"result": Pow(Field("b"), Field("e")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 13824, out["result"])
}

func TestPow10(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"e": 5,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":    0,
		"result": Pow10(Field("e")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 100000, out["result"])
}

func TestLt(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"a": 5,
		"b": 9,
		"c": 9,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"a<b": Lt(Field("a"), Field("b")),
		"b<a": Lt(Field("b"), Field("a")),
		"b<c": Lt(Field("b"), Field("c")),
		"c<b": Lt(Field("c"), Field("b")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, true, out["a<b"])
	assert.Equal(t, false, out["b<a"])
	assert.Equal(t, false, out["b<c"])
	assert.Equal(t, false, out["c<b"])
}

func TestLte(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"a": 5,
		"b": 9,
		"c": 9,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":  0,
		"a<=b": Lte(Field("a"), Field("b")),
		"b<=a": Lte(Field("b"), Field("a")),
		"b<=c": Lte(Field("b"), Field("c")),
		"c<=b": Lte(Field("c"), Field("b")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, true, out["a<=b"])
	assert.Equal(t, false, out["b<=a"])
	assert.Equal(t, true, out["b<=c"])
	assert.Equal(t, true, out["c<=b"])
}

func TestGt(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"a": 5,
		"b": 9,
		"c": 9,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"a>b": Gt(Field("a"), Field("b")),
		"b>a": Gt(Field("b"), Field("a")),
		"b>c": Gt(Field("b"), Field("c")),
		"c>b": Gt(Field("c"), Field("b")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, false, out["a>b"])
	assert.Equal(t, true, out["b>a"])
	assert.Equal(t, false, out["b>c"])
	assert.Equal(t, false, out["c>b"])
}

func TestGte(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"a": 5,
		"b": 9,
		"c": 9,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":  0,
		"a>=b": Gte(Field("a"), Field("b")),
		"b>=a": Gte(Field("b"), Field("a")),
		"b>=c": Gte(Field("b"), Field("c")),
		"c>=b": Gte(Field("c"), Field("b")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, false, out["a>=b"])
	assert.Equal(t, true, out["b>=a"])
	assert.Equal(t, true, out["b>=c"])
	assert.Equal(t, true, out["c>=b"])
}

func TestOr(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"a": true,
		"b": false,
		"c": false,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":     0,
		"a||b||c": Or(Field("a"), Field("b"), Field("c")),
		"a||c":    Or(Field("a"), Field("c")),
		"b||c":    Or(Field("b"), Field("c")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, true, out["a||b||c"])
	assert.Equal(t, true, out["a||c"])
	assert.Equal(t, false, out["b||c"])
}

func TestAnd(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"a": true,
		"b": true,
		"c": false,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":     0,
		"a&&b&&c": And(Field("a"), Field("b"), Field("c")),
		"a&&b":    And(Field("a"), Field("b")),
		"b&&c":    And(Field("b"), Field("c")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, false, out["a&&b&&c"])
	assert.Equal(t, true, out["a&&b"])
	assert.Equal(t, false, out["b&&c"])
}

func TestMod(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"b": 123,
		"d": 4,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":    0,
		"result": Mod(Field("b"), Field("d")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 3, out["result"])
}

func TestFloor(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"a": 99.9,
		"b": 70,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"a":   Floor(Field("a")),
		"b":   Floor(Field("b")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 99.0, out["a"])
	assert.Equal(t, 70, out["b"])
}

func TestFloorWithStep(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"a":  99.9,
		"sa": 0.25,
		"b":  71.9,
		"sb": 0.15,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"a":   FloorWithStep(Field("a"), Field("sa")),
		"b":   FloorWithStep(Field("b"), Field("sb")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 99.75, out["a"])
	assert.Equal(t, 71.85, out["b"])
}

func TestCeil(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"a": 99.9,
		"b": 70,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"a":   Ceil(Field("a")),
		"b":   Ceil(Field("b")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 100.0, out["a"])
	assert.Equal(t, 70, out["b"])
}

func TestCeilWithStep(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"a":  99.9,
		"sa": 0.6,
		"b":  71.4,
		"sb": 0.5,
	})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		"a":   CeilWithStep(Field("a"), Field("sa")),
		"b":   CeilWithStep(Field("b"), Field("sb")),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 100.2, out["a"])
	assert.Equal(t, 71.5, out["b"])
}

func TestCeilIn60System(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":  0,
		"05":   CeilIn60System(0.5),
		"10":   CeilIn60System(1.0),
		"11":   CeilIn60System(1.1),
		"19":   CeilIn60System(1.9),
		"20":   CeilIn60System(2.0),
		"21":   CeilIn60System(2.1),
		"49":   CeilIn60System(4.9),
		"50":   CeilIn60System(5.0),
		"51":   CeilIn60System(5.1),
		"99":   CeilIn60System(9.9),
		"100":  CeilIn60System(10.0),
		"101":  CeilIn60System(10.1),
		"149":  CeilIn60System(14.9),
		"150":  CeilIn60System(15.0),
		"151":  CeilIn60System(15.1),
		"299":  CeilIn60System(29.9),
		"300":  CeilIn60System(30.0),
		"301":  CeilIn60System(30.1),
		"599":  CeilIn60System(59.9),
		"600":  CeilIn60System(60.0),
		"1127": CeilIn60System(112.7),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 1, out["05"])
	assert.Equal(t, 1, out["10"])
	assert.Equal(t, 2, out["11"])
	assert.Equal(t, 2, out["19"])
	assert.Equal(t, 2, out["20"])
	assert.Equal(t, 5, out["21"])
	assert.Equal(t, 5, out["49"])
	assert.Equal(t, 5, out["50"])
	assert.Equal(t, 10, out["51"])
	assert.Equal(t, 10, out["99"])
	assert.Equal(t, 10, out["100"])
	assert.Equal(t, 15, out["101"])
	assert.Equal(t, 15, out["149"])
	assert.Equal(t, 15, out["150"])
	assert.Equal(t, 30, out["151"])
	assert.Equal(t, 30, out["299"])
	assert.Equal(t, 30, out["300"])
	assert.Equal(t, 60, out["301"])
	assert.Equal(t, 60, out["599"])
	assert.Equal(t, 60, out["600"])
	assert.Equal(t, 60, out["1127"])
}

func TestCeilIn24System(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id":   0,
		"05":    CeilIn24System(0.5),
		"10":    CeilIn24System(1.0),
		"11":    CeilIn24System(1.1),
		"19":    CeilIn24System(1.9),
		"20":    CeilIn24System(2.0),
		"21":    CeilIn24System(2.1),
		"29":    CeilIn24System(2.9),
		"30":    CeilIn24System(3.0),
		"31":    CeilIn24System(3.1),
		"59":    CeilIn24System(5.9),
		"60":    CeilIn24System(6.0),
		"61":    CeilIn24System(6.1),
		"119":   CeilIn24System(11.9),
		"120":   CeilIn24System(12.0),
		"121":   CeilIn24System(12.1),
		"239":   CeilIn24System(23.9),
		"240":   CeilIn24System(24.0),
		"12345": CeilIn24System(123.45),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	assert.Equal(t, 1, out["05"])
	assert.Equal(t, 1, out["10"])
	assert.Equal(t, 2, out["11"])
	assert.Equal(t, 2, out["19"])
	assert.Equal(t, 2, out["20"])
	assert.Equal(t, 3, out["21"])
	assert.Equal(t, 3, out["29"])
	assert.Equal(t, 3, out["30"])
	assert.Equal(t, 6, out["31"])
	assert.Equal(t, 6, out["59"])
	assert.Equal(t, 6, out["60"])
	assert.Equal(t, 12, out["61"])
	assert.Equal(t, 12, out["119"])
	assert.Equal(t, 12, out["120"])
	assert.Equal(t, 24, out["121"])
	assert.Equal(t, 24, out["239"])
	assert.Equal(t, 24, out["240"])
	assert.Equal(t, 24, out["12345"])
}

func TestCeilDateInSeconds(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)

	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{})

	p := NewPipeline()
	p.AddStage("project", bson.M{
		"_id": 0,
		// Seconds
		"01":  CeilDateSeconds(0.1),
		"1":   CeilDateSeconds(1),
		"11":  CeilDateSeconds(1.1),
		"2":   CeilDateSeconds(2),
		"21":  CeilDateSeconds(2.1),
		"3":   CeilDateSeconds(3),
		"5":   CeilDateSeconds(5),
		"51":  CeilDateSeconds(5.1),
		"75":  CeilDateSeconds(7.5),
		"10":  CeilDateSeconds(10),
		"101": CeilDateSeconds(10.1),
		"134": CeilDateSeconds(13.4),
		"15":  CeilDateSeconds(15),
		"151": CeilDateSeconds(15.1),
		"20":  CeilDateSeconds(20),
		"299": CeilDateSeconds(29.9),
		"30":  CeilDateSeconds(30),
		"301": CeilDateSeconds(30.1),
		"33":  CeilDateSeconds(33),
		"45":  CeilDateSeconds(45),
		"60":  CeilDateSeconds(60),
		// Minutes
		"601":   CeilDateSeconds(60.1),
		"100":   CeilDateSeconds(100),
		"120":   CeilDateSeconds(120),
		"1201":  CeilDateSeconds(120.1),
		"2005":  CeilDateSeconds(200.5),
		"300":   CeilDateSeconds(300),
		"3001":  CeilDateSeconds(300.1),
		"500":   CeilDateSeconds(500),
		"600":   CeilDateSeconds(600),
		"6001":  CeilDateSeconds(600.1),
		"700":   CeilDateSeconds(700),
		"900":   CeilDateSeconds(900),
		"9001":  CeilDateSeconds(900.1),
		"1200":  CeilDateSeconds(1200),
		"1800":  CeilDateSeconds(1800),
		"18001": CeilDateSeconds(1800.1),
		"3000":  CeilDateSeconds(3000),
		"3600":  CeilDateSeconds(3600),
		// Hours
		"36001":  CeilDateSeconds(3600.1),
		"5000":   CeilDateSeconds(5000),
		"7200":   CeilDateSeconds(7200),
		"72001":  CeilDateSeconds(7200.1),
		"10000":  CeilDateSeconds(10000),
		"10800":  CeilDateSeconds(10800),
		"108001": CeilDateSeconds(10800.1),
		"20000":  CeilDateSeconds(20000),
		"21600":  CeilDateSeconds(21600),
		"216001": CeilDateSeconds(21600.1),
		"40000":  CeilDateSeconds(40000),
		"43200":  CeilDateSeconds(43200),
		"432001": CeilDateSeconds(43200.1),
		"80000":  CeilDateSeconds(80000),
		"86400":  CeilDateSeconds(86400),
		// Days
		"864001": CeilDateSeconds(86400.1),
	})

	out := bson.M{}
	p.GetPipe(c).One(&out)

	// Seconds
	assert.Equal(t, 1, out["01"])
	assert.Equal(t, 1, out["1"])
	assert.Equal(t, 2, out["11"])
	assert.Equal(t, 2, out["2"])
	assert.Equal(t, 5, out["21"])
	assert.Equal(t, 5, out["3"])
	assert.Equal(t, 5, out["5"])
	assert.Equal(t, 10, out["51"])
	assert.Equal(t, 10, out["75"])
	assert.Equal(t, 10, out["10"])
	assert.Equal(t, 15, out["101"])
	assert.Equal(t, 15, out["134"])
	assert.Equal(t, 15, out["15"])
	assert.Equal(t, 30, out["151"])
	assert.Equal(t, 30, out["20"])
	assert.Equal(t, 30, out["299"])
	assert.Equal(t, 30, out["30"])
	assert.Equal(t, 60, out["301"])
	assert.Equal(t, 60, out["33"])
	assert.Equal(t, 60, out["45"])
	assert.Equal(t, 60, out["60"])
	// Minutes
	assert.Equal(t, 2*60, out["601"])
	assert.Equal(t, 2*60, out["100"])
	assert.Equal(t, 2*60, out["120"])
	assert.Equal(t, 5*60, out["1201"])
	assert.Equal(t, 5*60, out["2005"])
	assert.Equal(t, 5*60, out["300"])
	assert.Equal(t, 10*60, out["3001"])
	assert.Equal(t, 10*60, out["500"])
	assert.Equal(t, 10*60, out["600"])
	assert.Equal(t, 15*60, out["6001"])
	assert.Equal(t, 15*60, out["700"])
	assert.Equal(t, 15*60, out["900"])
	assert.Equal(t, 30*60, out["9001"])
	assert.Equal(t, 30*60, out["1200"])
	assert.Equal(t, 30*60, out["1800"])
	assert.Equal(t, 60*60, out["18001"])
	assert.Equal(t, 60*60, out["3000"])
	assert.Equal(t, 60*60, out["3600"])
	// Hours
	assert.Equal(t, 2*60*60, out["36001"])
	assert.Equal(t, 2*60*60, out["5000"])
	assert.Equal(t, 2*60*60, out["7200"])
	assert.Equal(t, 3*60*60, out["72001"])
	assert.Equal(t, 3*60*60, out["10000"])
	assert.Equal(t, 3*60*60, out["10800"])
	assert.Equal(t, 6*60*60, out["108001"])
	assert.Equal(t, 6*60*60, out["20000"])
	assert.Equal(t, 6*60*60, out["21600"])
	assert.Equal(t, 12*60*60, out["216001"])
	assert.Equal(t, 12*60*60, out["40000"])
	assert.Equal(t, 12*60*60, out["43200"])
	assert.Equal(t, 24*60*60, out["432001"])
	assert.Equal(t, 24*60*60, out["80000"])
	assert.Equal(t, 24*60*60, out["86400"])
	// Days
	assert.Equal(t, float64(2*24*60*60), out["864001"])
}
