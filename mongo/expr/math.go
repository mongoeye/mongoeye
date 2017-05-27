package expr

import "gopkg.in/mgo.v2/bson"

// Add encapsulates MongoDB operation $add.
func Add(a interface{}, b interface{}) bson.M {
	return bson.M{"$add": []interface{}{a, b}}
}

// Subtract encapsulates MongoDB operation $subtract.
func Subtract(a interface{}, b interface{}) bson.M {
	return bson.M{"$subtract": []interface{}{a, b}}
}

// Multiply encapsulates MongoDB operation $multiply.
func Multiply(a interface{}, b interface{}) bson.M {
	return bson.M{"$multiply": []interface{}{a, b}}
}

// Divide encapsulates MongoDB operation $divide.
func Divide(a interface{}, b interface{}) bson.M {
	return bson.M{"$divide": []interface{}{a, b}}
}

// Log10 encapsulates MongoDB operation $log10.
func Log10(a interface{}) bson.M {
	return bson.M{"$log10": a}
}

// Pow encapsulates MongoDB operation $pow.
func Pow(a interface{}, e interface{}) bson.M {
	return bson.M{"$pow": []interface{}{a, e}}
}

// Pow10 encapsulates MongoDB operation $pow.
func Pow10(e interface{}) bson.M {
	return Let(
		bson.M{
			"exp": e,
		},
		Cond(
			Eq("$$exp", 0),
			1,
			Pow(10, "$$exp"),
		),
	)
}

// Lt encapsulates MongoDB operation $lt.
func Lt(a interface{}, b interface{}) bson.M {
	return bson.M{"$lt": []interface{}{a, b}}
}

// Lte encapsulates MongoDB operation $lte.
func Lte(a interface{}, b interface{}) bson.M {
	return bson.M{"$lte": []interface{}{a, b}}
}

// Gt encapsulates MongoDB operation $gt.
func Gt(a interface{}, b interface{}) bson.M {
	return bson.M{"$gt": []interface{}{a, b}}
}

// Gte encapsulates MongoDB operation $gte.
func Gte(a interface{}, b interface{}) bson.M {
	return bson.M{"$gte": []interface{}{a, b}}
}

// Or encapsulates MongoDB operation $or.
func Or(items ...interface{}) bson.M {
	return bson.M{"$or": items}
}

// And encapsulates MongoDB operation $and
func And(items ...interface{}) bson.M {
	return bson.M{"$and": items}
}

// Mod encapsulates MongoDB operation $mod.
func Mod(a interface{}, b interface{}) bson.M {
	return bson.M{"$mod": []interface{}{a, b}}
}

// Floor encapsulates MongoDB operation $floor.
func Floor(input interface{}) bson.M {
	return bson.M{"$floor": input}
}

// FloorWithStep - floor value with given step.
func FloorWithStep(input interface{}, step interface{}) bson.M {
	return Multiply(
		Floor(
			Divide(
				input,
				step,
			),
		),
		step,
	)
}

// Ceil encapsulates MongoDB operation $ceil.
func Ceil(input interface{}) bson.M {
	return bson.M{"$ceil": input}
}

// CeilWithStep - ceil value with given step.
func CeilWithStep(input interface{}, step interface{}) bson.M {
	return Multiply(
		Ceil(
			Divide(
				input,
				step,
			),
		),
		step,
	)
}

// CeilIn60System - 1, 2, 5, 10, 15, 30, 60
func CeilIn60System(input interface{}) bson.M {
	sw := Switch()

	sw.AddBranch(
		Lte(input, 1),
		1,
	)

	sw.AddBranch(
		Lte(input, 2),
		2,
	)

	sw.AddBranch(
		Lte(input, 5),
		5,
	)

	sw.AddBranch(
		Lte(input, 10),
		10,
	)

	sw.AddBranch(
		Lte(input, 15),
		15,
	)

	sw.AddBranch(
		Lte(input, 30),
		30,
	)

	sw.SetDefault(60)

	return sw.Bson()
}

// CeilIn24System - 1, 2, 3, 6, 12, 24
func CeilIn24System(input interface{}) bson.M {
	sw := Switch()

	sw.AddBranch(
		Lte(input, 1),
		1,
	)

	sw.AddBranch(
		Lte(input, 2),
		2,
	)

	sw.AddBranch(
		Lte(input, 3),
		3,
	)

	sw.AddBranch(
		Lte(input, 6),
		6,
	)

	sw.AddBranch(
		Lte(input, 12),
		12,
	)

	sw.SetDefault(24)

	return sw.Bson()
}
