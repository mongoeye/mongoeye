package tests

import "testing"

var aggregationAlgorithmMinVersion = []int{3, 5, 10}

// HasMongoDBSampleStageSupport returns true if MongoDB support $sample aggregation.
func HasMongoDBSampleStageSupport() bool {
	// Decimal type is new in version 3.4
	return TestDbInfo.VersionAtLeast(3, 2)
}

// HasMongoDBDecimalSupport returns true if MongoDB support decimal type.
func HasMongoDBDecimalSupport() bool {
	// Decimal type is new in version 3.4
	return TestDbInfo.VersionAtLeast(3, 4)
}

// IsMongoDBVersionOld return true if MongoDB version don't support all features.
func IsMongoDBVersionOld() bool {
	return !TestDbInfo.VersionAtLeast(aggregationAlgorithmMinVersion...)
}

// SkipTIfNotSupportAggregationAlgorithm skip test if MongoDB version don't support all features.
func SkipTIfNotSupportAggregationAlgorithm(t *testing.T) {
	if IsMongoDBVersionOld() {
		t.Skip("A newer version of the database is needed.")
	}
}

// SkipBIfNotSupportAggregationAlgorithm skip benchmark if MongoDB version don't support all features.
func SkipBIfNotSupportAggregationAlgorithm(b *testing.B) {
	if IsMongoDBVersionOld() {
		b.Skip("A newer version of the database is needed.")
	}
}
