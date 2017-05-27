package tests

import (
	"fmt"
	"github.com/elgs/gostrgen"
	"gopkg.in/mgo.v2"
	"os"
	"time"
)

// TestDbSession - connection to test database
var TestDbSession *mgo.Session

// TestDbUri - URI of test database
var TestDbUri = os.Getenv("TEST_MONGO_URI")

// TestDbInfo - server info of test database
var TestDbInfo mgo.BuildInfo

// BenchmarkDbSession - connection to benchmark database
var BenchmarkDbSession *mgo.Session

// BenchmarkDbUri - URI of benchmark database
var BenchmarkDbUri = os.Getenv("BENCHMARK_MONGO_URI")

// BenchmarkDb - name of benchmark database
var BenchmarkDb = os.Getenv("BENCHMARK_DB")

// BenchmarkCol - name of benchmark collection
var BenchmarkCol = os.Getenv("BENCHMARK_COL")

func init() {
	TestDbSession = connect(TestDbUri)
	BenchmarkDbSession = connect(BenchmarkDbUri)

	var err error
	TestDbInfo, err = TestDbSession.BuildInfo()
	if err != nil {
		panic(fmt.Errorf("Error: cannot get server info, msg: %s\n", err.Error()))
	}
}

func connect(url string) *mgo.Session {
	session, err := mgo.DialWithTimeout(url, time.Second)
	if err != nil {
		panic(fmt.Errorf("Error: cannot connect to: %s, msg: %s\n", url, err.Error()))
	}

	session.SetSyncTimeout(5 * time.Second)
	session.SetSocketTimeout(5 * time.Second)

	session.SetMode(mgo.Strong, true)
	session.SetSafe(&mgo.Safe{WMode: "majority"})

	err = session.Ping()
	if err != nil {
		panic(fmt.Errorf("Error: cannot ping to: %s, msg: %s\n", url, err.Error()))
	}

	session.SetSyncTimeout(20 * time.Minute)
	session.SetSocketTimeout(5 * time.Minute)

	return session
}

// CreateTestCollection creates test collection with random name.
func CreateTestCollection(s *mgo.Session) *mgo.Collection {
	random, err := gostrgen.RandGen(20, gostrgen.Lower|gostrgen.Digit, "", "")
	if err != nil {
		panic(err)
	}
	return s.DB("_test").C("_test_" + random)
}

// DropTestCollection drops test collection.
func DropTestCollection(c *mgo.Collection) {
	c.DropCollection()
}

// SetupTestCol creates test collection with random name.
func SetupTestCol() *mgo.Collection {
	return CreateTestCollection(TestDbSession)
}

// TearDownTestCol drops test collection.
func TearDownTestCol(c *mgo.Collection) {
	DropTestCollection(c)
}

// GetBenchmarkCol gets collection for benchmarks.
func GetBenchmarkCol() *mgo.Collection {
	return BenchmarkDbSession.DB(BenchmarkDb).C(BenchmarkCol)
}
