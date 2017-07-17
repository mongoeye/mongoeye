<a href="https://raw.githubusercontent.com/mongoeye/mongoeye/master/_misc/logo_name_small.png?v1" title="logo"><img src="https://raw.githubusercontent.com/mongoeye/mongoeye/master/_misc/logo_name_small.png?v1" width="300"/></a>


Schema and data analyzer for [MongoDB](https://www.mongodb.com) written in [Go](https://golang.org).

[![GoDoc](https://godoc.org/github.com/golang/gddo?status.svg)](https://godoc.org/github.com/mongoeye/mongoeye)
[![Coverage Status](https://coveralls.io/repos/github/mongoeye/mongoeye/badge.svg?branch=master)](https://coveralls.io/github/mongoeye/mongoeye?branch=master)
[![Build Status](https://travis-ci.org/mongoeye/mongoeye.svg?branch=master)](https://travis-ci.org/mongoeye/mongoeye)
[![Go Report Card](https://goreportcard.com/badge/github.com/mongoeye/mongoeye)](https://goreportcard.com/report/github.com/mongoeye/mongoeye)

## Table of Contents
      * [Overview](#overview)
         * [Key features](#key-features)
      * [Demo](#demo)
      * [Comparison](#comparison)
      * [Installation](#installation)
      * [Compilation](#compilation)
      * [Usage](#usage)
         * [Table output](#table-output)
         * [JSON and YAML output](#json-and-yaml-output)
      * [Description of features](#description-of-features)
         * [Base output](#base-output)
         * [Value - min, max, avg](#value---min-max-avg)
         * [Length - min, max, avg](#length---min-max-avg)
         * [Number of unique values](#number-of-unique-values)
         * [Frequency of values](#frequency-of-values)
         * [Histogram of value](#histogram-of-value)
         * [Histogram of length](#histogram-of-length)
         * [Histogram of weekday](#histogram-of-weekday)
         * [Histogram of hour](#histogram-of-hour)
      * [Flags](#flags)
            * [Connection options](#connection-options)
            * [Authentication](#authentication)
            * [Input options](#input-options)
            * [Output options](#output-options)
            * [Other options](#other-options)
         * [Environment variables](#environment-variables)
      * [TODO](#todo)
      * [Donation](#donation)
      * [License](#license)

## Overview

Mongoeye provides a quick overview of the data in your MongoDB database.

### Key features

* *Fast:*&nbsp; the fastest schema analyzer for MongoDB
* *Single binary:*&nbsp; pre-built [binaries](https://github.com/mongoeye/mongoeye/releases) for Windows, Linux, and MacOS (Darwin)
* *Local analysis:*&nbsp; quick local analysis using a parallel algorithm (MongoDB 2.0+)
* *Remote analysis:*&nbsp; distributed analysis in database using the aggregation framework (MongoDB 3.5.9+)
* *Rich features:*&nbsp; [histogram](https://en.wikipedia.org/wiki/Histogram) (value, length, weekday, hour), most frequent values, ... 
* *Integrable:*&nbsp; table, JSON or YAML output

## Demo

<a href="https://asciinema.org/a/129238" target="_blank" title="Open in asciinema.org"><img src="https://github.com/mongoeye/mongoeye/blob/doc/_misc/demo.gif?raw=true" /></a>

## Comparison

The speed of the analysis was compared with similar tools on the [test collection](https://github.com/mongoeye/mongoeye/blob/master/_contrib/dataset/companies.json) under the same conditions.

<a href="https://github.com/mongoeye/mongoeye/blob/master/_misc/comparison.png?raw=true" target="_blank" title="Open image"><img src="https://github.com/mongoeye/mongoeye/blob/master/_misc/comparison.png?raw=true" /></a>

Links: [Studio3T](https://studio3t.com), [MongoDB Compass](https://www.mongodb.com/products/compass), [Variety.js](https://github.com/variety/variety), [Schema.js](https://github.com/skratchdot/mongodb-schema)

## Installation

Mongoeye is one executable binary file. 

You can download the archive from [GitHub releases page](https://github.com/mongoeye/mongoeye/releases) and extract the binary file for your platform.

## Compilation

It is required to have [Go 1.8](https://golang.org). All external dependencies are part of the project in the [vendor](https://github.com/mongoeye/mongoeye/tree/master/vendor) directory.

Compilation process:
```
$ go get github.com/mongoeye/mongoeye
$ cd $GOPATH/src/github.com/mongoeye/mongoeye
$ make build
```
 
For development, you need additional dependencies that can be installed using `make get-deps`.

The test architecture uses the [Docker](https://www.docker.com) to create the testing MongoDB database.

If you want to contribute to this project, see the actions in [Makefile](https://github.com/mongoeye/mongoeye/blob/master/Makefile) and the [_contrib](https://github.com/mongoeye/mongoeye/tree/master/_contrib) directory.

## Usage

```
mongoeye [host] database collection [flags]
```

The command `mongoeye --help` lists all available options.

### Table output

Default output format is table. It shows only schema without other analyzes.

Example table output:
```
            KEY            │ COUNT  │   %    
────────────────────────────────────────────
  all documents            │ 2548   │        
  analyzed documents       │ 1000   │  39.2  
                           │        │        
  _id -> objectId          │ 1000   │ 100.0  
  address                  │ 1000   │ 100.0  
  │ -> int                 │    1   │   0.1  
  └╴-> string              │  999   │  99.9  
  address line 2 -> string │ 1000   │ 100.0  
  name -> string           │ 1000   │ 100.0  
  outcode -> string        │ 1000   │ 100.0  
  postcode -> string       │ 1000   │ 100.0  
  rating                   │ 1000   │ 100.0  
  │ -> int                 │  523   │  52.3  
  │ -> double              │  451   │  45.1  
  └╴-> string              │   26   │   2.6  
  type_of_food -> string   │ 1000   │ 100.0  
  URL -> string            │ 1000   │ 100.0  

OK  0.190s (local analysis)
    1000/2548 docs (39.2%)
    9 fields, depth 2
```

### JSON and YAML output

Outputs in JSON and YAML format return the results of all analyzes.

Use `--format json` or `--format yaml` flags to set these formats.

For output to a file use the option `-F /path/to/file`.

## Description of features

### Base output

```yaml
- name: rating
  level: 0
  count: 1000
  types:
  - type: int
    count: 549
    ...
```

### Value - min, max, avg

```yaml
value:
  min: 11.565586
  max: 60.206787
  avg: 38.51128
```

### Length - min, max, avg

```yaml
length:
  min: 29
  max: 153
  avg: 112
```

### Number of unique values

```yaml
unique: 894
```

### Frequency of values

```yaml
mostFrequent:
- value: USD
  count: 599
- value: EUR
  count: 21
- value: GBP
  count: 5
- value: CAD
  count: 4
leastFrequent:
- value: EUR
  count: 21
- value: GBP
  count: 5
- value: CAD
  count: 4
- value: JPY
  count: 3
```

### Histogram of value

```yaml
histogramOfValue:
  start: 2.5
  end: 6
  range: 3.5
  step: 0.5
  numOfSteps: 7
  intervals: [7, 0, 17, 0, 202, 0, 219]
```

### Histogram of length

```yaml
histogramOfLength:
  start: 0
  end: 300
  range: 300
  step: 50
  numOfSteps: 6
  intervals: [96, 78, 3, 1, 1, 0]
```

### Histogram of weekday

```yaml
histogramOfWeekday: [5, 48, 23, 124, 45, 15, 87]
```

### Histogram of hour

```yaml
histogramOfHour: [47, 73, 18, 26, 30, 46, 91, 13, 28, 11, 52, 99, 76, 25, 94, 51, 87, 86, 19, 22, 11, 62, 28, 47]
```

## Flags

#### Connection options
```
--host                    mongodb host (default "localhost:27017")
--connection-mode         connection mode (default "SecondaryPreferred")
--connection-timeout      connection timeout (default 5)
--socket-timeout          socket timeout (default 300)
--sync-timeout            sync timeout (default 300)
```

#### Authentication
```
-u, --user                username for authentication (default "admin")
-p, --password            password for authentication
    --auth-db             auth database (default "admin")
    --auth-mech           auth mechanism
```

#### Input options
```
    --db                  database for analysis
    --col                 collection for analysis
-q, --query               documents query (json)
-s, --scope               all, first:N, last:N, random:N (default "random:1000")
-d, --depth               max depth in nested documents (default 2)
```

#### Output options
```
    --full                all available analyzes
-v, --value               get min, max, avg value
-l, --length              get min, max, avg length
-V, --value-hist          get value histogram
    --value-hist-steps    max steps of value histogram >=3 (default 100)
-L, --length-hist         get length histogram
    --length-hist-steps   max steps of length histogram >=3 (default 100)
-W, --weekday-hist        get weekday histogram for dates
-H, --hour-hist           get hour histogram for dates
    --count-unique        get count of unique values
    --most-freq           get the N most frequent values
    --least-freq          get the N least frequent values
-f, --format              output format: table, json, yaml (default "table")
-F, --file                path to the output file
```

#### Other options
```
-t, --timezone            timezone, eg. UTC, Europe/Berlin (default "local")
    --use-aggregation     analyze with aggregation framework (mongodb 3.5.6+)
    --string-max-length   max string length (default 100)
    --array-max-length    analyze only first N array elements (default 20)
    --concurrency         number of local processes (default 0 = auto)
    --buffer              size of the buffer between local stages (default 5000)
    --batch               size of batch from database (default 500)
    --no-color            disable color output
    --version             show version
-h, --help                show this help
```

### Environment variables

Environment variables can also be used for configuration. 

The names of the environment variables have the `MONGOEYE_` prefix and match the flags.

Instead of the `--count-unique` flag, for example, you can use `export MONGOEYE_COUNT-UNIQUE=true`.

## TODO

* Create a shared library for integration into other languages (Python, Node.js, ...)
* TLS/SSL support
* Create a [web interface](https://github.com/mongoeye/mongoeye-ui).

## Donation

If is this tool useful to you, so feel free to support its further development.

[![paypal](https://www.paypalobjects.com/en_US/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=JEMPF6RQJP7XA)


## License

Mongoeye is under the GPL-3.0 license. See the [LICENSE](LICENSE.md) file for details.

<sub title="Ad maiorem Dei gloriam. To the greater glory of God."><sub>
AMDG
</sub></sub>



