.PHONY: build

PACKAGES = $(shell go list ./... | grep -v /vendor/)
DIRS = $(shell go list ./... | grep -v /vendor/ | sed 's~^github.com/mongoeye/mongoeye~.~' | grep -v '^.$$')

ci : get-deps coverage fmt-check lint

all: fmt-fix lint test build

get-deps:
	ls ./bin/gometalinter || curl -L https://git.io/vp6lP | sh
	go get github.com/kyoh86/richgo
	go get github.com/mattn/goveralls
	go get golang.org/x/lint/golint
	go mod vendor

fmt-check:
	gofmt -s -l main.go $(DIRS)

fmt-fix:
	gofmt -s -l -w main.go $(DIRS)

lint:
	golint -min_confidence 0.85 -set_exit_status $(DIRS)

test:
	bash -c "_contrib/test.sh"

coverage:
	bash -c "_contrib/coverage.sh"

benchmark:
	go test -v -run=^$$ -bench=. -count=2 -benchtime=1s -benchmem -parallel=1 $(PACKAGES)

benchmark-stages:
	go test -v -run=^$$ -bench=Full$$ -count=2 -benchtime=1s -benchmem -parallel=1 $(PACKAGES)

build: build-cross build-tar build-zip

build-cross:
	GOOS=linux   GOARCH=amd64 CGO_ENABLED=0 go build -o _release/mongoeye/linux/amd64/mongoeye        github.com/mongoeye/mongoeye
	GOOS=linux   GOARCH=arm64 CGO_ENABLED=0 go build -o _release/mongoeye/linux/arm64/mongoeye        github.com/mongoeye/mongoeye
	GOOS=linux   GOARCH=arm   CGO_ENABLED=0 go build -o _release/mongoeye/linux/arm/mongoeye          github.com/mongoeye/mongoeye
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -o _release/mongoeye/windows/amd64/mongoeye.exe  github.com/mongoeye/mongoeye
	GOOS=darwin  GOARCH=amd64 CGO_ENABLED=0 go build -o _release/mongoeye/darwin/amd64/mongoeye       github.com/mongoeye/mongoeye

build-tar:
	tar -cvzf _release/mongoeye.tar.gz -C _release mongoeye

build-zip:
	cd _release; zip -r mongoeye.zip mongoeye

demo-gif:
	bash -c "_contrib/demo/record-gif.sh"

demo-asciinema:
	bash -c "asciinema rec -t 'Mongoeye demo' -y -c ./_contrib/demo/demo.sh"



