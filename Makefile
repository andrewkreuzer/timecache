all: test build

release: test build docker

test:
	go test -v ./...

build:
	mkdir -p build
	go build -o build/timecache ./cmd

clean:
	rm -r build/*

.PHONY: test build clean
