CGO_CFLAGS ?= -DHAVE_STRCHRNUL

test:
	go mod tidy
	go clean -testcache
	env CGO_CFLAGS="$(CGO_CFLAGS)" go test -count=1 -failfast -timeout 20s -race ./...

cover:
	go clean -testcache
	env CGO_CFLAGS="$(CGO_CFLAGS)" go test -count=1 -coverprofile=go-cover.profile -timeout 5s ./...
	go tool cover -html=go-cover.profile
	rm go-cover.profile

build:
	mkdir -p bin
	env CGO_CFLAGS="$(CGO_CFLAGS)" go build -o ./bin/sqlvet .
