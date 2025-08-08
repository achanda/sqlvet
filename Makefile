CGO_CFLAGS ?= -DHAVE_STRCHRNUL

test:
	go mod tidy
	env CGO_CFLAGS="$(CGO_CFLAGS)" go test -failfast -timeout 20s -race ./...

cover:
	env CGO_CFLAGS="$(CGO_CFLAGS)" go test -coverprofile=go-cover.profile -timeout 5s ./...
	go tool cover -html=go-cover.profile
	rm go-cover.profile
