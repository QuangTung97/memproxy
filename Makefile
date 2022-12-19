.PHONY: lint install-tools test test-race coverage benchmark

lint:
	go fmt ./...
	go vet ./...
	revive -config revive.toml -formatter friendly ./...

test:
	go test -p 1 -covermode=count -coverprofile coverage.out ./...

test-race:
	go test -race -p 1 -covermode=count -coverprofile coverage.out ./...

install-tools:
	go install github.com/matryer/moq
	go install github.com/mgechev/revive

coverage:
	go tool cover -func coverage.out | grep ^total

benchmark:
	go test -bench=. ./...
