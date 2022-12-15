.PHONY: lint install-tools test coverage

lint:
	go fmt ./...
	go vet ./...
	revive -config revive.toml -formatter friendly ./...

test:
	go test -p 1 -covermode=count -coverprofile memproxy-coverage.cov ./...

install-tools:
	go install github.com/matryer/moq
	go install github.com/mgechev/revive

coverage:
	go tool cover -func memproxy-coverage.cov | grep ^total
