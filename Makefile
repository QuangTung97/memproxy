.PHONY: lint install-tools test test-race coverage benchmark compare new_to_old membench profile

lint:
	go fmt ./...
	go vet ./...
	revive -config revive.toml -formatter friendly ./...

test:
	go test -v -p 1 -count=1 -covermode=count -coverprofile=coverage.out ./...

test-race:
	go test -v -p 1 -race -count=1 ./...

install-tools:
	go install github.com/matryer/moq
	go install github.com/mgechev/revive
	go install golang.org/x/perf/cmd/benchstat

coverage:
	go tool cover -func coverage.out | grep ^total

benchmark:
	go test -run="^Benchmark" -bench=. -count=10 ./... > benchmark_new.txt

compare:
	benchstat benchmark_old.txt benchmark_new.txt

new_to_old:
	mv benchmark_new.txt benchmark_old.txt

membench:
	go test -run="^Benchmark" -bench=. -benchmem ./...

profile:
	go tool pprof -http=:8080 ./item/bench_profile.out
