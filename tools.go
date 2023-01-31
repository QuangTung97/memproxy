//go:build tools
// +build tools

package tools

import (
	_ "github.com/matryer/moq"
	_ "github.com/mgechev/revive"
	_ "golang.org/x/perf/cmd/benchstat"
)
