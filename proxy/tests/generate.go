package tests

import "github.com/QuangTung97/memproxy/proxy"

// ServerStats ...
type ServerStats = proxy.ServerStats

//go:generate moq -rm -out proxy_mocks_test.go . ServerStats
