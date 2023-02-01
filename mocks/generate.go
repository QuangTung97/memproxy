package mocks

import "github.com/QuangTung97/memproxy"

// Memcache ...
type Memcache = memproxy.Memcache

// Pipeline ...
type Pipeline = memproxy.Pipeline

// SessionProvider ...
type SessionProvider = memproxy.SessionProvider

// Session ...
type Session = memproxy.Session

//go:generate moq -rm -out memproxy_mocks.go . Memcache Pipeline SessionProvider Session
