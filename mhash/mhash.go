package mhash

import (
	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/item"
)

// Null ...
type Null[T any] struct {
	Valid bool
	Data  T
}

// Hash ...
type Hash[T item.Value, K item.Key] struct {
	sess     memproxy.Session
	pipeline memproxy.Pipeline
}

// New ...
func New[T item.Value, K item.Key](
	sess memproxy.Session,
	pipeline memproxy.Pipeline,
) *Hash[T, K] {
	return &Hash[T, K]{
		sess:     sess,
		pipeline: pipeline,
	}
}
