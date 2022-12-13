package mapcache

import (
	"context"
	"github.com/QuangTung97/memproxy"
)

// Provider for user managed size log
type Provider interface {
	New(ctx context.Context,
		sess memproxy.Session, pipeline memproxy.Pipeline,
		rootKey string, sizeLog uint64,
	) MapCache
}

// TODO AutoSizeProvider

// MapCache for handling big hash tables in memcached
type MapCache interface {
	Get(key string, options GetOptions) func() (GetResponse, error)
	Delete(key string, options DeleteOptions) func() (DeleteResponse, error)
}

// Filler ...
type Filler interface {
	GetBucket(ctx context.Context, rootKey string, hash uint64) func() (GetBucketsResponse, error)
}

// GetOptions ...
type GetOptions struct {
}

// GetResponse ...
type GetResponse struct {
	Found bool
	Data  []byte
}

// Entry ...
type Entry struct {
	Key  string
	Data []byte
}

// Bucket ...
type Bucket struct {
	Hash uint64
}

// GetBucketsResponse ...
type GetBucketsResponse struct {
	Entries []Entry
}

// DeleteOptions ...
type DeleteOptions struct {
}

// DeleteResponse ...
type DeleteResponse struct {
}
