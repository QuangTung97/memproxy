package mapcache

import (
	"context"
	"github.com/QuangTung97/memproxy"
)

//go:generate moq -rm -out mapcache_mocks_test.go . Filler

// NewOptions options to call Get Bucket
type NewOptions struct {
	Params interface{}
}

// Provider for user managed size log
type Provider interface {
	New(ctx context.Context,
		sess memproxy.Session, rootKey string,
		sizeLog SizeLog, options NewOptions,
	) MapCache
}

// TODO AutoSizeProvider

// MapCache for handling big hash tables in memcached
type MapCache interface {
	Get(key string, options GetOptions) func() (GetResponse, error)
	DeleteKeys(key string, options DeleteKeyOptions) []string
}

// Filler ...
type Filler interface {
	GetBucket(ctx context.Context, options NewOptions, hashRange HashRange) func() (GetBucketResponse, error)
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

// GetBucketResponse ...
type GetBucketResponse struct {
	Entries []Entry
}

// DeleteKeyOptions ...
type DeleteKeyOptions struct {
}

// DeleteResponse ...
type DeleteResponse struct {
}

// SizeLog ...
type SizeLog struct {
	Current  uint64 // current size log value
	Previous uint64 // previous size log value
	Version  uint64
}

// CacheBucketContent ...
type CacheBucketContent struct {
	OriginSizeLogVersion uint64
	Entries              []Entry
}

// HashRange ...
type HashRange struct {
	Begin uint64 // inclusive
	End   uint64 // inclusive
}
