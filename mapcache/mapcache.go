package mapcache

import (
	"context"
	"github.com/QuangTung97/memproxy"
)

//go:generate moq -rm -out mapcache_mocks_test.go . Filler FillerFactory

// NewOptions options to call Get Bucket
type NewOptions struct {
	Params interface{}
}

// Provider for user managed size log
// this interface is thread safe
type Provider interface {
	New(
		ctx context.Context, sess memproxy.Session,
		rootKey string, sizeLog SizeLog, options NewOptions,
	) MapCache
}

// InvalidatorFactory is thread safe
type InvalidatorFactory interface {
	New(rootKey string, sizeLog SizeLog) Invalidator
}

// AutoSizeProvider for automatic managing size logs
// this interface is thread safe
type AutoSizeProvider interface {
	New(
		ctx context.Context, sess memproxy.Session,
		rootKey string, options NewOptions,
	) MapCache
}

// MapCache for handling big hash tables in memcached
// this interface is NOT thread safe
type MapCache interface {
	Get(key string, options GetOptions) func() (GetResponse, error)
}

// Invalidator ...
type Invalidator interface {
	// DeleteKeys compute deleted keys
	DeleteKeys(key string, options DeleteKeyOptions) []string
}

// FillerFactory MUST BE thread safe
type FillerFactory interface {
	// New with params is pipeline filler params
	New(params interface{}) Filler
}

// Filler not need to be thread safe
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
