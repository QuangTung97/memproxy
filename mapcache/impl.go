package mapcache

import (
	"context"
	"github.com/QuangTung97/memproxy"
	"strconv"
	"strings"
)

type providerImpl struct {
	client memproxy.Memcache
}

var _ Provider = &providerImpl{}

// NewProvider ...
func NewProvider(
	client memproxy.Memcache, fillerFactory FillerFactory,
) Provider {
	return &providerImpl{
		client: memproxy.NewFillerMemcache(client, &fillerFactoryImpl{factory: fillerFactory}),
	}
}

// New ...
func (p *providerImpl) New(
	ctx context.Context, sess memproxy.Session,
	rootKey string, sizeLog SizeLog, options NewOptions,
) MapCache {
	m := &mapCacheImpl{}
	initMapCacheImpl(ctx, m, p.client, sess, rootKey, options)
	m.conf.sizeLog = sizeLog
	return m
}

func initMapCacheImpl(
	ctx context.Context,
	m *mapCacheImpl, client memproxy.Memcache,
	sess memproxy.Session,
	rootKey string, options NewOptions,
) {
	*m = mapCacheImpl{
		sess: sess,
		conf: mapCacheConfig{
			rootKey: rootKey,
		},
		pipeline: client.Pipeline(ctx, sess),
		options:  options,
	}
}

type mapCacheConfig struct {
	rootKey string
	sizeLog SizeLog
}

type mapCacheImpl struct {
	sess     memproxy.Session
	conf     mapCacheConfig
	pipeline memproxy.Pipeline
	options  NewOptions
}

func (c mapCacheConfig) getBucketCacheKey(
	keyHash uint64, sizeLog uint64, version uint64,
) string {
	sizeLogStr := strconv.FormatUint(sizeLog, 10)
	sizeLogVersion := strconv.FormatUint(version, 10)

	var buf strings.Builder
	buf.WriteString(c.rootKey)
	buf.WriteString(":")
	buf.WriteString(sizeLogStr)
	buf.WriteString(":")
	buf.WriteString(sizeLogVersion)
	buf.WriteString(":")
	buf.WriteString(computeBucketKey(keyHash, sizeLog))

	return buf.String()
}

func (c mapCacheConfig) getHighCacheKey(keyHash uint64) string {
	return c.getBucketCacheKey(keyHash, c.sizeLog.Current, c.sizeLog.Version)
}

func (c mapCacheConfig) getLowCacheKey(keyHash uint64) string {
	return c.getBucketCacheKey(keyHash, c.sizeLog.Previous, c.sizeLog.Version-1)
}

func findEntryInList(entries []Entry, key string) (Entry, bool) {
	for _, e := range entries {
		if e.Key == key {
			return e, true
		}
	}
	return Entry{}, false
}

type fillParams struct {
	key       string
	hashRange HashRange

	sizeLogVersion uint64

	completed bool
	resp      GetResponse
	err       error

	lowKeyGetFn  func() (memproxy.GetResponse, error)
	lowKeyGetFn2 func() (memproxy.GetResponse, error)
	newOptions   NewOptions
}

func doGetHandleLeaseResponse(params *fillParams, leaseGetFn func() (memproxy.LeaseGetResponse, error)) {
	leaseGetResp, err := leaseGetFn()
	if err != nil {
		params.setError(err)
		return
	}

	if params.completed {
		return
	}

	var bucket CacheBucketContent
	bucket, err = unmarshalCacheBucket(leaseGetResp.Data)
	if err != nil {
		params.setError(err)
		return
	}

	params.setResponse(bucket.Entries)
}

// Get ...
func (m *mapCacheImpl) Get(
	key string, _ GetOptions,
) func() (GetResponse, error) {
	keyHash := hashFunc(key)

	highKey := m.conf.getHighCacheKey(keyHash)
	fn := m.pipeline.Get(highKey, memproxy.GetOptions{})

	hashRange := computeHashRange(keyHash, m.conf.sizeLog.Current)
	params := &fillParams{
		key: key,
	}

	m.sess.AddNextCall(func() {
		getResp, err := fn()
		if err != nil {
			params.setError(err)
			return
		}

		if getResp.Found {
			var bucket CacheBucketContent
			bucket, err = unmarshalCacheBucket(getResp.Data)
			if err != nil {
				params.setError(err)
				return
			}
			params.setResponse(bucket.Entries)
			return
		}

		params.hashRange = hashRange
		params.sizeLogVersion = m.conf.sizeLog.Version
		params.newOptions = m.options

		leaseGetFn := m.pipeline.LeaseGet(highKey, memproxy.LeaseGetOptions{
			FillParams: params,
		})

		if m.conf.sizeLog.Previous == m.conf.sizeLog.Current+1 {
			lowKey1 := m.conf.getLowCacheKey(hashRange.Begin)
			lowKey2 := m.conf.getLowCacheKey(hashRange.End)

			params.lowKeyGetFn = m.pipeline.Get(lowKey1, memproxy.GetOptions{})
			params.lowKeyGetFn2 = m.pipeline.Get(lowKey2, memproxy.GetOptions{})
		} else {
			lowKey := m.conf.getLowCacheKey(keyHash)
			params.lowKeyGetFn = m.pipeline.Get(lowKey, memproxy.GetOptions{})
		}

		m.sess.AddNextCall(func() {
			doGetHandleLeaseResponse(params, leaseGetFn)
		})
	})

	return func() (GetResponse, error) {
		m.sess.Execute()
		return params.resp, params.err
	}
}
