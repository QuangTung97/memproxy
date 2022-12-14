package mapcache

import (
	"context"
	"github.com/QuangTung97/memproxy"
	"strconv"
)

type providerImpl struct {
	client memproxy.Memcache
}

var _ Provider = &providerImpl{}

// NewProvider ...
func NewProvider(
	client memproxy.Memcache, filler Filler,
) Provider {
	return &providerImpl{
		client: memproxy.NewFillerMemcache(client, &memproxyFiller{filler: filler}),
	}
}

// New ...
func (p *providerImpl) New(
	ctx context.Context, sess memproxy.Session,
	rootKey string, sizeLog SizeLog,
) MapCache {
	return &mapCacheImpl{
		sess:     sess,
		rootKey:  rootKey,
		sizeLog:  sizeLog,
		pipeline: p.client.Pipeline(ctx, sess),
	}
}

type mapCacheImpl struct {
	sess     memproxy.Session
	rootKey  string
	sizeLog  SizeLog
	pipeline memproxy.Pipeline
}

func (p *mapCacheImpl) getCacheKey(keyHash uint64, sizeLog uint64) string {
	sizeLogStr := strconv.FormatUint(sizeLog, 10)
	return p.rootKey + ":" + sizeLogStr + ":" + computeBucketKey(keyHash, sizeLog)
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
	sess memproxy.Session

	rootKey   string
	key       string
	hashRange HashRange
	sizeLog   uint64

	resp GetResponse
	err  error
}

// Get ...
func (p *mapCacheImpl) Get(
	key string, _ GetOptions,
) func() (GetResponse, error) {
	keyHash := hashFunc(key)

	highKey := p.getCacheKey(keyHash, p.sizeLog.Current)
	fn := p.pipeline.Get(highKey, memproxy.GetOptions{})

	hashRange := computeHashRange(keyHash, p.sizeLog.Current)
	params := &fillParams{
		sess: p.sess,

		rootKey:   p.rootKey,
		key:       key,
		hashRange: hashRange,
		sizeLog:   p.sizeLog.Current,
	}

	p.sess.AddNextCall(func() {
		getResp, err := fn()
		if err != nil {
			return
		}

		if getResp.Found {
			var bucket CacheBucketContent
			bucket, err = unmarshalCacheBucket(getResp.Data)
			if err != nil {
				return
			}
			entry, ok := findEntryInList(bucket.Entries, key)
			if ok {
				params.resp = GetResponse{
					Found: true,
					Data:  entry.Data,
				}
			} else {
			}
			return
		}

		leaseGetFn := p.pipeline.LeaseGet(highKey, memproxy.LeaseGetOptions{
			FillParams: params,
		})

		lowKey := p.getCacheKey(keyHash, p.sizeLog.Previous)
		p.pipeline.Get(lowKey, memproxy.GetOptions{})

		p.sess.AddNextCall(func() {
			var leaseGetResp memproxy.LeaseGetResponse
			leaseGetResp, err = leaseGetFn()
			if err != nil {
				return
			}

			var bucket CacheBucketContent
			bucket, err = unmarshalCacheBucket(leaseGetResp.Data)
			if err != nil {
				return
			}

			entry, ok := findEntryInList(bucket.Entries, key)
			if ok {
				params.resp = GetResponse{
					Found: true,
					Data:  entry.Data,
				}
			} else {
				// TODO
			}
		})
	})

	return func() (GetResponse, error) {
		p.sess.Execute()
		return params.resp, params.err
	}
}

// DeleteKey ...
func (p *mapCacheImpl) DeleteKey(
	key string, options DeleteKeyOptions,
) string {
	return ""
}

type memproxyFiller struct {
	filler Filler
}

var _ memproxy.Filler = &memproxyFiller{}

func (f *memproxyFiller) Fill(
	ctx context.Context, p interface{}, _ string,
	completeFn func(resp memproxy.FillResponse, err error),
) {
	params := p.(*fillParams)
	fn := f.filler.GetBucket(ctx, params.rootKey, params.hashRange)
	params.sess.AddNextCall(func() {
		getResp, err := fn()
		if err != nil {
			// TODO
		}

		entry, ok := findEntryInList(getResp.Entries, params.key)
		if !ok {
			// TODO
		}

		params.resp = GetResponse{
			Found: true,
			Data:  entry.Data,
		}

		completeFn(memproxy.FillResponse{
			Data: marshalCacheBucket(CacheBucketContent{
				OriginSizeLogVersion: params.sizeLog,
				Entries:              getResp.Entries,
			}),
		}, nil)
	})
}
