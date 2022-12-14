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
	rootKey string, sizeLog SizeLog, options NewOptions,
) MapCache {
	return &mapCacheImpl{
		sess:     sess,
		rootKey:  rootKey,
		sizeLog:  sizeLog,
		pipeline: p.client.Pipeline(ctx, sess),
		options:  options,
	}
}

type mapCacheImpl struct {
	sess     memproxy.Session
	rootKey  string
	sizeLog  SizeLog
	pipeline memproxy.Pipeline
	options  NewOptions
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

	key       string
	hashRange HashRange
	sizeLog   uint64

	completed bool
	resp      GetResponse
	err       error

	lowKeyGetFn func() (memproxy.GetResponse, error)
	newOptions  NewOptions
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

		key:       key,
		hashRange: hashRange,
		sizeLog:   p.sizeLog.Current,

		newOptions: p.options,
	}

	p.sess.AddNextCall(func() {
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

		leaseGetFn := p.pipeline.LeaseGet(highKey, memproxy.LeaseGetOptions{
			FillParams: params,
		})

		lowKey := p.getCacheKey(keyHash, p.sizeLog.Previous)
		params.lowKeyGetFn = p.pipeline.Get(lowKey, memproxy.GetOptions{})

		p.sess.AddNextCall(func() {
			var leaseGetResp memproxy.LeaseGetResponse
			leaseGetResp, err = leaseGetFn()
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

func (p *fillParams) setResponse(entries []Entry) {
	p.completed = true

	entry, ok := findEntryInList(entries, p.key)
	if ok {
		p.resp = GetResponse{
			Found: true,
			Data:  entry.Data,
		}
	} else {
		p.resp = GetResponse{}
	}
}

func (p *fillParams) setError(err error) {
	p.resp = GetResponse{}
	p.err = err
}

func (f *memproxyFiller) Fill(
	ctx context.Context, p interface{}, _ string,
	completeFn func(resp memproxy.FillResponse, err error),
) {
	params := p.(*fillParams)

	doComplete := func(entries []Entry, originVersion uint64) {
		params.setResponse(entries)
		completeFn(memproxy.FillResponse{
			Data: marshalCacheBucket(CacheBucketContent{
				OriginSizeLogVersion: originVersion,
				Entries:              entries, // TODO filtering
			}),
		}, nil)
	}

	cacheGetResp, err := params.lowKeyGetFn()
	if err != nil {
		completeFn(memproxy.FillResponse{}, err)
		return
	}

	if cacheGetResp.Found {
		bucket, err := unmarshalCacheBucket(cacheGetResp.Data)
		if err != nil {
			completeFn(memproxy.FillResponse{}, err)
			return
		}
		doComplete(bucket.Entries, bucket.OriginSizeLogVersion)
		return
	}

	fn := f.filler.GetBucket(ctx, params.newOptions, params.hashRange)
	params.sess.AddNextCall(func() {
		getResp, err := fn()
		if err != nil {
			completeFn(memproxy.FillResponse{}, err)
			return
		}
		doComplete(getResp.Entries, params.sizeLog)
	})
}
