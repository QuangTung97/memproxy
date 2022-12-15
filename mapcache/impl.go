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

	lowKeyGetFn  func() (memproxy.GetResponse, error)
	lowKeyGetFn2 func() (memproxy.GetResponse, error)
	newOptions   NewOptions
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

		if p.sizeLog.Previous == p.sizeLog.Current+1 {
			lowKey1 := p.getCacheKey(hashRange.Begin, p.sizeLog.Previous)
			lowKey2 := p.getCacheKey(hashRange.End, p.sizeLog.Previous)

			params.lowKeyGetFn = p.pipeline.Get(lowKey1, memproxy.GetOptions{})
			params.lowKeyGetFn2 = p.pipeline.Get(lowKey2, memproxy.GetOptions{})
		} else {
			lowKey := p.getCacheKey(keyHash, p.sizeLog.Previous)
			params.lowKeyGetFn = p.pipeline.Get(lowKey, memproxy.GetOptions{})
		}

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

// DeleteKeys ...
func (p *mapCacheImpl) DeleteKeys(
	key string, options DeleteKeyOptions,
) []string {
	return nil
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

func filterEntriesByHashRange(entries []Entry, hashRange HashRange) []Entry {
	result := make([]Entry, 0, len(entries)/2)
	for _, e := range entries {
		h := hashFunc(e.Key)
		if h >= hashRange.Begin && h <= hashRange.End {
			result = append(result, e)
		}
	}
	return result
}

func (*memproxyFiller) handleSingleLowerBucket(
	params *fillParams,
	completeFn func(resp memproxy.FillResponse, err error),
	doComplete func(entries []Entry, sizeLogVersion uint64),
) bool {
	cacheGetResp, err := params.lowKeyGetFn()
	if err != nil {
		completeFn(memproxy.FillResponse{}, err)
		return true
	}

	if !cacheGetResp.Found {
		return false
	}

	bucket, err := unmarshalCacheBucket(cacheGetResp.Data)
	if err != nil {
		completeFn(memproxy.FillResponse{}, err)
		return true
	}

	// TODO Check Origin Size Version

	entries := filterEntriesByHashRange(bucket.Entries, params.hashRange)
	doComplete(entries, bucket.OriginSizeLogVersion)
	return true
}

func (*memproxyFiller) handleTwoLowerBuckets(
	params *fillParams,
	completeFn func(resp memproxy.FillResponse, err error),
	doComplete func(entries []Entry, sizeLogVersion uint64),
) bool {
	getCacheResp1, err := params.lowKeyGetFn()
	if err != nil {
		// TODO
		return true
	}

	getCacheResp2, err := params.lowKeyGetFn2()
	if err != nil {
		// TODO
		return true
	}

	// TODO
	// TODO Check Size Equal
	if !getCacheResp1.Found {
		return false
	}

	// TODO Second Not Found

	bucket1, err := unmarshalCacheBucket(getCacheResp1.Data)
	if err != nil {
		// TODO
		return true
	}

	bucket2, err := unmarshalCacheBucket(getCacheResp2.Data)
	if err != nil {
		// TODO
		return true
	}

	entries := append(bucket1.Entries, bucket2.Entries...)
	doComplete(entries, bucket1.OriginSizeLogVersion)
	return true
}

func (f *memproxyFiller) handleLowerBuckets(
	params *fillParams,
	completeFn func(resp memproxy.FillResponse, err error),
	doComplete func(entries []Entry, sizeLogVersion uint64),
) bool {
	if params.lowKeyGetFn2 != nil {
		return f.handleTwoLowerBuckets(params, completeFn, doComplete)
	}
	return f.handleSingleLowerBucket(params, completeFn, doComplete)
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
				Entries:              entries,
			}),
		}, nil)
	}

	finished := f.handleLowerBuckets(params, completeFn, doComplete)
	if finished {
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
