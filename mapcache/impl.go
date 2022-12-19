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

func (p *mapCacheImpl) getBucketCacheKey(keyHash uint64, sizeLog uint64, version uint64) string {
	sizeLogStr := strconv.FormatUint(sizeLog, 10)
	sizeLogVersion := strconv.FormatUint(version, 10)

	var buf strings.Builder
	buf.WriteString(p.rootKey)
	buf.WriteString(":")
	buf.WriteString(sizeLogStr)
	buf.WriteString(":")
	buf.WriteString(sizeLogVersion)
	buf.WriteString(":")
	buf.WriteString(computeBucketKey(keyHash, sizeLog))

	return buf.String()
}

func (p *mapCacheImpl) getHighCacheKey(keyHash uint64) string {
	return p.getBucketCacheKey(keyHash, p.sizeLog.Current, p.sizeLog.Version)
}

func (p *mapCacheImpl) getLowCacheKey(keyHash uint64) string {
	return p.getBucketCacheKey(keyHash, p.sizeLog.Previous, p.sizeLog.Version-1)
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

	sizeLog        uint64
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
func (p *mapCacheImpl) Get(
	key string, _ GetOptions,
) func() (GetResponse, error) {
	keyHash := hashFunc(key)

	highKey := p.getHighCacheKey(keyHash)
	fn := p.pipeline.Get(highKey, memproxy.GetOptions{})

	hashRange := computeHashRange(keyHash, p.sizeLog.Current)
	params := &fillParams{
		sess: p.sess,

		key:       key,
		hashRange: hashRange,

		sizeLog:        p.sizeLog.Current,
		sizeLogVersion: p.sizeLog.Version,

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
			lowKey1 := p.getLowCacheKey(hashRange.Begin)
			lowKey2 := p.getLowCacheKey(hashRange.End)

			params.lowKeyGetFn = p.pipeline.Get(lowKey1, memproxy.GetOptions{})
			params.lowKeyGetFn2 = p.pipeline.Get(lowKey2, memproxy.GetOptions{})
		} else {
			lowKey := p.getLowCacheKey(keyHash)
			params.lowKeyGetFn = p.pipeline.Get(lowKey, memproxy.GetOptions{})
		}

		p.sess.AddNextCall(func() {
			doGetHandleLeaseResponse(params, leaseGetFn)
		})
	})

	return func() (GetResponse, error) {
		p.sess.Execute()
		return params.resp, params.err
	}
}

// DeleteKeys ...
func (p *mapCacheImpl) DeleteKeys(
	key string, _ DeleteKeyOptions,
) []string {
	keyHash := hashFunc(key)

	result := make([]string, 0, 3)
	result = append(result, p.getHighCacheKey(keyHash))
	if p.sizeLog.Previous > p.sizeLog.Current {
		hashRange := computeHashRange(keyHash, p.sizeLog.Current)
		result = append(result, p.getLowCacheKey(hashRange.Begin))
		result = append(result, p.getLowCacheKey(hashRange.End))
	} else {
		result = append(result, p.getLowCacheKey(keyHash))
	}
	return result
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

type checkValidResult struct {
	doFill bool
	bucket CacheBucketContent
}

func (p *fillParams) isValidResponse(getResp memproxy.GetResponse) (checkValidResult, error) {
	if !getResp.Found {
		return checkValidResult{
			doFill: true,
		}, nil
	}

	bucket, err := unmarshalCacheBucket(getResp.Data)
	if err != nil {
		return checkValidResult{}, err
	}

	if bucket.OriginSizeLogVersion != p.sizeLogVersion-1 {
		return checkValidResult{
			doFill: true,
		}, nil
	}

	return checkValidResult{
		bucket: bucket,
	}, nil
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
) (doFill bool) {
	cacheGetResp, err := params.lowKeyGetFn()
	if err != nil {
		completeFn(memproxy.FillResponse{}, err)
		return false
	}

	result, err := params.isValidResponse(cacheGetResp)
	if err != nil {
		completeFn(memproxy.FillResponse{}, err)
		return false
	}
	if result.doFill {
		return true
	}

	entries := filterEntriesByHashRange(result.bucket.Entries, params.hashRange)
	doComplete(entries, result.bucket.OriginSizeLogVersion)
	return false
}

func (*memproxyFiller) handleTwoLowerBuckets(
	params *fillParams,
	completeFn func(resp memproxy.FillResponse, err error),
	doComplete func(entries []Entry, sizeLogVersion uint64),
) (doFill bool) {
	getCacheResp1, err := params.lowKeyGetFn()
	if err != nil {
		completeFn(memproxy.FillResponse{}, err)
		return false
	}

	getCacheResp2, err := params.lowKeyGetFn2()
	if err != nil {
		completeFn(memproxy.FillResponse{}, err)
		return false
	}

	result1, err := params.isValidResponse(getCacheResp1)
	if err != nil {
		completeFn(memproxy.FillResponse{}, err)
		return false
	}
	if result1.doFill {
		return true
	}

	result2, err := params.isValidResponse(getCacheResp2)
	if err != nil {
		completeFn(memproxy.FillResponse{}, err)
		return false
	}
	if result2.doFill {
		return true
	}

	entries := append(result1.bucket.Entries, result2.bucket.Entries...)
	doComplete(entries, result1.bucket.OriginSizeLogVersion)
	return false
}

func (f *memproxyFiller) handleLowerBuckets(
	params *fillParams,
	completeFn func(resp memproxy.FillResponse, err error),
	doComplete func(entries []Entry, sizeLogVersion uint64),
) (continuing bool) {
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

	continuing := f.handleLowerBuckets(params, completeFn, doComplete)
	if !continuing {
		return
	}

	fn := f.filler.GetBucket(ctx, params.newOptions, params.hashRange)
	params.sess.AddNextCall(func() {
		getResp, err := fn()
		if err != nil {
			completeFn(memproxy.FillResponse{}, err)
			return
		}
		doComplete(getResp.Entries, params.sizeLogVersion)
	})
}
