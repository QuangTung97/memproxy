package mapcache

import (
	"context"
	"github.com/QuangTung97/memproxy"
	"strconv"
)

type providerImpl struct {
	filler Filler
}

var _ Provider = &providerImpl{}

// NewProvider ...
func NewProvider(filler Filler) Provider {
	return &providerImpl{
		filler: filler,
	}
}

// New ...
func (p *providerImpl) New(
	ctx context.Context,
	sess memproxy.Session, pipeline memproxy.Pipeline,
	rootKey string, sizeLog SizeLog,
) MapCache {
	return &mapCacheImpl{
		sess:     sess,
		rootKey:  rootKey,
		sizeLog:  sizeLog,
		pipeline: pipeline,
		filler:   p.filler,
	}
}

type mapCacheImpl struct {
	sess     memproxy.Session
	rootKey  string
	sizeLog  SizeLog
	pipeline memproxy.Pipeline
	filler   Filler
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

// Get ...
func (p *mapCacheImpl) Get(
	key string, _ GetOptions,
) func() (GetResponse, error) {
	keyHash := hashFunc(key)
	highKey := p.getCacheKey(keyHash, p.sizeLog.Current)
	fn := p.pipeline.Get(highKey, memproxy.GetOptions{})

	var resp GetResponse
	var err error
	p.sess.AddNextCall(func() {
		var getResp memproxy.GetResponse
		getResp, err = fn()
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
				resp = GetResponse{
					Found: true,
					Data:  entry.Data,
				}
			} else {
			}
			return
		}

		lowKey := p.getCacheKey(keyHash, p.sizeLog.Previous)
		leaseGetFn := p.pipeline.LeaseGet(highKey, memproxy.LeaseGetOptions{})
		p.pipeline.Get(lowKey, memproxy.GetOptions{})

		p.sess.AddNextCall(func() {
			var leaseGetResp memproxy.LeaseGetResponse
			leaseGetResp, err = leaseGetFn()
			if err != nil {
				return
			}

			hashRange := computeHashRange(keyHash, p.sizeLog.Current)

			getBucketFn := p.filler.GetBucket(context.TODO(), p.rootKey, hashRange)

			p.sess.AddNextCall(func() {
				var bucketResp GetBucketResponse
				bucketResp, err = getBucketFn()
				if err != nil {
					return
				}

				entry, ok := findEntryInList(bucketResp.Entries, key)
				if ok {
					p.pipeline.LeaseSet(highKey, entry.Data, leaseGetResp.CAS, memproxy.LeaseSetOptions{})
					resp = GetResponse{
						Found: true,
						Data:  entry.Data,
					}
				} else {
					// TODO Not Found
					// TODO Delete CAS
				}
			})
		})
	})

	return func() (GetResponse, error) {
		p.sess.Execute()
		return resp, err
	}
}

// DeleteKey ...
func (p *mapCacheImpl) DeleteKey(
	key string, options DeleteKeyOptions,
) string {
	return ""
}
