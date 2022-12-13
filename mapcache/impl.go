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
	rootKey string, sizeLog uint64,
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
	sizeLog  uint64
	pipeline memproxy.Pipeline
	filler   Filler
}

func (p *mapCacheImpl) getCacheKey(key string, sizeLog uint64) string {
	sizeLogStr := strconv.FormatUint(sizeLog, 10)
	return p.rootKey + sizeLogStr + ":" + computeBucketKey(key, sizeLog)
}

// Get ...
func (p *mapCacheImpl) Get(
	key string, _ GetOptions,
) func() (GetResponse, error) {
	highKey := p.getCacheKey(key, p.sizeLog)
	fn := p.pipeline.Get(highKey, memproxy.GetOptions{})

	var resp GetResponse
	var err error
	p.sess.AddNextCall(func() {
		_, err = fn()
		if err != nil {
			return
		}

		lowKey := p.getCacheKey(key, p.sizeLog-1)
		leaseGetFn := p.pipeline.LeaseGet(highKey, memproxy.LeaseGetOptions{})
		p.pipeline.LeaseGet(lowKey, memproxy.LeaseGetOptions{})

		p.sess.AddNextCall(func() {
			var leaseGetResp memproxy.LeaseGetResponse
			leaseGetResp, err = leaseGetFn()
			if err != nil {
				return
			}

			getBucketFn := p.filler.GetBucket(context.TODO(), p.rootKey, hashFunc(key))

			p.sess.AddNextCall(func() {
				var bucketResp GetBucketsResponse
				bucketResp, err = getBucketFn()
				if err != nil {
					return
				}

				for _, entry := range bucketResp.Entries {
					if entry.Key == key {
						p.pipeline.LeaseSet(highKey, entry.Data, leaseGetResp.CAS, memproxy.LeaseSetOptions{})
						break
					}
				}
			})
		})
	})

	return func() (GetResponse, error) {
		p.sess.Execute()
		return resp, err
	}
}

// Delete ...
func (p *mapCacheImpl) Delete(
	key string, options DeleteOptions,
) func() (DeleteResponse, error) {
	return func() (DeleteResponse, error) {
		return DeleteResponse{}, nil
	}
}
