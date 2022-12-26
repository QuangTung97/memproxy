package mapcache

import (
	"context"
	"github.com/QuangTung97/memproxy"
)

type fillerFactoryImpl struct {
	factory FillerFactory
}

type fillerImpl struct {
	sess   memproxy.Session
	filler Filler
}

var _ memproxy.FillerFactory = &fillerFactoryImpl{}

func (f *fillerFactoryImpl) New(sess memproxy.Session) memproxy.Filler {
	return &fillerImpl{
		sess:   sess,
		filler: f.factory.New(),
	}
}

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

func (*fillerImpl) handleSingleLowerBucket(
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

func (*fillerImpl) handleTwoLowerBuckets(
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

func (f *fillerImpl) handleLowerBuckets(
	params *fillParams,
	completeFn func(resp memproxy.FillResponse, err error),
	doComplete func(entries []Entry, sizeLogVersion uint64),
) (continuing bool) {
	if params.lowKeyGetFn2 != nil {
		return f.handleTwoLowerBuckets(params, completeFn, doComplete)
	}
	return f.handleSingleLowerBucket(params, completeFn, doComplete)
}

func (f *fillerImpl) Fill(
	ctx context.Context, p interface{},
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
	f.sess.AddNextCall(func() {
		getResp, err := fn()
		if err != nil {
			completeFn(memproxy.FillResponse{}, err)
			return
		}
		doComplete(getResp.Entries, params.sizeLogVersion)
	})
}

type autoSizeFillerImpl struct {
}

type autoSizeFiller struct {
}
