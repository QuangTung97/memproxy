package mapcache

import (
	"context"
	"github.com/QuangTung97/memproxy"
	"github.com/stretchr/testify/assert"
	"testing"
)

type autoSizeTest struct {
	mc MapCache
}

func newAutoSizeTest() *autoSizeTest {
	client := &memproxy.MemcacheMock{}
	pipe := &memproxy.PipelineMock{}

	pipe.DeleteFunc = func(key string, options memproxy.DeleteOptions) func() (memproxy.DeleteResponse, error) {
		return nil
	}

	client.PipelineFunc = func(
		ctx context.Context, sess memproxy.Session, options ...memproxy.PipelineOption,
	) memproxy.Pipeline {
		return pipe
	}

	sess := &memproxy.SessionMock{}
	var calls []func()
	sess.AddNextCallFunc = func(fn func()) {
		calls = append(calls, fn)
	}
	sess.ExecuteFunc = func() {
		for len(calls) > 0 {
			nextCalls := calls
			calls = nil
			for _, fn := range nextCalls {
				fn()
			}
		}
	}

	filler := &FillerMock{}
	fillerFactory := &FillerFactoryMock{
		NewFunc: func(params interface{}) Filler {
			return filler
		},
	}

	const rootKey = "rootkey"

	mc := NewAutoSizeProvider(client, fillerFactory).New(
		newTestContext(), sess, rootKey, NewOptions{Params: "auto-root-params"})
	return &autoSizeTest{
		mc: mc,
	}
}

func TestAutoSize(t *testing.T) {
	t.Skip()

	m := newAutoSizeTest()

	const key1 = "KEY01"

	resp, err := m.mc.Get(key1, GetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, GetResponse{}, resp)
}
