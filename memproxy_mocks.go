// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package memproxy

import (
	"context"
	"sync"
	"time"
)

// Ensure, that MemcacheMock does implement Memcache.
// If this is not the case, regenerate this file with moq.
var _ Memcache = &MemcacheMock{}

// MemcacheMock is a mock implementation of Memcache.
//
//	func TestSomethingThatUsesMemcache(t *testing.T) {
//
//		// make and configure a mocked Memcache
//		mockedMemcache := &MemcacheMock{
//			PipelineFunc: func(ctx context.Context, sess Session, options ...PipelineOption) Pipeline {
//				panic("mock out the Pipeline method")
//			},
//		}
//
//		// use mockedMemcache in code that requires Memcache
//		// and then make assertions.
//
//	}
type MemcacheMock struct {
	// PipelineFunc mocks the Pipeline method.
	PipelineFunc func(ctx context.Context, sess Session, options ...PipelineOption) Pipeline

	// calls tracks calls to the methods.
	calls struct {
		// Pipeline holds details about calls to the Pipeline method.
		Pipeline []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// Sess is the sess argument value.
			Sess Session
			// Options is the options argument value.
			Options []PipelineOption
		}
	}
	lockPipeline sync.RWMutex
}

// Pipeline calls PipelineFunc.
func (mock *MemcacheMock) Pipeline(ctx context.Context, sess Session, options ...PipelineOption) Pipeline {
	if mock.PipelineFunc == nil {
		panic("MemcacheMock.PipelineFunc: method is nil but Memcache.Pipeline was just called")
	}
	callInfo := struct {
		Ctx     context.Context
		Sess    Session
		Options []PipelineOption
	}{
		Ctx:     ctx,
		Sess:    sess,
		Options: options,
	}
	mock.lockPipeline.Lock()
	mock.calls.Pipeline = append(mock.calls.Pipeline, callInfo)
	mock.lockPipeline.Unlock()
	return mock.PipelineFunc(ctx, sess, options...)
}

// PipelineCalls gets all the calls that were made to Pipeline.
// Check the length with:
//
//	len(mockedMemcache.PipelineCalls())
func (mock *MemcacheMock) PipelineCalls() []struct {
	Ctx     context.Context
	Sess    Session
	Options []PipelineOption
} {
	var calls []struct {
		Ctx     context.Context
		Sess    Session
		Options []PipelineOption
	}
	mock.lockPipeline.RLock()
	calls = mock.calls.Pipeline
	mock.lockPipeline.RUnlock()
	return calls
}

// Ensure, that PipelineMock does implement Pipeline.
// If this is not the case, regenerate this file with moq.
var _ Pipeline = &PipelineMock{}

// PipelineMock is a mock implementation of Pipeline.
//
//	func TestSomethingThatUsesPipeline(t *testing.T) {
//
//		// make and configure a mocked Pipeline
//		mockedPipeline := &PipelineMock{
//			DeleteFunc: func(key string, options DeleteOptions) func() (DeleteResponse, error) {
//				panic("mock out the Delete method")
//			},
//			ExecuteFunc: func()  {
//				panic("mock out the Execute method")
//			},
//			FinishFunc: func()  {
//				panic("mock out the Finish method")
//			},
//			GetFunc: func(key string, options GetOptions) func() (GetResponse, error) {
//				panic("mock out the Get method")
//			},
//			LeaseGetFunc: func(key string, options LeaseGetOptions) func() (LeaseGetResponse, error) {
//				panic("mock out the LeaseGet method")
//			},
//			LeaseSetFunc: func(key string, data []byte, cas uint64, options LeaseSetOptions) func() (LeaseSetResponse, error) {
//				panic("mock out the LeaseSet method")
//			},
//			LowerSessionFunc: func() Session {
//				panic("mock out the LowerSession method")
//			},
//		}
//
//		// use mockedPipeline in code that requires Pipeline
//		// and then make assertions.
//
//	}
type PipelineMock struct {
	// DeleteFunc mocks the Delete method.
	DeleteFunc func(key string, options DeleteOptions) func() (DeleteResponse, error)

	// ExecuteFunc mocks the Execute method.
	ExecuteFunc func()

	// FinishFunc mocks the Finish method.
	FinishFunc func()

	// GetFunc mocks the Get method.
	GetFunc func(key string, options GetOptions) func() (GetResponse, error)

	// LeaseGetFunc mocks the LeaseGet method.
	LeaseGetFunc func(key string, options LeaseGetOptions) func() (LeaseGetResponse, error)

	// LeaseSetFunc mocks the LeaseSet method.
	LeaseSetFunc func(key string, data []byte, cas uint64, options LeaseSetOptions) func() (LeaseSetResponse, error)

	// LowerSessionFunc mocks the LowerSession method.
	LowerSessionFunc func() Session

	// calls tracks calls to the methods.
	calls struct {
		// Delete holds details about calls to the Delete method.
		Delete []struct {
			// Key is the key argument value.
			Key string
			// Options is the options argument value.
			Options DeleteOptions
		}
		// Execute holds details about calls to the Execute method.
		Execute []struct {
		}
		// Finish holds details about calls to the Finish method.
		Finish []struct {
		}
		// Get holds details about calls to the Get method.
		Get []struct {
			// Key is the key argument value.
			Key string
			// Options is the options argument value.
			Options GetOptions
		}
		// LeaseGet holds details about calls to the LeaseGet method.
		LeaseGet []struct {
			// Key is the key argument value.
			Key string
			// Options is the options argument value.
			Options LeaseGetOptions
		}
		// LeaseSet holds details about calls to the LeaseSet method.
		LeaseSet []struct {
			// Key is the key argument value.
			Key string
			// Data is the data argument value.
			Data []byte
			// Cas is the cas argument value.
			Cas uint64
			// Options is the options argument value.
			Options LeaseSetOptions
		}
		// LowerSession holds details about calls to the LowerSession method.
		LowerSession []struct {
		}
	}
	lockDelete       sync.RWMutex
	lockExecute      sync.RWMutex
	lockFinish       sync.RWMutex
	lockGet          sync.RWMutex
	lockLeaseGet     sync.RWMutex
	lockLeaseSet     sync.RWMutex
	lockLowerSession sync.RWMutex
}

// Delete calls DeleteFunc.
func (mock *PipelineMock) Delete(key string, options DeleteOptions) func() (DeleteResponse, error) {
	if mock.DeleteFunc == nil {
		panic("PipelineMock.DeleteFunc: method is nil but Pipeline.Delete was just called")
	}
	callInfo := struct {
		Key     string
		Options DeleteOptions
	}{
		Key:     key,
		Options: options,
	}
	mock.lockDelete.Lock()
	mock.calls.Delete = append(mock.calls.Delete, callInfo)
	mock.lockDelete.Unlock()
	return mock.DeleteFunc(key, options)
}

// DeleteCalls gets all the calls that were made to Delete.
// Check the length with:
//
//	len(mockedPipeline.DeleteCalls())
func (mock *PipelineMock) DeleteCalls() []struct {
	Key     string
	Options DeleteOptions
} {
	var calls []struct {
		Key     string
		Options DeleteOptions
	}
	mock.lockDelete.RLock()
	calls = mock.calls.Delete
	mock.lockDelete.RUnlock()
	return calls
}

// Execute calls ExecuteFunc.
func (mock *PipelineMock) Execute() {
	if mock.ExecuteFunc == nil {
		panic("PipelineMock.ExecuteFunc: method is nil but Pipeline.Execute was just called")
	}
	callInfo := struct {
	}{}
	mock.lockExecute.Lock()
	mock.calls.Execute = append(mock.calls.Execute, callInfo)
	mock.lockExecute.Unlock()
	mock.ExecuteFunc()
}

// ExecuteCalls gets all the calls that were made to Execute.
// Check the length with:
//
//	len(mockedPipeline.ExecuteCalls())
func (mock *PipelineMock) ExecuteCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockExecute.RLock()
	calls = mock.calls.Execute
	mock.lockExecute.RUnlock()
	return calls
}

// Finish calls FinishFunc.
func (mock *PipelineMock) Finish() {
	if mock.FinishFunc == nil {
		panic("PipelineMock.FinishFunc: method is nil but Pipeline.Finish was just called")
	}
	callInfo := struct {
	}{}
	mock.lockFinish.Lock()
	mock.calls.Finish = append(mock.calls.Finish, callInfo)
	mock.lockFinish.Unlock()
	mock.FinishFunc()
}

// FinishCalls gets all the calls that were made to Finish.
// Check the length with:
//
//	len(mockedPipeline.FinishCalls())
func (mock *PipelineMock) FinishCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockFinish.RLock()
	calls = mock.calls.Finish
	mock.lockFinish.RUnlock()
	return calls
}

// Get calls GetFunc.
func (mock *PipelineMock) Get(key string, options GetOptions) func() (GetResponse, error) {
	if mock.GetFunc == nil {
		panic("PipelineMock.GetFunc: method is nil but Pipeline.Get was just called")
	}
	callInfo := struct {
		Key     string
		Options GetOptions
	}{
		Key:     key,
		Options: options,
	}
	mock.lockGet.Lock()
	mock.calls.Get = append(mock.calls.Get, callInfo)
	mock.lockGet.Unlock()
	return mock.GetFunc(key, options)
}

// GetCalls gets all the calls that were made to Get.
// Check the length with:
//
//	len(mockedPipeline.GetCalls())
func (mock *PipelineMock) GetCalls() []struct {
	Key     string
	Options GetOptions
} {
	var calls []struct {
		Key     string
		Options GetOptions
	}
	mock.lockGet.RLock()
	calls = mock.calls.Get
	mock.lockGet.RUnlock()
	return calls
}

// LeaseGet calls LeaseGetFunc.
func (mock *PipelineMock) LeaseGet(key string, options LeaseGetOptions) func() (LeaseGetResponse, error) {
	if mock.LeaseGetFunc == nil {
		panic("PipelineMock.LeaseGetFunc: method is nil but Pipeline.LeaseGet was just called")
	}
	callInfo := struct {
		Key     string
		Options LeaseGetOptions
	}{
		Key:     key,
		Options: options,
	}
	mock.lockLeaseGet.Lock()
	mock.calls.LeaseGet = append(mock.calls.LeaseGet, callInfo)
	mock.lockLeaseGet.Unlock()
	return mock.LeaseGetFunc(key, options)
}

// LeaseGetCalls gets all the calls that were made to LeaseGet.
// Check the length with:
//
//	len(mockedPipeline.LeaseGetCalls())
func (mock *PipelineMock) LeaseGetCalls() []struct {
	Key     string
	Options LeaseGetOptions
} {
	var calls []struct {
		Key     string
		Options LeaseGetOptions
	}
	mock.lockLeaseGet.RLock()
	calls = mock.calls.LeaseGet
	mock.lockLeaseGet.RUnlock()
	return calls
}

// LeaseSet calls LeaseSetFunc.
func (mock *PipelineMock) LeaseSet(key string, data []byte, cas uint64, options LeaseSetOptions) func() (LeaseSetResponse, error) {
	if mock.LeaseSetFunc == nil {
		panic("PipelineMock.LeaseSetFunc: method is nil but Pipeline.LeaseSet was just called")
	}
	callInfo := struct {
		Key     string
		Data    []byte
		Cas     uint64
		Options LeaseSetOptions
	}{
		Key:     key,
		Data:    data,
		Cas:     cas,
		Options: options,
	}
	mock.lockLeaseSet.Lock()
	mock.calls.LeaseSet = append(mock.calls.LeaseSet, callInfo)
	mock.lockLeaseSet.Unlock()
	return mock.LeaseSetFunc(key, data, cas, options)
}

// LeaseSetCalls gets all the calls that were made to LeaseSet.
// Check the length with:
//
//	len(mockedPipeline.LeaseSetCalls())
func (mock *PipelineMock) LeaseSetCalls() []struct {
	Key     string
	Data    []byte
	Cas     uint64
	Options LeaseSetOptions
} {
	var calls []struct {
		Key     string
		Data    []byte
		Cas     uint64
		Options LeaseSetOptions
	}
	mock.lockLeaseSet.RLock()
	calls = mock.calls.LeaseSet
	mock.lockLeaseSet.RUnlock()
	return calls
}

// LowerSession calls LowerSessionFunc.
func (mock *PipelineMock) LowerSession() Session {
	if mock.LowerSessionFunc == nil {
		panic("PipelineMock.LowerSessionFunc: method is nil but Pipeline.LowerSession was just called")
	}
	callInfo := struct {
	}{}
	mock.lockLowerSession.Lock()
	mock.calls.LowerSession = append(mock.calls.LowerSession, callInfo)
	mock.lockLowerSession.Unlock()
	return mock.LowerSessionFunc()
}

// LowerSessionCalls gets all the calls that were made to LowerSession.
// Check the length with:
//
//	len(mockedPipeline.LowerSessionCalls())
func (mock *PipelineMock) LowerSessionCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockLowerSession.RLock()
	calls = mock.calls.LowerSession
	mock.lockLowerSession.RUnlock()
	return calls
}

// Ensure, that SessionProviderMock does implement SessionProvider.
// If this is not the case, regenerate this file with moq.
var _ SessionProvider = &SessionProviderMock{}

// SessionProviderMock is a mock implementation of SessionProvider.
//
//	func TestSomethingThatUsesSessionProvider(t *testing.T) {
//
//		// make and configure a mocked SessionProvider
//		mockedSessionProvider := &SessionProviderMock{
//			NewFunc: func() Session {
//				panic("mock out the New method")
//			},
//		}
//
//		// use mockedSessionProvider in code that requires SessionProvider
//		// and then make assertions.
//
//	}
type SessionProviderMock struct {
	// NewFunc mocks the New method.
	NewFunc func() Session

	// calls tracks calls to the methods.
	calls struct {
		// New holds details about calls to the New method.
		New []struct {
		}
	}
	lockNew sync.RWMutex
}

// New calls NewFunc.
func (mock *SessionProviderMock) New() Session {
	if mock.NewFunc == nil {
		panic("SessionProviderMock.NewFunc: method is nil but SessionProvider.New was just called")
	}
	callInfo := struct {
	}{}
	mock.lockNew.Lock()
	mock.calls.New = append(mock.calls.New, callInfo)
	mock.lockNew.Unlock()
	return mock.NewFunc()
}

// NewCalls gets all the calls that were made to New.
// Check the length with:
//
//	len(mockedSessionProvider.NewCalls())
func (mock *SessionProviderMock) NewCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockNew.RLock()
	calls = mock.calls.New
	mock.lockNew.RUnlock()
	return calls
}

// Ensure, that SessionMock does implement Session.
// If this is not the case, regenerate this file with moq.
var _ Session = &SessionMock{}

// SessionMock is a mock implementation of Session.
//
//	func TestSomethingThatUsesSession(t *testing.T) {
//
//		// make and configure a mocked Session
//		mockedSession := &SessionMock{
//			AddDelayedCallFunc: func(d time.Duration, fn func())  {
//				panic("mock out the AddDelayedCall method")
//			},
//			AddNextCallFunc: func(fn func())  {
//				panic("mock out the AddNextCall method")
//			},
//			ExecuteFunc: func()  {
//				panic("mock out the Execute method")
//			},
//			GetLowerFunc: func() Session {
//				panic("mock out the GetLower method")
//			},
//		}
//
//		// use mockedSession in code that requires Session
//		// and then make assertions.
//
//	}
type SessionMock struct {
	// AddDelayedCallFunc mocks the AddDelayedCall method.
	AddDelayedCallFunc func(d time.Duration, fn func())

	// AddNextCallFunc mocks the AddNextCall method.
	AddNextCallFunc func(fn func())

	// ExecuteFunc mocks the Execute method.
	ExecuteFunc func()

	// GetLowerFunc mocks the GetLower method.
	GetLowerFunc func() Session

	// calls tracks calls to the methods.
	calls struct {
		// AddDelayedCall holds details about calls to the AddDelayedCall method.
		AddDelayedCall []struct {
			// D is the d argument value.
			D time.Duration
			// Fn is the fn argument value.
			Fn func()
		}
		// AddNextCall holds details about calls to the AddNextCall method.
		AddNextCall []struct {
			// Fn is the fn argument value.
			Fn func()
		}
		// Execute holds details about calls to the Execute method.
		Execute []struct {
		}
		// GetLower holds details about calls to the GetLower method.
		GetLower []struct {
		}
	}
	lockAddDelayedCall sync.RWMutex
	lockAddNextCall    sync.RWMutex
	lockExecute        sync.RWMutex
	lockGetLower       sync.RWMutex
}

// AddDelayedCall calls AddDelayedCallFunc.
func (mock *SessionMock) AddDelayedCall(d time.Duration, fn func()) {
	if mock.AddDelayedCallFunc == nil {
		panic("SessionMock.AddDelayedCallFunc: method is nil but Session.AddDelayedCall was just called")
	}
	callInfo := struct {
		D  time.Duration
		Fn func()
	}{
		D:  d,
		Fn: fn,
	}
	mock.lockAddDelayedCall.Lock()
	mock.calls.AddDelayedCall = append(mock.calls.AddDelayedCall, callInfo)
	mock.lockAddDelayedCall.Unlock()
	mock.AddDelayedCallFunc(d, fn)
}

// AddDelayedCallCalls gets all the calls that were made to AddDelayedCall.
// Check the length with:
//
//	len(mockedSession.AddDelayedCallCalls())
func (mock *SessionMock) AddDelayedCallCalls() []struct {
	D  time.Duration
	Fn func()
} {
	var calls []struct {
		D  time.Duration
		Fn func()
	}
	mock.lockAddDelayedCall.RLock()
	calls = mock.calls.AddDelayedCall
	mock.lockAddDelayedCall.RUnlock()
	return calls
}

// AddNextCall calls AddNextCallFunc.
func (mock *SessionMock) AddNextCall(fn func()) {
	if mock.AddNextCallFunc == nil {
		panic("SessionMock.AddNextCallFunc: method is nil but Session.AddNextCall was just called")
	}
	callInfo := struct {
		Fn func()
	}{
		Fn: fn,
	}
	mock.lockAddNextCall.Lock()
	mock.calls.AddNextCall = append(mock.calls.AddNextCall, callInfo)
	mock.lockAddNextCall.Unlock()
	mock.AddNextCallFunc(fn)
}

// AddNextCallCalls gets all the calls that were made to AddNextCall.
// Check the length with:
//
//	len(mockedSession.AddNextCallCalls())
func (mock *SessionMock) AddNextCallCalls() []struct {
	Fn func()
} {
	var calls []struct {
		Fn func()
	}
	mock.lockAddNextCall.RLock()
	calls = mock.calls.AddNextCall
	mock.lockAddNextCall.RUnlock()
	return calls
}

// Execute calls ExecuteFunc.
func (mock *SessionMock) Execute() {
	if mock.ExecuteFunc == nil {
		panic("SessionMock.ExecuteFunc: method is nil but Session.Execute was just called")
	}
	callInfo := struct {
	}{}
	mock.lockExecute.Lock()
	mock.calls.Execute = append(mock.calls.Execute, callInfo)
	mock.lockExecute.Unlock()
	mock.ExecuteFunc()
}

// ExecuteCalls gets all the calls that were made to Execute.
// Check the length with:
//
//	len(mockedSession.ExecuteCalls())
func (mock *SessionMock) ExecuteCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockExecute.RLock()
	calls = mock.calls.Execute
	mock.lockExecute.RUnlock()
	return calls
}

// GetLower calls GetLowerFunc.
func (mock *SessionMock) GetLower() Session {
	if mock.GetLowerFunc == nil {
		panic("SessionMock.GetLowerFunc: method is nil but Session.GetLower was just called")
	}
	callInfo := struct {
	}{}
	mock.lockGetLower.Lock()
	mock.calls.GetLower = append(mock.calls.GetLower, callInfo)
	mock.lockGetLower.Unlock()
	return mock.GetLowerFunc()
}

// GetLowerCalls gets all the calls that were made to GetLower.
// Check the length with:
//
//	len(mockedSession.GetLowerCalls())
func (mock *SessionMock) GetLowerCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockGetLower.RLock()
	calls = mock.calls.GetLower
	mock.lockGetLower.RUnlock()
	return calls
}
