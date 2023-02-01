// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package proxy

import (
	"sync"
)

// Ensure, that RouteMock does implement Route.
// If this is not the case, regenerate this file with moq.
var _ Route = &RouteMock{}

// RouteMock is a mock implementation of Route.
//
//	func TestSomethingThatUsesRoute(t *testing.T) {
//
//		// make and configure a mocked Route
//		mockedRoute := &RouteMock{
//			SelectServerFunc: func(key string, failedServers []ServerID) ServerID {
//				panic("mock out the SelectServer method")
//			},
//		}
//
//		// use mockedRoute in code that requires Route
//		// and then make assertions.
//
//	}
type RouteMock struct {
	// SelectServerFunc mocks the SelectServer method.
	SelectServerFunc func(key string, failedServers []ServerID) ServerID

	// calls tracks calls to the methods.
	calls struct {
		// SelectServer holds details about calls to the SelectServer method.
		SelectServer []struct {
			// Key is the key argument value.
			Key string
			// FailedServers is the failedServers argument value.
			FailedServers []ServerID
		}
	}
	lockSelectServer sync.RWMutex
}

// SelectServer calls SelectServerFunc.
func (mock *RouteMock) SelectServer(key string, failedServers []ServerID) ServerID {
	if mock.SelectServerFunc == nil {
		panic("RouteMock.SelectServerFunc: method is nil but Route.SelectServer was just called")
	}
	callInfo := struct {
		Key           string
		FailedServers []ServerID
	}{
		Key:           key,
		FailedServers: failedServers,
	}
	mock.lockSelectServer.Lock()
	mock.calls.SelectServer = append(mock.calls.SelectServer, callInfo)
	mock.lockSelectServer.Unlock()
	return mock.SelectServerFunc(key, failedServers)
}

// SelectServerCalls gets all the calls that were made to SelectServer.
// Check the length with:
//
//	len(mockedRoute.SelectServerCalls())
func (mock *RouteMock) SelectServerCalls() []struct {
	Key           string
	FailedServers []ServerID
} {
	var calls []struct {
		Key           string
		FailedServers []ServerID
	}
	mock.lockSelectServer.RLock()
	calls = mock.calls.SelectServer
	mock.lockSelectServer.RUnlock()
	return calls
}