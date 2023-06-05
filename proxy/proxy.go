package proxy

import (
	"context"
	"errors"
	"fmt"
	"github.com/QuangTung97/go-memcache/memcache"
	"github.com/QuangTung97/memproxy"
)

// Memcache is thread safe
type Memcache struct {
	sessProvider memproxy.SessionProvider
	clients      map[ServerID]memproxy.Memcache
	route        Route
}

type memcacheConfig struct {
	sessProvider memproxy.SessionProvider
}

func computeMemcacheConfig(options ...MemcacheOption) *memcacheConfig {
	conf := &memcacheConfig{
		sessProvider: memproxy.NewSessionProvider(),
	}
	for _, fn := range options {
		fn(conf)
	}
	return conf
}

// MemcacheOption ...
type MemcacheOption func(conf *memcacheConfig)

// WithMemcacheSessionProvider ...
func WithMemcacheSessionProvider(provider memproxy.SessionProvider) MemcacheOption {
	return func(conf *memcacheConfig) {
		conf.sessProvider = provider
	}
}

// New ...
func New[S ServerConfig](
	conf Config[S],
	newFunc func(conf S) memproxy.Memcache,
	options ...MemcacheOption,
) (*Memcache, error) {
	if len(conf.Servers) == 0 {
		return nil, errors.New("proxy: empty server list")
	}

	if conf.Route == nil {
		return nil, errors.New("proxy: route is nil")
	}

	memcacheConf := computeMemcacheConfig(options...)

	clients := map[ServerID]memproxy.Memcache{}

	for _, server := range conf.Servers {
		client := newFunc(server)
		clients[server.GetID()] = client
	}

	allServerIDs := conf.Route.AllServerIDs()
	for _, serverID := range allServerIDs {
		_, ok := clients[serverID]
		if !ok {
			return nil, fmt.Errorf("proxy: server id '%d' not in server list", serverID)
		}
	}

	return &Memcache{
		sessProvider: memcacheConf.sessProvider,
		clients:      clients,
		route:        conf.Route,
	}, nil
}

// Pipeline is NOT thread safe
type Pipeline struct {
	ctx context.Context

	client   *Memcache
	selector Selector

	sess        memproxy.Session
	pipeSession memproxy.Session

	pipelines map[ServerID]memproxy.Pipeline

	needExecServers []ServerID
	//revive:disable-next-line:nested-structs
	needExecServerSet map[ServerID]struct{}

	leaseSetServers map[string]leaseSetState
}

type leaseSetState struct {
	valid    bool // for preventing a special race condition
	serverID ServerID
}

// Pipeline ...
func (m *Memcache) Pipeline(
	ctx context.Context, options ...memproxy.PipelineOption,
) memproxy.Pipeline {
	conf := memproxy.ComputePipelineConfig(options)
	sess := conf.GetSession(m.sessProvider)

	return &Pipeline{
		ctx: ctx,

		client:   m,
		selector: m.route.NewSelector(),

		pipeSession: sess,
		sess:        sess.GetLower(),

		pipelines: map[ServerID]memproxy.Pipeline{},

		leaseSetServers: map[string]leaseSetState{},
	}
}

// Close ...
func (m *Memcache) Close() error {
	var lastErr error
	for _, client := range m.clients {
		err := client.Close()
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (p *Pipeline) getRoutePipeline(serverID ServerID) memproxy.Pipeline {
	pipe, existed := p.pipelines[serverID]
	if !existed {
		pipe = p.client.clients[serverID].Pipeline(p.ctx, memproxy.WithPipelineExistingSession(p.pipeSession))
		p.pipelines[serverID] = pipe
	}

	if p.needExecServerSet == nil {
		p.needExecServerSet = map[ServerID]struct{}{
			serverID: {},
		}
		p.needExecServers = append(p.needExecServers, serverID)
	} else if _, existed := p.needExecServerSet[serverID]; !existed {
		p.needExecServerSet[serverID] = struct{}{}
		p.needExecServers = append(p.needExecServers, serverID)
	}

	return pipe
}

func (p *Pipeline) doExecuteForAllServers() {
	for _, server := range p.needExecServers {
		pipe := p.pipelines[server]
		pipe.Execute()
	}
	p.needExecServers = nil
	p.needExecServerSet = nil
}

func (p *Pipeline) setKeyForLeaseSet(
	key string,
	resp memproxy.LeaseGetResponse,
	serverID ServerID,
) {
	if resp.Status == memproxy.LeaseGetStatusLeaseGranted || resp.Status == memproxy.LeaseGetStatusLeaseRejected {
		prev, ok := p.leaseSetServers[key]
		if ok {
			if prev.serverID != serverID {
				prev.valid = false
				p.leaseSetServers[key] = prev
				return
			}
			return
		}

		p.leaseSetServers[key] = leaseSetState{
			valid:    true,
			serverID: serverID,
		}
	}
}

type leaseGetState struct {
	pipe     *Pipeline
	serverID ServerID
	key      string
	options  memproxy.LeaseGetOptions

	fn func() (memproxy.LeaseGetResponse, error)

	resp memproxy.LeaseGetResponse
	err  error
}

func (s *leaseGetState) retryOnOtherNode() {
	s.pipe.doExecuteForAllServers()
	s.resp, s.err = s.fn()
	if s.err == nil {
		s.pipe.setKeyForLeaseSet(s.key, s.resp, s.serverID)
	}
}

func (s *leaseGetState) nextFunc() {
	s.pipe.doExecuteForAllServers()
	s.resp, s.err = s.fn()

	if s.err != nil {
		s.pipe.selector.SetFailedServer(s.serverID)
		if !s.pipe.selector.HasNextAvailableServer() {
			return
		}

		s.serverID = s.pipe.selector.SelectServer(s.key)

		pipe := s.pipe.getRoutePipeline(s.serverID)
		s.fn = pipe.LeaseGet(s.key, s.options)

		s.pipe.sess.AddNextCall(s.retryOnOtherNode)
		return
	}

	s.pipe.setKeyForLeaseSet(s.key, s.resp, s.serverID)
}

func (s *leaseGetState) returnFunc() (memproxy.LeaseGetResponse, error) {
	s.pipe.sess.Execute()
	s.pipe.selector.Reset()
	return s.resp, s.err
}

// LeaseGet ...
func (p *Pipeline) LeaseGet(
	key string, options memproxy.LeaseGetOptions,
) func() (memproxy.LeaseGetResponse, error) {
	serverID := p.selector.SelectServer(key)

	pipe := p.getRoutePipeline(serverID)
	fn := pipe.LeaseGet(key, options)

	state := &leaseGetState{
		pipe:     p,
		serverID: serverID,
		key:      key,
		options:  options,

		fn: fn,
	}

	p.sess.AddNextCall(state.nextFunc)
	return state.returnFunc
}

// LeaseSet ...
func (p *Pipeline) LeaseSet(
	key string, data []byte, cas uint64,
	options memproxy.LeaseSetOptions,
) func() (memproxy.LeaseSetResponse, error) {
	setState, ok := p.leaseSetServers[key]
	if !ok || !setState.valid {
		return func() (memproxy.LeaseSetResponse, error) {
			return memproxy.LeaseSetResponse{}, nil
		}
	}
	pipe := p.getRoutePipeline(setState.serverID)
	return pipe.LeaseSet(key, data, cas, options)
}

// Delete ...
func (p *Pipeline) Delete(
	key string, options memproxy.DeleteOptions,
) func() (memproxy.DeleteResponse, error) {
	serverIDs := p.selector.SelectForDelete(key)
	fnList := make([]func() (memproxy.DeleteResponse, error), 0, len(serverIDs))
	for _, id := range serverIDs {
		fnList = append(fnList, p.getRoutePipeline(id).Delete(key, options))
	}

	return func() (memproxy.DeleteResponse, error) {
		var lastErr error
		for _, fn := range fnList {
			_, err := fn()
			if err != nil {
				lastErr = err
			}
		}
		return memproxy.DeleteResponse{}, lastErr
	}
}

// Execute ...
func (p *Pipeline) Execute() {
	p.doExecuteForAllServers()
}

// Finish ...
func (p *Pipeline) Finish() {
	for _, server := range p.needExecServers {
		pipe := p.pipelines[server]
		pipe.Finish()
	}
	p.needExecServers = nil
	p.needExecServerSet = nil
}

// LowerSession returns a lower priority session
func (p *Pipeline) LowerSession() memproxy.Session {
	return p.sess.GetLower()
}

// NewSimpleStats ...
func NewSimpleStats(servers []SimpleServerConfig, options ...SimpleStatsOption) *SimpleServerStats {
	return NewSimpleServerStats[SimpleServerConfig](servers, NewSimpleStatsClient, options...)
}

// NewSimpleReplicatedMemcache ...
func NewSimpleReplicatedMemcache(
	servers []SimpleServerConfig,
	numConnsPerServer int,
	stats ServerStats,
	options ...ReplicatedRouteOption,
) (*Memcache, func(), error) {
	serverIDs := make([]ServerID, 0, len(servers))
	for _, s := range servers {
		serverIDs = append(serverIDs, s.GetID())
	}

	conf := Config[SimpleServerConfig]{
		Servers: servers,
		Route:   NewReplicatedRoute(serverIDs, stats, options...),
	}

	mc, err := New[SimpleServerConfig](
		conf,
		func(conf SimpleServerConfig) memproxy.Memcache {
			client, err := memcache.New(conf.Address(), numConnsPerServer)
			if err != nil {
				panic(err)
			}
			return memproxy.NewPlainMemcache(client)
		},
	)
	if err != nil {
		return nil, nil, err
	}

	return mc, func() { _ = mc.Close() }, nil
}
