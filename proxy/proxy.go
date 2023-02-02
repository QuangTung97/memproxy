package proxy

import (
	"context"
	"github.com/QuangTung97/memproxy"
)

// Memcache is thread safe
type Memcache struct {
	clients map[ServerID]memproxy.Memcache
	route   Route
}

// New ...
func New[S ServerConfig](
	conf Config[S],
	newFunc func(conf S) (memproxy.Memcache, error),
) (memproxy.Memcache, error) {
	clients := map[ServerID]memproxy.Memcache{}

	for _, server := range conf.Servers {
		client, err := newFunc(server)
		if err != nil {
			return nil, err
		}
		clients[server.GetID()] = client
	}

	return &Memcache{
		clients: clients,
		route:   conf.Route,
	}, nil
}

// Pipeline is NOT thread safe
type Pipeline struct {
	ctx context.Context

	client   *Memcache
	selector Selector

	sess        memproxy.Session
	pipeSession memproxy.Session

	pipelines       map[ServerID]memproxy.Pipeline
	leaseSetServers map[string]ServerID
}

// Pipeline ...
func (m *Memcache) Pipeline(
	ctx context.Context, sess memproxy.Session, _ ...memproxy.PipelineOption,
) memproxy.Pipeline {
	return &Pipeline{
		ctx: ctx,

		client:   m,
		selector: m.route.NewSelector(),

		pipeSession: sess,
		sess:        sess.GetLower(),

		pipelines:       map[ServerID]memproxy.Pipeline{},
		leaseSetServers: map[string]ServerID{},
	}
}

// Get ...
func (*Pipeline) Get(
	_ string, _ memproxy.GetOptions,
) func() (memproxy.GetResponse, error) {
	return func() (memproxy.GetResponse, error) {
		return memproxy.GetResponse{}, nil
	}
}

func (p *Pipeline) getRoutePipeline(serverID ServerID) memproxy.Pipeline {
	pipe, existed := p.pipelines[serverID]
	if !existed {
		pipe = p.client.clients[serverID].Pipeline(p.ctx, p.pipeSession)
		p.pipelines[serverID] = pipe
	}
	return pipe
}

// LeaseGet ...
func (p *Pipeline) LeaseGet(
	key string, options memproxy.LeaseGetOptions,
) func() (memproxy.LeaseGetResponse, error) {
	serverID := p.selector.SelectServer(key)
	pipe := p.getRoutePipeline(serverID)

	fn := pipe.LeaseGet(key, options)

	var resp memproxy.LeaseGetResponse
	var err error

	p.sess.AddNextCall(func() {
		resp, err = fn()
		if err != nil {
			p.selector.SetFailedServer(serverID)
			if !p.selector.HasNextAvailableServer() {
				return
			}

			serverID = p.selector.SelectServer(key)

			pipe := p.getRoutePipeline(serverID)
			fn = pipe.LeaseGet(key, options)

			p.sess.AddNextCall(func() {
				resp, err = fn()
				if err == nil && resp.Status == memproxy.LeaseGetStatusLeaseGranted {
					p.leaseSetServers[key] = serverID
				}
			})
			return
		}

		if resp.Status == memproxy.LeaseGetStatusLeaseGranted {
			p.leaseSetServers[key] = serverID
		}
	})

	return func() (memproxy.LeaseGetResponse, error) {
		// TODO Do Execute Flush Commands
		p.sess.Execute()
		p.selector.Reset()
		return resp, err
	}
}

// LeaseSet ...
func (p *Pipeline) LeaseSet(
	key string, data []byte, cas uint64,
	options memproxy.LeaseSetOptions,
) func() (memproxy.LeaseSetResponse, error) {
	serverID, ok := p.leaseSetServers[key]
	if !ok {
		return func() (memproxy.LeaseSetResponse, error) {
			return memproxy.LeaseSetResponse{}, nil
		}
	}
	pipe := p.getRoutePipeline(serverID)
	return pipe.LeaseSet(key, data, cas, options)
}

// Delete ...
func (*Pipeline) Delete(
	string, memproxy.DeleteOptions,
) func() (memproxy.DeleteResponse, error) {
	return func() (memproxy.DeleteResponse, error) {
		return memproxy.DeleteResponse{}, nil
	}
}

// Execute ...
func (*Pipeline) Execute() {
}

// Finish ...
func (*Pipeline) Finish() {
}

// LowerSession returns a lower priority session
func (p *Pipeline) LowerSession() memproxy.Session {
	return p.sess.GetLower()
}
