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

	client      *Memcache
	sess        memproxy.Session
	pipeSession memproxy.Session

	pipelines map[ServerID]memproxy.Pipeline

	failedServers []ServerID
}

// Pipeline ...
func (m *Memcache) Pipeline(
	ctx context.Context, sess memproxy.Session, _ ...memproxy.PipelineOption,
) memproxy.Pipeline {
	return &Pipeline{
		ctx: ctx,

		client:      m,
		pipeSession: sess,
		sess:        sess.GetLower(),

		pipelines: map[ServerID]memproxy.Pipeline{},
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
	serverID := p.client.route.SelectServer(key, p.failedServers)
	pipe := p.getRoutePipeline(serverID)

	fn := pipe.LeaseGet(key, options)

	var resp memproxy.LeaseGetResponse
	var err error

	p.sess.AddNextCall(func() {
		resp, err = fn()
		if err != nil {
			p.failedServers = append(p.failedServers, serverID)
			newServerID := p.client.route.SelectServer(key, p.failedServers)

			pipe := p.getRoutePipeline(newServerID)
			fn = pipe.LeaseGet(key, options)

			p.sess.AddNextCall(func() {
				resp, err = fn()
			})
		}
	})

	return func() (memproxy.LeaseGetResponse, error) {
		p.sess.Execute()
		return resp, err
	}
}

// LeaseSet ...
func (*Pipeline) LeaseSet(
	string, []byte, uint64,
	memproxy.LeaseSetOptions,
) func() (memproxy.LeaseSetResponse, error) {
	return func() (memproxy.LeaseSetResponse, error) {
		return memproxy.LeaseSetResponse{}, nil
	}
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
