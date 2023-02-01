package proxy

// ServerID ...
type ServerID int

// ServerConfig is a constraint for server config type
type ServerConfig interface {
	// GetID returns the server id, must be unique
	GetID() ServerID
}

//go:generate moq -rm -out proxy_mocks_test.go . Route

// Route ...
type Route interface {
	// SelectServer choose a server id
	SelectServer(key string, failedServers []ServerID) ServerID
}

// ReplicatedRouteConfig ...
type ReplicatedRouteConfig struct {
	Server ServerID
}

// ReplicatedRoute ...
type ReplicatedRoute struct {
	Children []ReplicatedRouteConfig
}

// Config ...
type Config[S ServerConfig] struct {
	Servers []S
	Route   Route
}

// SimpleServerConfig ...
type SimpleServerConfig struct {
	ID   ServerID
	Host string
	Port uint16
}

// GetID ...
func (c SimpleServerConfig) GetID() ServerID {
	return c.ID
}
