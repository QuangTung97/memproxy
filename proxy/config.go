package proxy

// ServerID ...
type ServerID int

// ServerConfig is a constraint for server config type
type ServerConfig interface {
	// GetID returns the server id, must be unique
	GetID() ServerID
}

//go:generate moq -rm -out proxy_mocks_test.go . Route Selector

// Route must be Thread Safe
type Route interface {
	// NewSelector ...
	NewSelector() Selector
}

// Selector is NOT thread safe
type Selector interface {
	// SetFailedServer ...
	SetFailedServer(server ServerID)

	// HasNextAvailableServer check if next available server ready to be fallback to
	HasNextAvailableServer() bool

	// SelectServer choose a server id
	SelectServer(key string) ServerID

	// SelectForDelete choose servers for deleting
	SelectForDelete(key string) []ServerID
}

// ReplicatedRoute ...
type ReplicatedRoute struct {
	Children []ServerID

	// MemScoreFunc for calculating scores from memory size (in bytes) of memcached servers
	MemScoreFunc func(memSize float64) float64

	// MinPercentage specify a lower bound for percentage of requests into memcached servers
	MinPercentage float64
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
