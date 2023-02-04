package proxy

import "fmt"

// ServerID ...
type ServerID int

// ServerConfig is a constraint for server config type
type ServerConfig interface {
	// GetID returns the server id, must be unique
	GetID() ServerID
}

//go:generate moq -rm -out proxy_mocks_test.go . Route Selector ServerStats

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

	// SelectServer choose a server id, will keep in this server id unless Reset is call or failed server added
	SelectServer(key string) ServerID

	// SelectForDelete choose servers for deleting
	SelectForDelete(key string) []ServerID

	// Reset the selection
	Reset()
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

// Address ...
func (c SimpleServerConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// ServerStats is thread safe
type ServerStats interface {
	// IsServerFailed check whether the server is currently not connected
	IsServerFailed(server ServerID) bool

	// NotifyServerFailed ...
	NotifyServerFailed(server ServerID)

	// GetMemUsage returns memory usage in bytes
	GetMemUsage(server ServerID) float64
}
