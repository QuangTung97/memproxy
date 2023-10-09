package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLeaseGetStatePool(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		s := getLeaseGetState()
		assert.Equal(t, &leaseGetState{}, s)

		s.serverID = 123

		putLeaseGetState(s)
		s = getLeaseGetState()
		assert.Equal(t, &leaseGetState{}, s)
	})
}
