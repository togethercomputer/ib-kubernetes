package plugins

import (
	"net"
	"time"
)

type SubnetManagerClient interface {
	// Name returns the name of the plugin
	Name() string

	// SpecVersion returns the version of the spec of the plugin
	Spec() string

	// Validate Check the client can reach the subnet manager and return error in case if it is not reachable.
	Validate() error

	// AddGuidsToPKey add pkey for the given guid.
	// It return error if failed.
	AddGuidsToPKey(pkey int, guids []net.HardwareAddr) error

	// AddGuidsToLimitedPKey add guids as limited members to pkey.
	// It return error if failed.
	AddGuidsToLimitedPKey(pkey int, guids []net.HardwareAddr) error

	// RemoveGuidsFromPKey remove guids for given pkey.
	// It return error if failed.
	RemoveGuidsFromPKey(pkey int, guids []net.HardwareAddr) error

	// ListGuidsInUse returns a list of all GUIDS associated with PKeys
	ListGuidsInUse() ([]string, error)

	// SetConfig allows the daemon to pass configuration to the plugin
	SetConfig(config map[string]interface{}) error

	// GetLastPKeyUpdateTimestamp returns the last update timestamp from the subnet manager.
	GetLastPKeyUpdateTimestamp() (time.Time, error)

	// GetServerTime returns the current time according to the subnet manager.
	GetServerTime() (time.Time, error)
}
