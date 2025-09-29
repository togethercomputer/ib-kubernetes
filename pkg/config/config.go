package config

import (
	"fmt"
	"strings"

	"github.com/caarlos0/env/v11"
	"github.com/rs/zerolog/log"
)

type DaemonConfig struct {
	// Interval between every check for the added and deleted pods
	PeriodicUpdate int `env:"DAEMON_PERIODIC_UPDATE" envDefault:"5"`
	GUIDPool       GUIDPoolConfig
	// Subnet manager plugin name
	Plugin string `env:"DAEMON_SM_PLUGIN"`
	// Subnet manager plugins path
	PluginPath string `env:"DAEMON_SM_PLUGIN_PATH" envDefault:"/plugins"`
	// Default partition key for limited membership
	DefaultLimitedPartition string `env:"DEFAULT_LIMITED_PARTITION"`
	// Enable IP over IB functionality
	EnableIPOverIB bool `env:"ENABLE_IP_OVER_IB" envDefault:"false"`
	// Enable index0 for primary pkey GUID additions
	EnableIndex0ForPrimaryPkey bool `env:"ENABLE_INDEX0_FOR_PRIMARY_PKEY" envDefault:"true"`
	// Managed resource names
	ManagedResourcesString string `env:"MANAGED_RESOURCE_NAMES"`
	ManagedResources       map[string]bool
}

type GUIDPoolConfig struct {
	// First guid in the pool
	RangeStart string `env:"GUID_POOL_RANGE_START" envDefault:"02:00:00:00:00:00:00:00"`
	// Last guid in the pool
	RangeEnd string `env:"GUID_POOL_RANGE_END" envDefault:"02:FF:FF:FF:FF:FF:FF:FF"`
}

func (dc *DaemonConfig) ReadConfig() error {
	log.Debug().Msg("Reading configuration environment variables")
	err := env.Parse(dc)

	// If IP over IB enabled - log at startup
	if dc.EnableIPOverIB {
		log.Warn().Msg("New partitions will be created with IP over IB enabled.")
	} else {
		log.Info().Msg("New partitions will be created with IP over IB disabled.")
	}

	// If index0 for primary pkey enabled - log at startup
	if dc.EnableIndex0ForPrimaryPkey {
		log.Info().Msg("Primary pkey GUID additions will be created with index0 enabled.")
	} else {
		log.Info().Msg("Primary pkey GUID additions will be created with index0 disabled.")
	}

	// If default limited partition is set - log at startup
	if dc.DefaultLimitedPartition != "" {
		log.Info().Msgf("Default limited partition is set to %s. New GUIDs will be added as limited members to this partition.", dc.DefaultLimitedPartition)
	} else {
		log.Info().Msg("Default limited partition is not set.")
	}

	// If managed resource names is set - log at startup
	log.Info().Msgf("ib-kubernetes will manage the following resources: %s.", dc.ManagedResourcesString)
	// Parse the managed resource names string into a set
	dc.ManagedResources = make(map[string]bool)
	for _, resource := range strings.Split(dc.ManagedResourcesString, ",") {
		if resource == "" {
			continue
		}
		dc.ManagedResources[resource] = true
	}

	return err
}

func (dc *DaemonConfig) ValidateConfig() error {
	log.Debug().Msgf("Validating configurations %+v", dc)
	if dc.PeriodicUpdate <= 0 {
		return fmt.Errorf("invalid \"PeriodicUpdate\" value %d", dc.PeriodicUpdate)
	}

	if dc.Plugin == "" {
		return fmt.Errorf("no plugin selected")
	}

	if len(dc.ManagedResources) == 0 {
		return fmt.Errorf("no managed resources names were provided")
	}
	return nil
}

func (dc *DaemonConfig) IsManagedResource(resourceName string) bool {
	_, ok := dc.ManagedResources[resourceName]
	return ok
}
