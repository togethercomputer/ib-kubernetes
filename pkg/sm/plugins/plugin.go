// Copyright 2025 NVIDIA CORPORATION & AFFILIATES
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

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
