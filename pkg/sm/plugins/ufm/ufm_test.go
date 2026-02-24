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

package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"

	"github.com/Mellanox/ib-kubernetes/pkg/drivers/http/mocks"
)

var _ = Describe("Ufm Subnet Manager Client plugin", func() {
	Context("Initialize", func() {
		AfterEach(func() {
			os.Clearenv()
		})
		It("Initialize ufm plugin", func() {
			Expect(os.Setenv("UFM_USERNAME", "admin")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_PASSWORD", "123456")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_ADDRESS", "1.1.1.1")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_PORT", "80")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_HTTP_SCHEMA", "http")).ToNot(HaveOccurred())
			plugin, err := Initialize()
			Expect(err).ToNot(HaveOccurred())
			Expect(plugin).ToNot(BeNil())
			Expect(plugin.Name()).To(Equal("ufm"))
			Expect(plugin.Spec()).To(Equal("1.0"))
		})
	})
	Context("newUfmPlugin", func() {
		AfterEach(func() {
			os.Clearenv()
		})
		It("newUfmPlugin ufm plugin", func() {
			Expect(os.Setenv("UFM_USERNAME", "admin")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_PASSWORD", "123456")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_ADDRESS", "1.1.1.1")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_HTTP_SCHEMA", "http")).ToNot(HaveOccurred())
			plugin, err := newUfmPlugin()
			Expect(err).ToNot(HaveOccurred())
			Expect(plugin).ToNot(BeNil())
			Expect(plugin.Name()).To(Equal("ufm"))
			Expect(plugin.Spec()).To(Equal("1.0"))
			Expect(plugin.conf.Port).To(Equal(80))
		})
		It("newUfmPlugin with missing address config", func() {
			Expect(os.Setenv("UFM_USERNAME", "admin")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_PASSWORD", "123456")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_HTTP_SCHEMA", "http")).ToNot(HaveOccurred())
			plugin, err := newUfmPlugin()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal(`missing one or more required fileds for ufm ["username", "password", "address"]`))
			Expect(plugin).To(BeNil())
		})
	})
	Context("Validate", func() {
		It("Validate connection to ufm", func() {
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return(nil, nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			err := plugin.Validate()
			Expect(err).ToNot(HaveOccurred())
		})
		It("Validate connection to ufm failed to connect", func() {
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return(nil, errors.New("failed"))

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			err := plugin.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("failed to connect to ufm subnet manager: failed"))
		})
	})
	Context("AddGuidsToPKey", func() {
		It("Add guid to valid pkey", func() {
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(`{"pkey": "0x1234"}`), nil)
			client.On("Post", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			guid, err := net.ParseMAC("11:22:33:44:55:66:77:88")
			Expect(err).ToNot(HaveOccurred())

			err = plugin.AddGuidsToPKey(0x1234, []net.HardwareAddr{guid})
			Expect(err).ToNot(HaveOccurred())
		})
		It("Add guid to valid pkey with index0 true", func() {
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(`{"pkey": "0x1234"}`), nil)
			client.On("Post", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{EnableIndex0ForPrimaryPkey: true}}
			guid, err := net.ParseMAC("11:22:33:44:55:66:77:88")
			Expect(err).ToNot(HaveOccurred())

			err = plugin.AddGuidsToPKey(0x1234, []net.HardwareAddr{guid})
			Expect(err).ToNot(HaveOccurred())

			// Verify the Post call was made with index0: true
			client.AssertCalled(GinkgoT(), "Post", mock.Anything, mock.Anything, mock.MatchedBy(func(data []byte) bool {
				return string(data) == `{"pkey": "0x1234", "guids": ["1122334455667788"], "membership": "full", "index0": true}`
			}))
		})
		It("Add guid to valid pkey with index0 false", func() {
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(`{"pkey": "0x1234"}`), nil)
			client.On("Post", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{EnableIndex0ForPrimaryPkey: false}}
			guid, err := net.ParseMAC("11:22:33:44:55:66:77:88")
			Expect(err).ToNot(HaveOccurred())

			err = plugin.AddGuidsToPKey(0x1234, []net.HardwareAddr{guid})
			Expect(err).ToNot(HaveOccurred())

			// Verify the Post call was made with index0: false
			client.AssertCalled(GinkgoT(), "Post", mock.Anything, mock.Anything, mock.MatchedBy(func(data []byte) bool {
				return string(data) == `{"pkey": "0x1234", "guids": ["1122334455667788"], "membership": "full", "index0": false}`
			}))
		})
		It("Add guid to invalid pkey", func() {
			plugin := &ufmPlugin{conf: UFMConfig{}}
			guid, err := net.ParseMAC("11:22:33:44:55:66:77:88")
			Expect(err).ToNot(HaveOccurred())

			err = plugin.AddGuidsToPKey(0xFFFF, []net.HardwareAddr{guid})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("invalid pkey 0xFFFF, out of range 0x0001 - 0xFFFE"))
		})
		It("Add guid to pkey failed from ufm", func() {
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(`{"pkey": "0x1234"}`), nil)
			client.On("Post", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed"))

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			guid, err := net.ParseMAC("11:22:33:44:55:66:77:88")
			Expect(err).ToNot(HaveOccurred())

			guids := []net.HardwareAddr{guid}
			pKey := 0x1234
			err = plugin.AddGuidsToPKey(pKey, guids)
			Expect(err).To(HaveOccurred())
			errMessage := fmt.Sprintf("failed to add guids %v to PKey 0x%04X with error: failed", guids, pKey)
			Expect(err.Error()).To(Equal(errMessage))
		})
	})
	Context("RemoveGuidsFromPKey", func() {
		It("Remove guid from valid pkey", func() {
			client := &mocks.Client{}
			client.On("Post", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			guid, err := net.ParseMAC("11:22:33:44:55:66:77:88")
			Expect(err).ToNot(HaveOccurred())

			err = plugin.RemoveGuidsFromPKey(0x1234, []net.HardwareAddr{guid})
			Expect(err).ToNot(HaveOccurred())
		})
		It("Remove guid from invalid pkey", func() {
			plugin := &ufmPlugin{conf: UFMConfig{}}
			guid, err := net.ParseMAC("11:22:33:44:55:66:77:88")
			Expect(err).ToNot(HaveOccurred())

			err = plugin.RemoveGuidsFromPKey(0xFFFF, []net.HardwareAddr{guid})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("invalid pkey 0xFFFF, out of range 0x0001 - 0xFFFE"))
		})
		It("Remove guid from pkey failed from ufm", func() {
			client := &mocks.Client{}
			client.On("Post", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed"))

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			guid, err := net.ParseMAC("11:22:33:44:55:66:77:88")
			Expect(err).ToNot(HaveOccurred())

			guids := []net.HardwareAddr{guid}
			pKey := 0x1234
			err = plugin.RemoveGuidsFromPKey(pKey, guids)
			Expect(err).To(HaveOccurred())
			errMessage := fmt.Sprintf("failed to delete guids %v from PKey 0x%04X, with error: failed",
				guids, pKey)
			errMsg := err.Error()
			Expect(&errMsg).To(Equal(&errMessage))
		})
	})
	Context("AddGuidsToLimitedPKey", func() {
		It("Add guid to valid limited pkey that exists", func() {
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(`{"pkey": "0x1234"}`), nil)
			client.On("Post", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			guid, err := net.ParseMAC("11:22:33:44:55:66:77:88")
			Expect(err).ToNot(HaveOccurred())

			err = plugin.AddGuidsToLimitedPKey(0x1234, []net.HardwareAddr{guid})
			Expect(err).ToNot(HaveOccurred())
		})
		It("Add guid to valid limited pkey with index0 false", func() {
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(`{"pkey": "0x1234"}`), nil)
			client.On("Post", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			guid, err := net.ParseMAC("11:22:33:44:55:66:77:88")
			Expect(err).ToNot(HaveOccurred())

			err = plugin.AddGuidsToLimitedPKey(0x1234, []net.HardwareAddr{guid})
			Expect(err).ToNot(HaveOccurred())

			// Verify the Post call was made with index0: false
			client.AssertCalled(GinkgoT(), "Post", mock.Anything, mock.Anything, mock.MatchedBy(func(data []byte) bool {
				return string(data) == `{"pkey": "0x1234", "guids": ["1122334455667788"], "membership": "limited", "index0": false}`
			}))
		})
		It("Add guid to limited pkey that does not exist", func() {
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return(nil, errors.New("404"))

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			guid, err := net.ParseMAC("11:22:33:44:55:66:77:88")
			Expect(err).ToNot(HaveOccurred())

			err = plugin.AddGuidsToLimitedPKey(0x1234, []net.HardwareAddr{guid})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("limited pkey 0x1234 does not exist, will not create it"))
		})
		It("Add guid to invalid limited pkey", func() {
			plugin := &ufmPlugin{conf: UFMConfig{}}
			guid, err := net.ParseMAC("11:22:33:44:55:66:77:88")
			Expect(err).ToNot(HaveOccurred())

			err = plugin.AddGuidsToLimitedPKey(0xFFFF, []net.HardwareAddr{guid})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("invalid pkey 0xFFFF, out of range 0x0001 - 0xFFFE"))
		})
		It("Add guid to limited pkey failed from ufm", func() {
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(`{"pkey": "0x1234"}`), nil)
			client.On("Post", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed"))

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			guid, err := net.ParseMAC("11:22:33:44:55:66:77:88")
			Expect(err).ToNot(HaveOccurred())

			guids := []net.HardwareAddr{guid}
			pKey := 0x1234
			err = plugin.AddGuidsToLimitedPKey(pKey, guids)
			Expect(err).To(HaveOccurred())
			errMessage := fmt.Sprintf("failed to add guids %v as limited members to PKey 0x%04X with error: failed", guids, pKey)
			Expect(err.Error()).To(Equal(errMessage))
		})
	})
	// Context("createEmptyPKey with EnableIPOverIB", func() {
	// 	It("Create pkey with IP over IB enabled", func() {
	// 		client := &mocks.Client{}
	// 		client.On("Post", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// 		plugin := &ufmPlugin{client: client, conf: UFMConfig{EnableIPOverIB: true}}
	// 		err := plugin.createEmptyPKey(0x1234)
	// 		Expect(err).ToNot(HaveOccurred())

	// 		// Verify the Post call was made with ip_over_ib: true
	// 		client.AssertCalled(GinkgoT(), "Post", mock.Anything, mock.Anything, mock.MatchedBy(func(data []byte) bool {
	// 			return string(data) == `{"pkey": "0x1234", "index0": true, "ip_over_ib": true, "mtu_limit": 4, "service_level": 0, "rate_limit": 300, "guids": [], "membership": "full"}`
	// 		}))
	// 	})
	// 	It("Create pkey with IP over IB disabled", func() {
	// 		client := &mocks.Client{}
	// 		client.On("Post", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// 		plugin := &ufmPlugin{client: client, conf: UFMConfig{EnableIPOverIB: false}}
	// 		err := plugin.createEmptyPKey(0x1234)
	// 		Expect(err).ToNot(HaveOccurred())

	// 		// Verify the Post call was made with ip_over_ib: false
	// 		client.AssertCalled(GinkgoT(), "Post", mock.Anything, mock.Anything, mock.MatchedBy(func(data []byte) bool {
	// 			return string(data) == `{"pkey": "0x1234", "index0": true, "ip_over_ib": false, "mtu_limit": 4, "service_level": 0, "rate_limit": 300, "guids": [], "membership": "full"}`
	// 		}))
	// 	})
	// })
	Context("UFMConfig with new environment variables", func() {
		AfterEach(func() {
			os.Clearenv()
		})
		It("newUfmPlugin with EnableIPOverIB and DefaultLimitedPartition config", func() {
			Expect(os.Setenv("UFM_USERNAME", "admin")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_PASSWORD", "123456")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_ADDRESS", "1.1.1.1")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_HTTP_SCHEMA", "http")).ToNot(HaveOccurred())
			Expect(os.Setenv("ENABLE_IP_OVER_IB", "true")).ToNot(HaveOccurred())
			Expect(os.Setenv("DEFAULT_LIMITED_PARTITION", "0x1")).ToNot(HaveOccurred())
			plugin, err := newUfmPlugin()
			Expect(err).ToNot(HaveOccurred())
			Expect(plugin).ToNot(BeNil())
			Expect(plugin.conf.EnableIPOverIB).To(BeTrue())
			Expect(plugin.conf.DefaultLimitedPartition).To(Equal("0x1"))
		})
		It("newUfmPlugin with EnableIndex0ForPrimaryPkey config", func() {
			Expect(os.Setenv("UFM_USERNAME", "admin")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_PASSWORD", "123456")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_ADDRESS", "1.1.1.1")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_HTTP_SCHEMA", "http")).ToNot(HaveOccurred())
			Expect(os.Setenv("ENABLE_INDEX0_FOR_PRIMARY_PKEY", "false")).ToNot(HaveOccurred())
			plugin, err := newUfmPlugin()
			Expect(err).ToNot(HaveOccurred())
			Expect(plugin).ToNot(BeNil())
			Expect(plugin.conf.EnableIndex0ForPrimaryPkey).To(BeFalse())
		})
		It("newUfmPlugin with default EnableIPOverIB config", func() {
			Expect(os.Setenv("UFM_USERNAME", "admin")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_PASSWORD", "123456")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_ADDRESS", "1.1.1.1")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_HTTP_SCHEMA", "http")).ToNot(HaveOccurred())
			plugin, err := newUfmPlugin()
			Expect(err).ToNot(HaveOccurred())
			Expect(plugin).ToNot(BeNil())
			Expect(plugin.conf.EnableIPOverIB).To(BeFalse())            // Default should be false
			Expect(plugin.conf.DefaultLimitedPartition).To(Equal(""))   // Default should be empty
			Expect(plugin.conf.EnableIndex0ForPrimaryPkey).To(BeTrue()) // Default should be true
		})
		It("newUfmPlugin with explicit false EnableIPOverIB config", func() {
			Expect(os.Setenv("UFM_USERNAME", "admin")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_PASSWORD", "123456")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_ADDRESS", "1.1.1.1")).ToNot(HaveOccurred())
			Expect(os.Setenv("UFM_HTTP_SCHEMA", "http")).ToNot(HaveOccurred())
			Expect(os.Setenv("ENABLE_IP_OVER_IB", "false")).ToNot(HaveOccurred())
			plugin, err := newUfmPlugin()
			Expect(err).ToNot(HaveOccurred())
			Expect(plugin).ToNot(BeNil())
			Expect(plugin.conf.EnableIPOverIB).To(BeFalse())
			Expect(plugin.conf.DefaultLimitedPartition).To(Equal(""))
		})
	})
	Context("ListGuidsInUse", func() {
		It("Remove guid from valid pkey", func() {
			testResponse := `{
				"0x7fff": {
					"guids": []
				},
				"0x7aff": {
					"test": "val"
				},
				"0x5": {
					"guids": [
						{
							"guid": "020000000000003e"
						},
						{
							"guid": "02000FF000FF0009"
						}
					]
				},
				"0x6": {
					"guids": [
						{
							"guid": "0200000000000000"
						}
					]
				}
			}`

			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(testResponse), nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			guids, err := plugin.ListGuidsInUse()
			Expect(err).ToNot(HaveOccurred())

			expectedGuids := map[string]string{
				"02:00:00:00:00:00:00:3e": "0x5",
				"02:00:0F:F0:00:FF:00:09": "0x5",
				"02:00:00:00:00:00:00:00": "0x6",
			}
			Expect(guids).To(Equal(expectedGuids))
		})
	})
	Context("GetLastPKeyUpdateTimestamp", func() {
		It("Get last update timestamp successfully with DD-MM-YYYY format (newer UFM)", func() {
			testResponse := `{"last_updated": "19-01-2026, 20:34:50"}`
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(testResponse), nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			timestamp, err := plugin.GetLastPKeyUpdateTimestamp()
			Expect(err).ToNot(HaveOccurred())
			Expect(timestamp.Year()).To(Equal(2026))
			Expect(timestamp.Month()).To(Equal(time.January))
			Expect(timestamp.Day()).To(Equal(19))
			Expect(timestamp.Hour()).To(Equal(20))
			Expect(timestamp.Minute()).To(Equal(34))
			Expect(timestamp.Second()).To(Equal(50))
		})
		It("Get last update timestamp successfully with double-digit day", func() {
			testResponse := `{"last_updated": "Thu Sep 13 11:42:39 UTC 2020"}`
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(testResponse), nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			timestamp, err := plugin.GetLastPKeyUpdateTimestamp()
			Expect(err).ToNot(HaveOccurred())
			Expect(timestamp.Year()).To(Equal(2020))
			Expect(timestamp.Month()).To(Equal(time.September))
			Expect(timestamp.Day()).To(Equal(13))
			Expect(timestamp.Hour()).To(Equal(11))
			Expect(timestamp.Minute()).To(Equal(42))
			Expect(timestamp.Second()).To(Equal(39))
		})
		It("Get last update timestamp successfully with single-digit day (padded)", func() {
			testResponse := `{"last_updated": "Thu Sep  3 11:42:39 UTC 2020"}`
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(testResponse), nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			timestamp, err := plugin.GetLastPKeyUpdateTimestamp()
			Expect(err).ToNot(HaveOccurred())
			Expect(timestamp.Year()).To(Equal(2020))
			Expect(timestamp.Month()).To(Equal(time.September))
			Expect(timestamp.Day()).To(Equal(3))
		})
		It("Get last update timestamp returns zero time when null", func() {
			testResponse := `{"last_updated": null}`
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(testResponse), nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			timestamp, err := plugin.GetLastPKeyUpdateTimestamp()
			Expect(err).ToNot(HaveOccurred())
			Expect(timestamp.IsZero()).To(BeTrue())
		})
		It("Get last update timestamp fails on network error", func() {
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return(nil, errors.New("network error"))

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			_, err := plugin.GetLastPKeyUpdateTimestamp()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get PKey last updated timestamp"))
		})
		It("Get last update timestamp fails on invalid JSON", func() {
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(`invalid json`), nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			_, err := plugin.GetLastPKeyUpdateTimestamp()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to parse PKey last updated response"))
		})
		It("Get last update timestamp fails on invalid timestamp format", func() {
			testResponse := `{"last_updated": "invalid-timestamp-format"}`
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(testResponse), nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			_, err := plugin.GetLastPKeyUpdateTimestamp()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to parse last_updated timestamp"))
		})
		It("timezone-aware format takes precedence and ignores SMTimezone", func() {
			// When UFM returns a string with timezone (e.g. UTC), we parse it as-is.
			// SMTimezone should not be used for this path.
			testResponse := `{"last_updated": "Thu Sep 13 11:42:39 UTC 2020"}`
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(testResponse), nil)

			nyLoc, _ := time.LoadLocation("America/New_York")
			plugin := &ufmPlugin{client: client, conf: UFMConfig{SMTimezone: "America/New_York"}, smLocation: nyLoc}
			timestamp, err := plugin.GetLastPKeyUpdateTimestamp()
			Expect(err).ToNot(HaveOccurred())
			// Result must be the instant in UTC (11:42:39 UTC), not interpreted in Eastern
			Expect(timestamp.UTC().Year()).To(Equal(2020))
			Expect(timestamp.UTC().Month()).To(Equal(time.September))
			Expect(timestamp.UTC().Day()).To(Equal(13))
			Expect(timestamp.UTC().Hour()).To(Equal(11))
			Expect(timestamp.UTC().Minute()).To(Equal(42))
			Expect(timestamp.UTC().Second()).To(Equal(39))
		})
		It("local format (DD-MM-YYYY) is parsed using SMTimezone and converted to UTC", func() {
			// 19-01-2026 15:34:50 in America/New_York (EST, UTC-5) = 20:34:50 UTC
			testResponse := `{"last_updated": "19-01-2026, 15:34:50"}`
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(testResponse), nil)

			nyLoc, _ := time.LoadLocation("America/New_York")
			plugin := &ufmPlugin{client: client, conf: UFMConfig{SMTimezone: "America/New_York"}, smLocation: nyLoc}
			timestamp, err := plugin.GetLastPKeyUpdateTimestamp()
			Expect(err).ToNot(HaveOccurred())
			Expect(timestamp.Year()).To(Equal(2026))
			Expect(timestamp.Month()).To(Equal(time.January))
			Expect(timestamp.Day()).To(Equal(19))
			Expect(timestamp.Hour()).To(Equal(20))
			Expect(timestamp.Minute()).To(Equal(34))
			Expect(timestamp.Second()).To(Equal(50))
		})
		It("nil smLocation falls back to UTC for local format", func() {
			// If SetConfig was never called, smLocation is nil.
			// Local-format timestamps should be parsed as UTC.
			testResponse := `{"last_updated": "19-01-2026, 15:34:50"}`
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(testResponse), nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			timestamp, err := plugin.GetLastPKeyUpdateTimestamp()
			Expect(err).ToNot(HaveOccurred())
			// With no timezone set, should be interpreted as UTC (no offset)
			Expect(timestamp.Hour()).To(Equal(15))
		})
		It("DST is handled automatically (summer EDT = UTC-4)", func() {
			// 15-07-2026 15:00:00 in America/New_York during July = EDT (UTC-4) = 19:00:00 UTC
			testResponse := `{"last_updated": "15-07-2026, 15:00:00"}`
			client := &mocks.Client{}
			client.On("Get", mock.Anything, mock.Anything).Return([]byte(testResponse), nil)

			nyLoc, _ := time.LoadLocation("America/New_York")
			plugin := &ufmPlugin{client: client, conf: UFMConfig{SMTimezone: "America/New_York"}, smLocation: nyLoc}
			timestamp, err := plugin.GetLastPKeyUpdateTimestamp()
			Expect(err).ToNot(HaveOccurred())
			Expect(timestamp.Year()).To(Equal(2026))
			Expect(timestamp.Month()).To(Equal(time.July))
			Expect(timestamp.Day()).To(Equal(15))
			// EDT is UTC-4, so 15:00 local = 19:00 UTC
			Expect(timestamp.Hour()).To(Equal(19))
			Expect(timestamp.Minute()).To(Equal(0))
		})
	})
	Context("SetConfig", func() {
		It("SetConfig with valid SM_TIMEZONE caches smLocation", func() {
			plugin := &ufmPlugin{conf: UFMConfig{}}
			err := plugin.SetConfig(map[string]interface{}{
				"SM_TIMEZONE": "America/New_York",
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(plugin.conf.SMTimezone).To(Equal("America/New_York"))
			Expect(plugin.smLocation).ToNot(BeNil())
			Expect(plugin.smLocation.String()).To(Equal("America/New_York"))
		})
		It("SetConfig with invalid SM_TIMEZONE returns error", func() {
			plugin := &ufmPlugin{conf: UFMConfig{}}
			err := plugin.SetConfig(map[string]interface{}{
				"SM_TIMEZONE": "Fake/Timezone",
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid SM_TIMEZONE"))
			Expect(plugin.smLocation).To(BeNil())
		})
	})
	Context("GetServerTime", func() {
		It("Get server time successfully", func() {
			expectedTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
			client := &mocks.Client{}
			client.On("GetServerTime", mock.Anything).Return(expectedTime, nil)

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			serverTime, err := plugin.GetServerTime()
			Expect(err).ToNot(HaveOccurred())
			Expect(serverTime).To(Equal(expectedTime))
		})
		It("Get server time fails on error", func() {
			client := &mocks.Client{}
			client.On("GetServerTime", mock.Anything).Return(time.Time{}, errors.New("connection refused"))

			plugin := &ufmPlugin{client: client, conf: UFMConfig{}}
			_, err := plugin.GetServerTime()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("connection refused"))
		})
	})
})
