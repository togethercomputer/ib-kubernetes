package daemon

import (
	"errors"
	"net"
	"time"

	v1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	kapi "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/Mellanox/ib-kubernetes/pkg/config"
	"github.com/Mellanox/ib-kubernetes/pkg/guid"
	k8sClientMocks "github.com/Mellanox/ib-kubernetes/pkg/k8s-client/mocks"
	smMocks "github.com/Mellanox/ib-kubernetes/pkg/sm/plugins/mocks"
	"github.com/Mellanox/ib-kubernetes/pkg/utils"
	"github.com/Mellanox/ib-kubernetes/pkg/watcher"
	resEventHandler "github.com/Mellanox/ib-kubernetes/pkg/watcher/handler"
	handlerMocks "github.com/Mellanox/ib-kubernetes/pkg/watcher/handler/mocks"
)

// testWatcher is a simple mock watcher for integration tests
type testWatcher struct {
	handler *handlerMocks.ResourceEventHandler
}

func (w *testWatcher) RunBackground() watcher.StopFunc {
	return func() {}
}

func (w *testWatcher) GetHandler() resEventHandler.ResourceEventHandler {
	return w.handler
}

// Helper function to create a test pod with network annotations
func createTestPod(name, namespace, networkAnnotation string) *kapi.Pod {
	return &kapi.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID("test-uid-" + name),
			Annotations: map[string]string{
				v1.NetworkAttachmentAnnot: networkAnnotation,
			},
		},
		Spec: kapi.PodSpec{
			NodeName: "test-node",
		},
		Status: kapi.PodStatus{
			Phase: kapi.PodPending,
		},
	}
}

// Helper function to create a test pod with GUID already assigned
func createTestPodWithGUID(name, namespace, networkAnnotation string) *kapi.Pod {
	return &kapi.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID("test-uid-" + name),
			Annotations: map[string]string{
				v1.NetworkAttachmentAnnot: networkAnnotation,
			},
		},
		Spec: kapi.PodSpec{
			NodeName: "test-node",
		},
	}
}

// Helper function to create a NAD config
func createNADConfig(pkey string) string {
	return `{"cniVersion":"0.3.1","type":"ib-sriov","pkey":"` + pkey + `"}`
}

var _ = Describe("Daemon", func() {
	Context("canProceedWithPkeyModification", func() {
		var (
			d        *daemon
			smClient *smMocks.SubnetManagerClient
		)

		BeforeEach(func() {
			smClient = &smMocks.SubnetManagerClient{}
			d = &daemon{
				smClient: smClient,
			}
		})

		It("returns false and updates timestamp when SM returns error (fail open)", func() {
			smClient.On("GetLastPKeyUpdateTimestamp").Return(time.Time{}, errors.New("connection error"))
			serverTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
			smClient.On("GetServerTime").Return(serverTime, nil)

			result := d.canProceedWithPkeyModification()

			Expect(result).To(BeFalse())
		})

		It("returns true when SM last_updated is null (zero time)", func() {
			smClient.On("GetLastPKeyUpdateTimestamp").Return(time.Time{}, nil)
			serverTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
			smClient.On("GetServerTime").Return(serverTime, nil)

			result := d.canProceedWithPkeyModification()

			Expect(result).To(BeTrue())
			Expect(d.lastPkeyAPICallTimestamp).To(Equal(serverTime))
		})

		It("returns true when no previous local timestamp stored", func() {
			lastUpdated := time.Date(2024, time.January, 15, 10, 0, 0, 0, time.UTC)
			smClient.On("GetLastPKeyUpdateTimestamp").Return(lastUpdated, nil)
			serverTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
			smClient.On("GetServerTime").Return(serverTime, nil)

			// lastPkeyAPICallTimestamp is zero (default)
			result := d.canProceedWithPkeyModification()

			Expect(result).To(BeTrue())
			Expect(d.lastPkeyAPICallTimestamp).To(Equal(serverTime))
		})

		It("returns true when SM last_updated > stored timestamp (previous op completed)", func() {
			// Stored timestamp from previous API call
			d.lastPkeyAPICallTimestamp = time.Date(2024, time.January, 15, 10, 0, 0, 0, time.UTC)

			// SM reports a newer last_updated - meaning our previous call completed
			smLastUpdated := time.Date(2024, time.January, 15, 10, 5, 0, 0, time.UTC)
			smClient.On("GetLastPKeyUpdateTimestamp").Return(smLastUpdated, nil)

			newServerTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
			smClient.On("GetServerTime").Return(newServerTime, nil)

			result := d.canProceedWithPkeyModification()

			Expect(result).To(BeTrue())
			Expect(d.lastPkeyAPICallTimestamp).To(Equal(newServerTime))
		})

		It("returns false when SM last_updated <= stored timestamp (previous op still in progress)", func() {
			// Stored timestamp from previous API call
			d.lastPkeyAPICallTimestamp = time.Date(2024, time.January, 15, 10, 0, 0, 0, time.UTC)

			// SM reports an older last_updated - meaning our previous call hasn't completed
			smLastUpdated := time.Date(2024, time.January, 15, 9, 55, 0, 0, time.UTC)
			smClient.On("GetLastPKeyUpdateTimestamp").Return(smLastUpdated, nil)

			result := d.canProceedWithPkeyModification()

			Expect(result).To(BeFalse())
			// Timestamp should NOT be updated when returning false
			Expect(d.lastPkeyAPICallTimestamp).To(Equal(time.Date(2024, time.January, 15, 10, 0, 0, 0, time.UTC)))
		})

		It("returns false when SM last_updated equals stored timestamp", func() {
			storedTime := time.Date(2024, time.January, 15, 10, 0, 0, 0, time.UTC)
			d.lastPkeyAPICallTimestamp = storedTime

			// SM reports the same last_updated
			smClient.On("GetLastPKeyUpdateTimestamp").Return(storedTime, nil)

			result := d.canProceedWithPkeyModification()

			Expect(result).To(BeFalse())
		})
	})

	Context("updateLastPkeyAPICallTimestamp", func() {
		var (
			d        *daemon
			smClient *smMocks.SubnetManagerClient
		)

		BeforeEach(func() {
			smClient = &smMocks.SubnetManagerClient{}
			d = &daemon{
				smClient: smClient,
			}
		})

		It("updates timestamp when GetServerTime succeeds", func() {
			serverTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
			smClient.On("GetServerTime").Return(serverTime, nil)

			d.updateLastPkeyAPICallTimestamp()

			Expect(d.lastPkeyAPICallTimestamp).To(Equal(serverTime))
		})

		It("does not update timestamp when GetServerTime returns error", func() {
			originalTime := time.Date(2024, time.January, 15, 9, 0, 0, 0, time.UTC)
			d.lastPkeyAPICallTimestamp = originalTime

			smClient.On("GetServerTime").Return(time.Time{}, errors.New("connection error"))

			d.updateLastPkeyAPICallTimestamp()

			// Timestamp should remain unchanged
			Expect(d.lastPkeyAPICallTimestamp).To(Equal(originalTime))
		})

		It("does not update timestamp when GetServerTime returns zero time", func() {
			originalTime := time.Date(2024, time.January, 15, 9, 0, 0, 0, time.UTC)
			d.lastPkeyAPICallTimestamp = originalTime

			smClient.On("GetServerTime").Return(time.Time{}, nil)

			d.updateLastPkeyAPICallTimestamp()

			// Timestamp should remain unchanged
			Expect(d.lastPkeyAPICallTimestamp).To(Equal(originalTime))
		})

		It("only updates if new time is after existing timestamp", func() {
			// Set a future timestamp
			futureTime := time.Date(2025, time.January, 15, 10, 0, 0, 0, time.UTC)
			d.lastPkeyAPICallTimestamp = futureTime

			// Server returns an older time
			olderServerTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
			smClient.On("GetServerTime").Return(olderServerTime, nil)

			d.updateLastPkeyAPICallTimestamp()

			// Should NOT update because server time is older
			Expect(d.lastPkeyAPICallTimestamp).To(Equal(futureTime))
		})

		It("updates when new time is after existing timestamp", func() {
			olderTime := time.Date(2024, time.January, 15, 9, 0, 0, 0, time.UTC)
			d.lastPkeyAPICallTimestamp = olderTime

			newerServerTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
			smClient.On("GetServerTime").Return(newerServerTime, nil)

			d.updateLastPkeyAPICallTimestamp()

			Expect(d.lastPkeyAPICallTimestamp).To(Equal(newerServerTime))
		})
	})

	Context("canProceedWithPkeyModification edge cases", func() {
		var (
			d        *daemon
			smClient *smMocks.SubnetManagerClient
		)

		BeforeEach(func() {
			smClient = &smMocks.SubnetManagerClient{}
			d = &daemon{
				smClient: smClient,
			}
		})

		It("handles GetServerTime failure gracefully when proceeding", func() {
			// SM returns null last_updated (should proceed)
			smClient.On("GetLastPKeyUpdateTimestamp").Return(time.Time{}, nil)
			// But GetServerTime fails
			smClient.On("GetServerTime").Return(time.Time{}, errors.New("server time error"))

			result := d.canProceedWithPkeyModification()

			// Should still return true (proceed) but timestamp won't be updated
			Expect(result).To(BeTrue())
			Expect(d.lastPkeyAPICallTimestamp.IsZero()).To(BeTrue())
		})

		It("concurrent calls are serialized by mutex", func() {
			// This test verifies the mutex protects the timestamp
			serverTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
			smClient.On("GetLastPKeyUpdateTimestamp").Return(time.Time{}, nil)
			smClient.On("GetServerTime").Return(serverTime, nil)

			// Run multiple goroutines to test mutex protection
			done := make(chan bool, 10)
			for i := 0; i < 10; i++ {
				go func() {
					result := d.canProceedWithPkeyModification()
					Expect(result).To(BeTrue())
					done <- true
				}()
			}

			// Wait for all goroutines
			for i := 0; i < 10; i++ {
				<-done
			}

			// Timestamp should be set correctly
			Expect(d.lastPkeyAPICallTimestamp).To(Equal(serverTime))
		})
	})

	// ============================================================================
	// Integration Tests for Multi-PKey and Multi-Network Scenarios
	// ============================================================================

	Context("Integration: Multiple PKeys and Networks", func() {
		var (
			d         *daemon
			smClient  *smMocks.SubnetManagerClient
			k8sClient *k8sClientMocks.Client
			handler   *handlerMocks.ResourceEventHandler
			addMap    *utils.SynchronizedMap
			deleteMap *utils.SynchronizedMap
			guidPool  guid.Pool
		)

		BeforeEach(func() {
			smClient = smMocks.NewSubnetManagerClient()
			k8sClient = &k8sClientMocks.Client{}
			handler = &handlerMocks.ResourceEventHandler{}
			addMap = utils.NewSynchronizedMap()
			deleteMap = utils.NewSynchronizedMap()

			// Create a real GUID pool for testing
			poolConfig := &config.GUIDPoolConfig{
				RangeStart: "02:00:00:00:00:00:00:00",
				RangeEnd:   "02:00:00:00:00:00:00:FF",
			}
			var err error
			guidPool, err = guid.NewPool(poolConfig)
			Expect(err).ToNot(HaveOccurred())

			// Setup handler mock to return our maps
			handler.On("GetResults").Return(addMap, deleteMap)

			d = &daemon{
				smClient:          smClient,
				kubeClient:        k8sClient,
				guidPool:          guidPool,
				guidPodNetworkMap: make(map[string]string),
				config:            config.DaemonConfig{},
				watcher:           &testWatcher{handler: handler},
			}
		})

		It("processes multiple networks with the same pkey in a single batch", func() {
			// Setup: Two networks with the same pkey (0x1234)
			network1ID := "default_network1"
			network2ID := "default_network2"

			pod1 := createTestPod("pod1", "default", `[{"name":"network1","namespace":"default"}]`)
			pod2 := createTestPod("pod2", "default", `[{"name":"network2","namespace":"default"}]`)

			addMap.Set(network1ID, []*kapi.Pod{pod1})
			addMap.Set(network2ID, []*kapi.Pod{pod2})

			// Setup NAD mocks - both networks use the same pkey
			nad1 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network1", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x1234")},
			}
			nad2 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network2", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x1234")},
			}
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network1").Return(nad1, nil)
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network2").Return(nad2, nil)
			k8sClient.On("AddFinalizerToPod", mock.Anything, mock.Anything).Return(nil)
			k8sClient.On("AddFinalizerToNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			k8sClient.On("SetAnnotationsOnPod", mock.Anything, mock.Anything).Return(nil)

			// Setup SM client mocks
			serverTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
			smClient.On("GetLastPKeyUpdateTimestamp").Return(time.Time{}, nil) // No previous updates
			smClient.On("GetServerTime").Return(serverTime, nil)
			smClient.On("AddGuidsToPKey", mock.Anything, mock.Anything).Return(nil)

			// Execute
			d.AddPeriodicUpdate()

			// Verify: AddGuidsToPKey should have been called once for pkey 0x1234
			// with GUIDs from both networks batched together
			smClient.AssertCalled(GinkgoT(), "AddGuidsToPKey", 0x1234, mock.Anything)

			// Verify networks were removed from addMap after successful processing
			_, exists1 := addMap.Get(network1ID)
			_, exists2 := addMap.Get(network2ID)
			Expect(exists1).To(BeFalse(), "network1 should be removed from addMap")
			Expect(exists2).To(BeFalse(), "network2 should be removed from addMap")
		})

		It("processes multiple networks with different pkeys separately", func() {
			// Setup: Two networks with different pkeys
			network1ID := "default_network1"
			network2ID := "default_network2"

			pod1 := createTestPod("pod1", "default", `[{"name":"network1","namespace":"default"}]`)
			pod2 := createTestPod("pod2", "default", `[{"name":"network2","namespace":"default"}]`)

			addMap.Set(network1ID, []*kapi.Pod{pod1})
			addMap.Set(network2ID, []*kapi.Pod{pod2})

			// Setup NAD mocks - different pkeys
			nad1 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network1", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x1234")},
			}
			nad2 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network2", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x5678")},
			}
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network1").Return(nad1, nil)
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network2").Return(nad2, nil)
			k8sClient.On("AddFinalizerToPod", mock.Anything, mock.Anything).Return(nil)
			k8sClient.On("AddFinalizerToNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			k8sClient.On("SetAnnotationsOnPod", mock.Anything, mock.Anything).Return(nil)

			// Setup SM client mocks - allow both calls
			serverTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
			smClient.On("GetLastPKeyUpdateTimestamp").Return(time.Time{}, nil)
			smClient.On("GetServerTime").Return(serverTime, nil)
			smClient.On("AddGuidsToPKey", mock.Anything, mock.Anything).Return(nil)

			// Execute
			d.AddPeriodicUpdate()

			// Verify: AddGuidsToPKey should have been called twice - once per pkey
			// Collect all AddGuidsToPKey calls and verify each pkey got exactly one call
			pkeysProcessed := make(map[int]int) // pkey -> number of GUIDs
			for _, call := range smClient.Calls {
				if call.Method == "AddGuidsToPKey" {
					pkey := call.Arguments.Get(0).(int)
					guids := call.Arguments.Get(1).([]net.HardwareAddr)
					pkeysProcessed[pkey] = len(guids)
				}
			}
			Expect(len(pkeysProcessed)).To(Equal(2), "should process exactly 2 different pkeys")
			Expect(pkeysProcessed[0x1234]).To(Equal(1), "pkey 0x1234 should have 1 GUID")
			Expect(pkeysProcessed[0x5678]).To(Equal(1), "pkey 0x5678 should have 1 GUID")
		})

		It("batches multiple pods per pkey across multiple networks", func() {
			// Setup: 4 networks, 4 pods
			// - pod1 and pod2 are attached to network1 AND network2 (both pkey 0x1234)
			// - pod3 and pod4 are attached to network3 AND network4 (both pkey 0x5678)
			// Expected: 2 AddGuidsToPKey calls
			//   - pkey 0x1234: 4 GUIDs (pod1+pod2 on network1, pod1+pod2 on network2)
			//   - pkey 0x5678: 4 GUIDs (pod3+pod4 on network3, pod3+pod4 on network4)
			network1ID := "default_network1" // pkey 0x1234
			network2ID := "default_network2" // pkey 0x1234 (same as network1)
			network3ID := "default_network3" // pkey 0x5678
			network4ID := "default_network4" // pkey 0x5678 (same as network3)

			// pod1 and pod2 each have both network1 and network2
			pod1 := createTestPod("pod1", "default", `[{"name":"network1","namespace":"default"},{"name":"network2","namespace":"default"}]`)
			pod2 := createTestPod("pod2", "default", `[{"name":"network1","namespace":"default"},{"name":"network2","namespace":"default"}]`)
			// pod3 and pod4 each have both network3 and network4
			pod3 := createTestPod("pod3", "default", `[{"name":"network3","namespace":"default"},{"name":"network4","namespace":"default"}]`)
			pod4 := createTestPod("pod4", "default", `[{"name":"network4","namespace":"default"},{"name":"network3","namespace":"default"}]`)

			// Each network has 2 pods
			addMap.Set(network1ID, []*kapi.Pod{pod1, pod2})
			addMap.Set(network2ID, []*kapi.Pod{pod1, pod2})
			addMap.Set(network3ID, []*kapi.Pod{pod3, pod4})
			addMap.Set(network4ID, []*kapi.Pod{pod3, pod4})

			// NAD mocks - network1/network2 share pkey 0x1234, network3/network4 share pkey 0x5678
			nad1 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network1", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x1234")},
			}
			nad2 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network2", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x1234")}, // Same pkey as network1
			}
			nad3 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network3", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x5678")},
			}
			nad4 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network4", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x5678")}, // Same pkey as network3
			}
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network1").Return(nad1, nil)
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network2").Return(nad2, nil)
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network3").Return(nad3, nil)
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network4").Return(nad4, nil)
			k8sClient.On("AddFinalizerToPod", mock.Anything, mock.Anything).Return(nil)
			k8sClient.On("AddFinalizerToNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			k8sClient.On("SetAnnotationsOnPod", mock.Anything, mock.Anything).Return(nil)

			serverTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
			smClient.On("GetLastPKeyUpdateTimestamp").Return(time.Time{}, nil)
			smClient.On("GetServerTime").Return(serverTime, nil)
			smClient.On("AddGuidsToPKey", mock.Anything, mock.Anything).Return(nil)

			// Execute
			d.AddPeriodicUpdate()

			// Verify batching per pkey
			pkeysProcessed := make(map[int]int) // pkey -> number of GUIDs
			addGuidsCallCount := 0
			for _, call := range smClient.Calls {
				if call.Method == "AddGuidsToPKey" {
					addGuidsCallCount++
					pkey := call.Arguments.Get(0).(int)
					guids := call.Arguments.Get(1).([]net.HardwareAddr)
					pkeysProcessed[pkey] = len(guids)
				}
			}

			// Should only have 2 AddGuidsToPKey calls (one per unique pkey), not 4
			Expect(addGuidsCallCount).To(Equal(2), "should batch into 2 calls (one per unique pkey)")
			Expect(len(pkeysProcessed)).To(Equal(2), "should process exactly 2 different pkeys")

			// pkey 0x1234 should have 4 GUIDs (pod1+pod2 from network1, pod1+pod2 from network2)
			Expect(pkeysProcessed[0x1234]).To(Equal(4), "pkey 0x1234 should batch 4 GUIDs from network1 and network2")

			// pkey 0x5678 should have 4 GUIDs (pod3+pod4 from network3, pod3+pod4 from network4)
			Expect(pkeysProcessed[0x5678]).To(Equal(4), "pkey 0x5678 should batch 4 GUIDs from network3 and network4")

			// Verify all networks removed from addMap
			_, exists1 := addMap.Get(network1ID)
			_, exists2 := addMap.Get(network2ID)
			_, exists3 := addMap.Get(network3ID)
			_, exists4 := addMap.Get(network4ID)
			Expect(exists1).To(BeFalse(), "network1 should be removed from addMap")
			Expect(exists2).To(BeFalse(), "network2 should be removed from addMap")
			Expect(exists3).To(BeFalse(), "network3 should be removed from addMap")
			Expect(exists4).To(BeFalse(), "network4 should be removed from addMap")
		})

		It("requeues networks when previous pkey modification is still in progress", func() {
			// Setup: One network that should be requeued
			network1ID := "default_network1"
			pod1 := createTestPod("pod1", "default", `[{"name":"network1","namespace":"default"}]`)
			addMap.Set(network1ID, []*kapi.Pod{pod1})

			nad1 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network1", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x1234")},
			}
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network1").Return(nad1, nil)
			k8sClient.On("AddFinalizerToPod", mock.Anything, mock.Anything).Return(nil)

			// Setup: Simulate previous operation still in progress
			// - Set a stored timestamp
			// - SM returns last_updated <= stored timestamp
			storedTime := time.Date(2024, time.January, 15, 10, 0, 0, 0, time.UTC)
			d.lastPkeyAPICallTimestamp = storedTime

			// SM reports an older last_updated - previous op still in progress
			smLastUpdated := time.Date(2024, time.January, 15, 9, 55, 0, 0, time.UTC)
			smClient.On("GetLastPKeyUpdateTimestamp").Return(smLastUpdated, nil)

			// Execute
			d.AddPeriodicUpdate()

			// Verify: AddGuidsToPKey should NOT have been called
			smClient.AssertNotCalled(GinkgoT(), "AddGuidsToPKey", mock.Anything, mock.Anything)

			// Verify: network should still be in addMap (requeued)
			_, exists := addMap.Get(network1ID)
			Expect(exists).To(BeTrue(), "network should remain in addMap for retry")
		})

		It("processes network on retry after previous operation completes", func() {
			// Setup: One network
			network1ID := "default_network1"
			pod1 := createTestPod("pod1", "default", `[{"name":"network1","namespace":"default"}]`)
			addMap.Set(network1ID, []*kapi.Pod{pod1})

			nad1 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network1", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x1234")},
			}
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network1").Return(nad1, nil)
			k8sClient.On("AddFinalizerToPod", mock.Anything, mock.Anything).Return(nil)
			k8sClient.On("AddFinalizerToNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			k8sClient.On("SetAnnotationsOnPod", mock.Anything, mock.Anything).Return(nil)

			// First call: previous op in progress
			storedTime := time.Date(2024, time.January, 15, 10, 0, 0, 0, time.UTC)
			d.lastPkeyAPICallTimestamp = storedTime

			smLastUpdated := time.Date(2024, time.January, 15, 9, 55, 0, 0, time.UTC)
			// First call - returns older timestamp (in progress)
			smClient.On("GetLastPKeyUpdateTimestamp").Return(smLastUpdated, nil).Once()

			// First execution - should be requeued
			d.AddPeriodicUpdate()

			// Verify network still in addMap
			_, exists := addMap.Get(network1ID)
			Expect(exists).To(BeTrue(), "network should be requeued after first call")

			// Second call - returns newer timestamp (completed)
			completedTime := time.Date(2024, time.January, 15, 10, 5, 0, 0, time.UTC)
			smClient.On("GetLastPKeyUpdateTimestamp").Return(completedTime, nil).Once()
			newServerTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
			smClient.On("GetServerTime").Return(newServerTime, nil)
			smClient.On("AddGuidsToPKey", mock.Anything, mock.Anything).Return(nil)

			// Second execution - should succeed
			d.AddPeriodicUpdate()

			// Verify network removed from addMap
			_, exists = addMap.Get(network1ID)
			Expect(exists).To(BeFalse(), "network should be processed after retry")
		})
	})

	Context("Integration: Delete Operations with Multiple PKeys", func() {
		var (
			d         *daemon
			smClient  *smMocks.SubnetManagerClient
			k8sClient *k8sClientMocks.Client
			handler   *handlerMocks.ResourceEventHandler
			addMap    *utils.SynchronizedMap
			deleteMap *utils.SynchronizedMap
			guidPool  guid.Pool
		)

		BeforeEach(func() {
			smClient = smMocks.NewSubnetManagerClient()
			k8sClient = &k8sClientMocks.Client{}
			handler = &handlerMocks.ResourceEventHandler{}
			addMap = utils.NewSynchronizedMap()
			deleteMap = utils.NewSynchronizedMap()

			poolConfig := &config.GUIDPoolConfig{
				RangeStart: "02:00:00:00:00:00:00:00",
				RangeEnd:   "02:00:00:00:00:00:00:FF",
			}
			var err error
			guidPool, err = guid.NewPool(poolConfig)
			Expect(err).ToNot(HaveOccurred())

			handler.On("GetResults").Return(addMap, deleteMap)

			d = &daemon{
				smClient:          smClient,
				kubeClient:        k8sClient,
				guidPool:          guidPool,
				guidPodNetworkMap: make(map[string]string),
				config:            config.DaemonConfig{},
				watcher:           &testWatcher{handler: handler},
			}
		})

		It("requeues delete operations when previous pkey modification is in progress", func() {
			// Setup: Pod with GUID that needs to be deleted
			network1ID := "default_network1"
			// Pod annotation indicates it has a GUID assigned (mellanox.infiniband.app: configured)
			pod1 := createTestPodWithGUID("pod1", "default",
				`[{"name":"network1","namespace":"default","cni-args":{"guid":"02:00:00:00:00:00:00:01","mellanox.infiniband.app":"configured"}}]`)
			deleteMap.Set(network1ID, []*kapi.Pod{pod1})

			nad1 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network1", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x1234")},
			}
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network1").Return(nad1, nil)
			k8sClient.On("GetPods", mock.Anything).Return(&kapi.PodList{}, nil)

			// Setup: Simulate previous operation still in progress
			storedTime := time.Date(2024, time.January, 15, 10, 0, 0, 0, time.UTC)
			d.lastPkeyAPICallTimestamp = storedTime

			smLastUpdated := time.Date(2024, time.January, 15, 9, 55, 0, 0, time.UTC)
			smClient.On("GetLastPKeyUpdateTimestamp").Return(smLastUpdated, nil)

			// Execute
			d.DeletePeriodicUpdate()

			// Verify: RemoveGuidsFromPKey should NOT have been called
			smClient.AssertNotCalled(GinkgoT(), "RemoveGuidsFromPKey", mock.Anything, mock.Anything)

			// Verify: network should still be in deleteMap (requeued)
			_, exists := deleteMap.Get(network1ID)
			Expect(exists).To(BeTrue(), "network should remain in deleteMap for retry")
		})

		It("batches delete operations for same pkey", func() {
			// Setup: Two pods on same network (same pkey) to be deleted
			network1ID := "default_network1"
			pod1 := createTestPodWithGUID("pod1", "default",
				`[{"name":"network1","namespace":"default","cni-args":{"guid":"02:00:00:00:00:00:00:01","mellanox.infiniband.app":"configured"}}]`)
			pod2 := createTestPodWithGUID("pod2", "default",
				`[{"name":"network1","namespace":"default","cni-args":{"guid":"02:00:00:00:00:00:00:02","mellanox.infiniband.app":"configured"}}]`)
			deleteMap.Set(network1ID, []*kapi.Pod{pod1, pod2})

			// Pre-allocate GUIDs in pool
			guid1, _ := net.ParseMAC("02:00:00:00:00:00:00:01")
			guid2, _ := net.ParseMAC("02:00:00:00:00:00:00:02")
			_ = guidPool.AllocateGUID(guid1.String())
			_ = guidPool.AllocateGUID(guid2.String())
			d.guidPodNetworkMap[guid1.String()] = "test-uid-pod1network1"
			d.guidPodNetworkMap[guid2.String()] = "test-uid-pod2network1"

			nad1 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network1", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x1234")},
			}
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network1").Return(nad1, nil)
			k8sClient.On("GetPods", mock.Anything).Return(&kapi.PodList{}, nil)
			k8sClient.On("RemoveFinalizerFromNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			k8sClient.On("RemoveFinalizerFromPod", mock.Anything, mock.Anything).Return(nil)

			// Allow the operation to proceed
			serverTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
			smClient.On("GetLastPKeyUpdateTimestamp").Return(time.Time{}, nil)
			smClient.On("GetServerTime").Return(serverTime, nil)
			smClient.On("RemoveGuidsFromPKey", mock.Anything, mock.Anything).Return(nil)

			// Execute
			d.DeletePeriodicUpdate()

			// Verify: RemoveGuidsFromPKey should have been called once with both GUIDs
			smClient.AssertCalled(GinkgoT(), "RemoveGuidsFromPKey", 0x1234, mock.Anything)

			// Verify network removed from deleteMap
			_, exists := deleteMap.Get(network1ID)
			Expect(exists).To(BeFalse(), "network should be removed from deleteMap")
		})

		It("batches delete operations per pkey across multiple networks", func() {
			// Setup: 4 networks, 4 pods
			// - pod1 and pod2 are attached to network1 AND network2 (both pkey 0x1234)
			// - pod3 and pod4 are attached to network3 AND network4 (both pkey 0x5678)
			// Expected: 2 RemoveGuidsFromPKey calls
			//   - pkey 0x1234: 4 GUIDs (pod1+pod2 on network1, pod1+pod2 on network2)
			//   - pkey 0x5678: 4 GUIDs (pod3+pod4 on network3, pod3+pod4 on network4)
			network1ID := "default_network1" // pkey 0x1234
			network2ID := "default_network2" // pkey 0x1234 (same as network1)
			network3ID := "default_network3" // pkey 0x5678
			network4ID := "default_network4" // pkey 0x5678 (same as network3)

			// pod1 and pod2 each have both network1 and network2
			pod1 := createTestPodWithGUID("pod1", "default",
				`[{"name":"network1","namespace":"default","cni-args":{"guid":"02:00:00:00:00:00:00:01","mellanox.infiniband.app":"configured"}},{"name":"network2","namespace":"default","cni-args":{"guid":"02:00:00:00:00:00:00:02","mellanox.infiniband.app":"configured"}}]`)
			pod2 := createTestPodWithGUID("pod2", "default",
				`[{"name":"network1","namespace":"default","cni-args":{"guid":"02:00:00:00:00:00:00:03","mellanox.infiniband.app":"configured"}},{"name":"network2","namespace":"default","cni-args":{"guid":"02:00:00:00:00:00:00:04","mellanox.infiniband.app":"configured"}}]`)
			// pod3 and pod4 each have both network3 and network4
			pod3 := createTestPodWithGUID("pod3", "default",
				`[{"name":"network3","namespace":"default","cni-args":{"guid":"02:00:00:00:00:00:00:05","mellanox.infiniband.app":"configured"}},{"name":"network4","namespace":"default","cni-args":{"guid":"02:00:00:00:00:00:00:06","mellanox.infiniband.app":"configured"}}]`)
			pod4 := createTestPodWithGUID("pod4", "default",
				`[{"name":"network3","namespace":"default","cni-args":{"guid":"02:00:00:00:00:00:00:07","mellanox.infiniband.app":"configured"}},{"name":"network4","namespace":"default","cni-args":{"guid":"02:00:00:00:00:00:00:08","mellanox.infiniband.app":"configured"}}]`)

			// Each network has 2 pods
			deleteMap.Set(network1ID, []*kapi.Pod{pod1, pod2})
			deleteMap.Set(network2ID, []*kapi.Pod{pod1, pod2})
			deleteMap.Set(network3ID, []*kapi.Pod{pod3, pod4})
			deleteMap.Set(network4ID, []*kapi.Pod{pod3, pod4})

			// Pre-allocate GUIDs in pool and map them
			guids := []string{
				"02:00:00:00:00:00:00:01", "02:00:00:00:00:00:00:02",
				"02:00:00:00:00:00:00:03", "02:00:00:00:00:00:00:04",
				"02:00:00:00:00:00:00:05", "02:00:00:00:00:00:00:06",
				"02:00:00:00:00:00:00:07", "02:00:00:00:00:00:00:08",
			}
			guidToPodNetwork := map[string]string{
				"02:00:00:00:00:00:00:01": "test-uid-pod1network1",
				"02:00:00:00:00:00:00:02": "test-uid-pod1network2",
				"02:00:00:00:00:00:00:03": "test-uid-pod2network1",
				"02:00:00:00:00:00:00:04": "test-uid-pod2network2",
				"02:00:00:00:00:00:00:05": "test-uid-pod3network3",
				"02:00:00:00:00:00:00:06": "test-uid-pod3network4",
				"02:00:00:00:00:00:00:07": "test-uid-pod4network3",
				"02:00:00:00:00:00:00:08": "test-uid-pod4network4",
			}
			for _, g := range guids {
				parsedGUID, _ := net.ParseMAC(g)
				_ = guidPool.AllocateGUID(parsedGUID.String())
				d.guidPodNetworkMap[parsedGUID.String()] = guidToPodNetwork[g]
			}

			// NAD mocks
			nad1 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network1", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x1234")},
			}
			nad2 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network2", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x1234")}, // Same pkey as network1
			}
			nad3 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network3", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x5678")},
			}
			nad4 := &v1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "network4", Namespace: "default"},
				Spec:       v1.NetworkAttachmentDefinitionSpec{Config: createNADConfig("0x5678")}, // Same pkey as network3
			}
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network1").Return(nad1, nil)
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network2").Return(nad2, nil)
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network3").Return(nad3, nil)
			k8sClient.On("GetNetworkAttachmentDefinition", "default", "network4").Return(nad4, nil)
			k8sClient.On("GetPods", mock.Anything).Return(&kapi.PodList{}, nil)
			k8sClient.On("RemoveFinalizerFromNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			k8sClient.On("RemoveFinalizerFromPod", mock.Anything, mock.Anything).Return(nil)

			// Allow operations to proceed
			serverTime := time.Date(2024, time.January, 15, 10, 30, 0, 0, time.UTC)
			smClient.On("GetLastPKeyUpdateTimestamp").Return(time.Time{}, nil)
			smClient.On("GetServerTime").Return(serverTime, nil)
			smClient.On("RemoveGuidsFromPKey", mock.Anything, mock.Anything).Return(nil)

			// Execute
			d.DeletePeriodicUpdate()

			// Verify batching per pkey
			pkeysProcessed := make(map[int]int) // pkey -> number of GUIDs
			removeGuidsCallCount := 0
			for _, call := range smClient.Calls {
				if call.Method == "RemoveGuidsFromPKey" {
					removeGuidsCallCount++
					pkey := call.Arguments.Get(0).(int)
					guids := call.Arguments.Get(1).([]net.HardwareAddr)
					pkeysProcessed[pkey] = len(guids)
				}
			}

			// Should only have 2 RemoveGuidsFromPKey calls (one per unique pkey), not 4
			Expect(removeGuidsCallCount).To(Equal(2), "should batch into 2 calls (one per unique pkey)")
			Expect(len(pkeysProcessed)).To(Equal(2), "should process exactly 2 different pkeys")

			// pkey 0x1234 should have 4 GUIDs (pod1+pod2 from network1, pod1+pod2 from network2)
			Expect(pkeysProcessed[0x1234]).To(Equal(4), "pkey 0x1234 should batch 4 GUIDs from network1 and network2")

			// pkey 0x5678 should have 4 GUIDs (pod3+pod4 from network3, pod3+pod4 from network4)
			Expect(pkeysProcessed[0x5678]).To(Equal(4), "pkey 0x5678 should batch 4 GUIDs from network3 and network4")

			// Verify all networks removed from deleteMap
			_, exists1 := deleteMap.Get(network1ID)
			_, exists2 := deleteMap.Get(network2ID)
			_, exists3 := deleteMap.Get(network3ID)
			_, exists4 := deleteMap.Get(network4ID)
			Expect(exists1).To(BeFalse(), "network1 should be removed from deleteMap")
			Expect(exists2).To(BeFalse(), "network2 should be removed from deleteMap")
			Expect(exists3).To(BeFalse(), "network3 should be removed from deleteMap")
			Expect(exists4).To(BeFalse(), "network4 should be removed from deleteMap")
		})
	})
})
