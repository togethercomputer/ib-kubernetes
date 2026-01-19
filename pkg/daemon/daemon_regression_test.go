package daemon

import (
	"encoding/json"
	"fmt"

	v1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	. "github.com/onsi/ginkgo"
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
	watcherMocks "github.com/Mellanox/ib-kubernetes/pkg/watcher/mocks"

	resEvenHandler "github.com/Mellanox/ib-kubernetes/pkg/watcher/handler"
)

// Helper function to create a NetworkAttachmentDefinition for IB-SRIOV
func createIbSriovNAD(namespace, name, pkey string) *v1.NetworkAttachmentDefinition {
	config := map[string]interface{}{
		"type": "ib-sriov",
		"pkey": pkey,
		"capabilities": map[string]bool{
			"infinibandGUID": false,
		},
	}
	configBytes, _ := json.Marshal(config)

	return &v1.NetworkAttachmentDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.NetworkAttachmentDefinitionSpec{
			Config: string(configBytes),
		},
	}
}

// Helper function to create a pod with multiple networks
func createPodWithMultipleNetworks(name, namespace string, networks []v1.NetworkSelectionElement) *kapi.Pod {
	netAnnotation, _ := json.Marshal(networks)
	return &kapi.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			UID:         types.UID(fmt.Sprintf("%s-%s-uid", namespace, name)),
			Annotations: map[string]string{v1.NetworkAttachmentAnnot: string(netAnnotation)},
		},
		Spec: kapi.PodSpec{
			NodeName: "test-node",
		},
	}
}

// Helper function to create network selection element
func createNetworkSelection(name, namespace string) v1.NetworkSelectionElement {
	return v1.NetworkSelectionElement{
		Name:      name,
		Namespace: namespace,
	}
}

// Helper function to create configured network with GUID
func createConfiguredNetwork(name, namespace, guid string) v1.NetworkSelectionElement {
	return v1.NetworkSelectionElement{
		Name:      name,
		Namespace: namespace,
		CNIArgs: &map[string]interface{}{
			"guid":                     guid,
			utils.InfiniBandAnnotation: utils.ConfiguredInfiniBandPod,
		},
	}
}

// containsGUID checks if a GUID string is present in a slice
func containsGUID(guids []string, target string) bool {
	for _, g := range guids {
		if g == target {
			return true
		}
	}
	return false
}

var _ = Describe("Daemon Multi-Network Regression Tests", func() {
	var (
		mockSMClient  *smMocks.SubnetManagerClient
		mockK8sClient *k8sClientMocks.Client
		mockWatcher   *watcherMocks.MockWatcher
		podHandler    resEvenHandler.ResourceEventHandler
		testDaemon    *daemon
		testGuidPool  guid.Pool
	)

	BeforeEach(func() {
		// Create mocks
		mockSMClient = smMocks.NewSubnetManagerClient()
		mockK8sClient = &k8sClientMocks.Client{}
		podHandler = resEvenHandler.NewPodEventHandler()
		mockWatcher = watcherMocks.NewWatcher(podHandler)

		// Setup GUID pool
		var err error
		testGuidPool, err = guid.NewPool(&config.GUIDPoolConfig{
			RangeStart: "02:00:00:00:00:00:00:00",
			RangeEnd:   "02:00:00:00:00:00:FF:FF",
		})
		Expect(err).NotTo(HaveOccurred())

		// Create daemon for testing
		testDaemon = &daemon{
			config: config.DaemonConfig{
				PeriodicUpdate: 5,
				Plugin:         "test",
			},
			watcher:           mockWatcher,
			kubeClient:        mockK8sClient,
			guidPool:          testGuidPool,
			smClient:          mockSMClient,
			guidPodNetworkMap: make(map[string]string),
		}

		// Setup default mock behaviors
		mockSMClient.On("Name").Return("test-sm").Maybe()
		mockSMClient.On("AddGuidsToPKey", mock.Anything, mock.Anything).Return(nil).Maybe()
		mockSMClient.On("AddGuidsToLimitedPKey", mock.Anything, mock.Anything).Return(nil).Maybe()
		mockSMClient.On("RemoveGuidsFromPKey", mock.Anything, mock.Anything).Return(nil).Maybe()
		mockSMClient.On("ListGuidsInUse").Return([]string{}, nil).Maybe()
	})

	AfterEach(func() {
		mockSMClient.Reset()
	})

	Context("AddPeriodicUpdate with multiple networks", func() {
		It("should allocate all GUIDs for a pod with 2 networks on different pkeys", func() {
			// Setup NADs
			nad1 := createIbSriovNAD("default", "network1", "0x0001")
			nad2 := createIbSriovNAD("default", "network2", "0x0002")

			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "network1").Return(nad1, nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "network2").Return(nad2, nil)
			mockK8sClient.On("SetAnnotationsOnPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("AddFinalizerToPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("AddFinalizerToNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			// Create pod with 2 networks
			networks := []v1.NetworkSelectionElement{
				createNetworkSelection("network1", "default"),
				createNetworkSelection("network2", "default"),
			}
			pod := createPodWithMultipleNetworks("test-pod", "default", networks)

			// Simulate pods being added via the handler
			addMap, _ := podHandler.GetResults()
			addMap.Set("default_network1", []*kapi.Pod{pod})
			addMap.Set("default_network2", []*kapi.Pod{pod})

			// Run AddPeriodicUpdate
			testDaemon.AddPeriodicUpdate()

			// Verify: all GUIDs should be allocated to UFM (one per network)
			allAddedGUIDs := mockSMClient.GetAllAddedGUIDs()
			Expect(len(allAddedGUIDs)).To(Equal(2), "Expected 2 GUIDs to be allocated (one per network)")

			// Verify GUIDs were added to correct pkeys
			pkey1GUIDs := mockSMClient.GetAddedGUIDsForPKey(0x0001)
			pkey2GUIDs := mockSMClient.GetAddedGUIDsForPKey(0x0002)
			Expect(len(pkey1GUIDs)).To(Equal(1), "Expected 1 GUID on pkey 0x0001")
			Expect(len(pkey2GUIDs)).To(Equal(1), "Expected 1 GUID on pkey 0x0002")
		})

		It("should allocate all GUIDs for a pod with 3 networks on the same pkey", func() {
			// All networks use the same pkey
			nad := createIbSriovNAD("default", "network", "0x0001")

			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "network1").Return(
				createIbSriovNAD("default", "network1", "0x0001"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "network2").Return(
				createIbSriovNAD("default", "network2", "0x0001"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "network3").Return(
				createIbSriovNAD("default", "network3", "0x0001"), nil)
			mockK8sClient.On("SetAnnotationsOnPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("AddFinalizerToPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("AddFinalizerToNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			_ = nad // avoid unused variable warning

			// Create pod with 3 networks
			networks := []v1.NetworkSelectionElement{
				createNetworkSelection("network1", "default"),
				createNetworkSelection("network2", "default"),
				createNetworkSelection("network3", "default"),
			}
			pod := createPodWithMultipleNetworks("test-pod", "default", networks)

			// Simulate pods being added
			addMap, _ := podHandler.GetResults()
			addMap.Set("default_network1", []*kapi.Pod{pod})
			addMap.Set("default_network2", []*kapi.Pod{pod})
			addMap.Set("default_network3", []*kapi.Pod{pod})

			// Run AddPeriodicUpdate
			testDaemon.AddPeriodicUpdate()

			// Verify: all 3 GUIDs should be allocated to the same pkey
			pkey1GUIDs := mockSMClient.GetAddedGUIDsForPKey(0x0001)
			Expect(len(pkey1GUIDs)).To(Equal(3), "Expected 3 GUIDs to be allocated on pkey 0x0001")
		})

		It("should allocate all GUIDs for multiple pods each with multiple networks", func() {
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "network1").Return(
				createIbSriovNAD("default", "network1", "0x0001"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "network2").Return(
				createIbSriovNAD("default", "network2", "0x0002"), nil)
			mockK8sClient.On("SetAnnotationsOnPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("AddFinalizerToPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("AddFinalizerToNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			// Create 3 pods, each with 2 networks
			var allPods []*kapi.Pod
			for i := 1; i <= 3; i++ {
				networks := []v1.NetworkSelectionElement{
					createNetworkSelection("network1", "default"),
					createNetworkSelection("network2", "default"),
				}
				pod := createPodWithMultipleNetworks(fmt.Sprintf("test-pod-%d", i), "default", networks)
				allPods = append(allPods, pod)
			}

			// Simulate pods being added
			addMap, _ := podHandler.GetResults()
			addMap.Set("default_network1", allPods)
			addMap.Set("default_network2", allPods)

			// Run AddPeriodicUpdate
			testDaemon.AddPeriodicUpdate()

			// Verify: total 6 GUIDs should be allocated (3 pods x 2 networks)
			allAddedGUIDs := mockSMClient.GetAllAddedGUIDs()
			Expect(len(allAddedGUIDs)).To(Equal(6), "Expected 6 GUIDs to be allocated (3 pods x 2 networks)")

			// Verify distribution across pkeys
			pkey1GUIDs := mockSMClient.GetAddedGUIDsForPKey(0x0001)
			pkey2GUIDs := mockSMClient.GetAddedGUIDsForPKey(0x0002)
			Expect(len(pkey1GUIDs)).To(Equal(3), "Expected 3 GUIDs on pkey 0x0001")
			Expect(len(pkey2GUIDs)).To(Equal(3), "Expected 3 GUIDs on pkey 0x0002")
		})

		It("should allocate GUIDs to all 5 networks on a single pod with mixed pkeys", func() {
			// Setup 5 NADs with different pkeys
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "net-a").Return(
				createIbSriovNAD("default", "net-a", "0x0001"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "net-b").Return(
				createIbSriovNAD("default", "net-b", "0x0002"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "net-c").Return(
				createIbSriovNAD("default", "net-c", "0x0001"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "net-d").Return(
				createIbSriovNAD("default", "net-d", "0x0003"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "net-e").Return(
				createIbSriovNAD("default", "net-e", "0x0002"), nil)
			mockK8sClient.On("SetAnnotationsOnPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("AddFinalizerToPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("AddFinalizerToNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			// Create pod with 5 networks
			networks := []v1.NetworkSelectionElement{
				createNetworkSelection("net-a", "default"),
				createNetworkSelection("net-b", "default"),
				createNetworkSelection("net-c", "default"),
				createNetworkSelection("net-d", "default"),
				createNetworkSelection("net-e", "default"),
			}
			pod := createPodWithMultipleNetworks("multi-net-pod", "default", networks)

			// Simulate pods being added
			addMap, _ := podHandler.GetResults()
			addMap.Set("default_net-a", []*kapi.Pod{pod})
			addMap.Set("default_net-b", []*kapi.Pod{pod})
			addMap.Set("default_net-c", []*kapi.Pod{pod})
			addMap.Set("default_net-d", []*kapi.Pod{pod})
			addMap.Set("default_net-e", []*kapi.Pod{pod})

			// Run AddPeriodicUpdate
			testDaemon.AddPeriodicUpdate()

			// Verify: all 5 GUIDs should be allocated
			allAddedGUIDs := mockSMClient.GetAllAddedGUIDs()
			Expect(len(allAddedGUIDs)).To(Equal(5), "Expected 5 GUIDs to be allocated (one per network)")

			// Verify distribution: pkey 0x0001 should have 2 (net-a, net-c)
			//                      pkey 0x0002 should have 2 (net-b, net-e)
			//                      pkey 0x0003 should have 1 (net-d)
			pkey1GUIDs := mockSMClient.GetAddedGUIDsForPKey(0x0001)
			pkey2GUIDs := mockSMClient.GetAddedGUIDsForPKey(0x0002)
			pkey3GUIDs := mockSMClient.GetAddedGUIDsForPKey(0x0003)
			Expect(len(pkey1GUIDs)).To(Equal(2), "Expected 2 GUIDs on pkey 0x0001")
			Expect(len(pkey2GUIDs)).To(Equal(2), "Expected 2 GUIDs on pkey 0x0002")
			Expect(len(pkey3GUIDs)).To(Equal(1), "Expected 1 GUID on pkey 0x0003")
		})
	})

	Context("DeletePeriodicUpdate with multiple networks", func() {
		It("should remove all GUIDs for a pod with 2 networks on different pkeys", func() {
			// Setup NADs
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "network1").Return(
				createIbSriovNAD("default", "network1", "0x0001"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "network2").Return(
				createIbSriovNAD("default", "network2", "0x0002"), nil)
			mockK8sClient.On("RemoveFinalizerFromPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("GetPods", mock.Anything).Return(&kapi.PodList{}, nil)
			mockK8sClient.On("RemoveFinalizerFromNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			// Pre-allocate GUIDs in the pool (simulating previously allocated GUIDs)
			guid1 := "02:00:00:00:00:00:00:01"
			guid2 := "02:00:00:00:00:00:00:02"
			Expect(testGuidPool.AllocateGUID(guid1)).To(Succeed())
			Expect(testGuidPool.AllocateGUID(guid2)).To(Succeed())

			// Create pod with configured networks (having GUIDs)
			networks := []v1.NetworkSelectionElement{
				createConfiguredNetwork("network1", "default", guid1),
				createConfiguredNetwork("network2", "default", guid2),
			}
			netAnnotation, _ := json.Marshal(networks)
			pod := &kapi.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-pod",
					Namespace:   "default",
					UID:         "test-pod-uid",
					Annotations: map[string]string{v1.NetworkAttachmentAnnot: string(netAnnotation)},
				},
				Spec: kapi.PodSpec{NodeName: "test-node"},
			}

			// Update guid map
			testDaemon.guidPodNetworkMap[guid1] = "test-pod-uid_default_network1"
			testDaemon.guidPodNetworkMap[guid2] = "test-pod-uid_default_network2"

			// Simulate pods being deleted
			_, deleteMap := podHandler.GetResults()
			deleteMap.Set("default_network1", []*kapi.Pod{pod})
			deleteMap.Set("default_network2", []*kapi.Pod{pod})

			// Run DeletePeriodicUpdate
			testDaemon.DeletePeriodicUpdate()

			// Verify: all GUIDs should be removed from UFM
			allRemovedGUIDs := mockSMClient.GetAllRemovedGUIDs()
			Expect(len(allRemovedGUIDs)).To(Equal(2), "Expected 2 GUIDs to be removed")
			Expect(containsGUID(allRemovedGUIDs, guid1)).To(BeTrue(), "GUID1 should be removed")
			Expect(containsGUID(allRemovedGUIDs, guid2)).To(BeTrue(), "GUID2 should be removed")

			// Verify GUIDs were removed from correct pkeys
			pkey1GUIDs := mockSMClient.GetRemovedGUIDsForPKey(0x0001)
			pkey2GUIDs := mockSMClient.GetRemovedGUIDsForPKey(0x0002)
			Expect(containsGUID(pkey1GUIDs, guid1)).To(BeTrue())
			Expect(containsGUID(pkey2GUIDs, guid2)).To(BeTrue())
		})

		It("should remove all GUIDs for a pod with 4 networks on same pkey", func() {
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "net1").Return(
				createIbSriovNAD("default", "net1", "0x0001"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "net2").Return(
				createIbSriovNAD("default", "net2", "0x0001"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "net3").Return(
				createIbSriovNAD("default", "net3", "0x0001"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "net4").Return(
				createIbSriovNAD("default", "net4", "0x0001"), nil)
			mockK8sClient.On("RemoveFinalizerFromPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("GetPods", mock.Anything).Return(&kapi.PodList{}, nil)
			mockK8sClient.On("RemoveFinalizerFromNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			// Pre-allocate GUIDs
			guids := []string{
				"02:00:00:00:00:00:00:01",
				"02:00:00:00:00:00:00:02",
				"02:00:00:00:00:00:00:03",
				"02:00:00:00:00:00:00:04",
			}
			for _, g := range guids {
				Expect(testGuidPool.AllocateGUID(g)).To(Succeed())
			}

			// Create pod with configured networks
			networks := []v1.NetworkSelectionElement{
				createConfiguredNetwork("net1", "default", guids[0]),
				createConfiguredNetwork("net2", "default", guids[1]),
				createConfiguredNetwork("net3", "default", guids[2]),
				createConfiguredNetwork("net4", "default", guids[3]),
			}
			netAnnotation, _ := json.Marshal(networks)
			pod := &kapi.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "multi-net-pod",
					Namespace:   "default",
					UID:         "multi-net-pod-uid",
					Annotations: map[string]string{v1.NetworkAttachmentAnnot: string(netAnnotation)},
				},
				Spec: kapi.PodSpec{NodeName: "test-node"},
			}

			// Update guid map
			for i, g := range guids {
				testDaemon.guidPodNetworkMap[g] = fmt.Sprintf("multi-net-pod-uid_default_net%d", i+1)
			}

			// Simulate deletion
			_, deleteMap := podHandler.GetResults()
			deleteMap.Set("default_net1", []*kapi.Pod{pod})
			deleteMap.Set("default_net2", []*kapi.Pod{pod})
			deleteMap.Set("default_net3", []*kapi.Pod{pod})
			deleteMap.Set("default_net4", []*kapi.Pod{pod})

			// Run DeletePeriodicUpdate
			testDaemon.DeletePeriodicUpdate()

			// Verify: all 4 GUIDs should be removed
			allRemovedGUIDs := mockSMClient.GetAllRemovedGUIDs()
			Expect(len(allRemovedGUIDs)).To(Equal(4), "Expected 4 GUIDs to be removed")
			for _, g := range guids {
				Expect(containsGUID(allRemovedGUIDs, g)).To(BeTrue(), fmt.Sprintf("GUID %s should be removed", g))
			}
		})

		It("should remove all GUIDs for multiple pods each with multiple networks", func() {
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "network1").Return(
				createIbSriovNAD("default", "network1", "0x0001"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "network2").Return(
				createIbSriovNAD("default", "network2", "0x0002"), nil)
			mockK8sClient.On("RemoveFinalizerFromPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("GetPods", mock.Anything).Return(&kapi.PodList{}, nil)
			mockK8sClient.On("RemoveFinalizerFromNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			// Create 3 pods, each with 2 networks (6 GUIDs total)
			var pods1, pods2 []*kapi.Pod
			var allGUIDs []string
			for i := 1; i <= 3; i++ {
				guid1 := fmt.Sprintf("02:00:00:00:00:00:00:%02x", i*2-1)
				guid2 := fmt.Sprintf("02:00:00:00:00:00:00:%02x", i*2)
				allGUIDs = append(allGUIDs, guid1, guid2)

				Expect(testGuidPool.AllocateGUID(guid1)).To(Succeed())
				Expect(testGuidPool.AllocateGUID(guid2)).To(Succeed())

				networks := []v1.NetworkSelectionElement{
					createConfiguredNetwork("network1", "default", guid1),
					createConfiguredNetwork("network2", "default", guid2),
				}
				netAnnotation, _ := json.Marshal(networks)
				pod := &kapi.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:        fmt.Sprintf("pod-%d", i),
						Namespace:   "default",
						UID:         types.UID(fmt.Sprintf("pod-%d-uid", i)),
						Annotations: map[string]string{v1.NetworkAttachmentAnnot: string(netAnnotation)},
					},
					Spec: kapi.PodSpec{NodeName: "test-node"},
				}
				pods1 = append(pods1, pod)
				pods2 = append(pods2, pod)

				testDaemon.guidPodNetworkMap[guid1] = fmt.Sprintf("pod-%d-uid_default_network1", i)
				testDaemon.guidPodNetworkMap[guid2] = fmt.Sprintf("pod-%d-uid_default_network2", i)
			}

			// Simulate deletion
			_, deleteMap := podHandler.GetResults()
			deleteMap.Set("default_network1", pods1)
			deleteMap.Set("default_network2", pods2)

			// Run DeletePeriodicUpdate
			testDaemon.DeletePeriodicUpdate()

			// Verify: all 6 GUIDs should be removed
			allRemovedGUIDs := mockSMClient.GetAllRemovedGUIDs()
			Expect(len(allRemovedGUIDs)).To(Equal(6), "Expected 6 GUIDs to be removed (3 pods x 2 networks)")
			for _, g := range allGUIDs {
				Expect(containsGUID(allRemovedGUIDs, g)).To(BeTrue(), fmt.Sprintf("GUID %s should be removed", g))
			}
		})
	})

	Context("End-to-end regression: Add then Delete", func() {
		It("should correctly add and then remove all GUIDs for pod with 3 networks", func() {
			// Setup NADs
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "net-x").Return(
				createIbSriovNAD("default", "net-x", "0x0001"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "net-y").Return(
				createIbSriovNAD("default", "net-y", "0x0002"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "net-z").Return(
				createIbSriovNAD("default", "net-z", "0x0003"), nil)
			mockK8sClient.On("SetAnnotationsOnPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("AddFinalizerToPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("AddFinalizerToNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("RemoveFinalizerFromPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("GetPods", mock.Anything).Return(&kapi.PodList{}, nil)
			mockK8sClient.On("RemoveFinalizerFromNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			// Phase 1: Add pod with 3 networks
			networks := []v1.NetworkSelectionElement{
				createNetworkSelection("net-x", "default"),
				createNetworkSelection("net-y", "default"),
				createNetworkSelection("net-z", "default"),
			}
			pod := createPodWithMultipleNetworks("e2e-pod", "default", networks)

			addMap, _ := podHandler.GetResults()
			addMap.Set("default_net-x", []*kapi.Pod{pod})
			addMap.Set("default_net-y", []*kapi.Pod{pod})
			addMap.Set("default_net-z", []*kapi.Pod{pod})

			testDaemon.AddPeriodicUpdate()

			// Verify add
			addedGUIDs := mockSMClient.GetAllAddedGUIDs()
			Expect(len(addedGUIDs)).To(Equal(3), "Expected 3 GUIDs to be allocated")

			// Phase 2: Delete the pod
			// Re-create pod with the allocated GUIDs (as configured networks)
			configuredNetworks := []v1.NetworkSelectionElement{
				createConfiguredNetwork("net-x", "default", addedGUIDs[0]),
				createConfiguredNetwork("net-y", "default", addedGUIDs[1]),
				createConfiguredNetwork("net-z", "default", addedGUIDs[2]),
			}
			netAnnotation, _ := json.Marshal(configuredNetworks)
			deletePod := &kapi.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "e2e-pod",
					Namespace:   "default",
					UID:         pod.UID,
					Annotations: map[string]string{v1.NetworkAttachmentAnnot: string(netAnnotation)},
				},
				Spec: kapi.PodSpec{NodeName: "test-node"},
			}

			_, deleteMap := podHandler.GetResults()
			deleteMap.Set("default_net-x", []*kapi.Pod{deletePod})
			deleteMap.Set("default_net-y", []*kapi.Pod{deletePod})
			deleteMap.Set("default_net-z", []*kapi.Pod{deletePod})

			testDaemon.DeletePeriodicUpdate()

			// Verify delete - all GUIDs that were added should now be removed
			removedGUIDs := mockSMClient.GetAllRemovedGUIDs()
			Expect(len(removedGUIDs)).To(Equal(3), "Expected 3 GUIDs to be removed")
			for _, addedGUID := range addedGUIDs {
				Expect(containsGUID(removedGUIDs, addedGUID)).To(BeTrue(),
					fmt.Sprintf("Added GUID %s should be removed", addedGUID))
			}
		})
	})

	Context("Regression: GUID counts across different scenarios", func() {
		It("should handle 10 pods with 2 networks each (20 total GUIDs)", func() {
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "net-a").Return(
				createIbSriovNAD("default", "net-a", "0x0001"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "default", "net-b").Return(
				createIbSriovNAD("default", "net-b", "0x0002"), nil)
			mockK8sClient.On("SetAnnotationsOnPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("AddFinalizerToPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("AddFinalizerToNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			var podsNetA, podsNetB []*kapi.Pod
			for i := 1; i <= 10; i++ {
				networks := []v1.NetworkSelectionElement{
					createNetworkSelection("net-a", "default"),
					createNetworkSelection("net-b", "default"),
				}
				pod := createPodWithMultipleNetworks(fmt.Sprintf("pod-%d", i), "default", networks)
				podsNetA = append(podsNetA, pod)
				podsNetB = append(podsNetB, pod)
			}

			addMap, _ := podHandler.GetResults()
			addMap.Set("default_net-a", podsNetA)
			addMap.Set("default_net-b", podsNetB)

			testDaemon.AddPeriodicUpdate()

			// Verify
			allGUIDs := mockSMClient.GetAllAddedGUIDs()
			Expect(len(allGUIDs)).To(Equal(20), "Expected 20 GUIDs (10 pods x 2 networks)")

			pkey1GUIDs := mockSMClient.GetAddedGUIDsForPKey(0x0001)
			pkey2GUIDs := mockSMClient.GetAddedGUIDsForPKey(0x0002)
			Expect(len(pkey1GUIDs)).To(Equal(10), "Expected 10 GUIDs on pkey 0x0001")
			Expect(len(pkey2GUIDs)).To(Equal(10), "Expected 10 GUIDs on pkey 0x0002")
		})

		It("should handle pod with networks in different namespaces", func() {
			mockK8sClient.On("GetNetworkAttachmentDefinition", "ns1", "net1").Return(
				createIbSriovNAD("ns1", "net1", "0x0001"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "ns2", "net2").Return(
				createIbSriovNAD("ns2", "net2", "0x0002"), nil)
			mockK8sClient.On("GetNetworkAttachmentDefinition", "ns3", "net3").Return(
				createIbSriovNAD("ns3", "net3", "0x0003"), nil)
			mockK8sClient.On("SetAnnotationsOnPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("AddFinalizerToPod", mock.Anything, mock.Anything).Return(nil)
			mockK8sClient.On("AddFinalizerToNetworkAttachmentDefinition", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			// Pod with networks from different namespaces
			networks := []v1.NetworkSelectionElement{
				createNetworkSelection("net1", "ns1"),
				createNetworkSelection("net2", "ns2"),
				createNetworkSelection("net3", "ns3"),
			}
			netAnnotation, _ := json.Marshal(networks)
			pod := &kapi.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "multi-ns-pod",
					Namespace:   "default",
					UID:         "multi-ns-pod-uid",
					Annotations: map[string]string{v1.NetworkAttachmentAnnot: string(netAnnotation)},
				},
				Spec: kapi.PodSpec{NodeName: "test-node"},
			}

			addMap, _ := podHandler.GetResults()
			addMap.Set("ns1_net1", []*kapi.Pod{pod})
			addMap.Set("ns2_net2", []*kapi.Pod{pod})
			addMap.Set("ns3_net3", []*kapi.Pod{pod})

			testDaemon.AddPeriodicUpdate()

			// Verify
			allGUIDs := mockSMClient.GetAllAddedGUIDs()
			Expect(len(allGUIDs)).To(Equal(3), "Expected 3 GUIDs for 3 networks across namespaces")
		})
	})
})
