package daemon

import (
	"encoding/json"
	"fmt"
	"net"

	v1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	kapi "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/Mellanox/ib-kubernetes/pkg/config"
	"github.com/Mellanox/ib-kubernetes/pkg/guid"
	k8sClientMock "github.com/Mellanox/ib-kubernetes/pkg/k8s-client/mocks"
)

// Mock subnet manager client for testing
type mockSubnetManagerClient struct {
	mock.Mock
}

func (m *mockSubnetManagerClient) Name() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockSubnetManagerClient) Spec() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockSubnetManagerClient) Validate() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockSubnetManagerClient) AddGuidsToPKey(pkey int, guids []net.HardwareAddr) error {
	args := m.Called(pkey, guids)
	return args.Error(0)
}

func (m *mockSubnetManagerClient) AddGuidsToLimitedPKey(pkey int, guids []net.HardwareAddr) error {
	args := m.Called(pkey, guids)
	return args.Error(0)
}

func (m *mockSubnetManagerClient) RemoveGuidsFromPKey(pkey int, guids []net.HardwareAddr) error {
	args := m.Called(pkey, guids)
	return args.Error(0)
}

func (m *mockSubnetManagerClient) ListGuidsInUse() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

func (m *mockSubnetManagerClient) SetConfig(config map[string]interface{}) error {
	args := m.Called(config)
	return args.Error(0)
}

// Create a mock daemon for testing
func createTestDaemon() (*daemon, *k8sClientMock.Client, *mockSubnetManagerClient) {
	kubeClientMock := &k8sClientMock.Client{}
	smClientMock := &mockSubnetManagerClient{}

	// Set up basic mock responses
	smClientMock.On("Name").Return("test-sm")
	smClientMock.On("Spec").Return("1.0")
	smClientMock.On("Validate").Return(nil)
	smClientMock.On("SetConfig", mock.Anything).Return(nil)
	smClientMock.On("ListGuidsInUse").Return([]string{}, nil)

	// Create test configuration
	testConfig := config.DaemonConfig{
		PeriodicUpdate: 1,
		GUIDPool: config.GUIDPoolConfig{
			RangeStart: "02:00:00:00:00:00:00:00",
			RangeEnd:   "02:00:00:00:00:00:00:FF",
		},
		Plugin:                     "test",
		PluginPath:                 "/test",
		ManagedResourcesString:     "intel.com/ib_sriov,nvidia.com/ib_sriov",
		ManagedResources:           map[string]bool{"intel.com/ib_sriov": true, "nvidia.com/ib_sriov": true},
		EnableIPOverIB:             false,
		EnableIndex0ForPrimaryPkey: true,
		DefaultLimitedPartition:    "",
	}

	// Create GUID pool
	guidPool, _ := guid.NewPool(&testConfig.GUIDPool)

	return &daemon{
		config:            testConfig,
		kubeClient:        kubeClientMock,
		guidPool:          guidPool,
		smClient:          smClientMock,
		guidPodNetworkMap: make(map[string]string),
	}, kubeClientMock, smClientMock
}

// Helper function to create test pods with InfiniBand network annotations
func createTestPod(name, namespace string, networks []testNetworkSpec) *kapi.Pod {
	networkAnnotations := make([]map[string]interface{}, 0)

	for _, network := range networks {
		netAnnot := map[string]interface{}{
			"name":      network.Name,
			"namespace": network.Namespace,
		}
		if network.GUID != "" {
			netAnnot["cni-args"] = map[string]interface{}{
				"guid":                    network.GUID,
				"mellanox.infiniband.app": "configured",
			}
		} else if network.EnableIB {
			netAnnot["cni-args"] = map[string]interface{}{
				"mellanox.infiniband.app": "configured",
			}
		}
		networkAnnotations = append(networkAnnotations, netAnnot)
	}

	netAnnotJson, _ := json.Marshal(networkAnnotations)

	return &kapi.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       "test-pod-uid",
			Annotations: map[string]string{
				v1.NetworkAttachmentAnnot: string(netAnnotJson),
			},
		},
		Spec: kapi.PodSpec{
			NodeName: "test-node",
		},
	}
}

type testNetworkSpec struct {
	Name      string
	Namespace string
	GUID      string
	EnableIB  bool
}

// Helper function to create test NetworkAttachmentDefinition
func createTestNAD(name, namespace, resourceName string) *v1.NetworkAttachmentDefinition {
	spec := map[string]interface{}{
		"type":         "ib-sriov",
		"pkey":         "0x1",
		"capabilities": map[string]bool{"infinibandGUID": true},
	}
	specJson, _ := json.Marshal(spec)

	return &v1.NetworkAttachmentDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				"k8s.v1.cni.cncf.io/resourceName": resourceName,
			},
		},
		Spec: v1.NetworkAttachmentDefinitionSpec{
			Config: string(specJson),
		},
	}
}

var _ = Describe("Daemon Finalizer Tests", func() {
	var (
		testDaemon *daemon
		kubeClient *k8sClientMock.Client
		smClient   *mockSubnetManagerClient
	)

	BeforeEach(func() {
		testDaemon, kubeClient, smClient = createTestDaemon()
	})

	Describe("Double finalizer create and delete", func() {
		Context("When creating pods with both nic types", func() {
			It("Should add finalizers to both pod and NAD, then remove them on delete", func() {
				// Create test pod with two different InfiniBand networks
				networks := []testNetworkSpec{
					{Name: "ib-network1", Namespace: "default", EnableIB: true},
					{Name: "ib-network2", Namespace: "default", EnableIB: true},
				}
				testPod := createTestPod("test-pod", "default", networks)

				// Create corresponding NADs
				nad1 := createTestNAD("ib-network1", "default", "intel.com/ib_sriov")
				nad2 := createTestNAD("ib-network2", "default", "nvidia.com/ib_sriov")

				// Mock k8s client responses for NADs
				kubeClient.On("GetNetworkAttachmentDefinition", "default", "ib-network1").Return(nad1, nil)
				kubeClient.On("GetNetworkAttachmentDefinition", "default", "ib-network2").Return(nad2, nil)

				// Mock finalizer addition on pods
				kubeClient.On("AddFinalizerToPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "ib-network1")).Return(nil)
				kubeClient.On("AddFinalizerToPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "ib-network2")).Return(nil)

				// Mock finalizer addition on NADs
				kubeClient.On("AddFinalizerToNetworkAttachmentDefinition", "default", "ib-network1", GUIDInUFMFinalizer).Return(nil)
				kubeClient.On("AddFinalizerToNetworkAttachmentDefinition", "default", "ib-network2", GUIDInUFMFinalizer).Return(nil)

				// Mock pod annotation updates
				kubeClient.On("GetPod", testPod.Namespace, testPod.Name).Return(testPod, nil)
				kubeClient.On("SetAnnotationsOnPod", mock.AnythingOfType("*v1.Pod"), mock.AnythingOfType("map[string]string")).Return(nil)

				// Mock SM client calls
				smClient.On("AddGuidsToPKey", 1, mock.AnythingOfType("[]net.HardwareAddr")).Return(nil)

				// Test adding finalizers for network1
				err := testDaemon.addPodFinalizer(testPod, "ib-network1")
				Expect(err).ToNot(HaveOccurred())

				err = testDaemon.addNADFinalizer("default", "ib-network1")
				Expect(err).ToNot(HaveOccurred())

				// Test adding finalizers for network2
				err = testDaemon.addPodFinalizer(testPod, "ib-network2")
				Expect(err).ToNot(HaveOccurred())

				err = testDaemon.addNADFinalizer("default", "ib-network2")
				Expect(err).ToNot(HaveOccurred())

				// Verify finalizer calls were made
				kubeClient.AssertCalled(GinkgoT(), "AddFinalizerToPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "ib-network1"))
				kubeClient.AssertCalled(GinkgoT(), "AddFinalizerToPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "ib-network2"))
				kubeClient.AssertCalled(GinkgoT(), "AddFinalizerToNetworkAttachmentDefinition", "default", "ib-network1", GUIDInUFMFinalizer)
				kubeClient.AssertCalled(GinkgoT(), "AddFinalizerToNetworkAttachmentDefinition", "default", "ib-network2", GUIDInUFMFinalizer)

				// Now test finalizer removal
				// Mock Pod list for checking if any pods using networks
				emptyPodList := &kapi.PodList{Items: []kapi.Pod{}}
				kubeClient.On("GetPods", kapi.NamespaceAll).Return(emptyPodList, nil)

				// Mock finalizer removal from pods
				kubeClient.On("RemoveFinalizerFromPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "ib-network1")).Return(nil)
				kubeClient.On("RemoveFinalizerFromPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "ib-network2")).Return(nil)

				// Mock finalizer removal from NADs
				kubeClient.On("RemoveFinalizerFromNetworkAttachmentDefinition", "default", "ib-network1", GUIDInUFMFinalizer).Return(nil)
				kubeClient.On("RemoveFinalizerFromNetworkAttachmentDefinition", "default", "ib-network2", GUIDInUFMFinalizer).Return(nil)

				// Mock SM client GUID removal
				smClient.On("RemoveGuidsFromPKey", 1, mock.AnythingOfType("[]net.HardwareAddr")).Return(nil)

				// Test removing finalizers
				err = testDaemon.removePodFinalizer(testPod, "ib-network1")
				Expect(err).ToNot(HaveOccurred())

				err = testDaemon.removePodFinalizer(testPod, "ib-network2")
				Expect(err).ToNot(HaveOccurred())

				err = testDaemon.removeNADFinalizerIfSafe("default", "ib-network1")
				Expect(err).ToNot(HaveOccurred())

				err = testDaemon.removeNADFinalizerIfSafe("default", "ib-network2")
				Expect(err).ToNot(HaveOccurred())

				// Verify finalizer removal calls were made
				kubeClient.AssertCalled(GinkgoT(), "RemoveFinalizerFromPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "ib-network1"))
				kubeClient.AssertCalled(GinkgoT(), "RemoveFinalizerFromPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "ib-network2"))
				kubeClient.AssertCalled(GinkgoT(), "RemoveFinalizerFromNetworkAttachmentDefinition", "default", "ib-network1", GUIDInUFMFinalizer)
				kubeClient.AssertCalled(GinkgoT(), "RemoveFinalizerFromNetworkAttachmentDefinition", "default", "ib-network2", GUIDInUFMFinalizer)
			})
		})
	})

	Describe("Finalizer exclusivity", func() {
		Context("When external finalizers are present", func() {
			It("Should not remove NAD finalizer if other pods are still using the network", func() {
				// Create test pod using network with GUID (so it's detected as using the network)
				networks := []testNetworkSpec{
					{Name: "ib-network1", Namespace: "default", GUID: "02:00:00:00:00:00:00:01", EnableIB: true},
				}
				testPod2 := createTestPod("test-pod2", "default", networks)

				// Create NAD
				nad := createTestNAD("ib-network1", "default", "intel.com/ib_sriov")

				// Mock k8s client responses
				kubeClient.On("GetNetworkAttachmentDefinition", "default", "ib-network1").Return(nad, nil)

				// Mock Pod list showing other pods still using the network
				podsUsingNetwork := &kapi.PodList{
					Items: []kapi.Pod{*testPod2}, // testPod2 is still using the network with GUID
				}
				kubeClient.On("GetPods", kapi.NamespaceAll).Return(podsUsingNetwork, nil)

				// Attempt to remove NAD finalizer - should not remove it because other pods exist
				err := testDaemon.removeNADFinalizerIfSafe("default", "ib-network1")
				Expect(err).ToNot(HaveOccurred())

				// Verify that RemoveFinalizerFromNetworkAttachmentDefinition was NOT called
				kubeClient.AssertNotCalled(GinkgoT(), "RemoveFinalizerFromNetworkAttachmentDefinition", "default", "ib-network1", GUIDInUFMFinalizer)
			})

			It("Should handle co-existing with other ib-kubernetes finalizers", func() {
				// This test simulates the scenario where an external ib-kubernetes adds a finalizer
				// and this ib-kubernetes should not remove the finalizer.

				networks := []testNetworkSpec{
					{Name: "ib-network1", Namespace: "default", EnableIB: true},
				}
				testPod := createTestPod("test-pod", "default", networks)

				// Add an external finalizer to the pod metadata
				externalFinalizer := fmt.Sprintf("%s-%s", PodGUIDFinalizer, "external-ib-network1")
				testPod.Finalizers = []string{
					fmt.Sprintf("%s-%s", PodGUIDFinalizer, "ib-network1"),
					externalFinalizer, // Placed by a separate ib-kubernetes
				}

				nad := createTestNAD("ib-network1", "default", "intel.com/ib_sriov")

				// Mock k8s client responses
				kubeClient.On("GetNetworkAttachmentDefinition", "default", "ib-network1").Return(nad, nil)
				kubeClient.On("RemoveFinalizerFromPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "ib-network1")).Return(nil)

				// Mock empty pods list for NAD finalizer removal
				emptyPodList := &kapi.PodList{Items: []kapi.Pod{}}
				kubeClient.On("GetPods", kapi.NamespaceAll).Return(emptyPodList, nil)
				kubeClient.On("RemoveFinalizerFromNetworkAttachmentDefinition", "default", "ib-network1", GUIDInUFMFinalizer).Return(nil)

				// ib-kubernetes can successfully remove its own finalizer
				err := testDaemon.removePodFinalizer(testPod, "ib-network1")
				Expect(err).ToNot(HaveOccurred())

				err = testDaemon.removeNADFinalizerIfSafe("default", "ib-network1")
				Expect(err).ToNot(HaveOccurred())

				// Verify ib-kubernetes finalizer was removed
				kubeClient.AssertCalled(GinkgoT(), "RemoveFinalizerFromPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "ib-network1"))
				kubeClient.AssertCalled(GinkgoT(), "RemoveFinalizerFromNetworkAttachmentDefinition", "default", "ib-network1", GUIDInUFMFinalizer)

				// Ensure the external finalizer is still present
				Expect(testPod.Finalizers).To(ContainElement(externalFinalizer))
			})
		})
	})

	Describe("Create exclusivity", func() {
		Context("When pod uses network not managed by this daemon", func() {
			It("Should not add finalizers to unmanaged networks", func() {
				// Create NAD with unmanaged resource
				nad := createTestNAD("non-ib-network", "default", "unmanaged.com/resource")

				// Mock k8s client to return the NAD
				kubeClient.On("GetNetworkAttachmentDefinition", "default", "non-ib-network").Return(nad, nil)

				// Test getIbSriovNetwork - should return nil for unmanaged resource
				networkName, ibCniSpec, err := testDaemon.getIbSriovNetwork("default_non-ib-network")
				Expect(err).To(MatchError(fmt.Errorf("network %s uses resource %s which is not managed by this daemon", "non-ib-network", "unmanaged.com/resource")))
				Expect(networkName).To(BeEmpty())
				Expect(ibCniSpec).To(BeNil())
			})

			It("Should not process networks without resourceName annotation", func() {
				// Create NAD without resourceName annotation
				nad := &v1.NetworkAttachmentDefinition{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "no-resource-network",
						Namespace:   "default",
						Annotations: map[string]string{}, // No resourceName annotation
					},
					Spec: v1.NetworkAttachmentDefinitionSpec{
						Config: `{"type": "ib-sriov", "pkey": "0x1"}`,
					},
				}

				kubeClient.On("GetNetworkAttachmentDefinition", "default", "no-resource-network").Return(nad, nil)

				// Test getIbSriovNetwork - should return nil for network without resourceName
				networkName, ibCniSpec, err := testDaemon.getIbSriovNetwork("default_no-resource-network")
				Expect(err).To(MatchError(fmt.Errorf("network %s uses resource %s which is not managed by this daemon", "no-resource-network", "")))
				Expect(networkName).To(BeEmpty())
				Expect(ibCniSpec).To(BeNil())
			})
		})

		Context("When pod uses managed network", func() {
			It("Should add finalizers for managed resources", func() {
				// Create test pod with managed network
				networks := []testNetworkSpec{
					{Name: "managed-network", Namespace: "default", EnableIB: true},
				}
				testPod := createTestPod("test-pod", "default", networks)

				// Create NAD with managed resource
				nad := createTestNAD("managed-network", "default", "intel.com/ib_sriov")

				kubeClient.On("GetNetworkAttachmentDefinition", "default", "managed-network").Return(nad, nil)
				kubeClient.On("AddFinalizerToPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "managed-network")).Return(nil)
				kubeClient.On("AddFinalizerToNetworkAttachmentDefinition", "default", "managed-network", GUIDInUFMFinalizer).Return(nil)

				// Test getIbSriovNetwork - should return valid spec for managed resource
				networkName, ibCniSpec, err := testDaemon.getIbSriovNetwork("default_managed-network")
				Expect(err).ToNot(HaveOccurred())
				Expect(networkName).To(Equal("managed-network"))
				Expect(ibCniSpec).ToNot(BeNil())
				Expect(ibCniSpec.Type).To(Equal("ib-sriov"))
				Expect(ibCniSpec.PKey).To(Equal("0x1"))

				// Test finalizer addition
				err = testDaemon.addPodFinalizer(testPod, "managed-network")
				Expect(err).ToNot(HaveOccurred())

				err = testDaemon.addNADFinalizer("default", "managed-network")
				Expect(err).ToNot(HaveOccurred())

				// Verify finalizer methods were called for managed network
				kubeClient.AssertCalled(GinkgoT(), "AddFinalizerToPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "managed-network"))
				kubeClient.AssertCalled(GinkgoT(), "AddFinalizerToNetworkAttachmentDefinition", "default", "managed-network", GUIDInUFMFinalizer)
			})
		})
	})

	Describe("GUID Pool Management with Finalizers", func() {
		Context("When allocating and releasing GUIDs", func() {
			It("Should properly manage GUID allocation lifecycle with finalizers", func() {
				// Create test pod
				networks := []testNetworkSpec{
					{Name: "ib-network", Namespace: "default", EnableIB: true},
				}
				testPod := createTestPod("test-pod", "default", networks)

				// Create NAD
				nad := createTestNAD("ib-network", "default", "intel.com/ib_sriov")

				kubeClient.On("GetNetworkAttachmentDefinition", "default", "ib-network").Return(nad, nil)
				kubeClient.On("AddFinalizerToPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "ib-network")).Return(nil)
				kubeClient.On("AddFinalizerToNetworkAttachmentDefinition", "default", "ib-network", GUIDInUFMFinalizer).Return(nil)
				kubeClient.On("RemoveFinalizerFromPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "ib-network")).Return(nil)
				kubeClient.On("RemoveFinalizerFromNetworkAttachmentDefinition", "default", "ib-network", GUIDInUFMFinalizer).Return(nil)

				// Mock empty pod list for safe NAD finalizer removal
				emptyPodList := &kapi.PodList{Items: []kapi.Pod{}}
				kubeClient.On("GetPods", kapi.NamespaceAll).Return(emptyPodList, nil)

				// Generate a GUID
				guidAddr, err := testDaemon.guidPool.GenerateGUID()
				Expect(err).ToNot(HaveOccurred())

				podNetworkID := fmt.Sprintf("%s_ib-network", testPod.UID)
				allocatedGUID := guidAddr.String()

				// Test GUID allocation
				err = testDaemon.allocatePodNetworkGUID(allocatedGUID, podNetworkID, testPod.UID)
				Expect(err).ToNot(HaveOccurred())

				// Verify GUID was allocated
				Expect(testDaemon.guidPodNetworkMap[allocatedGUID]).To(Equal(podNetworkID))

				// Add finalizers
				err = testDaemon.addPodFinalizer(testPod, "ib-network")
				Expect(err).ToNot(HaveOccurred())

				err = testDaemon.addNADFinalizer("default", "ib-network")
				Expect(err).ToNot(HaveOccurred())

				// Release GUID and remove finalizers
				err = testDaemon.guidPool.ReleaseGUID(allocatedGUID)
				Expect(err).ToNot(HaveOccurred())

				delete(testDaemon.guidPodNetworkMap, allocatedGUID)

				err = testDaemon.removePodFinalizer(testPod, "ib-network")
				Expect(err).ToNot(HaveOccurred())

				err = testDaemon.removeNADFinalizerIfSafe("default", "ib-network")
				Expect(err).ToNot(HaveOccurred())

				// Verify GUID was released and mapping cleared
				Expect(testDaemon.guidPodNetworkMap[allocatedGUID]).To(BeEmpty())

				// Verify finalizer calls
				kubeClient.AssertCalled(GinkgoT(), "AddFinalizerToPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "ib-network"))
				kubeClient.AssertCalled(GinkgoT(), "AddFinalizerToNetworkAttachmentDefinition", "default", "ib-network", GUIDInUFMFinalizer)
				kubeClient.AssertCalled(GinkgoT(), "RemoveFinalizerFromPod", testPod, fmt.Sprintf("%s-%s", PodGUIDFinalizer, "ib-network"))
				kubeClient.AssertCalled(GinkgoT(), "RemoveFinalizerFromNetworkAttachmentDefinition", "default", "ib-network", GUIDInUFMFinalizer)
			})
		})
	})
})
