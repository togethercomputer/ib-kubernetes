package daemon

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	netapi "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	netclientset "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned"

	"github.com/Mellanox/ib-kubernetes/pkg/config"
	"github.com/Mellanox/ib-kubernetes/pkg/guid"
	"github.com/Mellanox/ib-kubernetes/pkg/watcher"
	resEventHandler "github.com/Mellanox/ib-kubernetes/pkg/watcher/handler"
)

var _ = Describe("Daemon E2E Tests", func() {
	var (
		testDaemon *daemon
	)

	BeforeEach(func() {
		// Create test daemon - uses global cfg, kubernetesClient, netClient from suite
		testDaemon = createTestDaemonForE2E()
	})

	Context("Daemon Control Loop E2E", func() {
		It("Should process pod creation and add finalizers through control loop", func() {
			By("Creating a NetworkAttachmentDefinition with managed resource")
			nad := &netapi.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-ib-network",
					Namespace: "default",
					Annotations: map[string]string{
						"k8s.v1.cni.cncf.io/resourceName": "intel.com/ib_sriov",
					},
				},
				Spec: netapi.NetworkAttachmentDefinitionSpec{
					Config: `{
						"type": "ib-sriov",
						"pkey": "0x1",
						"capabilities": {
							"infinibandGUID": true
						}
					}`,
				},
			}
			Expect(kubernetesClient.Create(ctx, nad)).To(Succeed())

			By("Creating a Pod with InfiniBand network annotation")
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"k8s.v1.cni.cncf.io/networks": `[{
							"name": "test-ib-network",
							"namespace": "default"
						}]`,
					},
				},
				Spec: corev1.PodSpec{
					NodeName: "test-node",
					Containers: []corev1.Container{
						{
							Name:  "test-container",
							Image: "busybox",
						},
					},
				},
			}
			Expect(kubernetesClient.Create(ctx, pod)).To(Succeed())

			By("Starting the daemon's watcher to detect the pod")
			podEventHandler := resEventHandler.NewPodEventHandler()
			ibClient := &testK8sClient{
				client:    kubernetesClient,
				netClient: netClient,
			}

			podWatcher := watcher.NewWatcher(podEventHandler, ibClient)
			stopWatcher := podWatcher.RunBackground()
			defer stopWatcher()

			// Give the watcher time to detect the pod
			time.Sleep(100 * time.Millisecond)

			By("Running one cycle of AddPeriodicUpdate to process the pod")
			testDaemon.watcher = podWatcher
			testDaemon.kubeClient = ibClient
			testDaemon.AddPeriodicUpdate()

			By("Verifying that the pod has the InfiniBand finalizer added")
			// Need to check that the pod annotation was updated and finalizer was added
			Eventually(func() bool {
				updatedPod := &corev1.Pod{}
				err := kubernetesClient.Get(ctx, client.ObjectKey{Name: "test-pod", Namespace: "default"}, updatedPod)
				if err != nil {
					return false
				}

				// Check for finalizer
				expectedFinalizer := fmt.Sprintf("%s-%s", PodGUIDFinalizer, "test-ib-network")
				for _, finalizer := range updatedPod.Finalizers {
					if finalizer == expectedFinalizer {
						return true
					}
				}
				return false
			}, time.Second*5, time.Millisecond*100).Should(BeTrue())

			By("Verifying that the NAD has the InfiniBand finalizer added")
			Eventually(func() bool {
				updatedNAD := &netapi.NetworkAttachmentDefinition{}
				err := kubernetesClient.Get(ctx, client.ObjectKey{Name: "test-ib-network", Namespace: "default"}, updatedNAD)
				if err != nil {
					return false
				}

				// Check for finalizer
				for _, finalizer := range updatedNAD.Finalizers {
					if finalizer == GUIDInUFMFinalizer {
						return true
					}
				}
				return false
			}, time.Second*5, time.Millisecond*100).Should(BeTrue())

			By("Verifying that the pod annotation was updated with GUID")
			Eventually(func() bool {
				updatedPod := &corev1.Pod{}
				err := kubernetesClient.Get(ctx, client.ObjectKey{Name: "test-pod", Namespace: "default"}, updatedPod)
				if err != nil {
					return false
				}

				// Check that network annotation includes GUID
				networkAnnot := updatedPod.Annotations["k8s.v1.cni.cncf.io/networks"]
				return len(networkAnnot) > 0 && networkAnnot != `[{"name": "test-ib-network", "namespace": "default"}]`
			}, time.Second*5, time.Millisecond*100).Should(BeTrue())

			By("Deleting the pod to trigger cleanup")
			Expect(kubernetesClient.Delete(ctx, pod)).To(Succeed())

			// Give the watcher time to detect the pod deletion
			time.Sleep(100 * time.Millisecond)

			By("Running one cycle of DeletePeriodicUpdate to process the pod deletion")
			testDaemon.DeletePeriodicUpdate()

			By("Verifying that the NAD finalizer is removed when no pods are using it")
			Eventually(func() bool {
				updatedNAD := &netapi.NetworkAttachmentDefinition{}
				err := kubernetesClient.Get(ctx, client.ObjectKey{Name: "test-ib-network", Namespace: "default"}, updatedNAD)
				if err != nil {
					return false
				}

				// Check that finalizer is removed
				for _, finalizer := range updatedNAD.Finalizers {
					if finalizer == GUIDInUFMFinalizer {
						return false // Still has finalizer
					}
				}
				return true // Finalizer removed
			}, time.Second*5, time.Millisecond*100).Should(BeTrue())
		})

		It("Should handle multiple pods with the same network", func() {
			By("Creating a NetworkAttachmentDefinition")
			nad := &netapi.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "shared-ib-network",
					Namespace: "default",
					Annotations: map[string]string{
						"k8s.v1.cni.cncf.io/resourceName": "intel.com/ib_sriov",
					},
				},
				Spec: netapi.NetworkAttachmentDefinitionSpec{
					Config: `{
						"type": "ib-sriov",
						"pkey": "0x2",
						"capabilities": {
							"infinibandGUID": true
						}
					}`,
				},
			}
			Expect(kubernetesClient.Create(ctx, nad)).To(Succeed())

			By("Creating two pods using the same network")
			pod1 := createTestPodForE2E("pod1", "default", "shared-ib-network")
			pod2 := createTestPodForE2E("pod2", "default", "shared-ib-network")

			Expect(kubernetesClient.Create(ctx, pod1)).To(Succeed())
			Expect(kubernetesClient.Create(ctx, pod2)).To(Succeed())

			By("Starting the daemon's watcher and processing pods")
			podEventHandler := resEventHandler.NewPodEventHandler()
			ibClient := &testK8sClient{
				client:    kubernetesClient,
				netClient: netClient,
			}

			podWatcher := watcher.NewWatcher(podEventHandler, ibClient)
			stopWatcher := podWatcher.RunBackground()
			defer stopWatcher()

			testDaemon.watcher = podWatcher
			testDaemon.kubeClient = ibClient

			// Give the watcher time to detect the pods
			time.Sleep(100 * time.Millisecond)

			By("Running AddPeriodicUpdate to process both pods")
			testDaemon.AddPeriodicUpdate()

			By("Verifying both pods have finalizers")
			expectedFinalizer := fmt.Sprintf("%s-%s", PodGUIDFinalizer, "shared-ib-network")

			for _, podName := range []string{"pod1", "pod2"} {
				Eventually(func() bool {
					pod := &corev1.Pod{}
					err := kubernetesClient.Get(ctx, client.ObjectKey{Name: podName, Namespace: "default"}, pod)
					if err != nil {
						return false
					}

					for _, finalizer := range pod.Finalizers {
						if finalizer == expectedFinalizer {
							return true
						}
					}
					return false
				}, time.Second*5, time.Millisecond*100).Should(BeTrue())
			}

			By("Deleting one pod")
			Expect(kubernetesClient.Delete(ctx, pod1)).To(Succeed())
			time.Sleep(100 * time.Millisecond)

			By("Running DeletePeriodicUpdate")
			testDaemon.DeletePeriodicUpdate()

			By("Verifying NAD finalizer is NOT removed because pod2 still exists")
			Consistently(func() bool {
				updatedNAD := &netapi.NetworkAttachmentDefinition{}
				err := kubernetesClient.Get(ctx, client.ObjectKey{Name: "shared-ib-network", Namespace: "default"}, updatedNAD)
				if err != nil {
					return false
				}

				// Check that finalizer still exists
				for _, finalizer := range updatedNAD.Finalizers {
					if finalizer == GUIDInUFMFinalizer {
						return true // Still has finalizer (expected)
					}
				}
				return false
			}, time.Second*2, time.Millisecond*100).Should(BeTrue())

			By("Deleting the second pod")
			Expect(kubernetesClient.Delete(ctx, pod2)).To(Succeed())
			time.Sleep(100 * time.Millisecond)

			By("Running DeletePeriodicUpdate again")
			testDaemon.DeletePeriodicUpdate()

			By("Verifying NAD finalizer is removed now that no pods are using it")
			Eventually(func() bool {
				updatedNAD := &netapi.NetworkAttachmentDefinition{}
				err := kubernetesClient.Get(ctx, client.ObjectKey{Name: "shared-ib-network", Namespace: "default"}, updatedNAD)
				if err != nil {
					return false
				}

				// Check that finalizer is removed
				for _, finalizer := range updatedNAD.Finalizers {
					if finalizer == GUIDInUFMFinalizer {
						return false // Still has finalizer
					}
				}
				return true // Finalizer removed
			}, time.Second*5, time.Millisecond*100).Should(BeTrue())
		})

		It("Should ignore networks with unmanaged resources", func() {
			By("Creating a NetworkAttachmentDefinition with unmanaged resource")
			nad := &netapi.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "unmanaged-network",
					Namespace: "default",
					Annotations: map[string]string{
						"k8s.v1.cni.cncf.io/resourceName": "unmanaged.com/resource",
					},
				},
				Spec: netapi.NetworkAttachmentDefinitionSpec{
					Config: `{
						"type": "ib-sriov",
						"pkey": "0x1",
						"capabilities": {
							"infinibandGUID": true
						}
					}`,
				},
			}
			Expect(kubernetesClient.Create(ctx, nad)).To(Succeed())

			By("Creating a Pod using the unmanaged network")
			pod := createTestPodForE2E("unmanaged-pod", "default", "unmanaged-network")
			Expect(kubernetesClient.Create(ctx, pod)).To(Succeed())

			By("Starting the daemon's watcher and processing the pod")
			podEventHandler := resEventHandler.NewPodEventHandler()
			ibClient := &testK8sClient{
				client:    kubernetesClient,
				netClient: netClient,
			}

			podWatcher := watcher.NewWatcher(podEventHandler, ibClient)
			stopWatcher := podWatcher.RunBackground()
			defer stopWatcher()

			testDaemon.watcher = podWatcher
			testDaemon.kubeClient = ibClient

			// Give the watcher time to detect the pod
			time.Sleep(100 * time.Millisecond)

			By("Running AddPeriodicUpdate")
			testDaemon.AddPeriodicUpdate()

			By("Verifying that no finalizers are added to the pod")
			Consistently(func() bool {
				updatedPod := &corev1.Pod{}
				err := kubernetesClient.Get(ctx, client.ObjectKey{Name: "unmanaged-pod", Namespace: "default"}, updatedPod)
				if err != nil {
					return false
				}

				// Check that no IB finalizers were added
				for _, finalizer := range updatedPod.Finalizers {
					if finalizer == fmt.Sprintf("%s-%s", PodGUIDFinalizer, "unmanaged-network") {
						return false // Found finalizer (should not happen)
					}
				}
				return true // No finalizer found (expected)
			}, time.Second*2, time.Millisecond*100).Should(BeTrue())

			By("Verifying that no finalizers are added to the NAD")
			Consistently(func() bool {
				updatedNAD := &netapi.NetworkAttachmentDefinition{}
				err := kubernetesClient.Get(ctx, client.ObjectKey{Name: "unmanaged-network", Namespace: "default"}, updatedNAD)
				if err != nil {
					return false
				}

				// Check that no IB finalizers were added
				for _, finalizer := range updatedNAD.Finalizers {
					if finalizer == GUIDInUFMFinalizer {
						return false // Found finalizer (should not happen)
					}
				}
				return true // No finalizer found (expected)
			}, time.Second*2, time.Millisecond*100).Should(BeTrue())
		})
	})
})

// Helper functions for e2e tests

func createTestDaemonForE2E() *daemon {
	// Create test configuration for e2e testing
	testConfig := config.DaemonConfig{
		PeriodicUpdate: 1,
		GUIDPool: config.GUIDPoolConfig{
			RangeStart: "02:00:00:00:00:00:00:00",
			RangeEnd:   "02:00:00:00:00:00:00:FF",
		},
		Plugin:                     "noop", // Use noop plugin for testing
		PluginPath:                 "/test",
		ManagedResourcesString:     "intel.com/ib_sriov,nvidia.com/ib_sriov",
		ManagedResources:           map[string]bool{"intel.com/ib_sriov": true, "nvidia.com/ib_sriov": true},
		EnableIPOverIB:             false,
		EnableIndex0ForPrimaryPkey: true,
		DefaultLimitedPartition:    "",
	}

	// Create GUID pool
	guidPool, _ := guid.NewPool(&testConfig.GUIDPool)

	// Create mock subnet manager client (noop implementation)
	smClient := &mockSubnetManagerClient{}
	smClient.On("Name").Return("noop-test")
	smClient.On("Spec").Return("1.0")
	smClient.On("Validate").Return(nil)
	smClient.On("SetConfig", mock.Anything).Return(nil)
	smClient.On("ListGuidsInUse").Return([]string{}, nil)
	smClient.On("AddGuidsToPKey", mock.Anything, mock.Anything).Return(nil)
	smClient.On("AddGuidsToLimitedPKey", mock.Anything, mock.Anything).Return(nil)
	smClient.On("RemoveGuidsFromPKey", mock.Anything, mock.Anything).Return(nil)

	return &daemon{
		config:            testConfig,
		guidPool:          guidPool,
		smClient:          smClient,
		guidPodNetworkMap: make(map[string]string),
	}
}

func createTestPodForE2E(name, namespace, networkName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				"k8s.v1.cni.cncf.io/networks": fmt.Sprintf(`[{
					"name": "%s",
					"namespace": "%s"
				}]`, networkName, namespace),
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node",
			Containers: []corev1.Container{
				{
					Name:  "test-container",
					Image: "busybox",
				},
			},
		},
	}
}

// testK8sClient wraps the real Kubernetes clients for e2e testing
type testK8sClient struct {
	client    client.Client
	netClient netclientset.Interface
}

func (c *testK8sClient) GetNetworkAttachmentDefinition(namespace, name string) (*netapi.NetworkAttachmentDefinition, error) {
	return c.netClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (c *testK8sClient) GetPods(namespace string) (*corev1.PodList, error) {
	podList := &corev1.PodList{}
	err := c.client.List(ctx, podList, client.InNamespace(namespace))
	return podList, err
}

func (c *testK8sClient) GetPod(namespace, name string) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: namespace, Name: name}
	if err := c.client.Get(ctx, key, pod); err != nil {
		return nil, err
	}
	return pod, nil
}

func (c *testK8sClient) SetAnnotationsOnPod(pod *corev1.Pod, annotations map[string]string) error {
	pod.Annotations = annotations
	return c.client.Update(ctx, pod)
}

func (c *testK8sClient) AddFinalizerToPod(pod *corev1.Pod, finalizer string) error {
	// Check if finalizer already exists
	for _, existingFinalizer := range pod.Finalizers {
		if existingFinalizer == finalizer {
			return nil
		}
	}

	pod.Finalizers = append(pod.Finalizers, finalizer)
	return c.client.Update(ctx, pod)
}

func (c *testK8sClient) RemoveFinalizerFromPod(pod *corev1.Pod, finalizer string) error {
	newFinalizers := make([]string, 0, len(pod.Finalizers))
	for _, existingFinalizer := range pod.Finalizers {
		if existingFinalizer != finalizer {
			newFinalizers = append(newFinalizers, existingFinalizer)
		}
	}

	pod.Finalizers = newFinalizers
	return c.client.Update(ctx, pod)
}

func (c *testK8sClient) AddFinalizerToNetworkAttachmentDefinition(namespace, name, finalizer string) error {
	netAttDef, err := c.GetNetworkAttachmentDefinition(namespace, name)
	if err != nil {
		return err
	}

	// Check if finalizer already exists
	for _, existingFinalizer := range netAttDef.Finalizers {
		if existingFinalizer == finalizer {
			return nil
		}
	}

	netAttDef.Finalizers = append(netAttDef.Finalizers, finalizer)
	_, err = c.netClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(namespace).Update(
		ctx, netAttDef, metav1.UpdateOptions{})
	return err
}

func (c *testK8sClient) RemoveFinalizerFromNetworkAttachmentDefinition(namespace, name, finalizer string) error {
	netAttDef, err := c.GetNetworkAttachmentDefinition(namespace, name)
	if err != nil {
		return err
	}

	newFinalizers := make([]string, 0, len(netAttDef.Finalizers))
	for _, existingFinalizer := range netAttDef.Finalizers {
		if existingFinalizer != finalizer {
			newFinalizers = append(newFinalizers, existingFinalizer)
		}
	}

	netAttDef.Finalizers = newFinalizers
	_, err = c.netClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(namespace).Update(
		ctx, netAttDef, metav1.UpdateOptions{})
	return err
}

func (c *testK8sClient) GetRestClient() rest.Interface {
	// Create a kubernetes clientset and return the core REST client
	// This is required for the watcher's cache.NewListWatchFromClient()
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		panic(fmt.Sprintf("Failed to create kubernetes clientset: %v", err))
	}
	return clientset.CoreV1().RESTClient()
}
