package k8sclient

import (
	"context"
	"fmt"
	"reflect"
	"time"

	netapi "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	netclient "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned/typed/k8s.cni.cncf.io/v1"
	"github.com/rs/zerolog/log"
	kapi "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

type Client interface {
	GetPods(namespace string) (*kapi.PodList, error)
	GetPod(namespace, name string) (*kapi.Pod, error)
	SetAnnotationsOnPod(pod *kapi.Pod, annotations map[string]string) error
	GetNetworkAttachmentDefinition(namespace, name string) (*netapi.NetworkAttachmentDefinition, error)
	GetRestClient() rest.Interface
	AddFinalizerToNetworkAttachmentDefinition(namespace, name, finalizer string) error
	RemoveFinalizerFromNetworkAttachmentDefinition(namespace, name, finalizer string) error
	AddFinalizerToPod(pod *kapi.Pod, finalizer string) error
	RemoveFinalizerFromPod(pod *kapi.Pod, finalizer string) error
}

type client struct {
	clientset kubernetes.Interface
	netClient netclient.K8sCniCncfIoV1Interface
}

var backoffValues = wait.Backoff{Duration: 1 * time.Second, Factor: 1.6, Jitter: 0.1, Steps: 6}

// NewK8sClient returns a kubernetes client
func NewK8sClient() (Client, error) {
	// Get a config to talk to the api server
	log.Debug().Msg("Setting up kubernetes client")
	conf, err := config.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to set up client config error %v", err)
	}

	clientset, err := kubernetes.NewForConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("unable to create a kubernetes client error %v", err)
	}

	netClient, err := netclient.NewForConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("unable to create a network attachment client: %v", err)
	}

	return &client{clientset: clientset, netClient: netClient}, nil
}

// GetPods obtains the Pods resources from kubernetes api server for given namespace
func (c *client) GetPods(namespace string) (*kapi.PodList, error) {
	log.Debug().Msgf("getting pods in namespace %s", namespace)
	return c.clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
}

// GetPod obtains a single Pod resource from the kubernetes api server
func (c *client) GetPod(namespace, name string) (*kapi.Pod, error) {
	log.Debug().Msgf("getting pod namespace: %s, name: %s", namespace, name)
	return c.clientset.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// SetAnnotationsOnPod takes the pod object and map of key/value string pairs to set as annotations
func (c *client) SetAnnotationsOnPod(pod *kapi.Pod, annotations map[string]string) error {
	log.Info().Msgf("Setting annotation on pod, namespace: %s, podName: %s, annotations: %v",
		pod.Namespace, pod.Name, annotations)

	// Get the latest version of the pod
	currentPod, err := c.clientset.CoreV1().Pods(pod.Namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// Check if there are any conflicts with the current view of the pod's annotations and the latest version.
	if currentPod.Annotations != nil {
		if !reflect.DeepEqual(currentPod.Annotations, pod.Annotations) {
			return fmt.Errorf("conflict with the current view of the pod's annotations and the latest version")
		}
	}

	currentPod.Annotations = annotations

	// Update the pod with retry and backoff
	err = wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		_, err = c.clientset.CoreV1().Pods(pod.Namespace).Update(
			context.Background(), currentPod, metav1.UpdateOptions{})
		return err == nil, nil
	})
	return err
}

// GetNetworkAttachmentDefinition returns the network crd from kubernetes api server for given namespace and name
func (c *client) GetNetworkAttachmentDefinition(namespace, name string) (*netapi.NetworkAttachmentDefinition, error) {
	log.Debug().Msgf("getting NetworkAttachmentDefinition namespace %s, name: %s", namespace, name)
	return c.netClient.NetworkAttachmentDefinitions(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// GetRestClient returns the client rest api for k8s
func (c *client) GetRestClient() rest.Interface {
	return c.clientset.CoreV1().RESTClient()
}

// AddFinalizerToNetworkAttachmentDefinition adds a finalizer to a NetworkAttachmentDefinition
func (c *client) AddFinalizerToNetworkAttachmentDefinition(namespace, name, finalizer string) error {
	netAttDef, err := c.GetNetworkAttachmentDefinition(namespace, name)
	if err != nil {
		return err
	}

	// Check if finalizer already exists
	for _, existingFinalizer := range netAttDef.Finalizers {
		if existingFinalizer == finalizer {
			return nil // Finalizer already exists, nothing to do
		}
	}

	// Add the finalizer
	netAttDef.Finalizers = append(netAttDef.Finalizers, finalizer)

	// Update the NetworkAttachmentDefinition
	_, err = c.netClient.NetworkAttachmentDefinitions(namespace).Update(
		context.Background(), netAttDef, metav1.UpdateOptions{})
	return err
}

// RemoveFinalizerFromNetworkAttachmentDefinition removes a finalizer from a NetworkAttachmentDefinition
func (c *client) RemoveFinalizerFromNetworkAttachmentDefinition(namespace, name, finalizer string) error {
	netAttDef, err := c.GetNetworkAttachmentDefinition(namespace, name)
	if err != nil {
		return err
	}

	// Check if finalizer exists
	var found bool
	var updatedFinalizers []string
	for _, existingFinalizer := range netAttDef.Finalizers {
		if existingFinalizer != finalizer {
			updatedFinalizers = append(updatedFinalizers, existingFinalizer)
		} else {
			found = true
		}
	}

	// If finalizer wasn't found, nothing to do
	if !found {
		return nil
	}

	// Update finalizers
	netAttDef.Finalizers = updatedFinalizers

	// Update the NetworkAttachmentDefinition
	_, err = c.netClient.NetworkAttachmentDefinitions(namespace).Update(
		context.Background(), netAttDef, metav1.UpdateOptions{})
	return err
}

// AddFinalizerToPod adds a finalizer to a Pod
func (c *client) AddFinalizerToPod(pod *kapi.Pod, finalizer string) error {
	// Get the latest version of the pod
	currentPod, err := c.clientset.CoreV1().Pods(pod.Namespace).Get(
		context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// Check if finalizer already exists
	for _, existingFinalizer := range currentPod.Finalizers {
		if existingFinalizer == finalizer {
			return nil // Finalizer already exists, nothing to do
		}
	}

	// Add the finalizer
	currentPod.Finalizers = append(currentPod.Finalizers, finalizer)

	// Update the Pod with retry and backoff
	err = wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		_, err = c.clientset.CoreV1().Pods(pod.Namespace).Update(
			context.Background(), currentPod, metav1.UpdateOptions{})
		return err == nil, nil
	})
	return err
}

// RemoveFinalizerFromPod removes a finalizer from a Pod
func (c *client) RemoveFinalizerFromPod(pod *kapi.Pod, finalizer string) error {
	// Get the latest version of the pod
	currentPod, err := c.clientset.CoreV1().Pods(pod.Namespace).Get(
		context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// Check if finalizer exists and remove it
	var found bool
	var updatedFinalizers []string
	for _, existingFinalizer := range currentPod.Finalizers {
		if existingFinalizer != finalizer {
			updatedFinalizers = append(updatedFinalizers, existingFinalizer)
		} else {
			found = true
		}
	}

	// If finalizer wasn't found, nothing to do
	if !found {
		return nil
	}

	// Update finalizers
	currentPod.Finalizers = updatedFinalizers

	// Update the Pod with retry and backoff
	err = wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		_, err = c.clientset.CoreV1().Pods(pod.Namespace).Update(
			context.Background(), currentPod, metav1.UpdateOptions{})
		return err == nil, nil
	})
	return err
}
