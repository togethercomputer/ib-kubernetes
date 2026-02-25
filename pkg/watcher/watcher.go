package watcher

import (
	kapi "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"

	k8sClient "github.com/Mellanox/ib-kubernetes/pkg/k8s-client"
	resEventHandler "github.com/Mellanox/ib-kubernetes/pkg/watcher/handler"
)

var kubevirtSelectorString = SelectorMustValidateFromSet(labels.Set{"kubevirt.io": "virt-launcher"}).String()

type StopFunc func()

type Watcher interface {
	// Run Watcher in the background, listening for k8s resource events, until StopFunc is called
	RunBackground() StopFunc
	// Get ResourceEventHandler
	GetHandler() resEventHandler.ResourceEventHandler
}

type watcher struct {
	eventHandler resEventHandler.ResourceEventHandler
	watchList    cache.ListerWatcher
}

func NewWatcher(eventHandler resEventHandler.ResourceEventHandler, client k8sClient.Client) Watcher {
	resource := eventHandler.GetResourceObject().GetObjectKind().GroupVersionKind().Kind
	filterByKubevirtLabel := func(options *metav1.ListOptions) {
		options.LabelSelector = kubevirtSelectorString
	}
	watchList := cache.NewFilteredListWatchFromClient(
		client.GetRestClient(), resource, kapi.NamespaceAll, filterByKubevirtLabel)
	return &watcher{eventHandler: eventHandler, watchList: watchList}
}

// Run Watcher in the background, listening for k8s resource events, until StopFunc is called
func (w *watcher) RunBackground() StopFunc {
	stopChan := make(chan struct{})
	_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: w.watchList,
		ObjectType:    w.eventHandler.GetResourceObject(),
		ResyncPeriod:  0,
		Handler:       w.eventHandler,
	})
	go controller.Run(stopChan)
	return func() {
		stopChan <- struct{}{}
		close(stopChan)
	}
}

func (w *watcher) GetHandler() resEventHandler.ResourceEventHandler {
	return w.eventHandler
}

// SelectorMustValidateFromSet acts like regex.MustCompile for labels.Selector objects.
// It will attempt to validate the selector from the label set and panic if it fails.
func SelectorMustValidateFromSet(set labels.Set) labels.Selector {
	selector, err := labels.ValidatedSelectorFromSet(set)
	if err != nil {
		panic(err)
	}
	return selector
}
