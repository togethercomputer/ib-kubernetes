package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"

	v1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	netAttUtils "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/utils"
	"github.com/rs/zerolog/log"
	kapi "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/Mellanox/ib-kubernetes/pkg/config"
	"github.com/Mellanox/ib-kubernetes/pkg/guid"
	k8sClient "github.com/Mellanox/ib-kubernetes/pkg/k8s-client"
	"github.com/Mellanox/ib-kubernetes/pkg/sm"
	"github.com/Mellanox/ib-kubernetes/pkg/sm/plugins"
	"github.com/Mellanox/ib-kubernetes/pkg/utils"
	"github.com/Mellanox/ib-kubernetes/pkg/watcher"
	resEvenHandler "github.com/Mellanox/ib-kubernetes/pkg/watcher/handler"
)

const GUIDInUFMFinalizer = "ufm.together.ai/guid-cleanup-protection"
const PodGUIDFinalizer = "ufm.together.ai/pod-guid-cleanup-protection"

type Daemon interface {
	// Execute Daemon loop, returns when os.Interrupt signal is received
	Run()
}

type daemon struct {
	config                        config.DaemonConfig
	watcher                       watcher.Watcher
	kubeClient                    k8sClient.Client
	guidPool                      guid.Pool
	smClient                      plugins.SubnetManagerClient
	guidPodNetworkMap             map[string]string // allocated guid mapped to the pod and network
	lastPkeyAPICallTimestamp      time.Time         // timestamp of the last initiated pkey modification API call
	lastPkeyAPICallTimestampMutex sync.Mutex        // proctects lastPkeyAPICallTimestamp
}

// Temporary struct used to proceed pods' networks
type podNetworkInfo struct {
	pod       *kapi.Pod
	ibNetwork *v1.NetworkSelectionElement
	networks  []*v1.NetworkSelectionElement
	addr      net.HardwareAddr // GUID allocated for ibNetwork and saved as net.HardwareAddr
}

type networksMap struct {
	theMap map[types.UID][]*v1.NetworkSelectionElement
}

// canProceedWithPkeyModification avoids making pkey modification calls before the previous call is completed.
// It queries the SM's pkey last_updated timestamp and compares it to the last time the client has made a call to the pkey API.
// If last_updated timestamp > stored API call timestamp, our previous operation likely completed and we can proceed.
func (d *daemon) canProceedWithPkeyModification() bool {
	lastPkeyUpdateTimestamp, err := d.smClient.GetLastPKeyUpdateTimestamp()
	d.lastPkeyAPICallTimestampMutex.Lock()
	// I'm not a huge fan of wrapping a network call in a mutex, but I think it's necessary to avoid a TOCTOU race.
	defer d.lastPkeyAPICallTimestampMutex.Unlock()
	lastAPICallTimestamp := d.lastPkeyAPICallTimestamp
	if err != nil {
		log.Warn().Msgf("failed to get SM pkey last_updated timestamp for canProceedWithPkeyModification check: %v", err)
		// If we can't get the timestamp, don't proceed
		return false
	}

	// SM returns null for last_updated when no PKey updates have been done yet
	// In this case, we can proceed since there's nothing in progress
	if lastPkeyUpdateTimestamp.IsZero() {
		log.Debug().Msgf("SM pkey last_updated is null (no updates yet), proceeding with pkey modification call")
		d.updateLastPkeyAPICallTimestamp()
		return true
	}

	// First call from our side - no previous timestamp stored
	if lastAPICallTimestamp.IsZero() {
		log.Debug().Msgf("no previous timestamp stored locally, proceeding with SM call")
		d.updateLastPkeyAPICallTimestamp()
		return true
	}

	// Check if last_updated timestamp > stored API call timestamp
	// If the SM's last_updated has advanced past our stored API call timestamp, our previous operation likely completed
	if lastPkeyUpdateTimestamp.After(lastAPICallTimestamp) {
		log.Debug().Msgf("SM pkey last_updated %v > stored timestamp %v, proceeding",
			lastPkeyUpdateTimestamp, lastAPICallTimestamp)
		d.updateLastPkeyAPICallTimestamp()
		return true
	}

	log.Info().Msgf("pkey last_updated %v <= stored timestamp %v, skipping pkey modification call this cycle (previous op may still be in progress)",
		lastPkeyUpdateTimestamp, lastAPICallTimestamp)
	return false
}

// updateLastPkeyAPICallTimestamp updates the stored server side timestamp when a pkey modification API call was made.
func (d *daemon) updateLastPkeyAPICallTimestamp() {
	currentTimestamp, err := d.smClient.GetServerTime()
	if err != nil {
		log.Warn().Msgf("failed to get SM current time: %v", err)
		return
	}

	if currentTimestamp.IsZero() {
		log.Warn().Msg("failed to get SM current time: returned time is 0")
		return
	}

	if currentTimestamp.After(d.lastPkeyAPICallTimestamp) {
		d.lastPkeyAPICallTimestamp = currentTimestamp
	}
	log.Debug().Msgf("Updated last pkey modification timestamp to %v", currentTimestamp)
}

// Exponential backoff ~26 sec + 6 * <api call time>
// NOTE: k8s client has built in exponential backoff, which ib-kubernetes don't use.
// In case client's backoff was configured time may dramatically increase.
// NOTE: ufm client has default timeout on request operation for 30 seconds.
var backoffValues = wait.Backoff{Duration: 1 * time.Second, Factor: 1.6, Jitter: 0.1, Steps: 6}

// Return networks mapped to the pod. If mapping not exist it is created
func (n *networksMap) getPodNetworks(pod *kapi.Pod) ([]*v1.NetworkSelectionElement, error) {
	var err error
	networks, ok := n.theMap[pod.UID]
	if !ok {
		networks, err = netAttUtils.ParsePodNetworkAnnotation(pod)
		if err != nil {
			return nil, fmt.Errorf("failed to read pod networkName annotations pod namespace %s name %s, with error: %v",
				pod.Namespace, pod.Name, err)
		}

		n.theMap[pod.UID] = networks
	}
	return networks, nil
}

// NewDaemon initializes the need components including k8s client, subnet manager client plugins, and guid pool.
// It returns error in case of failure.
func NewDaemon() (Daemon, error) {
	daemonConfig := config.DaemonConfig{}
	if err := daemonConfig.ReadConfig(); err != nil {
		return nil, err
	}

	if err := daemonConfig.ValidateConfig(); err != nil {
		return nil, err
	}

	podEventHandler := resEvenHandler.NewPodEventHandler()
	client, err := k8sClient.NewK8sClient()
	if err != nil {
		return nil, err
	}

	pluginLoader := sm.NewPluginLoader()
	getSmClientFunc, err := pluginLoader.LoadPlugin(path.Join(
		daemonConfig.PluginPath, daemonConfig.Plugin+".so"), sm.InitializePluginFunc)
	if err != nil {
		return nil, err
	}

	smClient, err := getSmClientFunc()
	if err != nil {
		return nil, err
	}

	// Pass configuration from daemon to the plugin
	pluginConfig := map[string]interface{}{
		"ENABLE_IP_OVER_IB":              daemonConfig.EnableIPOverIB,
		"DEFAULT_LIMITED_PARTITION":      daemonConfig.DefaultLimitedPartition,
		"ENABLE_INDEX0_FOR_PRIMARY_PKEY": daemonConfig.EnableIndex0ForPrimaryPkey,
	}
	if err := smClient.SetConfig(pluginConfig); err != nil {
		log.Warn().Msgf("Failed to set configuration on subnet manager plugin: %v", err)
	}

	// Try to validate if subnet manager is reachable in backoff loop
	var validateErr error
	if err := wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		if err := smClient.Validate(); err != nil {
			log.Warn().Msgf("%v", err)
			validateErr = err
			return false, nil
		}
		return true, nil
	}); err != nil {
		return nil, validateErr
	}

	guidPool, err := guid.NewPool(&daemonConfig.GUIDPool)
	if err != nil {
		return nil, err
	}

	// Reset guid pool with already allocated guids to avoid collisions
	err = syncGUIDPool(smClient, guidPool)
	if err != nil {
		return nil, err
	}

	podWatcher := watcher.NewWatcher(podEventHandler, client)
	return &daemon{
		config:            daemonConfig,
		watcher:           podWatcher,
		kubeClient:        client,
		guidPool:          guidPool,
		smClient:          smClient,
		guidPodNetworkMap: make(map[string]string),
	}, nil
}

func (d *daemon) Run() {
	// setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Init the guid pool
	if err := d.initPool(); err != nil {
		log.Error().Msgf("initPool(): Daemon could not init the guid pool: %v", err)
		os.Exit(1)
	}

	// Run periodic tasks
	// closing the channel will stop the goroutines executed in the wait.Until() calls below
	stopPeriodicsChan := make(chan struct{})
	go wait.Until(d.AddPeriodicUpdate, time.Duration(d.config.PeriodicUpdate)*time.Second, stopPeriodicsChan)
	go wait.Until(d.DeletePeriodicUpdate, time.Duration(d.config.PeriodicUpdate)*time.Second, stopPeriodicsChan)
	defer close(stopPeriodicsChan)

	// Run Watcher in background, calling watcherStopFunc() will stop the watcher
	watcherStopFunc := d.watcher.RunBackground()
	defer watcherStopFunc()

	// Run until interrupted by os signals
	sig := <-sigChan
	log.Info().Msgf("Received signal %s. Terminating...", sig)
}

// If network identified by networkID is IbSriov return network name and spec
//
//nolint:nilerr
func (d *daemon) getIbSriovNetwork(networkID string) (string, *utils.IbSriovCniSpec, error) {
	networkNamespace, networkName, err := utils.ParseNetworkID(networkID)
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse network id %s with error: %v", networkID, err)
	}

	// Try to get net-attach-def in backoff loop
	var netAttInfo *v1.NetworkAttachmentDefinition
	if err = wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		netAttInfo, err = d.kubeClient.GetNetworkAttachmentDefinition(networkNamespace, networkName)
		if err != nil {
			log.Warn().Msgf("failed to get networkName attachment %s with error %v",
				networkName, err)
			return false, nil
		}
		return true, nil
	}); err != nil {
		return "", nil, fmt.Errorf("failed to get networkName attachment %s", networkName)
	}
	log.Debug().Msgf("networkName attachment %v", netAttInfo)

	networkSpec := make(map[string]interface{})
	err = json.Unmarshal([]byte(netAttInfo.Spec.Config), &networkSpec)
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse networkName attachment %s with error: %v", networkName, err)
	}
	log.Debug().Msgf("networkName attachment spec %+v", networkSpec)

	ibCniSpec, err := utils.GetIbSriovCniFromNetwork(networkSpec)
	if err != nil {
		return "", nil, fmt.Errorf(
			"failed to get InfiniBand SR-IOV CNI spec from network attachment %+v, with error %v",
			networkSpec, err)
	}

	log.Debug().Msgf("ib-sriov CNI spec %+v", ibCniSpec)
	return networkName, ibCniSpec, nil
}

// Return pod network info
func getPodNetworkInfo(netName string, pod *kapi.Pod, netMap networksMap) (*podNetworkInfo, error) {
	networks, err := netMap.getPodNetworks(pod)
	if err != nil {
		return nil, err
	}

	var network *v1.NetworkSelectionElement
	network, err = utils.GetPodNetwork(networks, netName)
	if err != nil {
		return nil, fmt.Errorf("failed to get pod network spec for network %s with error: %v", netName, err)
	}

	return &podNetworkInfo{
		pod:       pod,
		networks:  networks,
		ibNetwork: network,
	}, nil
}

// addPodFinalizer adds the GUID cleanup finalizer to a pod
func (d *daemon) addPodFinalizer(pod *kapi.Pod) error {
	return wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		if err := d.kubeClient.AddFinalizerToPod(pod, PodGUIDFinalizer); err != nil {
			log.Warn().Msgf("failed to add finalizer to pod %s/%s: %v",
				pod.Namespace, pod.Name, err)
			return false, nil
		}
		return true, nil
	})
}

// removePodFinalizer removes the GUID cleanup finalizer from a pod
func (d *daemon) removePodFinalizer(pod *kapi.Pod) error {
	return wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		err := d.kubeClient.RemoveFinalizerFromPod(pod, PodGUIDFinalizer)
		if kerrors.IsNotFound(err) {
			// Pod has already been deleted, nothing to do.
			// This is expected as once the first network removes the finalizer, the pod will be deleted,
			// but the subsequent networks will also try to remove the finalizer.
			// TODO(Nik): This can lead to issues if a GUID removal fails, it'll stay stuck in UFM.  A more correct solution would be to wait until the "refcount" of guids on the pod is 0, but this will require better state management between failures.
			return false, err
		} else if err != nil {
			log.Warn().Msgf("failed to remove finalizer from pod %s/%s: %v",
				pod.Namespace, pod.Name, err)
			return false, nil
		}
		return true, nil
	})
}

// addNADFinalizer adds the GUID cleanup finalizer to a NetworkAttachmentDefinition
func (d *daemon) addNADFinalizer(networkNamespace, networkName string) error {
	return wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		if err := d.kubeClient.AddFinalizerToNetworkAttachmentDefinition(
			networkNamespace, networkName, GUIDInUFMFinalizer); err != nil {
			log.Warn().Msgf("failed to add finalizer to NetworkAttachmentDefinition %s/%s: %v",
				networkNamespace, networkName, err)
			return false, nil
		}
		return true, nil
	})
}

// removeNADFinalizerIfSafe removes the finalizer from NAD only if no pods are using the network
func (d *daemon) removeNADFinalizerIfSafe(networkNamespace, networkName string) error {
	podsUsingNetwork, err := d.checkIfAnyPodsUsingNetwork(networkNamespace, networkName)
	if err != nil {
		return fmt.Errorf("failed to check if pods are still using network %s/%s: %v",
			networkNamespace, networkName, err)
	}

	if podsUsingNetwork {
		log.Info().Msgf("NAD finalizer not removed from %s/%s - other pods still using this network",
			networkNamespace, networkName)
		return nil
	}

	return wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		if err := d.kubeClient.RemoveFinalizerFromNetworkAttachmentDefinition(
			networkNamespace, networkName, GUIDInUFMFinalizer); err != nil {
			log.Warn().Msgf("failed to remove finalizer from NetworkAttachmentDefinition %s/%s: %v",
				networkNamespace, networkName, err)
			return false, nil
		}
		return true, nil
	})
}

// addGUIDsToPKeyWithLimitedPartition adds GUIDs to both main pkey and limited partition if configured
func (d *daemon) addGUIDsToPKeyWithLimitedPartition(pKey int, guidList []net.HardwareAddr) error {
	// Add to main pKey
	if err := wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		if err := d.smClient.AddGuidsToPKey(pKey, guidList); err != nil {
			log.Warn().Msgf("failed to config pKey with subnet manager %s with error : %v",
				d.smClient.Name(), err)
			return false, nil
		}
		return true, nil
	}); err != nil {
		return fmt.Errorf("failed to config pKey with subnet manager %s", d.smClient.Name())
	}

	// Add to limited partition if configured
	if d.config.DefaultLimitedPartition != "" {
		limitedPKey, err := utils.ParsePKey(d.config.DefaultLimitedPartition)
		if err != nil {
			log.Error().Msgf("failed to parse DEFAULT_LIMITED_PARTITION %s: %v", d.config.DefaultLimitedPartition, err)
		} else {
			if err := wait.ExponentialBackoff(backoffValues, func() (bool, error) {
				if err := d.smClient.AddGuidsToLimitedPKey(limitedPKey, guidList); err != nil {
					log.Warn().Msgf("failed to add GUIDs to limited partition 0x%04X with subnet manager %s with error: %v",
						limitedPKey, d.smClient.Name(), err)
					return false, nil
				}
				return true, nil
			}); err != nil {
				log.Error().Msgf("failed to add GUIDs to limited partition 0x%04X with subnet manager %s", limitedPKey, d.smClient.Name())
			} else {
				log.Info().Msgf("successfully added GUIDs %v to limited partition 0x%04X", guidList, limitedPKey)
			}
		}
	}

	return nil
}

// removeGUIDsFromPKeyWithLimitedPartition removes GUIDs from both main pkey and limited partition if configured
func (d *daemon) removeGUIDsFromPKeyWithLimitedPartition(pKey int, guidList []net.HardwareAddr) error {
	// Remove from main pKey
	if err := wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		if err := d.smClient.RemoveGuidsFromPKey(pKey, guidList); err != nil {
			log.Warn().Msgf("failed to remove guids from pKey 0x%04X with subnet manager %s with error: %v",
				pKey, d.smClient.Name(), err)
			return false, nil
		}
		return true, nil
	}); err != nil {
		return fmt.Errorf("failed to remove guids from pKey 0x%04X with subnet manager %s", pKey, d.smClient.Name())
	}

	// Remove from limited partition if configured
	if d.config.DefaultLimitedPartition != "" {
		limitedPKey, err := utils.ParsePKey(d.config.DefaultLimitedPartition)
		if err != nil {
			log.Error().Msgf("failed to parse DEFAULT_LIMITED_PARTITION %s: %v", d.config.DefaultLimitedPartition, err)
		} else {
			if err := wait.ExponentialBackoff(backoffValues, func() (bool, error) {
				if err := d.smClient.RemoveGuidsFromPKey(limitedPKey, guidList); err != nil {
					log.Warn().Msgf("failed to remove GUIDs from limited partition 0x%04X with subnet manager %s with error: %v",
						limitedPKey, d.smClient.Name(), err)
					return false, nil
				}
				return true, nil
			}); err != nil {
				log.Error().Msgf("failed to remove GUIDs from limited partition 0x%04X with subnet manager %s", limitedPKey, d.smClient.Name())
			} else {
				log.Info().Msgf("successfully removed GUIDs %v from limited partition 0x%04X", guidList, limitedPKey)
			}
		}
	}

	return nil
}

// Verify if GUID already exist for given network ID and allocates new one if not
func (d *daemon) allocatePodNetworkGUID(allocatedGUID, podNetworkID string, podUID types.UID) error {
	if mappedID, exist := d.guidPodNetworkMap[allocatedGUID]; exist {
		if podNetworkID != mappedID {
			return fmt.Errorf("failed to allocate requested guid %s, already allocated for %s",
				allocatedGUID, mappedID)
		}
	} else if err := d.guidPool.AllocateGUID(allocatedGUID); err != nil {
		return fmt.Errorf("failed to allocate GUID for pod ID %s, wit error: %v", podUID, err)
	} else {
		d.guidPodNetworkMap[allocatedGUID] = podNetworkID
	}

	return nil
}

// Allocate network GUID, update Pod's networks annotation and add GUID to the podNetworkInfo instance
func (d *daemon) processNetworkGUID(networkID string, spec *utils.IbSriovCniSpec, pi *podNetworkInfo) error {
	var guidAddr guid.GUID
	allocatedGUID, err := utils.GetPodNetworkGUID(pi.ibNetwork)
	podNetworkID := utils.GeneratePodNetworkID(pi.pod, networkID)
	if err == nil {
		log.Warn().Msgf("GUID Already allocated for : %v", networkID)
		// User allocated guid manually or Pod's network was rescheduled
		guidAddr, err = guid.ParseGUID(allocatedGUID)
		if err != nil {
			return fmt.Errorf("failed to parse user allocated guid %s with error: %v", allocatedGUID, err)
		}

		err = d.allocatePodNetworkGUID(allocatedGUID, podNetworkID, pi.pod.UID)
		if err != nil {
			return err
		}
	} else {
		guidAddr, err = d.guidPool.GenerateGUID()
		// TODO(Nik): This error handling is funky.  We sync the GUID pool but then don't re-assign a guidAddr so we're left with a 0 guid
		if err != nil {
			switch {
			case errors.Is(err, guid.ErrGUIDPoolExhausted):
				err = syncGUIDPool(d.smClient, d.guidPool)
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("failed to generate GUID for pod ID %s, with error: %v", pi.pod.UID, err)
			}
		}

		allocatedGUID = guidAddr.String()
		err = d.allocatePodNetworkGUID(allocatedGUID, podNetworkID, pi.pod.UID)
		if err != nil {
			return err
		}

		err = utils.SetPodNetworkGUID(pi.ibNetwork, allocatedGUID, spec.Capabilities["infinibandGUID"])
		if err != nil {
			return fmt.Errorf("failed to set pod network guid with error: %v ", err)
		}

		// Update Pod's network annotation here, so if network will be rescheduled we wouldn't allocate it again
		netAnnotations, err := json.Marshal(pi.networks)
		if err != nil {
			return fmt.Errorf("failed to dump networks %+v of pod into json with error: %v", pi.networks, err)
		}

		pi.pod.Annotations[v1.NetworkAttachmentAnnot] = string(netAnnotations)
	}

	// used GUID as net.HardwareAddress to use it in sm plugin which receive []net.HardwareAddress as parameter
	pi.addr = guidAddr.HardWareAddress()
	return nil
}

func syncGUIDPool(smClient plugins.SubnetManagerClient, guidPool guid.Pool) error {
	usedGuids, err := smClient.ListGuidsInUse()
	if err != nil {
		return err
	}

	// Reset guid pool with already allocated guids to avoid collisions
	err = guidPool.Reset(usedGuids)
	if err != nil {
		return err
	}
	return nil
}

// Update and set Pod's network annotation.
// If failed to update annotation, pod's GUID added into the list to be removed from Pkey.
func (d *daemon) updatePodNetworkAnnotation(pi *podNetworkInfo, removedList *[]net.HardwareAddr) error {
	if pi.ibNetwork.CNIArgs == nil {
		pi.ibNetwork.CNIArgs = &map[string]interface{}{}
	}

	(*pi.ibNetwork.CNIArgs)[utils.InfiniBandAnnotation] = utils.ConfiguredInfiniBandPod
	netAnnotations, err := json.Marshal(pi.networks)
	if err != nil {
		return fmt.Errorf("failed to dump networks %+v of pod into json with error: %v", pi.networks, err)
	}

	pi.pod.Annotations[v1.NetworkAttachmentAnnot] = string(netAnnotations)

	// Try to set pod's annotations in backoff loop
	if err = wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		log.Info().Msgf("updatePodNetworkAnnotation(): Updating pod annotation for pod: %s with anootation: %s", pi.pod.Name, pi.pod.Annotations)
		if err = d.kubeClient.SetAnnotationsOnPod(pi.pod, pi.pod.Annotations); err != nil {
			if kerrors.IsNotFound(err) {
				return false, err
			}
			log.Warn().Msgf("failed to update pod annotations with err: %v", err)
			return false, nil
		}
		log.Info().Msgf("updatePodNetworkAnnotation(): Success on updating pod annotation for pod: %s with anootation: %s", pi.pod.Name, pi.pod.Annotations)
		return true, nil
	}); err != nil {
		log.Error().Msgf("failed to update pod annotations")

		if err = d.guidPool.ReleaseGUID(pi.addr.String()); err != nil {
			log.Warn().Msgf("failed to release guid \"%s\" from removed pod \"%s\" in namespace "+
				"\"%s\" with error: %v", pi.addr.String(), pi.pod.Name, pi.pod.Namespace, err)
		} else {
			delete(d.guidPodNetworkMap, pi.addr.String())
		}

		*removedList = append(*removedList, pi.addr)
	}

	return nil
}

//nolint:nilerr
func (d *daemon) AddPeriodicUpdate() {
	log.Info().Msgf("running periodic add update")
	addMap, _ := d.watcher.GetHandler().GetResults()
	addMap.Lock()
	defer addMap.Unlock()
	// Contains ALL pods' networks
	netMap := networksMap{theMap: make(map[types.UID][]*v1.NetworkSelectionElement)}
	// Pkey -> NetworkID -> GUID list (for batching pKey add operations)
	pkeyToNetworkIdToGUIDs := make(map[int]map[string][]net.HardwareAddr)
	// Pkey -> NetworkID -> passedPods
	pkeyToNetworkIdToPassedPods := make(map[int]map[string][]*podNetworkInfo)

	// Add map is indexed by networkID and the value is a list of creating pods using that network.
	for networkID, podsInterface := range addMap.Items {
		log.Info().Msgf("processing network networkID %s", networkID)
		pods, ok := podsInterface.([]*kapi.Pod)
		if !ok {
			log.Error().Msgf(
				"invalid value for add map networks expected pods array \"[]*kubernetes.Pod\", found %T",
				podsInterface)
			continue
		}

		if len(pods) == 0 {
			continue
		}

		// Fetch cni spec (for the pkey) and network name from the networkID
		log.Info().Msgf("processing network networkID %s", networkID)
		networkName, ibCniSpec, err := d.getIbSriovNetwork(networkID)
		if err != nil {
			addMap.UnSafeRemove(networkID)
			log.Error().Msgf("droping network: %v", err)
			continue
		}

		// Extract/generate GUIDs from the pods corresponding to the network
		var guidList []net.HardwareAddr
		var passedPods []*podNetworkInfo
		for _, pod := range pods {
			log.Info().Msgf("pod namespace %s name %s", pod.Namespace, pod.Name)
			var pi *podNetworkInfo
			pi, err = getPodNetworkInfo(networkName, pod, netMap)
			if err != nil {
				log.Error().Msgf("%v", err)
				continue
			}
			if err = d.processNetworkGUID(networkName, ibCniSpec, pi); err != nil {
				log.Error().Msgf("%v", err)
				continue
			}

			// Add finalizer to pod since it now has a GUID that needs cleanup (GUID is allocated for the pod but has not been persisted to the pod yet)
			// TODO(Nik): What happens if we fail to add the finalizer, does the GUID ever get GC'd?
			if err = d.addPodFinalizer(pi.pod); err != nil {
				log.Error().Msgf("failed to add finalizer to pod %s/%s: %v", pi.pod.Namespace, pi.pod.Name, err)
				continue
				// TODO(Nik): If a pod fails to add the finalizer, it doesn't get reprocessed
			} else {
				log.Info().Msgf("added finalizer %s to pod %s/%s",
					PodGUIDFinalizer, pi.pod.Namespace, pi.pod.Name)
			}

			guidList = append(guidList, pi.addr)
			passedPods = append(passedPods, pi)
		}
		log.Info().Interface("pods", passedPods).Msg("Passed pods")

		if ibCniSpec.PKey != "" && len(guidList) != 0 {
			var pKey int
			pKey, err = utils.ParsePKey(ibCniSpec.PKey)
			if err != nil {
				log.Error().Msgf("failed to parse PKey %s with error: %v", ibCniSpec.PKey, err)
				continue
			}

			// Add to pKey map for batched processing
			if pkeyToNetworkIdToGUIDs[pKey] == nil {
				pkeyToNetworkIdToGUIDs[pKey] = make(map[string][]net.HardwareAddr)
			}
			pkeyToNetworkIdToGUIDs[pKey][networkID] = guidList
			if pkeyToNetworkIdToPassedPods[pKey] == nil {
				pkeyToNetworkIdToPassedPods[pKey] = make(map[string][]*podNetworkInfo)
			}
			pkeyToNetworkIdToPassedPods[pKey][networkID] = passedPods
		} else {
			// If the network is not in a pKey, update the annotations now.
			var removedGUIDList []net.HardwareAddr // Unused since we don't have a pKey to remove GUIDs from
			for _, pi := range passedPods {
				log.Info().Msgf("Updating annotations for the pod %s, network %s", pi.pod.Name, pi.ibNetwork.Name)
				err = d.updatePodNetworkAnnotation(pi, &removedGUIDList)
				if err != nil {
					log.Error().Msgf("%v", err)
				}
			}
			addMap.UnSafeRemove(networkID)
		}
	}

	// Add all GUIDs to pKeys in UFM
	for pKey, networkIDToGUIDs := range pkeyToNetworkIdToGUIDs {
		// Gather all GUIDs across all networks for this pKey
		guidsToAdd := make([]net.HardwareAddr, 0)
		for networkID, guidList := range networkIDToGUIDs {
			log.Debug().Msgf("adding guids: %v for networkID %s to pKey %d", guidList, networkID, pKey)
			guidsToAdd = append(guidsToAdd, guidList...)
		}

		// Batch add GUIDs to pKey (main and limited partition)
		if !d.canProceedWithPkeyModification() {
			log.Warn().Msgf("previous pkey modification was not completed, requeuing")
			continue
		}
		if err := d.addGUIDsToPKeyWithLimitedPartition(pKey, guidsToAdd); err != nil {
			log.Error().Msgf("%v", err)
			continue
		}

		// Add finalizer to NetworkAttachmentDefinition for each network
		for networkID := range networkIDToGUIDs {
			networkNamespace, networkName, _ := utils.ParseNetworkID(networkID)
			if err := d.addNADFinalizer(networkNamespace, networkName); err != nil {
				log.Error().Msgf("failed to add finalizer to NetworkAttachmentDefinition %s/%s: %v",
					networkNamespace, networkName, err)
				// TODO(Nik): Retry/continue?
			} else {
				log.Info().Msgf("added finalizer %s to NetworkAttachmentDefinition %s/%s",
					GUIDInUFMFinalizer, networkNamespace, networkName)
			}
		}

		// Update annotations for all pods across all networks in this pKey
		var allRemovedGUIDs []net.HardwareAddr
		for _, passedPods := range pkeyToNetworkIdToPassedPods[pKey] {
			for _, pi := range passedPods {
				log.Info().Msgf("Updating annotations for the pod %s, network %s", pi.pod.Name, pi.ibNetwork.Name)
				var removedGUIDList []net.HardwareAddr
				if err := d.updatePodNetworkAnnotation(pi, &removedGUIDList); err != nil {
					log.Error().Msgf("%v", err)
					// TODO(Nik): Retry/continue?
				}
				allRemovedGUIDs = append(allRemovedGUIDs, removedGUIDList...)
			}
		}

		// Batch remove GUIDs from pKey if any were removed during annotation updates
		if len(allRemovedGUIDs) != 0 {
			if err := d.removeGUIDsFromPKeyWithLimitedPartition(pKey, allRemovedGUIDs); err != nil {
				log.Warn().Msgf("%v", err)
				continue
			}

			// Note: NAD finalizer is not removed here during pod addition
			// It will only be removed during pod deletion when all pods using this NAD are cleaned up
		}

		// If all steps succeed, remove networkIDs from the addMap
		for networkID := range networkIDToGUIDs {
			addMap.UnSafeRemove(networkID)
		}
	}

	log.Info().Msg("add periodic update finished")
}

// get GUID from Pod's network
func getPodGUIDForNetwork(pod *kapi.Pod, networkName string) (net.HardwareAddr, error) {
	networks, netErr := netAttUtils.ParsePodNetworkAnnotation(pod)
	if netErr != nil {
		return nil, fmt.Errorf("failed to read pod networkName annotations pod namespace %s name %s, with error: %v",
			pod.Namespace, pod.Name, netErr)
	}

	network, netErr := utils.GetPodNetwork(networks, networkName)
	if netErr != nil {
		return nil, fmt.Errorf("failed to get pod networkName spec %s with error: %v", networkName, netErr)
	}

	if !utils.IsPodNetworkConfiguredWithInfiniBand(network) {
		return nil, fmt.Errorf("network %+v is not InfiniBand configured", network)
	}

	allocatedGUID, netErr := utils.GetPodNetworkGUID(network)
	if netErr != nil {
		return nil, netErr
	}

	guidAddr, guidErr := net.ParseMAC(allocatedGUID)
	if guidErr != nil {
		return nil, fmt.Errorf("failed to parse allocated Pod GUID, error: %v", guidErr)
	}

	return guidAddr, nil
}

//nolint:nilerr
func (d *daemon) DeletePeriodicUpdate() {
	log.Info().Msg("running delete periodic update")
	_, deleteMap := d.watcher.GetHandler().GetResults()
	deleteMap.Lock()
	defer deleteMap.Unlock()
	// Pkey -> NetworkID -> GUIDs
	pkeyToNetworkIdToGUIDs := make(map[int]map[string][]net.HardwareAddr)
	// maps GUID string -> pod
	podGUIDMap := make(map[string]*kapi.Pod)
	// Delete map is indexed by networkID and the value is a list of deleting pods using that network.
	for networkID, podsInterface := range deleteMap.Items {
		log.Info().Msgf("processing network networkID %s", networkID)
		pods, ok := podsInterface.([]*kapi.Pod)
		if !ok {
			log.Error().Msgf("invalid value for add map networks expected pods array \"[]*kubernetes.Pod\", found %T",
				podsInterface)
			continue
		}

		if len(pods) == 0 {
			continue
		}

		// Fetch cni spec (for the pkey) and network name from the networkID
		networkName, ibCniSpec, err := d.getIbSriovNetwork(networkID)
		if err != nil {
			deleteMap.UnSafeRemove(networkID)
			log.Warn().Msgf("droping network: %v", err)
			continue
		}

		// Extract GUIDs from the pods corresponding to the network
		var guidList []net.HardwareAddr
		var guidAddr net.HardwareAddr
		for _, pod := range pods {
			log.Debug().Msgf("pod namespace %s name %s", pod.Namespace, pod.Name)
			guidAddr, err = getPodGUIDForNetwork(pod, networkName)
			if err != nil {
				log.Error().Msgf("%v", err)
				continue
			}

			guidList = append(guidList, guidAddr)
			podGUIDMap[guidAddr.String()] = pod
		}

		if ibCniSpec.PKey != "" && len(guidList) != 0 {
			pKey, pkeyErr := utils.ParsePKey(ibCniSpec.PKey)
			if pkeyErr != nil {
				log.Error().Msgf("failed to parse PKey %s with error: %v", ibCniSpec.PKey, pkeyErr)
				continue
			}

			// Add guidList and networkID to pKey map for removal from pKey
			if pkeyToNetworkIdToGUIDs[pKey] == nil {
				pkeyToNetworkIdToGUIDs[pKey] = make(map[string][]net.HardwareAddr)
			}
			pkeyToNetworkIdToGUIDs[pKey][networkID] = guidList
		} else {
			// If the network is not in a pKey, remove it from the guidPool now.
			for _, guidAddr := range guidList {
				if err = d.guidPool.ReleaseGUID(guidAddr.String()); err != nil {
					log.Error().Msgf("%v", err)
					continue
				}

				delete(d.guidPodNetworkMap, guidAddr.String())

				// Remove finalizer from pod after successfully cleaning up GUID
				// TODO(Nik): This could be problematic if some are in a pkey and some aren't and they fail or split across batches.
				if pod, exists := podGUIDMap[guidAddr.String()]; exists {
					err = d.removePodFinalizer(pod)
					if kerrors.IsNotFound(err) {
						log.Warn().Msgf("attempted to remove finalizer from pod %s/%s that has already been deleted, nothing to do", pod.Namespace, pod.Name)
					} else if err != nil {
						log.Error().Msgf("failed to remove finalizer from pod %s/%s: %v", pod.Namespace, pod.Name, err)
						// Continue?
					} else {
						log.Info().Msgf("removed finalizer %s from pod %s/%s",
							PodGUIDFinalizer, pod.Namespace, pod.Name)
					}
				}
			}
			deleteMap.UnSafeRemove(networkID)
		}
	}

	// Remove all guids for all networks
	for pKey, networkIDToGUIDs := range pkeyToNetworkIdToGUIDs {
		guidsToRelease := make([]net.HardwareAddr, 0)
		for networkID, guidList := range networkIDToGUIDs {
			log.Debug().Msgf("releasing guids: %v for networkID %s", guidList, networkID)
			guidsToRelease = append(guidsToRelease, guidList...)
		}

		// Remove GUIDs from pKeys (main and limited partition)
		if !d.canProceedWithPkeyModification() {
			log.Warn().Msgf("previous pkey modification was not completed, requeuing")
			continue
		}
		if err := d.removeGUIDsFromPKeyWithLimitedPartition(pKey, guidsToRelease); err != nil {
			log.Warn().Msgf("%v", err)
			continue
		}

		// Check if NAD finalizer can be safely removed for each network
		for networkID := range networkIDToGUIDs {
			networkNamespace, networkName, _ := utils.ParseNetworkID(networkID)
			if err := d.removeNADFinalizerIfSafe(networkNamespace, networkName); err != nil {
				log.Error().Msgf("failed to remove NAD finalizer for %s/%s: %v", networkNamespace, networkName, err)
				// TODO(Nik): Retry/continue?
			} else {
				log.Info().Msgf("checked and potentially removed finalizer %s from NetworkAttachmentDefinition %s/%s",
					GUIDInUFMFinalizer, networkNamespace, networkName)
			}
		}

		// Release guids from the guidPool
		for _, guidAddr := range guidsToRelease {
			if err := d.guidPool.ReleaseGUID(guidAddr.String()); err != nil {
				log.Error().Msgf("%v", err)
				continue // TODO(Nik): Double continue
			}

			delete(d.guidPodNetworkMap, guidAddr.String())

			// Remove finalizer from pod after successfully cleaning up GUID
			if pod, exists := podGUIDMap[guidAddr.String()]; exists {
				err := d.removePodFinalizer(pod)
				if kerrors.IsNotFound(err) {
					log.Warn().Msgf("attempted to remove finalizer from pod %s/%s that has already been deleted, nothing to do", pod.Namespace, pod.Name)
				} else if err != nil {
					log.Error().Msgf("failed to remove finalizer from pod %s/%s: %v", pod.Namespace, pod.Name, err)
					// TODO(Nik): Retry/continue?
				} else {
					log.Info().Msgf("removed finalizer %s from pod %s/%s",
						PodGUIDFinalizer, pod.Namespace, pod.Name)
				}
			}
		}

		// If all steps succeed, remove networkIDs from the deleteMap
		for networkID := range networkIDToGUIDs {
			deleteMap.UnSafeRemove(networkID)
		}
	}

	log.Info().Msg("delete periodic update finished")
}

// initPool check the guids that are already allocated by the running pods
func (d *daemon) initPool() error {
	log.Info().Msg("Initializing GUID pool.")

	// Try to get pod list from k8s client in backoff loop
	var pods *kapi.PodList
	if err := wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		var err error
		if pods, err = d.kubeClient.GetPods(kapi.NamespaceAll); err != nil {
			log.Warn().Msgf("failed to get pods from kubernetes: %v", err)
			return false, nil
		}
		return true, nil
	}); err != nil {
		err = fmt.Errorf("failed to get pods from kubernetes")
		log.Error().Msgf("%v", err)
		return err
	}

	for index := range pods.Items {
		log.Debug().Msgf("checking pod for network annotations %v", pods.Items[index])
		pod := pods.Items[index]
		networks, err := netAttUtils.ParsePodNetworkAnnotation(&pod)
		if err != nil {
			continue
		}

		for _, network := range networks {
			if !utils.IsPodNetworkConfiguredWithInfiniBand(network) {
				continue
			}

			podGUID, err := utils.GetPodNetworkGUID(network)
			if err != nil {
				continue
			}
			podNetworkID := string(pod.UID) + network.Name
			if _, exist := d.guidPodNetworkMap[podGUID]; exist {
				if podNetworkID != d.guidPodNetworkMap[podGUID] {
					return fmt.Errorf("failed to allocate requested guid %s, already allocated for %s",
						podGUID, d.guidPodNetworkMap[podGUID])
				}
				continue
			}

			if err = d.guidPool.AllocateGUID(podGUID); err != nil {
				err = fmt.Errorf("failed to allocate guid for running pod: %v", err)
				log.Error().Msgf("%v", err)
				continue
			}

			d.guidPodNetworkMap[podGUID] = podNetworkID
		}
	}

	return nil
}

// checkIfAnyPodsUsingNetwork checks if there are any pods still using the given network
func (d *daemon) checkIfAnyPodsUsingNetwork(networkNamespace, networkName string) (bool, error) {
	pods, err := d.kubeClient.GetPods(kapi.NamespaceAll)
	if err != nil {
		return false, fmt.Errorf("failed to get pods: %v", err)
	}

	for i := range pods.Items {
		pod := &pods.Items[i]

		// Skip pods that are being deleted (have deletion timestamp)
		if pod.DeletionTimestamp != nil {
			continue
		}

		if !utils.HasNetworkAttachmentAnnot(pod) {
			continue
		}

		networks, err := netAttUtils.ParsePodNetworkAnnotation(pod)
		if err != nil {
			continue
		}

		for _, network := range networks {
			// Check if this pod uses the network we're checking
			if network.Namespace == networkNamespace && network.Name == networkName {
				// Check if this network is configured with InfiniBand and has a GUID
				if utils.IsPodNetworkConfiguredWithInfiniBand(network) && utils.PodNetworkHasGUID(network) {
					log.Debug().Msgf("Found pod %s/%s still using network %s/%s",
						pod.Namespace, pod.Name, networkNamespace, networkName)
					return true, nil
				}
			}
		}
	}

	return false, nil
}
