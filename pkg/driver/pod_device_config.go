/*
Copyright The Kubernetes Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/dranet/pkg/apis"
)

// PodConfig holds all the device configurations for a Pod, and can be extended
// with fields that are not specific to a single device.
type PodConfig struct {
	// DeviceConfigs maps the allocated network device names to their respective
	// configurations.
	DeviceConfigs map[string]DeviceConfig

	// LastNRIActivity timestamp is updated whenever an NRI hook processes
	// a container for this Pod. Used to track pod initialization progress.
	LastNRIActivity time.Time
}

// DeviceConfig holds the set of configurations to be applied for a single
// network device allocated to a Pod. This includes network interface settings,
// routes for the Pod's network namespace, and RDMA configurations.
type DeviceConfig struct {
	Claim types.NamespacedName `json:"claim"`

	// NetworkInterfaceConfigInHost is the config of the network interface as
	// seen in the host's network namespace BEFORE it was moved to the pod's
	// network namespace.
	NetworkInterfaceConfigInHost apis.NetworkConfig `json:"networkInterfaceConfigInHost"`

	// NetworkInterfaceConfigInPod contains all network-related configurations
	// (interface, routes, ethtool, sysctl) to be applied for this device in the
	// Pod's namespace.
	NetworkInterfaceConfigInPod apis.NetworkConfig `json:"networkInterfaceConfigInPod"`

	// RDMADevice holds RDMA-specific configurations if the network device
	// has associated RDMA capabilities.
	RDMADevice RDMAConfig `json:"rdmaDevice,omitempty"`
}

// RDMAConfig contains parameters for setting up an RDMA device associated
// with a network interface.
type RDMAConfig struct {
	// LinkDev is the name of the RDMA link device (e.g., "mlx5_0").
	// Depending on the type of device (RoCE, IB) it may have a network device
	// associated. For IB-only devices there is no associated network interface.
	LinkDev string `json:"linkDev,omitempty"`

	// DevChars is a list of user-space RDMA character
	// devices (e.g., "/dev/infiniband/uverbs0", "/dev/infiniband/rdma_cm")
	// that should be made available to the Pod.
	DevChars []LinuxDevice `json:"devChars,omitempty"`
}

type LinuxDevice struct {
	Path     string `json:"path"`
	Type     string `json:"type"`
	Major    int64  `json:"major"`
	Minor    int64  `json:"minor"`
	FileMode uint32 `json:"fileMode"`
	UID      uint32 `json:"uid"`
	GID      uint32 `json:"gid"`
}

// PodConfigStore provides a thread-safe, centralized store for all network
// device configurations across multiple Pods. It is indexed by the Pod's UID.
type PodConfigStore struct {
	mu      sync.RWMutex
	configs map[types.UID]PodConfig
}

// Compile-time interface check.
var _ podConfigStorer = &PodConfigStore{}

// NewPodConfigStore creates and returns a new instance of PodConfigStore.
func NewPodConfigStore() *PodConfigStore {
	return &PodConfigStore{
		configs: make(map[types.UID]PodConfig),
	}
}

// Close is a no-op for the in-memory store.
func (s *PodConfigStore) Close() error { return nil }

// UpdateLastNRIActivity updates the LastNRIActivity timestamp for a given Pod UID.
// If the PodConfig doesn't exist, it does nothing.
func (s *PodConfigStore) UpdateLastNRIActivity(podUID types.UID, timestamp time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if podConfig, ok := s.configs[podUID]; ok {
		podConfig.LastNRIActivity = timestamp
		s.configs[podUID] = podConfig
	}
}

// GetPodNRIActivities returns a map of Pod UIDs to their last NRI activity timestamp.
func (s *PodConfigStore) GetPodNRIActivities() map[types.UID]time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	activities := make(map[types.UID]time.Time, len(s.configs))
	for uid, config := range s.configs {
		activities[uid] = config.LastNRIActivity
	}
	return activities
}

// SetDeviceConfig stores the configuration for a specific device under a given Pod UID.
// If a configuration for the Pod UID or device name already exists, it will be overwritten.
func (s *PodConfigStore) SetDeviceConfig(podUID types.UID, deviceName string, config DeviceConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	podConfig, ok := s.configs[podUID]
	if !ok {
		podConfig = PodConfig{
			DeviceConfigs: make(map[string]DeviceConfig),
		}
		s.configs[podUID] = podConfig
	}
	podConfig.DeviceConfigs[deviceName] = config
	return nil
}

// GetDeviceConfig retrieves the configuration for a specific device under a given Pod UID.
// It returns the Config and true if found, otherwise an empty Config and false.
func (s *PodConfigStore) GetDeviceConfig(podUID types.UID, deviceName string) (DeviceConfig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if podConfig, ok := s.configs[podUID]; ok {
		config, found := podConfig.DeviceConfigs[deviceName]
		return config, found
	}
	return DeviceConfig{}, false
}

// DeletePod removes all configurations associated with a given Pod UID.
func (s *PodConfigStore) DeletePod(podUID types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.configs, podUID)
}

// ListPods returns the UIDs of all pods in the store.
func (s *PodConfigStore) ListPods() []types.UID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	uids := make([]types.UID, 0, len(s.configs))
	for uid := range s.configs {
		uids = append(uids, uid)
	}
	return uids
}

// GetPodConfig retrieves all configurations for a given Pod UID.
// It is indexed by the Pod's UID.
func (s *PodConfigStore) GetPodConfig(podUID types.UID) (PodConfig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	podConfig, found := s.configs[podUID]
	if !found {
		return PodConfig{}, false
	}
	// Return a copy to prevent external modification of the internal map
	configsCopy := make(map[string]DeviceConfig, len(podConfig.DeviceConfigs))
	for k, v := range podConfig.DeviceConfigs {
		configsCopy[k] = v
	}
	return PodConfig{
		DeviceConfigs:   configsCopy,
		LastNRIActivity: podConfig.LastNRIActivity,
	}, true
}

// DeleteClaim removes all configurations associated with a given claim and
// returns the list of Pod UIDs that were associated with it.
func (s *PodConfigStore) DeleteClaim(claim types.NamespacedName) []types.UID {
	s.mu.Lock()
	defer s.mu.Unlock()
	podsToDelete := []types.UID{}
	for uid, podConfig := range s.configs {
		for _, config := range podConfig.DeviceConfigs {
			if config.Claim == claim {
				podsToDelete = append(podsToDelete, uid)
				break // Found a match for this pod, no need to check other devices for the same pod
			}
		}
	}

	for _, uid := range podsToDelete {
		delete(s.configs, uid)
	}
	return podsToDelete
}
