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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// podConfigsBucket stores device configs. It serves as a root bucket
// containing nested buckets for each pod UID.
var podConfigsBucket = []byte("pod_configs")

// BoltPodConfigStore is a persistent implementation of podConfigStorer backed by bbolt.
// Device configs are persisted to disk so they survive daemon restarts.
// LastNRIActivity is ephemeral and kept in memory only.
type BoltPodConfigStore struct {
	db *bolt.DB

	// nriActivities tracks the last NRI activity per pod UID.
	// This is ephemeral state used only for graceful shutdown coordination
	// and does not need persistence.
	mu            sync.RWMutex
	nriActivities map[types.UID]time.Time
}

// NewBoltPodConfigStore creates a new BoltPodConfigStore at the given path.
func NewBoltPodConfigStore(path string) (*BoltPodConfigStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return nil, fmt.Errorf("create pod config db directory: %w", err)
	}

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open pod config db: %w", err)
	}

	// Ensure the root bucket exists.
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(podConfigsBucket)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize pod config db bucket: %w", err)
	}

	return &BoltPodConfigStore{
		db:            db,
		nriActivities: make(map[types.UID]time.Time),
	}, nil
}

// Close closes the underlying bolt database.
func (s *BoltPodConfigStore) Close() error {
	return s.db.Close()
}

func (s *BoltPodConfigStore) SetDeviceConfig(podUID types.UID, deviceName string, config DeviceConfig) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(podConfigsBucket)
		if root == nil {
			return berrors.ErrBucketNotFound
		}
		podBucket, err := root.CreateBucketIfNotExists([]byte(podUID))
		if err != nil {
			return err
		}
		data, err := json.Marshal(config)
		if err != nil {
			return err
		}
		return podBucket.Put([]byte(deviceName), data)
	})
}

func (s *BoltPodConfigStore) GetDeviceConfig(podUID types.UID, deviceName string) (DeviceConfig, bool) {
	var config DeviceConfig
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(podConfigsBucket)
		if root == nil {
			return nil
		}
		podBucket := root.Bucket([]byte(podUID))
		if podBucket == nil {
			return nil
		}
		data := podBucket.Get([]byte(deviceName))
		if data == nil {
			return nil
		}
		found = true
		return json.Unmarshal(data, &config)
	})
	if err != nil {
		klog.Errorf("BoltPodConfigStore: failed to get device config for pod %s device %s: %v", podUID, deviceName, err)
		return DeviceConfig{}, false
	}
	return config, found
}

func (s *BoltPodConfigStore) GetPodConfig(podUID types.UID) (PodConfig, bool) {
	podConfig := PodConfig{
		DeviceConfigs: make(map[string]DeviceConfig),
	}
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(podConfigsBucket)
		if root == nil {
			return nil
		}
		podBucket := root.Bucket([]byte(podUID))
		if podBucket == nil {
			return nil
		}
		return podBucket.ForEach(func(k, v []byte) error {
			if v == nil {
				return nil // Skip nested buckets if any exist
			}
			var config DeviceConfig
			if err := json.Unmarshal(v, &config); err != nil {
				klog.Errorf("BoltPodConfigStore: failed to unmarshal device config for pod %s device %s: %v", podUID, string(k), err)
				return nil // skip corrupted entries
			}
			podConfig.DeviceConfigs[string(k)] = config
			found = true
			return nil
		})
	})
	if err != nil {
		klog.Errorf("BoltPodConfigStore: failed to get pod config for pod %s: %v", podUID, err)
		return PodConfig{}, false
	}
	if !found {
		return PodConfig{}, false
	}

	// Attach ephemeral NRI activity.
	s.mu.RLock()
	podConfig.LastNRIActivity = s.nriActivities[podUID]
	s.mu.RUnlock()

	return podConfig, true
}

func (s *BoltPodConfigStore) DeletePod(podUID types.UID) {
	err := s.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(podConfigsBucket)
		if root == nil {
			return berrors.ErrBucketNotFound
		}
		err := root.DeleteBucket([]byte(podUID))
		if err == berrors.ErrBucketNotFound {
			return nil // Pod already deleted or does not exist
		}
		return err
	})
	if err != nil {
		klog.Errorf("BoltPodConfigStore: failed to delete pod %s: %v", podUID, err)
	}

	s.mu.Lock()
	delete(s.nriActivities, podUID)
	s.mu.Unlock()
}

// ListPods returns the UIDs of all pods in the store.
func (s *BoltPodConfigStore) ListPods() []types.UID {
	var uids []types.UID
	err := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(podConfigsBucket)
		if root == nil {
			return nil
		}
		return root.ForEach(func(k, v []byte) error {
			if v == nil { // nested bucket = pod UID
				uids = append(uids, types.UID(string(k)))
			}
			return nil
		})
	})
	if err != nil {
		klog.Errorf("BoltPodConfigStore: failed to list pods: %v", err)
		return nil
	}
	return uids
}

func (s *BoltPodConfigStore) DeleteClaim(claim types.NamespacedName) []types.UID {
	var podsToDelete []types.UID
	err := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(podConfigsBucket)
		if root == nil {
			return nil
		}
		return root.ForEach(func(podUID, v []byte) error {
			if v != nil {
				return nil // Not a nested bucket
			}
			podBucket := root.Bucket(podUID)
			if podBucket == nil {
				return nil
			}
			matched := false
			err := podBucket.ForEach(func(deviceName, data []byte) error {
				if data == nil || matched {
					return nil
				}
				var config DeviceConfig
				if err := json.Unmarshal(data, &config); err != nil {
					return nil // skip corrupted entries
				}
				if config.Claim == claim {
					matched = true
				}
				return nil
			})
			if err != nil {
				return err
			}
			if matched {
				podsToDelete = append(podsToDelete, types.UID(string(podUID)))
			}
			return nil
		})
	})
	if err != nil {
		klog.Errorf("BoltPodConfigStore: failed to scan for claim %s: %v", claim, err)
		return nil
	}

	// Delete all entries for matched pods.
	for _, podUID := range podsToDelete {
		s.DeletePod(podUID)
	}
	return podsToDelete
}

func (s *BoltPodConfigStore) UpdateLastNRIActivity(podUID types.UID, timestamp time.Time) {
	// Only update if pod has persisted configs (i.e., exists in bolt).
	var exists bool
	err := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(podConfigsBucket)
		if root != nil {
			exists = root.Bucket([]byte(podUID)) != nil
		}
		return nil
	})
	if err != nil {
		klog.Errorf("BoltPodConfigStore: failed to check pod existence for %s: %v", podUID, err)
		return
	}
	if !exists {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nriActivities[podUID] = timestamp
}

func (s *BoltPodConfigStore) GetPodNRIActivities() map[types.UID]time.Time {
	// Return activities for all pods that exist in the bolt store.
	activities := make(map[types.UID]time.Time)

	// Collect all unique pod UIDs from bolt.
	err := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(podConfigsBucket)
		if root == nil {
			return nil
		}
		return root.ForEach(func(podUID, v []byte) error {
			if v == nil { // It's a bucket
				activities[types.UID(string(podUID))] = time.Time{}
			}
			return nil
		})
	})
	if err != nil {
		klog.Errorf("BoltPodConfigStore: failed to list pods for NRI activities: %v", err)
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for uid := range activities {
		if act, ok := s.nriActivities[uid]; ok {
			activities[uid] = act
		}
	}
	return activities
}

// Compile-time interface check.
var _ podConfigStorer = &BoltPodConfigStore{}
