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
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/dranet/pkg/apis"
)

func newTestBoltStore(t *testing.T) *BoltPodConfigStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	store, err := NewBoltPodConfigStore(dbPath)
	if err != nil {
		t.Fatalf("NewBoltPodConfigStore() error: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestBoltPodConfigStore_SetAndGet(t *testing.T) {
	store := newTestBoltStore(t)
	podUID := types.UID("test-pod-uid-1")
	deviceName := "eth0"
	config := DeviceConfig{
		Claim: types.NamespacedName{Namespace: "ns", Name: "claim1"},
		NetworkInterfaceConfigInPod: apis.NetworkConfig{
			Interface: apis.InterfaceConfig{Name: "eth0-pod"},
			Routes: []apis.RouteConfig{
				{Destination: "0.0.0.0/0", Gateway: "192.168.1.1"},
			},
			Ethtool: &apis.EthtoolConfig{
				Features: map[string]bool{"tx-checksumming": true},
			},
		},
		RDMADevice: RDMAConfig{LinkDev: "mlx5_0"},
	}

	// Test Get on non-existent item.
	_, found := store.GetDeviceConfig(podUID, deviceName)
	if found {
		t.Errorf("GetDeviceConfig() found a config before Set(), expected not found")
	}

	store.SetDeviceConfig(podUID, deviceName, config)

	retrievedConfig, found := store.GetDeviceConfig(podUID, deviceName)
	if !found {
		t.Fatalf("GetDeviceConfig() did not find config after Set()")
	}
	if !reflect.DeepEqual(retrievedConfig, config) {
		t.Errorf("GetDeviceConfig() = %+v, want %+v", retrievedConfig, config)
	}

	// Test Get with different deviceName.
	_, found = store.GetDeviceConfig(podUID, "eth1")
	if found {
		t.Errorf("GetDeviceConfig() found config for wrong deviceName")
	}

	// Test Get with different podUID.
	_, found = store.GetDeviceConfig(types.UID("other-pod-uid"), deviceName)
	if found {
		t.Errorf("GetDeviceConfig() found config for wrong podUID")
	}

	// Test overwriting.
	newConfig := DeviceConfig{
		NetworkInterfaceConfigInPod: apis.NetworkConfig{
			Interface: apis.InterfaceConfig{Name: "eth0-new"},
			Ethtool:   &apis.EthtoolConfig{PrivateFlags: map[string]bool{"custom-flag": false}},
		},
	}
	store.SetDeviceConfig(podUID, deviceName, newConfig)
	retrievedConfig, found = store.GetDeviceConfig(podUID, deviceName)
	if !found {
		t.Fatalf("GetDeviceConfig() did not find config after overwrite")
	}
	if !reflect.DeepEqual(retrievedConfig, newConfig) {
		t.Errorf("GetDeviceConfig() after overwrite = %+v, want %+v", retrievedConfig, newConfig)
	}
}

func TestBoltPodConfigStore_GetPodConfig(t *testing.T) {
	store := newTestBoltStore(t)
	podUID := types.UID("test-pod-uid-1")
	config1 := DeviceConfig{NetworkInterfaceConfigInPod: apis.NetworkConfig{Interface: apis.InterfaceConfig{Name: "eth0"}}}
	config2 := DeviceConfig{NetworkInterfaceConfigInPod: apis.NetworkConfig{Interface: apis.InterfaceConfig{Name: "eth1"}}}

	store.SetDeviceConfig(podUID, "eth0", config1)
	store.SetDeviceConfig(podUID, "eth1", config2)

	podConfig, found := store.GetPodConfig(podUID)
	if !found {
		t.Fatalf("GetPodConfig() did not find configs for podUID")
	}
	if len(podConfig.DeviceConfigs) != 2 {
		t.Errorf("GetPodConfig() returned %d configs, want 2", len(podConfig.DeviceConfigs))
	}
	if !reflect.DeepEqual(podConfig.DeviceConfigs["eth0"], config1) {
		t.Errorf("GetPodConfig()[eth0] = %+v, want %+v", podConfig.DeviceConfigs["eth0"], config1)
	}
	if !reflect.DeepEqual(podConfig.DeviceConfigs["eth1"], config2) {
		t.Errorf("GetPodConfig()[eth1] = %+v, want %+v", podConfig.DeviceConfigs["eth1"], config2)
	}

	// Non-existent pod.
	_, found = store.GetPodConfig(types.UID("non-existent"))
	if found {
		t.Errorf("GetPodConfig() found config for non-existent pod")
	}
}

func TestBoltPodConfigStore_DeletePod(t *testing.T) {
	store := newTestBoltStore(t)
	podUID1 := types.UID("pod-1")
	podUID2 := types.UID("pod-2")

	store.SetDeviceConfig(podUID1, "eth0", DeviceConfig{NetworkInterfaceConfigInPod: apis.NetworkConfig{Interface: apis.InterfaceConfig{Name: "p1eth0"}}})
	store.SetDeviceConfig(podUID1, "eth1", DeviceConfig{NetworkInterfaceConfigInPod: apis.NetworkConfig{Interface: apis.InterfaceConfig{Name: "p1eth1"}}})
	store.SetDeviceConfig(podUID2, "eth0", DeviceConfig{NetworkInterfaceConfigInPod: apis.NetworkConfig{Interface: apis.InterfaceConfig{Name: "p2eth0"}}})

	store.DeletePod(podUID1)

	_, found := store.GetPodConfig(podUID1)
	if found {
		t.Errorf("GetPodConfig() found config for deleted pod")
	}

	podConfig, found := store.GetPodConfig(podUID2)
	if !found {
		t.Fatalf("GetPodConfig() did not find config for other pod")
	}
	if len(podConfig.DeviceConfigs) != 1 {
		t.Errorf("Expected 1 device config for pod2, got %d", len(podConfig.DeviceConfigs))
	}

	// Delete non-existent pod - should not panic.
	store.DeletePod(types.UID("non-existent"))
}

func TestBoltPodConfigStore_DeleteClaim(t *testing.T) {
	store := newTestBoltStore(t)
	claim1 := types.NamespacedName{Namespace: "ns1", Name: "claim1"}
	claim2 := types.NamespacedName{Namespace: "ns1", Name: "claim2"}

	store.SetDeviceConfig("pod-1", "eth0", DeviceConfig{Claim: claim1})
	store.SetDeviceConfig("pod-1", "eth1", DeviceConfig{Claim: claim1})
	store.SetDeviceConfig("pod-2", "eth0", DeviceConfig{Claim: claim1})
	store.SetDeviceConfig("pod-3", "eth0", DeviceConfig{Claim: claim2})

	deletedPods := store.DeleteClaim(claim1)

	if len(deletedPods) != 2 {
		t.Errorf("DeleteClaim() returned %d pods, want 2", len(deletedPods))
	}

	_, found := store.GetPodConfig("pod-1")
	if found {
		t.Errorf("Pod-1 should have been deleted")
	}
	_, found = store.GetPodConfig("pod-2")
	if found {
		t.Errorf("Pod-2 should have been deleted")
	}

	podConfig, found := store.GetPodConfig("pod-3")
	if !found {
		t.Fatalf("Pod-3 should not have been deleted")
	}
	if len(podConfig.DeviceConfigs) != 1 {
		t.Errorf("Pod-3 should have 1 device config, got %d", len(podConfig.DeviceConfigs))
	}

	// Delete non-existent claim.
	deletedPods = store.DeleteClaim(types.NamespacedName{Namespace: "ns", Name: "not-exist"})
	if len(deletedPods) != 0 {
		t.Errorf("DeleteClaim() for non-existent claim returned %d pods, want 0", len(deletedPods))
	}
}

func TestBoltPodConfigStore_NRIActivity(t *testing.T) {
	store := newTestBoltStore(t)
	podUID := types.UID("pod-1")
	store.SetDeviceConfig(podUID, "eth0", DeviceConfig{})

	now := time.Now()
	store.UpdateLastNRIActivity(podUID, now)

	activities := store.GetPodNRIActivities()
	if len(activities) != 1 {
		t.Fatalf("Expected 1 activity, got %d", len(activities))
	}
	if !activities[podUID].Equal(now) {
		t.Errorf("Activity timestamp = %v, want %v", activities[podUID], now)
	}

	// UpdateLastNRIActivity for non-existent pod should do nothing.
	store.UpdateLastNRIActivity(types.UID("no-such-pod"), now)
	activities = store.GetPodNRIActivities()
	if len(activities) != 1 {
		t.Errorf("Expected 1 activity after update on non-existent pod, got %d", len(activities))
	}
}

func TestBoltPodConfigStore_Persistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "persist.db")

	config := DeviceConfig{
		Claim: types.NamespacedName{Namespace: "ns1", Name: "claim1"},
		NetworkInterfaceConfigInPod: apis.NetworkConfig{
			Interface: apis.InterfaceConfig{Name: "eth0-pod"},
			Routes:    []apis.RouteConfig{{Destination: "10.0.0.0/8", Gateway: "10.0.0.1"}},
		},
		RDMADevice: RDMAConfig{
			LinkDev: "mlx5_0",
			DevChars: []LinuxDevice{
				{Path: "/dev/infiniband/uverbs0", Type: "c", Major: 231, Minor: 0},
			},
		},
	}

	// Write data and close.
	store1, err := NewBoltPodConfigStore(dbPath)
	if err != nil {
		t.Fatalf("NewBoltPodConfigStore() error: %v", err)
	}
	store1.SetDeviceConfig("pod-1", "eth0", config)
	store1.Close()

	// Reopen and verify data persisted.
	store2, err := NewBoltPodConfigStore(dbPath)
	if err != nil {
		t.Fatalf("NewBoltPodConfigStore() reopen error: %v", err)
	}
	defer store2.Close()

	retrieved, found := store2.GetDeviceConfig("pod-1", "eth0")
	if !found {
		t.Fatalf("GetDeviceConfig() after reopen: not found")
	}
	if !reflect.DeepEqual(retrieved, config) {
		t.Errorf("GetDeviceConfig() after reopen = %+v, want %+v", retrieved, config)
	}

	podConfig, found := store2.GetPodConfig("pod-1")
	if !found {
		t.Fatalf("GetPodConfig() after reopen: not found")
	}
	if len(podConfig.DeviceConfigs) != 1 {
		t.Errorf("Expected 1 device config after reopen, got %d", len(podConfig.DeviceConfigs))
	}
}

func TestBoltPodConfigStore_ThreadSafety(t *testing.T) {
	store := newTestBoltStore(t)
	numGoroutines := 50
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			podUID := types.UID(fmt.Sprintf("pod-%d", i))
			deviceName := fmt.Sprintf("eth%d", i%2)
			config := DeviceConfig{
				NetworkInterfaceConfigInPod: apis.NetworkConfig{
					Interface: apis.InterfaceConfig{Name: fmt.Sprintf("dev-%d", i)},
				},
			}
			store.SetDeviceConfig(podUID, deviceName, config)
			retrieved, _ := store.GetDeviceConfig(podUID, deviceName)
			if !reflect.DeepEqual(retrieved, config) {
				t.Errorf("goroutine %d: Get() = %+v, want %+v", i, retrieved, config)
			}
			if i%10 == 0 {
				store.DeletePod(podUID)
				_, found := store.GetDeviceConfig(podUID, deviceName)
				if found {
					t.Errorf("goroutine %d: Get() found config after DeletePod()", i)
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestBoltPodConfigStore_Errors(t *testing.T) {
	t.Run("creates missing parent directory", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "nested", "path", "test.db")

		store, err := NewBoltPodConfigStore(dbPath)
		if err != nil {
			t.Fatalf("NewBoltPodConfigStore() error: %v", err)
		}
		defer store.Close()

		if _, err := os.Stat(filepath.Dir(dbPath)); err != nil {
			t.Fatalf("expected parent directory to exist: %v", err)
		}
	})

	t.Run("invalid db path", func(t *testing.T) {
		tempDir := t.TempDir()
		// Pass a directory path to force an error on bolt.Open.
		invalidDbPath := filepath.Join(tempDir, "is_a_dir")
		if err := os.Mkdir(invalidDbPath, 0755); err != nil {
			t.Fatalf("failed to mkdir: %v", err)
		}
		_, err := NewBoltPodConfigStore(invalidDbPath)
		if err == nil {
			t.Fatal("expected error when opening a directory as bolt db")
		}
	})

	t.Run("corrupted JSON data", func(t *testing.T) {
		store := newTestBoltStore(t)

		podUID := types.UID("pod-corrupt")
		deviceName := "net1"
		claim := types.NamespacedName{Namespace: "default", Name: "claim1"}

		// Inject invalid JSON directly into the bolt bucket.
		err := store.db.Update(func(tx *bolt.Tx) error {
			root := tx.Bucket(podConfigsBucket)
			podBucket, err := root.CreateBucketIfNotExists([]byte(podUID))
			if err != nil {
				return err
			}
			return podBucket.Put([]byte(deviceName), []byte("{invalid-json"))
		})
		if err != nil {
			t.Fatalf("failed to insert invalid json: %v", err)
		}

		// GetDeviceConfig should return false on unmarshal error.
		if _, found := store.GetDeviceConfig(podUID, deviceName); found {
			t.Error("expected not found due to json unmarshal error")
		}

		// GetPodConfig should skip the corrupted entry.
		podConfig, found := store.GetPodConfig(podUID)
		if found {
			t.Error("expected no valid devices in pod config")
		}
		if len(podConfig.DeviceConfigs) != 0 {
			t.Error("expected empty device configs map")
		}

		// DeleteClaim should skip corrupted entries gracefully.
		if uids := store.DeleteClaim(claim); len(uids) != 0 {
			t.Errorf("expected no pods deleted, got %d", len(uids))
		}
	})

	t.Run("missing root bucket", func(t *testing.T) {
		store := newTestBoltStore(t)

		podUID := types.UID("pod-missing-root")
		deviceName := "net1"
		claim := types.NamespacedName{Namespace: "default", Name: "claim1"}

		// Intentionally delete the root bucket to simulate DB corruption.
		if err := store.db.Update(func(tx *bolt.Tx) error {
			return tx.DeleteBucket(podConfigsBucket)
		}); err != nil {
			t.Fatalf("failed to delete bucket: %v", err)
		}

		// All operations should degrade gracefully.
		if err := store.SetDeviceConfig(podUID, deviceName, DeviceConfig{}); err == nil {
			t.Error("expected error from SetDeviceConfig with missing root bucket")
		}

		if _, found := store.GetDeviceConfig(podUID, deviceName); found {
			t.Error("expected not found with missing root bucket")
		}
		if _, found := store.GetPodConfig(podUID); found {
			t.Error("expected not found with missing root bucket")
		}

		store.DeletePod(podUID)

		if uids := store.DeleteClaim(claim); len(uids) != 0 {
			t.Errorf("expected no pods deleted, got %d", len(uids))
		}
		if acts := store.GetPodNRIActivities(); len(acts) != 0 {
			t.Errorf("expected empty activities map, got %d entries", len(acts))
		}
	})
}
