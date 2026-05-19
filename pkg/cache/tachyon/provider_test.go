// Copyright (c) KAITO authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tachyon

import (
	"context"
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	kaitov1beta1 "github.com/kaito-project/kaito/api/v1beta1"
	"github.com/kaito-project/kaito/pkg/cache"
)

func newFakeProvider(objects ...runtime.Object) *Provider {
	scheme := runtime.NewScheme()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			cacheGVR: "CacheList",
		}, objects...)
	return New(client, DefaultConfig())
}

func newReadyCache() *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "storage.azure.com/v1",
			"kind":       "Cache",
			"metadata": map[string]interface{}{
				"name":      "test-cache",
				"namespace": CacheNamespace,
			},
			"status": map[string]interface{}{
				"conditions": []interface{}{
					map[string]interface{}{
						"type":   "Ready",
						"status": "True",
						"reason": "CacheReady",
					},
				},
			},
		},
	}
}

func TestProviderName(t *testing.T) {
	p := newFakeProvider()
	if p.Name() != ProviderName {
		t.Errorf("expected %q, got %q", ProviderName, p.Name())
	}
}

func TestPodMutations_ModelWeightsOnly(t *testing.T) {
	p := newFakeProvider()
	ws := &kaitov1beta1.Workspace{
		Cache: &kaitov1beta1.CacheSpec{
			ModelWeights: &kaitov1beta1.ModelWeightsCacheConfig{
				Provider: "tachyon",
				Mode:     kaitov1beta1.CacheModeOpportunistic,
			},
		},
	}

	mutations, err := p.PodMutations(context.Background(), cache.CacheConcernModelWeights, ws, "microsoft/phi-4", "main")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Init container for SI library injection.
	if len(mutations.InitContainers) != 1 {
	}
	if mutations.InitContainers[0].Name != "tachyon-lib-loader" {
		t.Errorf("init container name: got %q, want %q", mutations.InitContainers[0].Name, "tachyon-lib-loader")
	}
	if mutations.InitContainers[0].Image != DefaultStorageInterceptImage {
		t.Errorf("init container image: got %q, want %q", mutations.InitContainers[0].Image, DefaultStorageInterceptImage)
	}

	// Shared volume for SI library.
	if len(mutations.Volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(mutations.Volumes))
	}
	if mutations.Volumes[0].Name != "tachyon-lib" {
		t.Errorf("volume name: got %q, want %q", mutations.Volumes[0].Name, "tachyon-lib")
	}

	// Volume mount for SI library.
	if len(mutations.VolumeMounts) != 1 {
		t.Fatalf("expected 1 volume mount, got %d", len(mutations.VolumeMounts))
	}
	if mutations.VolumeMounts[0].MountPath != "/opt/tachyon" {
		t.Errorf("volume mount path: got %q, want %q", mutations.VolumeMounts[0].MountPath, "/opt/tachyon")
	}

	// Env vars: 9 SI config + 1 KAITO_MODEL_PATH = 10 (BlobEndpoint is empty, but account name will be empty string)
	expectedEnvs := map[string]string{
		"LD_PRELOAD":                       "/opt/tachyon/libStorageIntercept.so",
		"SI_storagePath":                   "/mnt/models",
		"SI_type":                          "blob",
		"SI_azBlobStorageAccName":      "",
		"SI_azBlobStorageEndpointUrl":         "",
		"SI_azBlobContainerName":           "kaito-models",
		"SI_cacheEnable":                   "true",
		"SI_cacheServerDiscoveryEnabled":   "true",
		"SI_cacheServerDiscoveryEndpoint":  DefaultDiscoveryEndpoint,
		"KAITO_MODEL_PATH":                "/mnt/models/kaito-models/microsoft/phi-4/main",
	}
	if len(mutations.EnvVars) != len(expectedEnvs) {
		t.Fatalf("expected %d env vars, got %d: %v", len(expectedEnvs), len(mutations.EnvVars), mutations.EnvVars)
	}
	for _, env := range mutations.EnvVars {
		expected, ok := expectedEnvs[env.Name]
		if !ok {
			t.Errorf("unexpected env var: %s", env.Name)
			continue
		}
		if env.Value != expected {
			t.Errorf("env %s: expected %q, got %q", env.Name, expected, env.Value)
		}
	}
}

func TestPodMutations_ModelWeightsWithBlob(t *testing.T) {
	scheme := runtime.NewScheme()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			cacheGVR: "CacheList",
		})
	cfg := DefaultConfig()
	cfg.BlobEndpoint = "https://myaccount.blob.core.windows.net"
	cfg.BlobContainer = "models"
	p := New(client, cfg)

	ws := &kaitov1beta1.Workspace{
		Cache: &kaitov1beta1.CacheSpec{
			ModelWeights: &kaitov1beta1.ModelWeightsCacheConfig{
				Provider: "tachyon",
				Mode:     kaitov1beta1.CacheModeOpportunistic,
			},
		},
	}

	mutations, err := p.PodMutations(context.Background(), cache.CacheConcernModelWeights, ws, "microsoft/phi-4", "abc123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify account name extracted from endpoint.
	var accountNameFound bool
	var modelPathFound bool
	for _, env := range mutations.EnvVars {
		switch env.Name {
		case "SI_azBlobStorageAccName":
			accountNameFound = true
			if env.Value != "myaccount" {
				t.Errorf("SI_azBlobStorageAccName: expected %q, got %q", "myaccount", env.Value)
			}
		case "SI_azBlobContainerName":
			if env.Value != "models" {
				t.Errorf("SI_azBlobContainerName: expected %q, got %q", "models", env.Value)
			}
		case "KAITO_MODEL_PATH":
			modelPathFound = true
			expected := "/mnt/models/kaito-models/microsoft/phi-4/abc123"
			if env.Value != expected {
				t.Errorf("KAITO_MODEL_PATH: expected %q, got %q", expected, env.Value)
			}
		}
	}
	if !accountNameFound {
		t.Error("SI_azBlobStorageAccName env var not found")
	}
	if !modelPathFound {
		t.Error("KAITO_MODEL_PATH env var not found")
	}

	// Verify init container and volumes.
	if len(mutations.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(mutations.InitContainers))
	}
	if len(mutations.Volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(mutations.Volumes))
	}
	if len(mutations.VolumeMounts) != 1 {
		t.Fatalf("expected 1 volume mount, got %d", len(mutations.VolumeMounts))
	}
}

func TestPodMutations_KVCacheOnly(t *testing.T) {
	p := newFakeProvider()
	ws := &kaitov1beta1.Workspace{
		Cache: &kaitov1beta1.CacheSpec{
			KVCache: &kaitov1beta1.KVCacheConfig{
				Provider: "tachyon",
				Mode:     kaitov1beta1.CacheModeRequired,
			},
		},
	}

	mutations, err := p.PodMutations(context.Background(), cache.CacheConcernKVCache, ws, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mutations.EnvVars) != 1 {
		t.Fatalf("expected 1 env var for KV cache, got %d", len(mutations.EnvVars))
	}

	if mutations.EnvVars[0].Name != "VLLM_KV_TRANSFER_CONFIG" {
		t.Errorf("expected VLLM_KV_TRANSFER_CONFIG, got %s", mutations.EnvVars[0].Name)
	}

	var cfg kvTransferConfig
	if err := json.Unmarshal([]byte(mutations.EnvVars[0].Value), &cfg); err != nil {
		t.Fatalf("failed to parse KV config: %v", err)
	}
	if cfg.KVConnector != "TachyonKVConnector" {
		t.Errorf("expected TachyonKVConnector, got %s", cfg.KVConnector)
	}
	if cfg.Protocol != "tcp" {
		t.Errorf("expected tcp protocol, got %s", cfg.Protocol)
	}
}

func TestPodMutations_BothConcerns(t *testing.T) {
	p := newFakeProvider()
	ws := &kaitov1beta1.Workspace{
		Cache: &kaitov1beta1.CacheSpec{
			ModelWeights: &kaitov1beta1.ModelWeightsCacheConfig{
				Provider: "tachyon",
				Mode:     kaitov1beta1.CacheModeOpportunistic,
			},
			KVCache: &kaitov1beta1.KVCacheConfig{
				Provider: "tachyon",
				Mode:     kaitov1beta1.CacheModeOpportunistic,
			},
		},
	}

	// Model weights concern returns SI mutations only (no KV).
	mwMutations, err := p.PodMutations(context.Background(), cache.CacheConcernModelWeights, ws, "microsoft/phi-4", "main")
	if err != nil {
		t.Fatalf("model weights: unexpected error: %v", err)
	}
	// 9 SI env vars + 1 KAITO_MODEL_PATH = 10
	if len(mwMutations.EnvVars) != 10 {
		t.Fatalf("model weights: expected 10 env vars, got %d", len(mwMutations.EnvVars))
	}
	for _, env := range mwMutations.EnvVars {
		if env.Name == "VLLM_KV_TRANSFER_CONFIG" {
			t.Error("model weights concern should not include KV config")
		}
	}

	// KV concern returns only KV mutations (no SI).
	kvMutations, err := p.PodMutations(context.Background(), cache.CacheConcernKVCache, ws, "microsoft/phi-4", "main")
	if err != nil {
		t.Fatalf("KV cache: unexpected error: %v", err)
	}
	if len(kvMutations.EnvVars) != 1 {
		t.Fatalf("KV cache: expected 1 env var, got %d", len(kvMutations.EnvVars))
	}
	if kvMutations.EnvVars[0].Name != "VLLM_KV_TRANSFER_CONFIG" {
		t.Errorf("KV cache: expected VLLM_KV_TRANSFER_CONFIG, got %s", kvMutations.EnvVars[0].Name)
	}
	if len(kvMutations.InitContainers) != 0 {
		t.Error("KV concern should not include init containers")
	}
}

func TestPodMutations_ProviderConfigDisabled(t *testing.T) {
	// When the provider's config disables a concern, it returns empty mutations.
	scheme := runtime.NewScheme()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			cacheGVR: "CacheList",
		})
	cfg := DefaultConfig()
	cfg.ModelWeightsEnabled = false
	cfg.KVCacheEnabled = false
	p := New(client, cfg)

	ws := &kaitov1beta1.Workspace{
		Cache: &kaitov1beta1.CacheSpec{
			ModelWeights: &kaitov1beta1.ModelWeightsCacheConfig{
				Provider: "tachyon",
				Mode:     kaitov1beta1.CacheModeRequired,
			},
			KVCache: &kaitov1beta1.KVCacheConfig{
				Provider: "tachyon",
				Mode:     kaitov1beta1.CacheModeRequired,
			},
		},
	}

	mw, err := p.PodMutations(context.Background(), cache.CacheConcernModelWeights, ws, "microsoft/phi-4", "main")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mw.EnvVars) != 0 {
		t.Errorf("expected 0 env vars when model weights disabled in config, got %d", len(mw.EnvVars))
	}

	kv, err := p.PodMutations(context.Background(), cache.CacheConcernKVCache, ws, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(kv.EnvVars) != 0 {
		t.Errorf("expected 0 env vars when KV cache disabled in config, got %d", len(kv.EnvVars))
	}
}

func TestPodMutations_ConcernIsolation(t *testing.T) {
	// Calling with KVCache concern does not produce model weight mutations.
	p := newFakeProvider()
	ws := &kaitov1beta1.Workspace{
		Cache: &kaitov1beta1.CacheSpec{
			ModelWeights: &kaitov1beta1.ModelWeightsCacheConfig{
				Provider: "tachyon",
				Mode:     kaitov1beta1.CacheModeRequired,
			},
			KVCache: &kaitov1beta1.KVCacheConfig{
				Provider: "tachyon",
				Mode:     kaitov1beta1.CacheModeRequired,
			},
		},
	}

	// KV concern must not return SI env vars or init containers.
	kv, err := p.PodMutations(context.Background(), cache.CacheConcernKVCache, ws, "microsoft/phi-4", "main")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(kv.InitContainers) != 0 {
		t.Errorf("KV concern should not have init containers, got %d", len(kv.InitContainers))
	}
	for _, env := range kv.EnvVars {
		if env.Name == "LD_PRELOAD" || env.Name == "SI_storagePath" || env.Name == "KAITO_MODEL_PATH" {
			t.Errorf("KV concern should not include model weight env var %s", env.Name)
		}
	}

	// Model weights concern must not return KV env vars.
	mw, err := p.PodMutations(context.Background(), cache.CacheConcernModelWeights, ws, "microsoft/phi-4", "main")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, env := range mw.EnvVars {
		if env.Name == "VLLM_KV_TRANSFER_CONFIG" {
			t.Error("model weights concern should not include VLLM_KV_TRANSFER_CONFIG")
		}
	}
}

func TestCheckCacheReady(t *testing.T) {
	tests := []struct {
		name      string
		obj       *unstructured.Unstructured
		wantReady bool
		wantMsg   string
	}{
		{
			name:      "ready cache",
			obj:       newReadyCache(),
			wantReady: true,
			wantMsg:   "cache is ready",
		},
		{
			name: "not ready cache",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"type":   "Ready",
								"status": "False",
								"reason": "CacheInitializing",
							},
						},
					},
				},
			},
			wantReady: false,
			wantMsg:   "cache not ready: CacheInitializing",
		},
		{
			name: "no conditions",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"status": map[string]interface{}{},
				},
			},
			wantReady: false,
			wantMsg:   "no status conditions found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ready, msg := checkCacheReady(tt.obj)
			if ready != tt.wantReady {
				t.Errorf("ready: got %v, want %v", ready, tt.wantReady)
			}
			if msg != tt.wantMsg {
				t.Errorf("msg: got %q, want %q", msg, tt.wantMsg)
			}
		})
	}
}

func TestIsReady_NoCaches(t *testing.T) {
	p := newFakeProvider()
	ready, reason, err := p.IsReady(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ready {
		t.Error("expected not ready when no caches exist")
	}
	if reason != "no Tachyon Cache CR found" {
		t.Errorf("unexpected reason: %s", reason)
	}
}

func TestIsReady_WithReadyCache(t *testing.T) {
	cacheObj := newReadyCache()

	scheme := runtime.NewScheme()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			cacheGVR: "CacheList",
		}, cacheObj)
	p := New(client, DefaultConfig())

	ready, reason, err := p.IsReady(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ready {
		t.Errorf("expected ready, got not ready: %s", reason)
	}
}

// Ensure explicit registration works correctly.
func TestExplicitRegistration(t *testing.T) {
	p := newFakeProvider()
	cache.Register(p)

	got, err := cache.Get(kaitov1beta1.CacheProvider(ProviderName))
	if err != nil {
		t.Fatalf("tachyon provider not registered: %v", err)
	}
	if got.Name() != ProviderName {
		t.Errorf("registered provider name: got %q, want %q", got.Name(), ProviderName)
	}
}

// Suppress unused import warning.
var _ = metav1.Now
