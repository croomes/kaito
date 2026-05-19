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

package cache

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	kaitov1beta1 "github.com/kaito-project/kaito/api/v1beta1"
	"github.com/kaito-project/kaito/pkg/featuregates"
	pkgmodel "github.com/kaito-project/kaito/pkg/model"
	"github.com/kaito-project/kaito/pkg/utils/consts"
	"github.com/kaito-project/kaito/pkg/utils/generator"
)

// tachyonTestProvider simulates the Tachyon provider for integration tests.
// It returns realistic mutations matching the real Tachyon provider output.
type tachyonTestProvider struct {
	blobEndpoint  string
	blobContainer string
	blobPrefix    string
	discoveryEP   string
	siImage       string
}

func (p *tachyonTestProvider) Name() string { return "tachyon" }
func (p *tachyonTestProvider) IsAvailable(_ context.Context) (bool, error) {
	return true, nil
}
func (p *tachyonTestProvider) IsReady(_ context.Context) (bool, string, error) {
	return true, "ready", nil
}
func (p *tachyonTestProvider) PodMutations(_ context.Context, concern CacheConcern, ws *kaitov1beta1.Workspace, modelName, modelRevision string) (*PodMutations, error) {
	mutations := &PodMutations{}

	switch concern {
	case CacheConcernModelWeights:
		mutations.InitContainers = append(mutations.InitContainers, corev1.Container{
			Name:    "tachyon-lib-loader",
			Image:   p.siImage,
			Command: []string{"cp", "/lib/libStorageIntercept.so", "/opt/tachyon/libStorageIntercept.so"},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "tachyon-lib", MountPath: "/opt/tachyon"},
			},
		})
		mutations.Volumes = append(mutations.Volumes, corev1.Volume{
			Name: "tachyon-lib",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		})
		mutations.VolumeMounts = append(mutations.VolumeMounts, corev1.VolumeMount{
			Name:      "tachyon-lib",
			MountPath: "/opt/tachyon",
			ReadOnly:  true,
		})
		mutations.EnvVars = append(mutations.EnvVars,
			corev1.EnvVar{Name: "LD_PRELOAD", Value: "/opt/tachyon/libStorageIntercept.so"},
			corev1.EnvVar{Name: "SI_storagePath", Value: "/mnt/models"},
			corev1.EnvVar{Name: "SI_type", Value: "blob"},
			corev1.EnvVar{Name: "SI_azBlobStorageAccName", Value: "myaccount"},
			corev1.EnvVar{Name: "SI_azBlobStorageEndpointUrl", Value: p.blobEndpoint},
			corev1.EnvVar{Name: "SI_azBlobContainerName", Value: p.blobContainer},
			corev1.EnvVar{Name: "SI_cacheEnable", Value: "true"},
			corev1.EnvVar{Name: "SI_cacheServerDiscoveryEnabled", Value: "true"},
			corev1.EnvVar{Name: "SI_cacheServerDiscoveryEndpoint", Value: p.discoveryEP},
		)
		if modelName != "" {
			revision := modelRevision
			if revision == "" {
				revision = "main"
			}
			localPath := "/mnt/models/" + p.blobPrefix + "/" + modelName + "/" + revision
			mutations.EnvVars = append(mutations.EnvVars, corev1.EnvVar{
				Name:  "KAITO_MODEL_PATH",
				Value: localPath,
			})
		}

	case CacheConcernKVCache:
		mutations.EnvVars = append(mutations.EnvVars, corev1.EnvVar{
			Name:  "VLLM_KV_TRANSFER_CONFIG",
			Value: `{"kv_connector":"TachyonKVConnector","locator_nodes":"` + p.discoveryEP + `","protocol":"tcp"}`,
		})
	}

	return mutations, nil
}
func (p *tachyonTestProvider) Prewarm(_ context.Context, _ PrewarmRequest) error { return nil }
func (p *tachyonTestProvider) Cleanup(_ context.Context, _ PrewarmRequest) error  { return nil }

// mockModel implements pkgmodel.Model for tests.
type mockModel struct {
	name    string
	version string
}

func (m *mockModel) GetInferenceParameters() *pkgmodel.PresetParam {
	return &pkgmodel.PresetParam{
		Metadata: pkgmodel.Metadata{
			Name:    m.name,
			Version: m.version,
		},
		ReadinessTimeout: 30 * time.Minute,
	}
}
func (m *mockModel) GetTuningParameters() *pkgmodel.PresetParam { return nil }
func (m *mockModel) SupportDistributedInference() bool          { return false }
func (m *mockModel) SupportTuning() bool                        { return false }

func setupTachyonProvider() *tachyonTestProvider {
	p := &tachyonTestProvider{
		blobEndpoint:  "https://myaccount.blob.core.windows.net",
		blobContainer: "models",
		blobPrefix:    "kaito-models",
		discoveryEP:   "http://cacheserver-discovery.tachyon-cache-system.svc.cluster.local:9065",
		siImage:       "tachyontestacr.azurecr.io/cache-client-base:latest",
	}
	Register(p)
	return p
}

// TestSetCacheMutations_FullPipeline tests the complete mutation pipeline
// from feature gate check through to pod spec modification.
func TestSetCacheMutations_FullPipeline(t *testing.T) {
	// Register test provider.
	setupTachyonProvider()

	tests := []struct {
		name               string
		featureGateEnabled bool
		workspace          *kaitov1beta1.Workspace
		expectInitContainers int
		expectVolumes        int
		expectVolumeMounts   int
		expectEnvVars        []string // env var names to check
		expectNoEnvVars      []string // env vars that should NOT be present
	}{
		{
			name:               "feature gate disabled - no mutations",
			featureGateEnabled: false,
			workspace: &kaitov1beta1.Workspace{
				Cache: &kaitov1beta1.CacheSpec{
					ModelWeights: &kaitov1beta1.ModelWeightsCacheConfig{
						Provider: "tachyon",
						Mode:     kaitov1beta1.CacheModeRequired,
					},
				},
			},
			expectInitContainers: 0,
			expectVolumes:        0,
			expectVolumeMounts:   0,
			expectNoEnvVars:      []string{"LD_PRELOAD", "SI_storagePath"},
		},
		{
			name:               "model weights only - full SI injection",
			featureGateEnabled: true,
			workspace: &kaitov1beta1.Workspace{
				Cache: &kaitov1beta1.CacheSpec{
					ModelWeights: &kaitov1beta1.ModelWeightsCacheConfig{
						Provider: "tachyon",
						Mode:     kaitov1beta1.CacheModeOpportunistic,
					},
				},
			},
			expectInitContainers: 1,
			expectVolumes:        1,
			expectVolumeMounts:   1,
			expectEnvVars: []string{
				"LD_PRELOAD",
				"SI_storagePath",
				"SI_type",
				"SI_azBlobStorageAccName",
				"SI_azBlobStorageEndpointUrl",
				"SI_azBlobContainerName",
				"SI_cacheEnable",
				"SI_cacheServerDiscoveryEnabled",
				"SI_cacheServerDiscoveryEndpoint",
				"KAITO_MODEL_PATH",
			},
		},
		{
			name:               "KV cache only - no init container",
			featureGateEnabled: true,
			workspace: &kaitov1beta1.Workspace{
				Cache: &kaitov1beta1.CacheSpec{
					KVCache: &kaitov1beta1.KVCacheConfig{
						Provider: "tachyon",
						Mode:     kaitov1beta1.CacheModeRequired,
					},
				},
			},
			expectInitContainers: 0,
			expectVolumes:        0,
			expectVolumeMounts:   0,
			expectEnvVars:        []string{"VLLM_KV_TRANSFER_CONFIG"},
			expectNoEnvVars:      []string{"LD_PRELOAD", "KAITO_MODEL_PATH"},
		},
		{
			name:               "both model weights and KV cache",
			featureGateEnabled: true,
			workspace: &kaitov1beta1.Workspace{
				Cache: &kaitov1beta1.CacheSpec{
					ModelWeights: &kaitov1beta1.ModelWeightsCacheConfig{
						Provider: "tachyon",
						Mode:     kaitov1beta1.CacheModeOpportunistic,
					},
					KVCache: &kaitov1beta1.KVCacheConfig{
						Provider: "tachyon",
						Mode:     kaitov1beta1.CacheModeRequired,
					},
				},
			},
			expectInitContainers: 1,
			expectVolumes:        1,
			expectVolumeMounts:   1,
			expectEnvVars: []string{
				"LD_PRELOAD",
				"SI_storagePath",
				"KAITO_MODEL_PATH",
				"VLLM_KV_TRANSFER_CONFIG",
			},
		},
		{
			name:               "disabled mode - no mutations",
			featureGateEnabled: true,
			workspace: &kaitov1beta1.Workspace{
				Cache: &kaitov1beta1.CacheSpec{
					ModelWeights: &kaitov1beta1.ModelWeightsCacheConfig{
						Provider: "tachyon",
						Mode:     kaitov1beta1.CacheModeDisabled,
					},
				},
			},
			expectInitContainers: 0,
			expectVolumes:        0,
			expectVolumeMounts:   0,
			expectNoEnvVars:      []string{"LD_PRELOAD", "KAITO_MODEL_PATH"},
		},
		{
			name:               "nil cache - no mutations",
			featureGateEnabled: true,
			workspace:          &kaitov1beta1.Workspace{},
			expectInitContainers: 0,
			expectVolumes:        0,
			expectVolumeMounts:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set feature gate.
			featuregates.FeatureGates[consts.FeatureFlagDistributedCache] = tt.featureGateEnabled
			defer func() { featuregates.FeatureGates[consts.FeatureFlagDistributedCache] = false }()

			// Build a mock WorkspaceGeneratorContext with a model.
			ctx := &generator.WorkspaceGeneratorContext{
				Ctx:       context.Background(),
				Workspace: tt.workspace,
				Model:     &mockModel{name: "microsoft/phi-4"},
			}

			// Create a pod spec with an existing container (simulates vLLM).
			spec := &corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:    "vllm",
						Image:   "kaito-vllm:latest",
						Command: []string{"python3", "/workspace/vllm/inference_api.py"},
						Env:     []corev1.EnvVar{{Name: "EXISTING_VAR", Value: "keep"}},
					},
				},
			}

			// Execute the mutation pipeline.
			modifier := SetCacheMutations()
			err := modifier(ctx, spec)
			if err != nil {
				t.Fatalf("SetCacheMutations returned error: %v", err)
			}

			// Verify init containers.
			if len(spec.InitContainers) != tt.expectInitContainers {
				t.Errorf("expected %d init containers, got %d", tt.expectInitContainers, len(spec.InitContainers))
			}

			// Verify volumes.
			if len(spec.Volumes) != tt.expectVolumes {
				t.Errorf("expected %d volumes, got %d", tt.expectVolumes, len(spec.Volumes))
			}

			// Verify volume mounts on model container.
			if len(spec.Containers[0].VolumeMounts) != tt.expectVolumeMounts {
				t.Errorf("expected %d volume mounts, got %d", tt.expectVolumeMounts, len(spec.Containers[0].VolumeMounts))
			}

			// Verify expected env vars are present.
			envMap := make(map[string]string)
			for _, e := range spec.Containers[0].Env {
				envMap[e.Name] = e.Value
			}
			for _, name := range tt.expectEnvVars {
				if _, ok := envMap[name]; !ok {
					t.Errorf("expected env var %s to be present", name)
				}
			}

			// Verify excluded env vars are NOT present.
			for _, name := range tt.expectNoEnvVars {
				if _, ok := envMap[name]; ok {
					t.Errorf("expected env var %s to NOT be present", name)
				}
			}

			// Always verify existing env vars are preserved.
			if envMap["EXISTING_VAR"] != "keep" {
				t.Error("existing env var EXISTING_VAR was lost or modified")
			}
		})
	}
}

// TestSetCacheMutations_InitContainerDetails verifies the init container has
// the correct image, command, and volume mount configuration.
func TestSetCacheMutations_InitContainerDetails(t *testing.T) {
	setupTachyonProvider()
	featuregates.FeatureGates[consts.FeatureFlagDistributedCache] = true
	defer func() { featuregates.FeatureGates[consts.FeatureFlagDistributedCache] = false }()

	ws := &kaitov1beta1.Workspace{
		Cache: &kaitov1beta1.CacheSpec{
			ModelWeights: &kaitov1beta1.ModelWeightsCacheConfig{
				Provider: "tachyon",
				Mode:     kaitov1beta1.CacheModeRequired,
			},
		},
		Inference: &kaitov1beta1.InferenceSpec{
			Preset: &kaitov1beta1.PresetSpec{
				PresetMeta: kaitov1beta1.PresetMeta{Name: "microsoft/phi-4"},
			},
		},
	}

	ctx := &generator.WorkspaceGeneratorContext{
		Ctx:       context.Background(),
		Workspace: ws,
		Model:     &mockModel{name: "microsoft/phi-4"},
	}

	spec := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "model", Image: "vllm:latest"}},
	}

	modifier := SetCacheMutations()
	err := modifier(ctx, spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify init container details.
	if len(spec.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(spec.InitContainers))
	}
	initC := spec.InitContainers[0]
	if initC.Name != "tachyon-lib-loader" {
		t.Errorf("init container name: got %q, want %q", initC.Name, "tachyon-lib-loader")
	}
	if initC.Image != "tachyontestacr.azurecr.io/cache-client-base:latest" {
		t.Errorf("init container image: got %q", initC.Image)
	}
	if len(initC.Command) != 3 || initC.Command[0] != "cp" {
		t.Errorf("init container command: got %v", initC.Command)
	}
	if len(initC.VolumeMounts) != 1 || initC.VolumeMounts[0].Name != "tachyon-lib" {
		t.Errorf("init container volume mounts: got %v", initC.VolumeMounts)
	}

	// Verify shared volume is emptyDir.
	if len(spec.Volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(spec.Volumes))
	}
	if spec.Volumes[0].EmptyDir == nil {
		t.Error("expected emptyDir volume source")
	}

	// Verify model container volume mount is read-only.
	if len(spec.Containers[0].VolumeMounts) != 1 {
		t.Fatalf("expected 1 volume mount, got %d", len(spec.Containers[0].VolumeMounts))
	}
	if !spec.Containers[0].VolumeMounts[0].ReadOnly {
		t.Error("expected volume mount to be read-only")
	}
}

// TestSetCacheMutations_ModelPathDerivation verifies that KAITO_MODEL_PATH
// is correctly derived from the model name and blob prefix.
func TestSetCacheMutations_ModelPathDerivation(t *testing.T) {
	setupTachyonProvider()
	featuregates.FeatureGates[consts.FeatureFlagDistributedCache] = true
	defer func() { featuregates.FeatureGates[consts.FeatureFlagDistributedCache] = false }()

	tests := []struct {
		name             string
		presetName       string
		expectedPath     string
	}{
		{
			name:         "standard org/model format",
			presetName:   "microsoft/phi-4",
			expectedPath: "/mnt/models/kaito-models/microsoft/phi-4/main",
		},
		{
			name:         "nested org name",
			presetName:   "meta-llama/Llama-3.3-70B-Instruct",
			expectedPath: "/mnt/models/kaito-models/meta-llama/Llama-3.3-70B-Instruct/main",
		},
		{
			name:         "single segment model name",
			presetName:   "phi-4",
			expectedPath: "/mnt/models/kaito-models/phi-4/main",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ws := &kaitov1beta1.Workspace{
				Cache: &kaitov1beta1.CacheSpec{
					ModelWeights: &kaitov1beta1.ModelWeightsCacheConfig{
						Provider: "tachyon",
						Mode:     kaitov1beta1.CacheModeOpportunistic,
					},
				},
				Inference: &kaitov1beta1.InferenceSpec{
					Preset: &kaitov1beta1.PresetSpec{
						PresetMeta: kaitov1beta1.PresetMeta{Name: kaitov1beta1.ModelName(tt.presetName)},
					},
				},
			}

			ctx := &generator.WorkspaceGeneratorContext{
				Ctx:       context.Background(),
				Workspace: ws,
				Model:     &mockModel{name: tt.presetName},
			}
			spec := &corev1.PodSpec{
				Containers: []corev1.Container{{Name: "model"}},
			}

			modifier := SetCacheMutations()
			err := modifier(ctx, spec)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Find KAITO_MODEL_PATH.
			var modelPath string
			for _, e := range spec.Containers[0].Env {
				if e.Name == "KAITO_MODEL_PATH" {
					modelPath = e.Value
					break
				}
			}
			if modelPath != tt.expectedPath {
				t.Errorf("KAITO_MODEL_PATH: got %q, want %q", modelPath, tt.expectedPath)
			}
		})
	}
}

// TestSetCacheMutations_PreservesExistingPodSpec verifies that cache mutations
// don't interfere with existing pod spec elements.
func TestSetCacheMutations_PreservesExistingPodSpec(t *testing.T) {
	setupTachyonProvider()
	featuregates.FeatureGates[consts.FeatureFlagDistributedCache] = true
	defer func() { featuregates.FeatureGates[consts.FeatureFlagDistributedCache] = false }()

	ws := &kaitov1beta1.Workspace{
		Cache: &kaitov1beta1.CacheSpec{
			ModelWeights: &kaitov1beta1.ModelWeightsCacheConfig{
				Provider: "tachyon",
				Mode:     kaitov1beta1.CacheModeRequired,
			},
		},
		Inference: &kaitov1beta1.InferenceSpec{
			Preset: &kaitov1beta1.PresetSpec{
				PresetMeta: kaitov1beta1.PresetMeta{Name: "microsoft/phi-4"},
			},
		},
	}

	ctx := &generator.WorkspaceGeneratorContext{
		Ctx:       context.Background(),
		Workspace: ws,
		Model:     &mockModel{name: "microsoft/phi-4"},
	}

	// Pod spec with pre-existing volumes, mounts, env vars, and init containers.
	spec := &corev1.PodSpec{
		InitContainers: []corev1.Container{
			{Name: "existing-init", Image: "busybox"},
		},
		Containers: []corev1.Container{
			{
				Name:  "model",
				Image: "vllm:latest",
				Env: []corev1.EnvVar{
					{Name: "HF_TOKEN", Value: "secret"},
					{Name: "CUDA_VISIBLE_DEVICES", Value: "0,1"},
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "weights", MountPath: "/workspace/vllm/weights"},
				},
			},
		},
		Volumes: []corev1.Volume{
			{Name: "weights"},
		},
	}

	modifier := SetCacheMutations()
	err := modifier(ctx, spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify existing init container is preserved.
	if len(spec.InitContainers) != 2 {
		t.Fatalf("expected 2 init containers (1 existing + 1 cache), got %d", len(spec.InitContainers))
	}
	if spec.InitContainers[0].Name != "existing-init" {
		t.Error("existing init container was not preserved at position 0")
	}

	// Verify existing volumes preserved.
	if len(spec.Volumes) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(spec.Volumes))
	}

	// Verify existing env vars preserved.
	envMap := make(map[string]string)
	for _, e := range spec.Containers[0].Env {
		envMap[e.Name] = e.Value
	}
	if envMap["HF_TOKEN"] != "secret" {
		t.Error("HF_TOKEN env var was lost")
	}
	if envMap["CUDA_VISIBLE_DEVICES"] != "0,1" {
		t.Error("CUDA_VISIBLE_DEVICES env var was lost")
	}

	// Verify existing volume mounts preserved.
	if len(spec.Containers[0].VolumeMounts) != 2 {
		t.Fatalf("expected 2 volume mounts (1 existing + 1 cache), got %d", len(spec.Containers[0].VolumeMounts))
	}
}
