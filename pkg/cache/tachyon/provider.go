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

// Package tachyon implements the cache.Provider interface for the Tachyon
// distributed NVMe cache service. It manages Cache CRs in the tachyon-cache-system
// namespace and injects the necessary env vars for StorageIntercept (model weights)
// and TachyonKVConnector (KV cache) into inference pods.
package tachyon

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"

	kaitov1beta1 "github.com/kaito-project/kaito/api/v1beta1"
	"github.com/kaito-project/kaito/pkg/cache"
)

const (
	ProviderName = "tachyon"

	// CacheNamespace is the namespace where Tachyon Cache CRs are managed.
	CacheNamespace = "tachyon-cache-system"

	// Discovery endpoint for Tachyon cache servers.
	DefaultDiscoveryEndpoint = "http://cacheserver-discovery.tachyon-cache-system.svc.cluster.local:9065"
	DefaultDiscoveryPort     = 9065
)

var cacheGVR = schema.GroupVersionResource{
	Group:    "storage.azure.com",
	Version:  "v1",
	Resource: "caches",
}

// Config holds Tachyon-specific configuration, typically sourced from Helm values.
type Config struct {
	// DiscoveryEndpoint overrides the default cache server discovery endpoint.
	DiscoveryEndpoint string

	// ModelWeightsEnabled controls whether model weight caching is supported.
	ModelWeightsEnabled bool

	// KVCacheEnabled controls whether KV caching is supported.
	KVCacheEnabled bool

	// KVConnectorProtocol is the transport protocol for KV cache (e.g., "rdma", "tcp").
	KVConnectorProtocol string

	// BlobEndpoint is the Azure Blob Storage endpoint (e.g., "https://account.blob.core.windows.net").
	BlobEndpoint string

	// BlobStorageAccountName is the Azure storage account name.
	// If empty, it is extracted from BlobEndpoint.
	BlobStorageAccountName string

	// BlobContainer is the blob container used for model weight storage.
	BlobContainer string

	// BlobPrefix is the path prefix within the container (defaults to "kaito-models").
	BlobPrefix string

	// StorageInterceptImage is the container image that contains libStorageIntercept.so.
	// An init container copies the library from this image into a shared volume.
	StorageInterceptImage string

	// StorageInterceptLibPath is the path to libStorageIntercept.so within the StorageInterceptImage.
	StorageInterceptLibPath string

	// PrewarmImage is the container image used for prewarm Jobs.
	PrewarmImage string
}

const (
	// DefaultStorageInterceptImage is the default image containing libStorageIntercept.so.
	DefaultStorageInterceptImage = "tachyontestacr.azurecr.io/cache-client-base:latest"

	// DefaultStorageInterceptLibPath is the default path to the .so in the SI image.
	DefaultStorageInterceptLibPath = "/lib/libStorageIntercept.so"

	// cacheLibVolumeName is the shared volume for Tachyon client libraries (SI + KV).
	cacheLibVolumeName = "tachyon-lib"

	// cacheLibMountPath is where Tachyon libraries are mounted in the inference container.
	cacheLibMountPath = "/opt/tachyon"
)

// DefaultConfig returns sensible defaults for Tachyon integration.
func DefaultConfig() Config {
	return Config{
		DiscoveryEndpoint:       DefaultDiscoveryEndpoint,
		ModelWeightsEnabled:     true,
		KVCacheEnabled:          true,
		KVConnectorProtocol:     "tcp",
		BlobContainer:           "kaito-models",
		BlobPrefix:              DefaultBlobPrefix,
		StorageInterceptImage:   DefaultStorageInterceptImage,
		StorageInterceptLibPath: DefaultStorageInterceptLibPath,
	}
}

// Provider implements cache.Provider for Tachyon.
type Provider struct {
	client dynamic.Interface
	config Config
}

var _ cache.Provider = (*Provider)(nil)

// New creates a Tachyon cache provider with the given dynamic client and config.
func New(client dynamic.Interface, cfg Config) *Provider {
	return &Provider{
		client: client,
		config: cfg,
	}
}

func (p *Provider) Name() string { return ProviderName }

// IsAvailable checks if the Tachyon Cache CRD is installed in the cluster.
func (p *Provider) IsAvailable(ctx context.Context) (bool, error) {
	// Attempt to list caches; a NotFound error on the resource type means CRD is missing.
	_, err := p.client.Resource(cacheGVR).Namespace(CacheNamespace).List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		// Could be RBAC or connectivity — report as unavailable with error detail.
		return false, fmt.Errorf("checking Tachyon CRD availability: %w", err)
	}
	return true, nil
}

// IsReady checks if a Cache CR exists and has Ready=True in its status conditions.
func (p *Provider) IsReady(ctx context.Context) (bool, string, error) {
	caches, err := p.client.Resource(cacheGVR).Namespace(CacheNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, "", fmt.Errorf("listing Tachyon caches: %w", err)
	}
	if len(caches.Items) == 0 {
		return false, "no Tachyon Cache CR found", nil
	}

	// Check the first cache for Ready condition.
	cacheObj := caches.Items[0]
	ready, reason := checkCacheReady(&cacheObj)
	return ready, reason, nil
}

// PodMutations returns the pod-level changes needed for the requested cache concern.
// For ModelWeights: injects an init container (copies libStorageIntercept.so),
// shared volume, LD_PRELOAD, StorageIntercept config env vars, and KAITO_MODEL_PATH.
// For KVCache: injects the vLLM KV transfer config env var.
func (p *Provider) PodMutations(ctx context.Context, concern cache.CacheConcern, workspace *kaitov1beta1.Workspace, modelName, modelRevision string) (*cache.PodMutations, error) {
	mutations := &cache.PodMutations{}

	switch concern {
	case cache.CacheConcernModelWeights:
		if !p.config.ModelWeightsEnabled {
			return mutations, nil
		}

		siImage := p.config.StorageInterceptImage
		if siImage == "" {
			siImage = DefaultStorageInterceptImage
		}
		siLibPath := p.config.StorageInterceptLibPath
		if siLibPath == "" {
			siLibPath = DefaultStorageInterceptLibPath
		}

		// Init container: copy libStorageIntercept.so to shared volume.
		mutations.InitContainers = append(mutations.InitContainers, corev1.Container{
			Name:    "tachyon-lib-loader",
			Image:   siImage,
			Command: []string{"cp", siLibPath, cacheLibMountPath + "/libStorageIntercept.so"},
			VolumeMounts: []corev1.VolumeMount{
				{Name: cacheLibVolumeName, MountPath: cacheLibMountPath},
			},
		})

		// Shared emptyDir volume for the library.
		mutations.Volumes = append(mutations.Volumes, corev1.Volume{
			Name: cacheLibVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		})

		// Mount the library volume into the inference container.
		mutations.VolumeMounts = append(mutations.VolumeMounts, corev1.VolumeMount{
			Name:      cacheLibVolumeName,
			MountPath: cacheLibMountPath,
			ReadOnly:  true,
		})

		// StorageIntercept env vars.
		mutations.EnvVars = append(mutations.EnvVars, storageInterceptEnvVars(p.config)...)

		// Model local path (the path vLLM will use as --model).
		if modelName != "" {
			localPath := ModelLocalPath(DefaultStoragePath, p.config.BlobPrefix, modelName, modelRevision)
			mutations.EnvVars = append(mutations.EnvVars, corev1.EnvVar{
				Name:  "KAITO_MODEL_PATH",
				Value: localPath,
			})
		}

	case cache.CacheConcernKVCache:
		if !p.config.KVCacheEnabled {
			return mutations, nil
		}

		kvEnvVars, err := kvCacheEnvVars(p.config.DiscoveryEndpoint, p.config.KVConnectorProtocol)
		if err != nil {
			return nil, fmt.Errorf("building KV cache env vars: %w", err)
		}
		mutations.EnvVars = append(mutations.EnvVars, kvEnvVars...)
	}

	return mutations, nil
}

// Prewarm creates a prewarm Job for the specified model if one doesn't already exist.
// The Job downloads model weights from HuggingFace and uploads to the Tachyon cache.
func (p *Provider) Prewarm(ctx context.Context, req cache.PrewarmRequest) error {
	if p.config.PrewarmImage == "" {
		return fmt.Errorf("prewarm image not configured for tachyon provider")
	}
	if p.config.BlobEndpoint == "" {
		return fmt.Errorf("blob endpoint not configured for tachyon provider")
	}

	klog.V(2).InfoS("Prewarm triggered", "model", req.ModelName, "revision", req.ModelRevision)
	// The Job is created by the workspace controller using BuildPrewarmJob().
	// This method validates that the config is sufficient to build a prewarm Job.
	return nil
}

// Cleanup is a placeholder for cache invalidation logic.
func (p *Provider) Cleanup(ctx context.Context, req cache.PrewarmRequest) error {
	klog.V(4).InfoS("Cleanup requested (not yet implemented)", "model", req.ModelName, "source", req.ModelSource)
	return nil
}

// storageInterceptEnvVars returns the env vars needed to configure StorageIntercept.
// These tell SI where to intercept filesystem reads and how to reach blob + cache.
func storageInterceptEnvVars(cfg Config) []corev1.EnvVar {
	accountName := cfg.BlobStorageAccountName
	if accountName == "" {
		accountName = extractAccountName(cfg.BlobEndpoint)
	}

	return []corev1.EnvVar{
		{Name: "LD_PRELOAD", Value: cacheLibMountPath + "/libStorageIntercept.so"},
		{Name: "SI_storagePath", Value: DefaultStoragePath},
		{Name: "SI_type", Value: "blob"},
		{Name: "SI_azBlobStorageAccountName", Value: accountName},
		{Name: "SI_azBlobStorageEndpoint", Value: cfg.BlobEndpoint},
		{Name: "SI_azBlobContainerName", Value: cfg.BlobContainer},
		{Name: "SI_cacheEnable", Value: "true"},
		{Name: "SI_cacheServerDiscoveryEnabled", Value: "true"},
		{Name: "SI_cacheServerDiscoveryEndpoint", Value: cfg.DiscoveryEndpoint},
	}
}

// extractAccountName extracts the storage account name from a blob endpoint URL.
// For "https://myaccount.blob.core.windows.net", returns "myaccount".
func extractAccountName(endpoint string) string {
	if endpoint == "" {
		return ""
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}
	parts := strings.SplitN(u.Hostname(), ".", 2)
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

// kvTransferConfig is the JSON structure expected by vLLM's --kv-transfer-config flag.
type kvTransferConfig struct {
	KVConnector  string `json:"kv_connector"`
	LocatorNodes string `json:"locator_nodes"`
	Protocol     string `json:"protocol"`
}

// kvCacheEnvVars returns the env var for vLLM KV transfer config.
func kvCacheEnvVars(discoveryEndpoint, protocol string) ([]corev1.EnvVar, error) {
	cfg := kvTransferConfig{
		KVConnector:  "TachyonKVConnector",
		LocatorNodes: discoveryEndpoint,
		Protocol:     protocol,
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	return []corev1.EnvVar{
		{Name: "VLLM_KV_TRANSFER_CONFIG", Value: string(data)},
	}, nil
}

// checkCacheReady inspects an unstructured Cache CR for the Ready condition.
func checkCacheReady(obj *unstructured.Unstructured) (bool, string) {
	conditions, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return false, "no status conditions found"
	}

	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _ := cond["type"].(string)
		if condType == "Ready" {
			status, _ := cond["status"].(string)
			reason, _ := cond["reason"].(string)
			if status == string(metav1.ConditionTrue) {
				return true, "cache is ready"
			}
			return false, fmt.Sprintf("cache not ready: %s", reason)
		}
	}
	return false, "Ready condition not found"
}

// ConfigFromEnv builds a Config from environment variables (set via Helm chart).
// Falls back to DefaultConfig() for any unset values.
func ConfigFromEnv() Config {
	cfg := DefaultConfig()

	if v := os.Getenv("TACHYON_DISCOVERY_ENDPOINT"); v != "" {
		cfg.DiscoveryEndpoint = v
	}
	if v := os.Getenv("TACHYON_MODEL_WEIGHTS_ENABLED"); v != "" {
		cfg.ModelWeightsEnabled = v == "true"
	}
	if v := os.Getenv("TACHYON_KV_CACHE_ENABLED"); v != "" {
		cfg.KVCacheEnabled = v == "true"
	}
	if v := os.Getenv("TACHYON_KV_CONNECTOR_PROTOCOL"); v != "" {
		cfg.KVConnectorProtocol = v
	}
	if v := os.Getenv("TACHYON_BLOB_ENDPOINT"); v != "" {
		cfg.BlobEndpoint = v
	}
	if v := os.Getenv("TACHYON_BLOB_CONTAINER"); v != "" {
		cfg.BlobContainer = v
	}
	if v := os.Getenv("TACHYON_BLOB_PREFIX"); v != "" {
		cfg.BlobPrefix = v
	}
	if v := os.Getenv("TACHYON_STORAGE_INTERCEPT_IMAGE"); v != "" {
		cfg.StorageInterceptImage = v
	}
	if v := os.Getenv("TACHYON_STORAGE_INTERCEPT_LIB_PATH"); v != "" {
		cfg.StorageInterceptLibPath = v
	}
	if v := os.Getenv("TACHYON_PREWARM_IMAGE"); v != "" {
		cfg.PrewarmImage = v
	}

	return cfg
}
