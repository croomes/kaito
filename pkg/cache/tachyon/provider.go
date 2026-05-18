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

	// ModelWeights controls whether model weight caching is supported.
	ModelWeightsEnabled bool

	// KVCache controls whether KV caching is supported.
	KVCacheEnabled bool

	// KVConnectorProtocol is the transport protocol for KV cache (e.g., "rdma", "tcp").
	KVConnectorProtocol string

	// BlobEndpoint is the Azure Blob Storage endpoint (e.g., "https://account.blob.core.windows.net").
	BlobEndpoint string

	// BlobContainer is the blob container used for model weight storage.
	BlobContainer string

	// BlobPrefix is the path prefix within the container (defaults to "kaito-models").
	BlobPrefix string

	// PrewarmImage is the container image used for prewarm Jobs.
	PrewarmImage string
}

// DefaultConfig returns sensible defaults for Tachyon integration.
func DefaultConfig() Config {
	return Config{
		DiscoveryEndpoint:   DefaultDiscoveryEndpoint,
		ModelWeightsEnabled: true,
		KVCacheEnabled:      true,
		KVConnectorProtocol: "tcp",
		BlobContainer:       "kaito-models",
		BlobPrefix:          DefaultBlobPrefix,
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

// PodMutations returns env vars that enable Tachyon caching in model pods.
func (p *Provider) PodMutations(ctx context.Context, workspace *kaitov1beta1.Workspace, modelName, modelRevision string) (*cache.PodMutations, error) {
	mutations := &cache.PodMutations{}

	if workspace.Cache == nil {
		return mutations, nil
	}

	// Model weights cache env vars (StorageIntercept + blob model path).
	if workspace.Cache.ModelWeights != nil && workspace.Cache.ModelWeights.Mode != kaitov1beta1.CacheModeDisabled && p.config.ModelWeightsEnabled {
		mutations.EnvVars = append(mutations.EnvVars, modelWeightsEnvVars(p.config.DiscoveryEndpoint)...)

		// Inject the blob model path so vLLM loads from blob storage via cache.
		if p.config.BlobEndpoint != "" && modelName != "" {
			blobPath := ModelBlobPath(p.config.BlobEndpoint, p.config.BlobContainer, p.config.BlobPrefix, modelName, modelRevision)
			mutations.EnvVars = append(mutations.EnvVars, corev1.EnvVar{
				Name:  "MODEL_BLOB_URI",
				Value: blobPath,
			})
		}
	}

	// KV cache env vars (vLLM KV connector config).
	if workspace.Cache.KVCache != nil && workspace.Cache.KVCache.Mode != kaitov1beta1.CacheModeDisabled && p.config.KVCacheEnabled {
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

// modelWeightsEnvVars returns the env vars needed for StorageIntercept.
func modelWeightsEnvVars(discoveryEndpoint string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_ENABLED", Value: "1"},
		{Name: "SI_cacheEnable", Value: "true"},
		{Name: "SI_cacheServerDiscoveryEnabled", Value: "true"},
		{Name: "SI_cacheServerDiscoveryEndpoint", Value: discoveryEndpoint},
	}
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

func init() {
	cache.Register(&Provider{config: DefaultConfig()})
}
