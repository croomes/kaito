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

package dacs

import (
	"encoding/json"
	"fmt"

	kaitov1beta1 "github.com/kaito-project/kaito/api/v1beta1"
	"github.com/kaito-project/kaito/pkg/cache"
)

// init registers the DACS provider's conformance expectations so the
// provider-agnostic conformance suite automatically discovers and verifies it
// whenever this package is imported. All DACS-specific assertions (env var names,
// injection label, ImageVolume, discovery URL and KV transfer config shape) live
// here, keeping provider specifics out of the shared conformance suite.
func init() {
	cache.RegisterExpectations(cache.Expectations{
		Provider: kaitov1beta1.CacheProvider(ProviderName),
		// PodMutations does not touch the API server, so a nil client is safe for
		// offline mutation conformance.
		NewForConformance: func() cache.Provider { return New(nil, DefaultConfig()) },
		E2ECapable:        true,
		ModelWeights: cache.MutationExpectation{
			Supported: true,
			RequiredLabels: map[string]string{
				InjectLabelKey: InjectLabelValue,
			},
			RequiredEnvVars: []string{
				"RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_ENABLED",
				"RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_LIB",
				"LD_LIBRARY_PATH",
				"RUNAI_STREAMER_CACHE_ENABLED",
				"CACHE_DISCOVERY_URL",
				"CACHE_SERVER_PORT",
			},
			RequiredVolumes:      []string{ClientVolumeName},
			RequiredVolumeMounts: []string{ClientVolumeName},
			Validate:             validateModelWeightsMutations,
		},
		KVCache: cache.MutationExpectation{
			Supported: true,
			RequiredLabels: map[string]string{
				InjectLabelKey: InjectLabelValue,
			},
			RequiredEnvVars: []string{
				"VLLM_KV_TRANSFER_CONFIG",
			},
			Validate: validateKVCacheMutations,
		},
		E2EScenarios: dacsE2EScenarios(),
	})
}

// validateModelWeightsMutations performs DACS-specific deep validation of the
// model weights mutations (library path, discovery URL, server port).
func validateModelWeightsMutations(m *cache.PodMutations) []error {
	var errs []error

	if lib, ok := m.EnvValue("RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_LIB"); !ok || lib != ClientLibPath {
		errs = append(errs, fmt.Errorf("RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_LIB = %q, want %q", lib, ClientLibPath))
	}

	if url, ok := m.EnvValue("CACHE_DISCOVERY_URL"); !ok || url == "" {
		errs = append(errs, fmt.Errorf("CACHE_DISCOVERY_URL must be a non-empty discovery endpoint"))
	}

	wantPort := fmt.Sprintf("%d", DefaultDiscoveryPort)
	if port, ok := m.EnvValue("CACHE_SERVER_PORT"); !ok || port != wantPort {
		errs = append(errs, fmt.Errorf("CACHE_SERVER_PORT = %q, want %q", port, wantPort))
	}

	return errs
}

// validateKVCacheMutations performs DACS-specific deep validation of the KV cache
// mutations (the VLLM_KV_TRANSFER_CONFIG must be valid JSON naming the DACS connector).
func validateKVCacheMutations(m *cache.PodMutations) []error {
	var errs []error

	raw, ok := m.EnvValue("VLLM_KV_TRANSFER_CONFIG")
	if !ok || raw == "" {
		return []error{fmt.Errorf("VLLM_KV_TRANSFER_CONFIG must be a non-empty JSON value")}
	}

	var cfg struct {
		KVConnector            string                 `json:"kv_connector"`
		KVConnectorExtraConfig map[string]interface{} `json:"kv_connector_extra_config"`
	}
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return []error{fmt.Errorf("VLLM_KV_TRANSFER_CONFIG is not valid JSON: %w", err)}
	}
	if cfg.KVConnector == "" {
		errs = append(errs, fmt.Errorf("VLLM_KV_TRANSFER_CONFIG missing kv_connector"))
	}
	if _, ok := cfg.KVConnectorExtraConfig["locator_nodes"]; !ok {
		errs = append(errs, fmt.Errorf("VLLM_KV_TRANSFER_CONFIG missing kv_connector_extra_config.locator_nodes"))
	}

	return errs
}
