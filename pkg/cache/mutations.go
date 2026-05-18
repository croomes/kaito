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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	kaitov1beta1 "github.com/kaito-project/kaito/api/v1beta1"
	"github.com/kaito-project/kaito/pkg/featuregates"
	"github.com/kaito-project/kaito/pkg/utils/consts"
	"github.com/kaito-project/kaito/pkg/utils/generator"
)

// SetCacheMutations returns a pod spec modifier that injects cache provider
// env vars, volumes, volume mounts, and init containers into the inference pod.
// Returns nil (no modifier) when the feature gate is disabled or cache is not configured.
func SetCacheMutations() generator.TypedManifestModifier[generator.WorkspaceGeneratorContext, corev1.PodSpec] {
	return func(ctx *generator.WorkspaceGeneratorContext, spec *corev1.PodSpec) error {
		if !featuregates.FeatureGates[consts.FeatureFlagDistributedCache] {
			return nil
		}

		ws := ctx.Workspace
		if ws.Cache == nil {
			return nil
		}

		mutations, err := collectMutations(ctx.Ctx, ws)
		if err != nil {
			return fmt.Errorf("collecting cache mutations: %w", err)
		}

		applyMutations(spec, mutations)
		return nil
	}
}

// collectMutations gathers PodMutations from all configured cache providers.
func collectMutations(ctx context.Context, ws *kaitov1beta1.Workspace) (*PodMutations, error) {
	merged := &PodMutations{}

	// Model weights provider
	if ws.Cache.ModelWeights != nil && ws.Cache.ModelWeights.Mode != kaitov1beta1.CacheModeDisabled {
		p, err := Get(ws.Cache.ModelWeights.Provider)
		if err != nil {
			if ws.Cache.ModelWeights.Mode == kaitov1beta1.CacheModeRequired {
				return nil, fmt.Errorf("model weights cache provider %q: %w", ws.Cache.ModelWeights.Provider, err)
			}
			klog.V(2).InfoS("Model weights cache provider not available, skipping",
				"provider", ws.Cache.ModelWeights.Provider, "error", err)
		} else {
			m, err := p.PodMutations(ctx, ws)
			if err != nil {
				if ws.Cache.ModelWeights.Mode == kaitov1beta1.CacheModeRequired {
					return nil, fmt.Errorf("model weights cache mutations: %w", err)
				}
				klog.V(2).InfoS("Model weights cache mutations failed, skipping",
					"provider", ws.Cache.ModelWeights.Provider, "error", err)
			} else {
				mergeMutations(merged, m)
			}
		}
	}

	// KV cache provider
	if ws.Cache.KVCache != nil && ws.Cache.KVCache.Mode != kaitov1beta1.CacheModeDisabled {
		p, err := Get(ws.Cache.KVCache.Provider)
		if err != nil {
			if ws.Cache.KVCache.Mode == kaitov1beta1.CacheModeRequired {
				return nil, fmt.Errorf("KV cache provider %q: %w", ws.Cache.KVCache.Provider, err)
			}
			klog.V(2).InfoS("KV cache provider not available, skipping",
				"provider", ws.Cache.KVCache.Provider, "error", err)
		} else {
			m, err := p.PodMutations(ctx, ws)
			if err != nil {
				if ws.Cache.KVCache.Mode == kaitov1beta1.CacheModeRequired {
					return nil, fmt.Errorf("KV cache mutations: %w", err)
				}
				klog.V(2).InfoS("KV cache mutations failed, skipping",
					"provider", ws.Cache.KVCache.Provider, "error", err)
			} else {
				mergeMutations(merged, m)
			}
		}
	}

	return merged, nil
}

// mergeMutations appends src mutations into dst, deduplicating env vars by name.
func mergeMutations(dst, src *PodMutations) {
	if src == nil {
		return
	}

	// Deduplicate env vars by name (last wins).
	existingEnvs := make(map[string]struct{}, len(dst.EnvVars))
	for _, e := range dst.EnvVars {
		existingEnvs[e.Name] = struct{}{}
	}
	for _, e := range src.EnvVars {
		if _, exists := existingEnvs[e.Name]; !exists {
			dst.EnvVars = append(dst.EnvVars, e)
			existingEnvs[e.Name] = struct{}{}
		}
	}

	dst.Volumes = append(dst.Volumes, src.Volumes...)
	dst.VolumeMounts = append(dst.VolumeMounts, src.VolumeMounts...)
	dst.InitContainers = append(dst.InitContainers, src.InitContainers...)
}

// applyMutations injects the collected mutations into the pod spec.
func applyMutations(spec *corev1.PodSpec, mutations *PodMutations) {
	if mutations == nil || len(spec.Containers) == 0 {
		return
	}

	// Inject env vars and volume mounts into the first (model) container.
	spec.Containers[0].Env = append(spec.Containers[0].Env, mutations.EnvVars...)
	spec.Containers[0].VolumeMounts = append(spec.Containers[0].VolumeMounts, mutations.VolumeMounts...)

	// Inject volumes and init containers at the pod level.
	spec.Volumes = append(spec.Volumes, mutations.Volumes...)
	spec.InitContainers = append(spec.InitContainers, mutations.InitContainers...)
}
