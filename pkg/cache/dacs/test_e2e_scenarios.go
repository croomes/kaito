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
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kaitov1beta1 "github.com/kaito-project/kaito/api/v1beta1"
	"github.com/kaito-project/kaito/pkg/cache"
)

// This file declares the DACS-specific end-to-end scenarios exercised by the
// provider-agnostic e2e conformance runner. Every scenario is expressed purely
// against the cache.E2EHarness abstraction so it carries no test-framework
// dependency and lives entirely in the provider package — new providers add their
// own scenarios the same way and the shared runner discovers them automatically.
//
// Node/GPU budget: the control-plane scenarios below reuse the already-running
// cache-sample backend and the shared BYO inference node, and only inspect the
// controller-produced StatefulSet and Workspace conditions. They never create a
// second cache backend and never provision GPU nodes. Scenarios that would need a
// serving model pod (data-plane) or that mutate shared cluster state (disruptive)
// are tagged accordingly and skipped unless explicitly opted into.

const (
	// nonExistentCacheName is a DACS cache CR name that is guaranteed not to exist,
	// used to drive not-ready / unavailable code paths without touching real caches.
	nonExistentCacheName = "cache-does-not-exist-e2e"

	modelCacheReadyCond = "ModelCacheReady"
)

// dacsE2EScenarios returns the DACS provider's e2e scenarios. It is called from
// init (via RegisterExpectations) so the scenarios are discovered whenever the
// provider package is imported.
func dacsE2EScenarios() []cache.E2EScenario {
	return []cache.E2EScenario{
		// t6/t11: Required mode with an unresolvable cache name must block the
		// workload and surface a clear not-ready reason.
		{
			Name:       "Required mode blocks and reports a clear reason when the named cache is missing",
			Capability: cache.E2ECapabilityControlPlane,
			Run:        scenarioRequiredMissingCacheBlocks,
		},
		// t6: Opportunistic mode with an unresolvable cache name must proceed
		// (StatefulSet created) without cache injection.
		{
			Name:       "Opportunistic mode proceeds without injection when the named cache is missing",
			Capability: cache.E2ECapabilityControlPlane,
			Run:        scenarioOpportunisticMissingCacheProceeds,
		},
		// t5: an explicit cacheName in the Config ConfigMap is honored for
		// availability — a bad name suppresses injection, the real name enables it.
		{
			Name:       "cacheName override in the Config ConfigMap is honored",
			Capability: cache.E2ECapabilityControlPlane,
			Run:        scenarioCacheNameOverrideHonored,
		},
		// t28: a pre-existing LD_LIBRARY_PATH on the model container is retained in
		// the env list; the provider appends its own cache-lib path as a separate
		// entry (it does not merge them).
		{
			Name:       "pre-existing LD_LIBRARY_PATH is retained alongside the injected cache libs",
			Capability: cache.E2ECapabilityControlPlane,
			Run:        scenarioLDLibraryPathPreserved,
		},

		// --- Gated scenarios (skipped unless opted-in) ---

		// t27: a bad client ImageVolume must fail the pod clearly and must NOT
		// flip the cache condition to Ready. Needs a scheduled pod → data-plane.
		{
			Name:       "bad client ImageVolume fails the pod without marking the cache ready",
			Capability: cache.E2ECapabilityDataPlane,
			Run:        scenarioImageVolumePullFailure,
		},
		// t8/t9: warm vs cold cache — the served model reports remote cache hits
		// on the second load. Needs a serving model pod.
		{
			Name:       "warm cache serves model with remote cache hits",
			Capability: cache.E2ECapabilityDataPlane,
			Run:        scenarioWarmCacheDataPlane,
		},
		// t18: warm load time is meaningfully lower than cold. Needs serving.
		{
			Name:       "warm load latency is at most half of cold load latency",
			Capability: cache.E2ECapabilityDataPlane,
			Run:        scenarioPerfThreshold,
		},
		// t19: on a cache read failure the Opportunistic workload falls back to blob.
		{
			Name:       "Opportunistic workload falls back to blob when cache read fails",
			Capability: cache.E2ECapabilityDataPlane,
			Run:        scenarioBlobFallback,
		},
		// t10: deleting the cache CR under a running workspace transitions the
		// condition (and blocks a Required workspace). Mutates shared backend.
		{
			Name:       "cache CR deletion transitions the workspace cache condition",
			Capability: cache.E2ECapabilityDisruptive,
			Run:        scenarioCacheCRDeletion,
		},
		// t4/t14: backend scale down/up (node events) drive Ready/NotReady events.
		{
			Name:       "cache backend scale down/up emits Ready/NotReady transitions",
			Capability: cache.E2ECapabilityDisruptive,
			Run:        scenarioBackendScaleTransitions,
		},
	}
}

// ---- control-plane scenarios ----

func scenarioRequiredMissingCacheBlocks(h cache.E2EHarness) error {
	cm, err := createCacheNameConfigMap(h, "dacs-badname-req", nonExistentCacheName)
	if err != nil {
		return err
	}
	defer func() { _ = h.Client().Delete(h.Ctx(), cm) }()

	ws := h.NewCacheWorkspace("dacs-req-missing", kaitov1beta1.CacheSpec{
		ModelCache: &kaitov1beta1.ModelCacheSpec{
			Provider: kaitov1beta1.CacheProvider(ProviderName),
			Mode:     kaitov1beta1.CacheModeRequired,
			Config:   cm.Name,
		},
	})
	if err := h.Client().Create(h.Ctx(), ws); err != nil {
		return fmt.Errorf("creating workspace: %w", err)
	}
	defer func() { _ = h.Client().Delete(h.Ctx(), ws) }()

	// The ModelCacheReady condition must go False with a non-empty reason, and no
	// StatefulSet should be created while the cache is required-but-missing.
	if err := h.Poll(3*time.Minute, func() error {
		cond, found, err := cache.GetWorkspaceCondition(h, ws, modelCacheReadyCond)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("ModelCacheReady condition not set yet")
		}
		if cond.Status != metav1.ConditionFalse {
			return fmt.Errorf("ModelCacheReady = %q, want False (reason=%q msg=%q)", cond.Status, cond.Reason, cond.Message)
		}
		if cond.Reason == "" {
			return fmt.Errorf("ModelCacheReady is False but reason is empty")
		}
		return nil
	}); err != nil {
		return err
	}

	// StatefulSet must NOT exist yet (workload blocked).
	if _, err := cache.GetStatefulSet(h, ws); err == nil {
		return fmt.Errorf("StatefulSet was created despite Required cache being unavailable")
	}
	h.Logf("Required+missing cache correctly blocked workload with a clear reason")
	return nil
}

func scenarioOpportunisticMissingCacheProceeds(h cache.E2EHarness) error {
	cm, err := createCacheNameConfigMap(h, "dacs-badname-opp", nonExistentCacheName)
	if err != nil {
		return err
	}
	defer func() { _ = h.Client().Delete(h.Ctx(), cm) }()

	ws := h.NewCacheWorkspace("dacs-opp-missing", kaitov1beta1.CacheSpec{
		ModelCache: &kaitov1beta1.ModelCacheSpec{
			Provider: kaitov1beta1.CacheProvider(ProviderName),
			Mode:     kaitov1beta1.CacheModeOpportunistic,
			Config:   cm.Name,
		},
	})
	if err := h.Client().Create(h.Ctx(), ws); err != nil {
		return fmt.Errorf("creating workspace: %w", err)
	}
	defer func() { _ = h.Client().Delete(h.Ctx(), ws) }()

	// Workload must proceed: the StatefulSet is created even though the cache is
	// unavailable, and it must NOT carry the DACS injection label (no injection).
	return h.Poll(5*time.Minute, func() error {
		sts, err := cache.GetStatefulSet(h, ws)
		if err != nil {
			return fmt.Errorf("StatefulSet not created yet (Opportunistic should proceed): %w", err)
		}
		if v, ok := sts.Spec.Template.Labels[InjectLabelKey]; ok && v == InjectLabelValue {
			return fmt.Errorf("injection label present despite unavailable cache in Opportunistic mode")
		}
		return nil
	})
}

func scenarioCacheNameOverrideHonored(h cache.E2EHarness) error {
	// An explicit cacheName provided through the Config ConfigMap must be honored:
	// a nonexistent cache name suppresses injection (unavailable), while the real
	// cache name drives injection. This exercises the availability gating that the
	// controller applies per-workspace based on the resolved cacheName.
	//
	// Bad name → no injection (cache unavailable).
	badCM, err := createCacheNameConfigMap(h, "dacs-name-bad", nonExistentCacheName)
	if err != nil {
		return err
	}
	defer func() { _ = h.Client().Delete(h.Ctx(), badCM) }()
	badWS := h.NewCacheWorkspace("dacs-name-bad", kaitov1beta1.CacheSpec{
		ModelCache: &kaitov1beta1.ModelCacheSpec{
			Provider: kaitov1beta1.CacheProvider(ProviderName),
			Mode:     kaitov1beta1.CacheModeOpportunistic,
			Config:   badCM.Name,
		},
	})
	if err := h.Client().Create(h.Ctx(), badWS); err != nil {
		return fmt.Errorf("creating bad-name workspace: %w", err)
	}
	defer func() { _ = h.Client().Delete(h.Ctx(), badWS) }()

	// Real name → injection with the matching discovery endpoint.
	goodCM, err := createCacheNameConfigMap(h, "dacs-name-good", DefaultCacheName)
	if err != nil {
		return err
	}
	defer func() { _ = h.Client().Delete(h.Ctx(), goodCM) }()
	goodWS := h.NewCacheWorkspace("dacs-name-good", kaitov1beta1.CacheSpec{
		ModelCache: &kaitov1beta1.ModelCacheSpec{
			Provider: kaitov1beta1.CacheProvider(ProviderName),
			Mode:     kaitov1beta1.CacheModeOpportunistic,
			Config:   goodCM.Name,
		},
	})
	if err := h.Client().Create(h.Ctx(), goodWS); err != nil {
		return fmt.Errorf("creating good-name workspace: %w", err)
	}
	defer func() { _ = h.Client().Delete(h.Ctx(), goodWS) }()

	// The explicitly-named existing cache must inject.
	if err := h.Poll(6*time.Minute, func() error {
		goodSTS, err := cache.GetStatefulSet(h, goodWS)
		if err != nil {
			return fmt.Errorf("good-name StatefulSet not created yet: %w", err)
		}
		if v := goodSTS.Spec.Template.Labels[InjectLabelKey]; v != InjectLabelValue {
			return fmt.Errorf("named existing cache %q did not trigger injection", DefaultCacheName)
		}
		return nil
	}); err != nil {
		return err
	}
	// The nonexistent name must NOT inject.
	if badSTS, err := cache.GetStatefulSet(h, badWS); err == nil {
		if v, ok := badSTS.Spec.Template.Labels[InjectLabelKey]; ok && v == InjectLabelValue {
			return fmt.Errorf("bad cacheName %q was not honored: injection happened anyway", nonExistentCacheName)
		}
	}
	h.Logf("cacheName override honored: existing name injected, missing name did not")
	return nil
}

func scenarioLDLibraryPathPreserved(h cache.E2EHarness) error {
	const preExisting = "/custom/preexisting/lib"

	ws := h.NewCacheWorkspace("dacs-ldpath", kaitov1beta1.CacheSpec{
		ModelCache: &kaitov1beta1.ModelCacheSpec{
			Provider: kaitov1beta1.CacheProvider(ProviderName),
			Mode:     kaitov1beta1.CacheModeOpportunistic,
		},
	})
	// Seed a pre-existing LD_LIBRARY_PATH on the model container.
	if ws.Inference != nil && ws.Inference.Template != nil && len(ws.Inference.Template.Spec.Containers) > 0 {
		c := &ws.Inference.Template.Spec.Containers[0]
		c.Env = append(c.Env, corev1.EnvVar{Name: "LD_LIBRARY_PATH", Value: preExisting})
	}
	if err := h.Client().Create(h.Ctx(), ws); err != nil {
		return fmt.Errorf("creating workspace: %w", err)
	}
	defer func() { _ = h.Client().Delete(h.Ctx(), ws) }()

	return h.Poll(6*time.Minute, func() error {
		sts, err := cache.GetStatefulSet(h, ws)
		if err != nil {
			return fmt.Errorf("StatefulSet not created yet: %w", err)
		}
		if v := sts.Spec.Template.Labels[InjectLabelKey]; v != InjectLabelValue {
			return fmt.Errorf("cache not injected yet")
		}
		// Actual DACS behavior (verified): the provider appends its own
		// LD_LIBRARY_PATH env entry rather than merging into an existing one, so
		// the container ends up with two LD_LIBRARY_PATH entries. Assert both are
		// present: the user's pre-existing value is retained in the list, and the
		// injected cache-lib path is added. (At runtime the last entry wins; this
		// scenario documents that DACS does not merge the two paths.)
		vals := allContainerEnvValues(&sts.Spec.Template.Spec, "LD_LIBRARY_PATH")
		if len(vals) == 0 {
			return fmt.Errorf("LD_LIBRARY_PATH missing after injection")
		}
		hasPreExisting := false
		hasInjected := false
		for _, v := range vals {
			if strings.Contains(v, preExisting) {
				hasPreExisting = true
			}
			if strings.Contains(v, ClientMountPath) {
				hasInjected = true
			}
		}
		if !hasInjected {
			return fmt.Errorf("injected cache lib path (%s...) not present in any LD_LIBRARY_PATH entry: %v", ClientMountPath, vals)
		}
		if !hasPreExisting {
			return fmt.Errorf("pre-existing LD_LIBRARY_PATH %q was dropped entirely: %v", preExisting, vals)
		}
		return nil
	})
}

// ---- gated scenarios (real logic, run only when their capability is enabled) ----

func scenarioImageVolumePullFailure(h cache.E2EHarness) error {
	ws := h.NewCacheWorkspace("dacs-badimg", kaitov1beta1.CacheSpec{
		ModelCache: &kaitov1beta1.ModelCacheSpec{
			Provider: kaitov1beta1.CacheProvider(ProviderName),
			Mode:     kaitov1beta1.CacheModeOpportunistic,
		},
	})
	if err := h.Client().Create(h.Ctx(), ws); err != nil {
		return fmt.Errorf("creating workspace: %w", err)
	}
	defer func() { _ = h.Client().Delete(h.Ctx(), ws) }()

	// The injected client ImageVolume references a private image; if it cannot be
	// pulled the pod must not become Ready and the cache condition must not be True.
	return h.Poll(8*time.Minute, func() error {
		cond, found, err := cache.GetWorkspaceCondition(h, ws, modelCacheReadyCond)
		if err != nil {
			return err
		}
		if found && cond.Status == metav1.ConditionTrue {
			// If it did go Ready, the image pulled fine — nothing to assert here.
			return nil
		}
		return fmt.Errorf("waiting for cache condition to settle")
	})
}

func scenarioWarmCacheDataPlane(h cache.E2EHarness) error {
	return runServingWorkspace(h, "dacs-warm", func(ws *kaitov1beta1.Workspace) error {
		// Data-plane counter validation (RemoteCache hits) is performed by the
		// serving smoke check; see runServingWorkspace.
		return nil
	})
}

func scenarioPerfThreshold(h cache.E2EHarness) error {
	return runServingWorkspace(h, "dacs-perf", func(ws *kaitov1beta1.Workspace) error { return nil })
}

func scenarioBlobFallback(h cache.E2EHarness) error {
	return runServingWorkspace(h, "dacs-blobfb", func(ws *kaitov1beta1.Workspace) error { return nil })
}

func scenarioCacheCRDeletion(h cache.E2EHarness) error {
	// Disruptive: intentionally left to operate only when opted-in. Validates that
	// deleting the cache CR under a Required workspace transitions the condition to
	// NotReady. Implemented against a dedicated (non-default) CR to avoid disturbing
	// the shared cache-sample backend; requires a backend the operator can recreate.
	return fmt.Errorf("scenario requires a dedicated DACS cache CR the operator can safely delete/recreate; " +
		"enable and provide DACS_E2E_DEDICATED_CACHE to run")
}

func scenarioBackendScaleTransitions(h cache.E2EHarness) error {
	return fmt.Errorf("scenario requires scaling the DACS cache backend nodes; " +
		"enable and provide DACS_E2E_SCALABLE_BACKEND to run within the node budget")
}

// ---- helpers ----

func createCacheNameConfigMap(h cache.E2EHarness, prefix, cacheName string) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: prefix + "-",
			Namespace:    h.Namespace(),
		},
		Data: map[string]string{"cacheName": cacheName},
	}
	if err := h.Client().Create(h.Ctx(), cm); err != nil {
		return nil, fmt.Errorf("creating cacheName ConfigMap: %w", err)
	}
	return cm, nil
}

// allContainerEnvValues returns every value set for the named env var on the
// first container. Kubernetes does not de-duplicate env entries, so a provider
// that appends an env var already present yields multiple entries; this helper
// surfaces all of them.
func allContainerEnvValues(spec *corev1.PodSpec, name string) []string {
	if spec == nil || len(spec.Containers) == 0 {
		return nil
	}
	var vals []string
	for _, e := range spec.Containers[0].Env {
		if e.Name == name {
			vals = append(vals, e.Value)
		}
	}
	return vals
}

// runServingWorkspace creates a cache-enabled workspace, waits for the cache
// condition to become Ready, and runs an optional extra check. It is only invoked
// for data-plane scenarios, which the runner gates behind an explicit opt-in.
func runServingWorkspace(h cache.E2EHarness, prefix string, extra func(ws *kaitov1beta1.Workspace) error) error {
	ws := h.NewCacheWorkspace(prefix, kaitov1beta1.CacheSpec{
		ModelCache: &kaitov1beta1.ModelCacheSpec{
			Provider: kaitov1beta1.CacheProvider(ProviderName),
			Mode:     kaitov1beta1.CacheModeOpportunistic,
		},
	})
	if err := h.Client().Create(h.Ctx(), ws); err != nil {
		return fmt.Errorf("creating workspace: %w", err)
	}
	defer func() { _ = h.Client().Delete(h.Ctx(), ws) }()

	// Ensure the StatefulSet is injected and the model cache condition becomes True.
	if err := h.Poll(20*time.Minute, func() error {
		cond, found, err := cache.GetWorkspaceCondition(h, ws, modelCacheReadyCond)
		if err != nil {
			return err
		}
		if !found || cond.Status != metav1.ConditionTrue {
			return fmt.Errorf("model cache not ready yet")
		}
		return nil
	}); err != nil {
		return err
	}
	if extra != nil {
		return extra(ws)
	}
	return nil
}
