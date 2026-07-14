---
title: Distributed Cache
---

This document explains how to enable distributed caching for model weights and KV cache in KAITO using a pluggable cache provider (currently DACS).

## Overview

The distributed cache integration accelerates model loading and enables KV cache sharing across inference pods. It supports two independent caching concerns:

- **Model Weights** — Caches model files in a distributed NVMe cache, avoiding repeated downloads from blob storage. Uses filesystem interception (LD_PRELOAD) so existing model loading code works unchanged.
- **KV Cache** — Shares attention KV cache between inference pods for prefill/decode disaggregation or prompt cache sharing.

Each concern is independently configurable with its own provider and mode.

## Prerequisites

1. **KAITO** installed with the `distributedCache` feature gate enabled
2. **DACS cache operator** deployed in the cluster (manages Cache CRs in `dacs-cache-system` namespace)
3. **DACS CSI driver + mutating webhook** installed (handles library injection into labeled pods)
4. **Azure Workload Identity** configured on the KAITO nodes (for DefaultAzureCredential access to blob storage)

## Installation

### 1. Install DACS

Install the DACS distributed cache operator, CSI driver, and mutating webhook. The webhook automatically injects cache libraries into pods labeled with `dacs.azure.com/inject: "true"`.

```bash
helm install dacs-cache oci://<registry>/charts/dacs-cache \
  --namespace dacs-cache-system --create-namespace \
  --set cache.nodeSelectorKey=dacs.azure.com/cache-node \
  --set-string cache.nodeSelectorValue=enabled
```

### 2. Configure KAITO

Enable the distributed cache feature gate and configure the DACS provider in your Helm values:

```yaml
featureGates:
  distributedCache: true

cache:
  providers:
    dacs:
      enabled: true
      discoveryEndpoint: ""  # Auto-discovered from Cache CR status if empty
      kvCacheEnabled: true
      kvConnectorProtocol: "tcp"
      blobEndpoint: "https://<your-account>.blob.core.windows.net"  # For prewarm Jobs
      blobContainer: "kaito-models"
      blobPrefix: "kaito-models"
      prewarmImage: ""  # Set if using prewarm Jobs
```

Install or upgrade KAITO:

```bash
helm upgrade --install kaito charts/kaito/workspace -f values.yaml
```

## Workspace Configuration

Add a `cache` section to your Workspace spec:

```yaml
apiVersion: kaito.sh/v1beta1
kind: Workspace
metadata:
  name: phi-4-cached
resource:
  instanceType: "Standard_NC24ads_A100_v4"
  labelSelector:
    matchLabels:
      apps: phi-4
inference:
  preset:
    name: "microsoft/phi-4"
cache:
  modelCache:
    provider: dacs
    mode: Opportunistic
  kvCache:
    provider: dacs
    mode: Opportunistic
```

### Cache Modes

Each concern supports three modes:

| Mode | Behavior |
|------|----------|
| `Required` | Block pod deployment until cache infrastructure is ready. Workspace enters a waiting state if cache is unavailable. |
| `Opportunistic` | Use cache if available; proceed without it if unavailable. This is the recommended default. |
| `Disabled` | Do not interact with the cache for this concern. |

You can configure each concern independently. For example, use `Required` for model weights (to guarantee fast startup) but `Opportunistic` for KV cache (which is an optimization, not a requirement):

```yaml
cache:
  modelCache:
    provider: dacs
    mode: Required
  kvCache:
    provider: dacs
    mode: Opportunistic
```

### Cleanup on Delete

`modelCache.cleanupOnDelete` is reserved for future use:

```yaml
cache:
  modelCache:
    provider: dacs
    mode: Opportunistic
    cleanupOnDelete: true   # reserved — currently has no effect
```

:::note Not yet implemented
Setting `cleanupOnDelete: true` currently has **no effect**. Cached model chunks are not explicitly evicted from the DACS cache servers when a workspace is deleted — they are reclaimed by the cache servers' own TTL/eviction policy. The field is accepted today so specs remain forward-compatible once invalidation-on-delete is implemented.
:::

## How It Works

### Model Weights Caching

When model weight caching is enabled, KAITO applies the following to inference pods:

1. **Pod label** `dacs.azure.com/inject: "true"` — Triggers the DACS mutating webhook
2. **ImageVolume** with the DACS client library (`libStorageDirect.so`)
3. **RunAI streamer env vars** — Enables the cache layer for model streaming from Azure Blob

The RunAI model streamer (used with `--load-format=runai_streamer`) fetches model weights from Azure Blob Storage (using `az://` model paths). With cache enabled, reads go through the DACS NVMe cache layer — cache hits are served from local NVMe, misses fall through to blob storage and are cached for subsequent requests.

### Cache Readiness Conditions

KAITO reports cache status through the `ModelCacheReady` and `KVCacheReady` workspace conditions:

```bash
kubectl get workspace <name> -o jsonpath='{.status.conditions[?(@.type=="ModelCacheReady")]}'
```

:::note What "ready" means
These conditions reflect **cache backend (cluster) readiness** — i.e. the DACS `Cache` CR reports its `Ready` condition as `True` and the cache servers are reachable. They do **not** guarantee that this workspace's specific model weights have already been warmed into the cache.

As a result, a workspace can show `ModelCacheReady=True` while the first model load is still a cache **miss**. This is expected and harmless: a miss transparently falls through to Azure Blob Storage and populates the cache, so subsequent loads are served from NVMe. In `Required` mode, deployment is unblocked once the cache backend is ready, not once the model is fully warmed.
:::

### KV Cache Sharing

When KV caching is enabled, KAITO injects:

1. **Pod label** `dacs.azure.com/inject: "true"` — For KV connector library access
2. **VLLM_KV_TRANSFER_CONFIG** env var — Configures vLLM's KV transfer mechanism with:
   - Full connector class path: `dacs_client.connectors.vllm_connector.DacsKVConnector`
   - Discovery endpoint, protocol, and TTL settings

### Auto-Discovery

If `discoveryEndpoint` is left empty in the Helm configuration, KAITO automatically reads the endpoint from the Cache CR's `status.discoveryEndpoint` field. This enables zero-configuration when DACS is installed in the same cluster.

### Prewarm

If a `prewarmImage` is configured, KAITO can create Kubernetes Jobs that download model weights from Hugging Face and upload them to blob storage before the inference pod starts. Prewarm Jobs also receive the `dacs.azure.com/inject` label so the webhook injects cache client libraries.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Inference Pod (labeled: dacs.azure.com/inject=true)   │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │ Main Container (vLLM)                                │ │
│  │ LD_PRELOAD=libStorageIntercept.so (injected by hook) │ │
│  │ reads /mnt/models/...                                │ │
│  └──────────────────────────┬──────────────────────────┘ │
│                             │                             │
└─────────────────────────────┼─────────────────────────────┘
                              │ (intercepted reads)
                              ▼
              ┌───────────────────────────────┐
              │  DACS Cache (NVMe nodes)    │
              │  Fast path: local/remote NVMe  │
              └───────────────┬───────────────┘
                              │ (cache miss)
                              ▼
              ┌───────────────────────────────┐
              │  Azure Blob Storage            │
              │  (source of truth)             │
              └───────────────────────────────┘
```

## AI Runway Interoperability

When used with [AI Runway](https://github.com/Azure/airunway), the cache configuration flows through the AI Runway controller:

1. AI Runway detects cache configuration in the InferencePool spec
2. AI Runway resolves provider-specific config and writes it to `status.cache.kvCache`
3. KAITO reads the resolved config and applies pod mutations

This allows AI Runway to manage fleet-level cache policies while KAITO handles per-pod injection.

## Troubleshooting

### Cache not ready

If your Workspace is stuck with condition `ModelWeightsCacheReady=False`:

1. Check the DACS Cache CR status:
   ```bash
   kubectl get caches -n dacs-cache-system -o yaml
   ```
2. Verify the cache server pods are running:
   ```bash
   kubectl get pods -n dacs-cache-system
   ```
3. If using `Opportunistic` mode, the workspace will proceed without cache. Switch to investigate why the cache infrastructure isn't ready.

### Model loading errors

If the inference pod fails to load the model:

1. Verify the injection label is on the pod:
   ```bash
   kubectl get pod <pod> --show-labels | grep dacs
   ```
2. Check that the webhook injected `LD_PRELOAD`:
   ```bash
   kubectl exec <pod> -- env | grep -E "LD_PRELOAD|KAITO_MODEL"
   ```
3. Confirm the blob storage endpoint is accessible with Workload Identity credentials.
4. Check DACS webhook logs for injection errors:
   ```bash
   kubectl logs -n dacs-cache-system -l app=dacs-webhook
   ```

### Feature gate not active

If `cache` spec is ignored, ensure the feature gate is enabled:

```bash
kubectl get deploy -n kaito-workspace -o yaml | grep feature-gates
```

The output should include `distributedCache=true`.

## Configuration Reference

| Helm Value | Description | Default |
|---|---|---|
| `featureGates.distributedCache` | Enable the distributed cache feature | `false` |
| `cache.providers.dacs.enabled` | Register the DACS provider | `false` |
| `cache.providers.dacs.discoveryEndpoint` | DACS cache server discovery URL (auto-discovered if empty) | `""` |
| `cache.providers.dacs.kvCacheEnabled` | Enable KV cache support | `true` |
| `cache.providers.dacs.kvConnectorProtocol` | KV connector transport (`tcp` or `rdma`) | `tcp` |
| `cache.providers.dacs.clientImage` | OCI image bundling the DACS client libraries (mounted into inference pods) | `""` |
| `cache.providers.dacs.blobEndpoint` | Azure Blob Storage endpoint (for prewarm Jobs) | `""` |
| `cache.providers.dacs.blobContainer` | Blob container for model storage | `kaito-models` |
| `cache.providers.dacs.blobPrefix` | Path prefix within the container | `kaito-models` |
| `cache.providers.dacs.prewarmImage` | Image for prewarm Jobs | `""` |

:::warning glibc compatibility
The DACS client libraries (`libStorageDirect.so`) bundled in `cache.providers.dacs.clientImage`
are built against **glibc 2.35** (`manylinux_2_35`). Any inference/base image that consumes the
cache **must be glibc 2.35 or newer (Ubuntu 22.04+)**. On an older base the runai model streamer
will fail to load the library at runtime.
:::
