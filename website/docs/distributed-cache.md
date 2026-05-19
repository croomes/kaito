---
title: Distributed Cache
---

This document explains how to enable distributed caching for model weights and KV cache in KAITO using a pluggable cache provider (currently Tachyon).

## Overview

The distributed cache integration accelerates model loading and enables KV cache sharing across inference pods. It supports two independent caching concerns:

- **Model Weights** — Caches model files in a distributed NVMe cache, avoiding repeated downloads from blob storage. Uses filesystem interception (LD_PRELOAD) so existing model loading code works unchanged.
- **KV Cache** — Shares attention KV cache between inference pods for prefill/decode disaggregation or prompt cache sharing.

Each concern is independently configurable with its own provider and mode.

## Prerequisites

1. **KAITO** installed with the `distributedCache` feature gate enabled
2. **Tachyon operator** deployed in the cluster (manages Cache CRs in `tachyon-cache-system` namespace)
3. **Azure Workload Identity** configured on the KAITO nodes (for DefaultAzureCredential access to blob storage)
4. **Azure Blob Storage** account with a container for model weights

## Installation

Enable the distributed cache feature gate and configure the Tachyon provider in your Helm values:

```yaml
featureGates:
  distributedCache: true

cache:
  providers:
    tachyon:
      enabled: true
      discoveryEndpoint: "http://cacheserver-discovery.tachyon-cache-system.svc.cluster.local:9065"
      modelWeightsEnabled: true
      kvCacheEnabled: true
      kvConnectorProtocol: "tcp"
      blobEndpoint: "https://<your-account>.blob.core.windows.net"
      blobContainer: "kaito-models"
      blobPrefix: "kaito-models"
      storageInterceptImage: "tachyontestacr.azurecr.io/cache-client-base:latest"
      storageInterceptLibPath: "/lib/libStorageIntercept.so"
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
  modelWeights:
    provider: tachyon
    mode: Opportunistic
  kvCache:
    provider: tachyon
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
  modelWeights:
    provider: tachyon
    mode: Required
  kvCache:
    provider: tachyon
    mode: Opportunistic
```

## How It Works

### Model Weights Caching

When model weight caching is enabled, KAITO injects the following into inference pods:

1. **Init container** (`tachyon-lib-loader`) — Copies `libStorageIntercept.so` from the cache client image into a shared volume
2. **LD_PRELOAD** — Loads the StorageIntercept library into the inference process
3. **SI_* env vars** — Configure the library to intercept filesystem reads at `/mnt/models` and serve them from blob storage via the distributed cache
4. **KAITO_MODEL_PATH** — Set to the local path where the model appears (e.g., `/mnt/models/kaito-models/microsoft/phi-4/main`)

The inference runtime (vLLM) reads from `KAITO_MODEL_PATH` as if it were a local filesystem. StorageIntercept transparently fetches data from the Tachyon cache (NVMe-backed) or falls through to blob storage on cache miss.

### KV Cache Sharing

When KV caching is enabled, KAITO injects `VLLM_KV_TRANSFER_CONFIG` into inference pods. This configures vLLM's built-in KV transfer mechanism to use TachyonKVConnector for cross-pod KV cache sharing.

### Prewarm

If a `prewarmImage` is configured, KAITO can create Kubernetes Jobs that download model weights from Hugging Face and upload them to blob storage before the inference pod starts. This ensures the cache is populated ahead of time.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Inference Pod                                       │
│                                                      │
│  ┌──────────────┐    ┌───────────────────────────┐  │
│  │ Init:        │    │ Main Container (vLLM)      │  │
│  │ copy .so to  │───▶│ LD_PRELOAD=libSI.so       │  │
│  │ shared vol   │    │ reads /mnt/models/...      │  │
│  └──────────────┘    └─────────────┬─────────────┘  │
│                                    │                 │
└────────────────────────────────────┼─────────────────┘
                                     │ (intercepted reads)
                                     ▼
                     ┌───────────────────────────────┐
                     │  Tachyon Cache (NVMe nodes)    │
                     │  Fast path: local/remote NVMe  │
                     └───────────────┬───────────────┘
                                     │ (cache miss)
                                     ▼
                     ┌───────────────────────────────┐
                     │  Azure Blob Storage            │
                     │  (source of truth)             │
                     └───────────────────────────────┘
```

## Troubleshooting

### Cache not ready

If your Workspace is stuck with condition `ModelWeightsCacheReady=False`:

1. Check the Tachyon Cache CR status:
   ```bash
   kubectl get caches -n tachyon-cache-system -o yaml
   ```
2. Verify the cache server pods are running:
   ```bash
   kubectl get pods -n tachyon-cache-system
   ```
3. If using `Opportunistic` mode, the workspace will proceed without cache. Switch to investigate why the cache infrastructure isn't ready.

### Model loading errors

If the inference pod fails to load the model:

1. Check the init container logs:
   ```bash
   kubectl logs <pod> -c tachyon-lib-loader
   ```
2. Verify `LD_PRELOAD` and `SI_*` env vars are set:
   ```bash
   kubectl exec <pod> -- env | grep -E "LD_PRELOAD|SI_|KAITO_MODEL"
   ```
3. Confirm the blob storage endpoint and container are accessible with Workload Identity credentials.

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
| `cache.providers.tachyon.enabled` | Register the Tachyon provider | `false` |
| `cache.providers.tachyon.discoveryEndpoint` | Tachyon cache server discovery URL | `http://cacheserver-discovery.tachyon-cache-system.svc.cluster.local:9065` |
| `cache.providers.tachyon.modelWeightsEnabled` | Enable model weight caching support | `true` |
| `cache.providers.tachyon.kvCacheEnabled` | Enable KV cache support | `true` |
| `cache.providers.tachyon.kvConnectorProtocol` | KV connector transport (`tcp` or `rdma`) | `tcp` |
| `cache.providers.tachyon.blobEndpoint` | Azure Blob Storage endpoint | `""` (required) |
| `cache.providers.tachyon.blobContainer` | Blob container for model storage | `kaito-models` |
| `cache.providers.tachyon.blobPrefix` | Path prefix within the container | `kaito-models` |
| `cache.providers.tachyon.storageInterceptImage` | Image containing `libStorageIntercept.so` | `tachyontestacr.azurecr.io/cache-client-base:latest` |
| `cache.providers.tachyon.storageInterceptLibPath` | Path to `.so` in the image | `/lib/libStorageIntercept.so` |
| `cache.providers.tachyon.prewarmImage` | Image for prewarm Jobs | `""` |
