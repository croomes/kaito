---
title: DACS Model Cache Pre-Warming
authors:
  - "@croomes"
  - "@hasethuraman"
reviewers:
  - "@Fei-Guo"
  - "@chewong"
creation-date: 2026-08-31
last-updated: 2026-08-31
status: provisional
see-also:
  - "/docs/proposals/20260609-distributed-cache-integration.md"
  - "/docs/proposals/20260520-model-mirror.md"
---

# DACS Model Cache Pre-Warming

## Table of Contents

- [DACS Model Cache Pre-Warming](#dacs-model-cache-pre-warming)
  - [Table of Contents](#table-of-contents)
  - [Glossary](#glossary)
  - [Summary](#summary)
  - [Motivation](#motivation)
    - [Measured Performance](#measured-performance)
    - [Goals](#goals)
    - [Non-Goals/Future Work](#non-goalsfuture-work)
  - [Proposal](#proposal)
    - [DACS Cache Ring Transitions](#dacs-cache-ring-transitions)
    - [InferenceSet Sidecar Warm](#inferenceset-sidecar-warm)
    - [Ring Growth](#ring-growth)
    - [New ModelMirror on an Existing Ring](#new-modelmirror-on-an-existing-ring)
    - [Large-Model Warming](#large-model-warming)
    - [Execution and Failure Semantics](#execution-and-failure-semantics)
    - [Integration Boundary](#integration-boundary)
    - [Risks and Mitigations](#risks-and-mitigations)
  - [Alternatives](#alternatives)
  - [Upgrade Strategy](#upgrade-strategy)
  - [Additional Details](#additional-details)
    - [Test Plan](#test-plan)
  - [Implementation History](#implementation-history)

## Glossary

- **DACS**: The distributed model cache provider described by this proposal.
- **Cache Ring**: The set of ready DACS cache-server instances that jointly serve cached model data. Ring size is the number of ready members.
- **Cache Warming**: Reading a model through the DACS-aware model streamer before the main GPU model load so later loads can use cached data.

## Summary

This proposal defines DACS-specific model cache pre-warming for KAITO. The generic distributed-cache framework remains responsible for provider registration, lifecycle hooks, readiness, and pod mutations. The DACS provider uses those hooks to populate an empty cache ring with CPU resources before the inference engine loads the model onto a GPU.

The warming mechanism depends on how the model enters the DACS-backed workload. For a new InferenceSet that is not backed by ModelMirror, only StatefulSet ordinal zero runs a model-warmer sidecar, including when that InferenceSet creates the first ring member. Increasing an existing ring does not start another warmer. When a new ModelMirror is created while a ring already exists, a CPU-only Job warms the new model while ModelMirror synchronization is in progress. If that Job cannot warm the cache because CPU capacity is unavailable, ordinal zero of the first consuming InferenceSet performs the pull instead.

## Motivation

On an empty DACS cache, the inference pod performs a read-through load: each miss is downloaded from remote storage and uploaded to the cache. Measurements show that this generic cold-cache path is approximately 50% slower than direct run:ai streaming. Later replicas benefit from the populated cache, but the first GPU remains allocated and idle while it pays both the remote-read and cache-write cost.

Specialized optimizations reduce the gap, but still show more than 20% degradation and depend on tensor-model-specific knowledge of model layout and access patterns. DACS intentionally caches model-agnostic files and byte ranges. Adding tensor-aware behavior to that layer would require format-specific assumptions and tuning that do not fit its generic cache contract.

Until a generic DACS cold-cache optimization closes this gap across model sizes and node configurations, pre-warming moves cache population out of the GPU model-loading critical path. The model is streamed with CPU resources first, allowing the subsequent GPU load to read primarily from DACS. Initial measurements keep the resulting overhead below 10% relative to direct run:ai streaming while reducing GPU idle time.

### Measured Performance

The following results use Qwen3-Coder-30B-A3B-Instruct (56.9 GiB) with the run:ai model streamer and vLLM v0.22.1. "Time until streaming starts" is measured from the first container log to TRITON MoE selection, immediately before safetensor streaming begins. The cold-cache values are averages from three fresh InferenceSet creations, and the pre-warmed values are averages from ten fresh InferenceSet creations.

| # | Test path | Time until streaming starts | Stream duration | Stream throughput | First log to stream completion | Difference from direct run:ai |
|---:|-----------|----------------------------:|----------------:|------------------:|-------------------------------:|--------------------------------:|
| 1 | Direct run:ai | 35.00 s | 31.63 s | 1.8 GiB/s | 66.63 s | Baseline |
| 2 | DACS cold cache: vLLM read-through | 38.59 s | 64.11 s | 908.5 MiB/s | 102.70 s | +36.07 s (+54.1%) |
| 3 | DACS pre-warm: slim run:ai image | N/A (CPU sidecar) | 48.70 s | 1.17 GiB/s | N/A (runs before GPU streaming) | Outside the main vLLM comparison |
| 4 | Main vLLM pod after DACS pre-warm | 52.64 s | 17.88 s | 3.23 GiB/s | 70.51 s | **+3.88 s (+5.8%)** |

For the pre-warmed path, the end-to-end model-streaming overhead is:

```text
direct run:ai:  35.00 s startup + 31.63 s streaming = 66.63 s
pre-warmed:     52.64 s startup + 17.88 s streaming = 70.51 s
overhead:       70.51 s - 66.63 s = 3.88 s
degradation:    3.88 s / 66.63 s = 5.8%
```

The DACS pre-warmed path therefore remains within the 10% degradation target while moving cold-cache population to the CPU sidecar. In contrast, placing cache population on the main vLLM read path increases the same interval by 54.1%.

### Goals

- Populate a cold DACS ring before the main GPU model load.
- Use available CPU capacity instead of GPU resources for warming.
- Warm every new InferenceSet that is not backed by ModelMirror from StatefulSet ordinal zero.
- Bound InferenceSet cache-population work to one StatefulSet pod.
- Avoid unnecessary warming when an existing DACS ring gains members.
- Warm a new ModelMirror into an existing ring while model synchronization is in progress.
- Fall back to the first consuming InferenceSet's ordinal-zero sidecar when a ModelMirror warming Job cannot obtain CPU capacity.
- Allow very large models to be partitioned across multiple CPU warming Jobs without changing the inference runtime.
- Preserve read-through loading as the fallback when opportunistic warming fails.

### Non-Goals/Future Work

- Defining pre-warming behavior for cache providers other than DACS.
- Adding tensor-format-specific behavior to DACS.
- Changing DACS eviction or cache-ring rebalancing policies.
- Eliminating the existing DACS read-through fallback.

## Proposal

The DACS provider selects its warming mechanism from the cache-ring transition and the workload event.

### DACS Cache Ring Transitions

| Ring transition | Workload event | DACS warming mechanism | Rationale |
|-----------------|----------------|------------------------|-----------|
| `0 → 1` | New InferenceSet not backed by ModelMirror starts | Sidecar on StatefulSet ordinal zero | The newly provisioned inference node has CPU capacity available before GPU model loading. |
| Ring remains `N`, where `N ≥ 1` | New InferenceSet not backed by ModelMirror starts | Sidecar on the new StatefulSet's ordinal zero | The new model may be absent or evicted; one pod populates DACS before the remaining replicas load it. |
| `N → N+1`, where `N ≥ 1` | Existing DACS ring grows | No additional warmer | The cache backend owns placement and rebalancing across ring members. |
| Ring remains `N`, where `N ≥ 1` | ModelMirror is created | CPU-only Kubernetes Job; ordinal-zero sidecar fallback | CPU provisioning can overlap the longer ModelMirror operation. If CPU capacity is unavailable for the Job, the first consuming InferenceSet still populates DACS from pod zero. |

### InferenceSet Sidecar Warm

For every new InferenceSet that is not backed by ModelMirror, the DACS provider adds a model-warmer sidecar through the distributed-cache framework's pod-mutation mechanism. This applies both when the InferenceSet creates the first DACS ring member and when it uses an existing ring. A ModelMirror-backed InferenceSet also uses its ordinal-zero sidecar when the ModelMirror warming Job was unable to run because CPU capacity was unavailable.

- Every StatefulSet pod receives the same template, so the warmer determines the pod ordinal from the Downward API.
- Only ordinal zero streams the model. Warmers in higher ordinals remain idle.
- The warmer waits until the DACS discovery endpoint accepts connections before opening the model with the DACS-aware streamer. This prevents a download from bypassing a cache server that is still starting.
- The warmer streams tensors into CPU memory and releases them incrementally so the full model does not remain resident in the sidecar.
- The warmer remains alive after completion so sidecar termination does not affect the inference pod lifecycle.

The ordinal-zero stream runs for each new qualifying InferenceSet and may repeat when its StatefulSet is recreated. Re-pulling is safe because DACS eviction may have removed some or all cached data: existing chunks remain cache hits and missing chunks are repopulated. Restricting the stream to ordinal zero bounds origin and cache-population work to one pod, prevents all replicas from simultaneously reading the origin, and amortizes that one-pod cost across later replicas.

### Ring Growth

When a DACS ring grows from `N` to `N+1`, where `N ≥ 1`, ring growth alone does not trigger a warmer. DACS is responsible for placement or rebalancing when another cache server joins. This does not suppress the ordinal-zero sidecar when a separate, non-ModelMirror InferenceSet is created.

Treating ring growth as a warming trigger would add avoidable origin traffic and could cause every newly scheduled inference replica to repeat the same full-model stream.

### New ModelMirror on an Existing Ring

Creating a ModelMirror means a new model is being added and is not yet present in blob storage. The DACS integration does not perform a separate cache-presence check.

- ModelMirror reconciliation creates a CPU-only warming Job associated with the new ModelMirror.
- The Job can run on any suitable CPU node and does not request a GPU.
- The Job is created while ModelMirror synchronization is in progress. CPU-node provisioning therefore overlaps the longer mirror operation.
- The Job waits until the mirrored model is streamable, then reads it through the DACS-aware streamer into the existing ring.
- If the Job cannot obtain CPU capacity and therefore does not warm DACS, ordinal zero of the first InferenceSet that consumes the ModelMirror performs the pull through its sidecar. Higher ordinals remain idle, preserving the one-pod bound on origin traffic.

### Large-Model Warming

For a model whose sidecar warm takes longer than vLLM initialization, the sidecar and the main model streamer may run concurrently. Full cache population is not a prerequisite for the main streamer to begin:

1. The ordinal-zero sidecar starts first and streams model blobs in the same traversal order as the run:ai model streamer.
2. When the main vLLM streamer starts, it reads the blobs that the sidecar has already populated from DACS.
3. The sidecar continues warming later blobs ahead of the main streamer and completes its stream first.
4. If the main streamer reaches data that has not yet been warmed, the normal DACS read-through path preserves correctness.

This pipeline allows the main streamer to benefit from the populated portion of the cache without waiting for the entire larger model to be warmed.

For very large models, the same pre-warming abstraction can use multiple CPU warming Jobs. Each tensor file is stored as a separate blob, so the DACS provider can divide the model's blob list into disjoint partitions and assign each partition to a different Job pod. The Jobs populate the existing cache ring concurrently while avoiding duplicate reads of the same blob.

This parallel form applies in both DACS lifecycle paths:

- During ModelMirror, the warming Jobs are created while mirroring is in progress. CPU provisioning overlaps model synchronization, and each Job starts its assigned blobs as they become streamable.
- When a DACS ring already exists, multiple warming Jobs can populate different blobs before or alongside the first consuming InferenceSet. If the Jobs cannot obtain CPU capacity, the ordinal-zero sidecar remains the fallback.

Job parallelism is bounded by provider configuration and available CPU capacity. Increasing the number of Jobs changes only how the model's blobs are partitioned for warming; it does not change DACS cache semantics.

### Execution and Failure Semantics

The sidecar reports progress through container logs and events. ModelMirror warming uses the Kubernetes Job lifecycle.

- In `Required` mode, inference model loading waits for the DACS ring to become ready. A bounded timeout and condition message prevent indefinite blocking.
- In `Opportunistic` mode, inference may proceed if warming fails or times out. The main streamer uses the normal DACS read-through path.
- Sidecar and Job failures use bounded retries. CPU unavailability for the ModelMirror Job falls back to ordinal-zero sidecar warming when the first consuming InferenceSet starts.
- If eviction removes model data, a later ordinal-zero stream safely repopulates the missing content.

### Integration Boundary

The generic distributed-cache framework does not select these warming transitions. It exposes provider lifecycle and pod-mutation hooks. The DACS provider owns:

- Detecting the DACS ring state needed by its reconciliation.
- Injecting and configuring the model-warmer sidecar.
- Adding the sidecar to each new InferenceSet that is not backed by ModelMirror and restricting streaming to StatefulSet ordinal zero.
- Activating the ordinal-zero sidecar for a ModelMirror-backed InferenceSet when its CPU warming Job could not populate DACS.
- Waiting for the DACS discovery endpoint before streaming.
- Creating the CPU warming Job for a new ModelMirror on an existing ring.
- Partitioning very large models into disjoint blob sets for parallel CPU warming Jobs.
- Reporting DACS-specific warming status, failures, and performance metrics.

No Tachyon lock-retry behavior is used to coordinate cache readiness. Readiness is handled by the DACS warmer before it invokes the model streamer.

### Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| DACS starts after the warmer | The model is downloaded without populating DACS and the GPU pod repeats the remote load | Wait for the DACS discovery endpoint before streaming |
| Every StatefulSet replica warms the model | Duplicate origin traffic and unnecessary CPU and memory use | Permit streaming only from ordinal zero |
| Cold-cache read-through reduces throughput | The first GPU remains idle longer | Pre-warm with CPU resources before GPU model loading |
| Warming fails in opportunistic mode | The main model load remains cold | Preserve the normal DACS read-through fallback and report the failure |
| Cache eviction removes model data | A later load encounters misses | Allow ordinal zero to re-pull and repopulate missing content |
| CPU capacity for a ModelMirror warming Job is delayed or remains unavailable | Warming begins later or the model is not pre-warmed before inference starts | Create the Job during mirroring so CPU provisioning overlaps model synchronization; if capacity remains unavailable, fall back to the first consuming InferenceSet's ordinal-zero sidecar |
| Parallel warming Jobs process overlapping blobs | Duplicate origin reads and cache-population work | Partition the blob list into disjoint assignments and bound Job parallelism |

## Alternatives

**Use read-through warming only:** This requires the first GPU model load to populate DACS and retains the approximately 50% cold-cache degradation.

**Apply tensor-model-specific optimizations:** These still leave more than 20% degradation and do not fit DACS's model-agnostic file and byte-range cache contract.

**Warm from every StatefulSet replica:** This may reduce an individual replica's wait but multiplies origin traffic and creates a thundering herd. Ordinal-zero-only warming provides the same shared-cache benefit with bounded work.

## Upgrade Strategy

DACS pre-warming is enabled through DACS provider configuration. Workloads that do not select DACS retain the generic distributed-cache behavior. Disabling DACS pre-warming preserves the existing DACS read-through path.

## Additional Details

### Test Plan

- Verify only StatefulSet ordinal zero streams during the first ring creation.
- Verify a new InferenceSet that is not backed by ModelMirror streams from its ordinal-zero sidecar when a DACS ring already exists.
- Verify the warmer waits until the DACS discovery endpoint is reachable.
- Verify a successful warm causes the main model load to use DACS without origin reads.
- Verify increasing the ring above one member does not start another warmer.
- Verify ModelMirror creation on an existing ring creates a CPU-only warming Job.
- Verify the first consuming InferenceSet's ordinal-zero sidecar pulls the model when the ModelMirror warming Job cannot obtain CPU capacity.
- Verify the main vLLM streamer reads already populated blobs from DACS while a larger-model sidecar continues warming later blobs.
- Verify very-large-model Jobs receive disjoint blob partitions and populate them concurrently for both ModelMirror and an existing ring.
- Verify opportunistic mode falls back to read-through loading when warming fails.
- Verify recreation after cache eviction safely repopulates missing content from ordinal zero.
- Compare direct run:ai, DACS cold read-through, and DACS pre-warmed model-loading durations.

## Implementation History

- [x] 08/31/2026: Initial DACS pre-warming proposal drafted.
