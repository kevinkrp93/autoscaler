# Features

## Contents

<!-- toc -->
- [Per-VPA tuning (alpha)](#per-vpa-tuning-alpha)
- [Limits control](#limits-control)
- [Memory Value Humanization](#memory-value-humanization)
- [CPU Recommendation Rounding](#cpu-recommendation-rounding)
- [Memory Recommendation Rounding](#memory-recommendation-rounding)
- [In-Place Updates (<code>InPlaceOrRecreate</code>)](#in-place-updates-inplaceorrecreate)
  - [Usage](#usage)
  - [Behavior](#behavior)
  - [Skipping Disruption Budget for Non-Disruptive Updates](#skipping-disruption-budget-for-non-disruptive-updates)
    - [When Disruption Budgets Are Still Respected](#when-disruption-budgets-are-still-respected)
  - [Requirements:](#requirements)
  - [Limitations](#limitations)
  - [Fallback Behavior](#fallback-behavior)
  - [Monitoring](#monitoring)
<!-- /toc -->

## Per-VPA tuning (alpha)

This fork extends AEP-8026's `PerVPAConfig` feature with CPU histogram decay
and pod update sensitivity. Enable `--feature-gates=PerVPAConfig=true` on the
recommender, updater, and admission controller, and install the matching CRDs.
These fields are experimental fork extensions, not an approved upstream API.
One shared recommender and updater can process different settings concurrently.

```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: checkout
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: checkout
  updatePolicy:
    updateMode: Recreate
    podUpdateThreshold: 0.1
    inRecommendationBoundsEvictionLifetimeThreshold: 0s
  resourcePolicy:
    containerPolicies:
      - containerName: "*"
        cpuHistogramDecayHalfLife: 5m
```

`cpuHistogramDecayHalfLife` overrides `--cpu-histogram-decay-half-life` for a
named container, or all containers through `"*"`. Exact container policies take
precedence over the wildcard policy, following existing VPA policy resolution.
An omitted value uses the global flag. Half-life must be positive; it is a decay
rate, not a retention window. Memory decay is unchanged.

`podUpdateThreshold` overrides `--pod-update-threshold` for the whole VPA.
It must be nonnegative; zero is supported. It changes the existing
resource-difference threshold, not the updater's other eligibility rules, age
checks, out-of-range handling, disruption budgets, or minimum replicas.
Omitting the field retains the global setting.

`inRecommendationBoundsEvictionLifetimeThreshold` overrides
`--in-recommendation-bounds-eviction-lifetime-threshold` for the whole VPA.
It is a non-negative duration specifying the minimum pod age for updates
within the recommended range. `0s` removes that age wait, but the
resource-difference threshold and other update/disruption checks still apply.
Omitting the field retains the global flag, whose default remains 12 hours.
Out-of-range and quick-OOM handling are unchanged. This field is also gated
by `PerVPAConfig`; it does not change CPU histogram decay.

When a half-life changes, existing bucket weights and reference timestamps are
carried forward unchanged, as with restoring an existing VPA checkpoint under
a different global half-life. Historical sample weights are not recalculated,
and history is not reset. New samples use the selected half-life. Checkpoint
format and weight-preserving restoration remain unchanged. When histograms
are merged, their reference timestamps are aligned exactly, including when
a preserved timestamp is not a multiple of the newly selected half-life.
Alignment scales all buckets uniformly; it does not reconstruct or reweight
individual historical samples.

On Prometheus-backed startup, VPA policies are loaded before raw historical
samples are replayed. Those raw samples are weighted with their effective
per-container half-lives, unlike already-weighted checkpoint data.

With `PerVPAConfig` enabled, live aggregations are scoped to each VPA so changing
one VPA/container cannot modify another's history, even if they select the same
pods. The changed container's aggregate is copied under its new half-life and
its old private aggregate is removed; other containers retain their histories.
A global-default aggregate is also maintained to bootstrap newly added VPAs.
Sample routing checks the existing VPAs and fans out to matches; matching work
therefore scales with VPA count, while histogram copies and sample additions
grow with the number of matching VPAs. This includes copies of the unchanged
memory histogram. The feature gate defaults to false, retaining shared
aggregation without the additional per-sample VPA selector scans; admission
rejects the new fields when the gate is disabled.

A separate profile operator can translate named workload presets into these
VPA fields. Changing the workload's metadata annotation does not itself roll
out pods; the normal VPA update mechanism applies resulting recommendations.

To build only the three VPA component images, run from
`vertical-pod-autoscaler/` (not the repository root):

```sh
docker build -f pkg/recommender/Dockerfile -t vpa-recommender:profiles .
docker build -f pkg/updater/Dockerfile -t vpa-updater:profiles .
docker build -f pkg/admission-controller/Dockerfile -t vpa-admission-controller:profiles .
```

These builds do not build Cluster Autoscaler or other repository components.

Regression tests can be run without a cluster or container images:

```sh
go test ./pkg/recommender/... ./pkg/updater/... ./pkg/admission-controller/resource/vpa ./pkg/apis/autoscaling.k8s.io/v1
go test -race ./pkg/recommender/... ./pkg/updater/... ./pkg/admission-controller/resource/vpa ./pkg/apis/autoscaling.k8s.io/v1
```

Tests measure histogram bucket-weight ratios, actual CPU recommendations,
history preservation and isolation across profile changes, memory peak
replacement, checkpoint writes/restoration through fake API clients, and
raw-history replay. Updater tests use real resource-difference calculations
to cover threshold boundaries, pod totals, simultaneous profiles, and the
existing age, out-of-range, and quick-OOM rules. They do not replace
live-cluster validation of metrics collection, admission, or pod eviction.
An additional fake-cluster simulation runs two-minute and ten-minute profiles
concurrently through a shared recommender and updater selection path, persists
their recommendations, and changes only one VPA's decay and update threshold
to verify that the other VPA's settings, history, and recommendation stay intact.

## Limits control

When setting limits VPA will conform to
[resource policies](https://github.com/kubernetes/autoscaler/blob/vertical-pod-autoscaler-1.2.1/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1/types.go#L95-L103).
It will maintain limit to request ratio specified for all containers.

VPA will try to cap recommendations between min and max of
[limit ranges](https://kubernetes.io/docs/concepts/policy/limit-range/). If limit range conflicts
with VPA resource policy, VPA will follow VPA policy (and set values outside the limit
range).

To disable getting VPA recommendations for an individual container, set `mode` to `"Off"` in `containerPolicies`.

## Memory Value Humanization

> [!WARNING]
> DEPRECATED: This feature is deprecated as of VPA v1.5.0 and will be removed in a future version. Use `--round-memory-bytes` instead for memory recommendation formatting.

> [!NOTE]
> This feature was added in v1.3.0.


VPA can present memory recommendations in human-readable binary units (KiB, MiB, GiB, TiB) instead of raw bytes, making resource recommendations easier to understand. This feature is controlled by the `--humanize-memory` flag in the recommender component.

When enabled, memory values in recommendations will be:
- Converted to the most appropriate binary unit (KiB, MiB, GiB, or TiB)
- Displayed with up to 2 decimal places for precision
- Applied to target, lower bound, and upper bound recommendations

For example, instead of seeing a memory recommendation of `262144000` bytes, you would see `250.00Mi`.

Note: Due to the conversion to binary units and decimal place rounding, the humanized values may be slightly higher than the raw byte recommendations. For example, 1537 bytes would be shown as "1.50Ki" (1536 bytes). Consider this small difference when doing precise capacity planning.

To enable this feature, set the `--humanize-memory` flag to true when running the VPA recommender:
```bash
--humanize-memory=true
```

## CPU Recommendation Rounding

VPA can provide CPU recommendations rounded up to user-specified values, making it easier to interpret and configure resources. This feature is controlled by the `--round-cpu-millicores` flag in the recommender component.

When enabled, CPU recommendations will be:
- Rounded up to the nearest multiple of the specified millicore value
- Applied to target, lower bound, and upper bound recommendations

For example, with `--round-cpu-millicores=50`, a CPU recommendation of `79m` would be rounded up to `100m`, and a recommendation of `34m` would be rounded up to `50m`.

To enable this feature, set the --round-cpu-millicores flag when running the VPA recommender:

```bash
--round-cpu-millicores=50
```

## Memory Recommendation Rounding

VPA can provide Memory recommendations rounded up to user-specified values, making it easier to interpret and configure resources. This feature is controlled by the `--round-memory-bytes` flag in the recommender component.

When enabled, Memory recommendations will be:
- Rounded up to the nearest multiple of the specified bytes value
- Applied to target, lower bound, and upper bound recommendations

For example, with `--round-memory-bytes=134217728`, a memory recommendation of `200Mi` would be rounded up to `256Mi`, and a recommendation of `80Mi` would be rounded up to `128Mi`.

To enable this feature, set the `--round-memory-bytes` flag when running the VPA recommender:

```bash
--round-memory-bytes=134217728
```

## In-Place Updates (`InPlaceOrRecreate`)

> [!NOTE]
> FEATURE STATE:
> - VPA v1.4.0 [alpha]
> - VPA v1.5.0 [beta]
> - VPA v1.6.0 [ga]

VPA supports in-place updates to reduce disruption when applying resource recommendations. This feature leverages Kubernetes' in-place update capabilities (which is in beta as of Kubernetes 1.33) to modify container resources without requiring pod recreation.
For more information, see [AEP-4016: Support for in place updates in VPA](https://github.com/kubernetes/autoscaler/tree/master/vertical-pod-autoscaler/enhancements/4016-in-place-updates-support)

### Usage

To use in-place updates, set the VPA's `updateMode` to `InPlaceOrRecreate`:
```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: my-vpa
spec:
  updatePolicy:
    updateMode: "InPlaceOrRecreate"
```

### Behavior

When using `InPlaceOrRecreate` mode, VPA will first attempt to apply updates in-place, if in-place update fails, VPA will fall back to pod recreation.
Updates are attempted when:
* Container requests are outside the recommended bounds
* Quick OOM occurs
* For long-running pods (>12h), when recommendations differ significantly (>10%)

Important Notes

* Disruption Possibility: While in-place updates aim to minimize disruption, they cannot guarantee zero disruption as the container runtime is responsible for the actual resize operation.

* Memory Limit Downscaling: In the beta version, memory limit downscaling is not supported for pods with resizePolicy: PreferNoRestart. In such cases, VPA will fall back to pod recreation.

### Skipping Disruption Budget for Non-Disruptive Updates

By default, VPA respects disruption budgets (eviction tolerance, min replicas) even for in-place updates. However, when an in-place update doesn't require container restarts, it's truly non-disruptive and these checks may be unnecessarily restrictive.

The `--in-place-skip-disruption-budget` flag (default: `false`) allows VPA to skip disruption budget checks for in-place updates when all containers in the pod have `NotRequired` resize policy for both CPU and memory or no resize policy is defined.

#### When Disruption Budgets Are Still Respected

Even with this flag enabled, disruption budgets are enforced when:
* Any container has `RestartContainer` resize policy for any resource
* The update would result in pod eviction/recreation (fallback scenarios)


### Requirements:

* Kubernetes 1.33+ with `InPlacePodVerticalScaling` feature gate enabled
* VPA version 1.4.0+ with `InPlaceOrRecreate` feature gate enabled

### Limitations

* All containers in a pod are updated together (partial updates not supported)
* Memory downscaling requires careful consideration to prevent OOMs
* Updates still respect VPA's standard update conditions and timing restrictions
* In-place updates will fail if they would result in a change to the pod's QoS class

### Fallback Behavior

VPA will fall back to pod recreation in the following scenarios:

* In-place update is [infeasible](https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/1287-in-place-update-pod-resources/README.md#resize-status) (node resources, etc.)
* Update is [deferred](https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/1287-in-place-update-pod-resources/README.md#resize-status) for more than 5 minutes
* Update is in progress for more than 1 hour
* [Pod QoS](https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/) class would change due to the update
* Memory limit downscaling is required with [PreferNoRestart policy](https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/1287-in-place-update-pod-resources/README.md#container-resize-policy)

### Monitoring

VPA provides metrics to track in-place update operations:

* `vpa_updater_in_place_updatable_pods_total`: Number of pods matching in-place update criteria
* `vpa_updater_in_place_updated_pods_total`: Number of pods successfully updated in-place
* `vpa_updater_vpas_with_in_place_updatable_pods_total`: Number of VPAs with pods eligible for in-place updates
* `vpa_updater_vpas_with_in_place_updated_pods_total`: Number of VPAs with successfully in-place updated pods
* `vpa_updater_failed_in_place_update_attempts_total`: Number of failed attempts to update pods in-place.
