/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package model

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/features"
)

func TestProfileHistogramsWeightIdenticalPodSamplesDifferently(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	cluster := NewClusterState(testGcPeriod)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	states := make([]*AggregateContainerState, 0, 2)
	for _, profile := range []struct {
		name     string
		halfLife time.Duration
	}{
		{"aggressive", 5 * time.Minute},
		{"conservative", 24 * time.Hour},
	} {
		podID := PodID{Namespace: "apps", PodName: profile.name}
		podLabels := labels.Set{"app": profile.name}
		object := newCPUProfileVPA(podID.Namespace, profile.name, "*", profile.halfLife)
		require.NoError(t, cluster.AddOrUpdateVpa(object, podLabels.AsSelector()))
		cluster.AddOrUpdatePod(podID, podLabels, corev1.PodRunning)
		id := ContainerID{PodID: podID, ContainerName: "app"}
		require.NoError(t, cluster.AddOrUpdateContainer(id, nil))
		addProfileSample(t, cluster, id, start, ResourceCPU, CPUAmountFromCores(1))
		addProfileSample(t, cluster, id, start, ResourceMemory, MemoryAmountFromBytes(1<<30))
		addProfileSample(t, cluster, id, start.Add(20*time.Minute), ResourceCPU, CPUAmountFromCores(0.1))
		addProfileSample(t, cluster, id, start.Add(20*time.Minute), ResourceMemory, MemoryAmountFromBytes(2<<30))
		vpa := cluster.vpas[VpaID{Namespace: object.Namespace, VpaName: object.Name}]
		state := vpa.AggregateStateByContainerName()["app"]
		require.NotNil(t, state)
		states = append(states, state)
		assert.Equal(t, 2, state.TotalSamplesCount)
		assert.Equal(t, profile.halfLife, state.CPUHistogramDecayHalfLife)
		cpu, err := state.AggregateCPUUsage.SaveToChekpoint()
		require.NoError(t, err)
		assertProfileBucketRatio(t, cpu, 1, 0.1, math.Exp2(-float64(20*time.Minute)/float64(profile.halfLife)))
	}
	options := GetAggregationsConfig().CPUHistogramOptions
	assert.Equal(t, options.GetBucketStart(options.FindBucket(0.1)+1), states[0].AggregateCPUUsage.Percentile(0.9),
		"short decay should select the recent low CPU sample's bucket")
	assert.Equal(t, options.GetBucketStart(options.FindBucket(1)+1), states[1].AggregateCPUUsage.Percentile(0.9),
		"long decay should retain the old high CPU sample's bucket")
	memory0, err := states[0].AggregateMemoryPeaks.SaveToChekpoint()
	require.NoError(t, err)
	memory1, err := states[1].AggregateMemoryPeaks.SaveToChekpoint()
	require.NoError(t, err)
	assert.Equal(t, memory0, memory1, "CPU profile selection must not change memory weighting")
}

func TestLiveProfileChangePreservesWeightsAndAppliesNewDecay(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	cluster := NewClusterState(testGcPeriod)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	podID := PodID{Namespace: "apps", PodName: "pod"}
	id := ContainerID{PodID: podID, ContainerName: "app"}
	podLabels := labels.Set{"app": "shared"}
	object := newCPUProfileVPA("apps", "changing", "*", 5*time.Minute)
	other := newCPUProfileVPA("apps", "unchanged", "*", 24*time.Hour)
	for _, vpa := range []*vpa_types.VerticalPodAutoscaler{object, other} {
		require.NoError(t, cluster.AddOrUpdateVpa(vpa, podLabels.AsSelector()))
	}
	cluster.AddOrUpdatePod(podID, podLabels, corev1.PodRunning)
	require.NoError(t, cluster.AddOrUpdateContainer(id, nil))
	addProfileSample(t, cluster, id, start, ResourceCPU, CPUAmountFromCores(1))
	addProfileSample(t, cluster, id, start.Add(20*time.Minute), ResourceCPU, CPUAmountFromCores(0.1))
	addProfileSample(t, cluster, id, start, ResourceMemory, MemoryAmountFromBytes(1<<30))
	vpaID := VpaID{Namespace: "apps", VpaName: object.Name}
	otherID := VpaID{Namespace: "apps", VpaName: other.Name}
	before, err := cluster.vpas[vpaID].AggregateStateByContainerName()["app"].SaveToCheckpoint()
	require.NoError(t, err)
	otherBefore, err := cluster.vpas[otherID].AggregateStateByContainerName()["app"].SaveToCheckpoint()
	require.NoError(t, err)
	object.Spec.ResourcePolicy.ContainerPolicies[0].CPUHistogramDecayHalfLife.Duration = 10 * time.Minute
	require.NoError(t, cluster.AddOrUpdateVpa(object, podLabels.AsSelector()))
	after, err := cluster.vpas[vpaID].AggregateStateByContainerName()["app"].SaveToCheckpoint()
	require.NoError(t, err)
	assert.Equal(t, before.CPUHistogram, after.CPUHistogram)
	assert.Equal(t, before.MemoryHistogram, after.MemoryHistogram)
	otherAfter, err := cluster.vpas[otherID].AggregateStateByContainerName()["app"].SaveToCheckpoint()
	require.NoError(t, err)
	assert.Equal(t, otherBefore.CPUHistogram, otherAfter.CPUHistogram)
	assert.Equal(t, otherBefore.MemoryHistogram, otherAfter.MemoryHistogram)

	next := start.Add(25 * time.Minute)
	addProfileSample(t, cluster, id, next, ResourceCPU, CPUAmountFromCores(0.4))
	current := cluster.vpas[vpaID].AggregateStateByContainerName()["app"]
	cpu, err := current.AggregateCPUUsage.SaveToChekpoint()
	require.NoError(t, err)
	expectedAddedWeight := minSampleWeight * math.Exp2(float64(next.Sub(before.CPUHistogram.ReferenceTimestamp.Time))/float64(10*time.Minute))
	assert.InDelta(t, before.CPUHistogram.TotalWeight+expectedAddedWeight, cpu.TotalWeight, 1e-10,
		"the new sample must use the new decay, not the global or previous half-life")
	assertProfileBucketRatio(t, cpu, 1, 0.1, 1.0/16.0)
	assert.Equal(t, 3, current.TotalSamplesCount)
	memory, err := current.AggregateMemoryPeaks.SaveToChekpoint()
	require.NoError(t, err)
	assert.Equal(t, before.MemoryHistogram, *memory)

	// Returning through an existing profile must retain this VPA's current weights.
	for _, halfLife := range []time.Duration{24 * time.Hour, 5 * time.Minute} {
		previous, err := cluster.vpas[vpaID].AggregateStateByContainerName()["app"].SaveToCheckpoint()
		require.NoError(t, err)
		object.Spec.ResourcePolicy.ContainerPolicies[0].CPUHistogramDecayHalfLife.Duration = halfLife
		require.NoError(t, cluster.AddOrUpdateVpa(object, podLabels.AsSelector()))
		current := cluster.vpas[vpaID].AggregateStateByContainerName()["app"]
		checkpoint, err := current.SaveToCheckpoint()
		require.NoError(t, err)
		assert.Equal(t, previous.CPUHistogram, checkpoint.CPUHistogram)
		assert.Equal(t, 3, current.TotalSamplesCount)
		assert.Equal(t, halfLife, current.CPUHistogramDecayHalfLife)
	}
	beforeRemoval, err := cluster.vpas[vpaID].AggregateStateByContainerName()["app"].SaveToCheckpoint()
	require.NoError(t, err)
	object.Spec.ResourcePolicy = nil
	require.NoError(t, cluster.AddOrUpdateVpa(object, podLabels.AsSelector()))
	withoutOverride := cluster.vpas[vpaID].AggregateStateByContainerName()["app"]
	assert.Equal(t, GetAggregationsConfig().CPUHistogramDecayHalfLife, withoutOverride.CPUHistogramDecayHalfLife)
	afterRemoval, err := withoutOverride.SaveToCheckpoint()
	require.NoError(t, err)
	assert.Equal(t, beforeRemoval.CPUHistogram, afterRemoval.CPUHistogram, "removing an override must preserve history too")
	privateStates := 0
	for key := range cluster.aggregateStateMap {
		if key.OwnerVpaID() == vpaID {
			privateStates++
		}
	}
	assert.Equal(t, 1, privateStates, "old profile histories must not accumulate or be resurrected")
	require.NoError(t, cluster.DeleteVpa(vpaID))
	for key := range cluster.aggregateStateMap {
		assert.NotEqual(t, vpaID, key.OwnerVpaID(), "deletion must remove the VPA's private histograms")
	}
	assert.Equal(t, 3, cluster.vpas[otherID].AggregateStateByContainerName()["app"].TotalSamplesCount)
}

func TestContainerPoliciesRouteSamplesAtTheirEffectiveHalfLives(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	cluster := NewClusterState(testGcPeriod)
	podID := PodID{Namespace: "apps", PodName: "pod"}
	podLabels := labels.Set{"app": "example"}
	object := newCPUProfileVPA("apps", "profile", "*", time.Hour)
	object.Spec.ResourcePolicy.ContainerPolicies = append(object.Spec.ResourcePolicy.ContainerPolicies,
		vpa_types.ContainerResourcePolicy{ContainerName: "app", CPUHistogramDecayHalfLife: &metav1.Duration{Duration: 5 * time.Minute}},
		vpa_types.ContainerResourcePolicy{ContainerName: "sidecar"})
	require.NoError(t, cluster.AddOrUpdateVpa(object, podLabels.AsSelector()))
	cluster.AddOrUpdatePod(podID, podLabels, corev1.PodRunning)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, container := range []struct {
		name     string
		halfLife time.Duration
	}{
		{"app", 5 * time.Minute},
		{"worker", time.Hour},
		{"sidecar", GetAggregationsConfig().CPUHistogramDecayHalfLife},
	} {
		id := ContainerID{PodID: podID, ContainerName: container.name}
		require.NoError(t, cluster.AddOrUpdateContainer(id, nil))
		addProfileSample(t, cluster, id, start, ResourceCPU, CPUAmountFromCores(1))
		addProfileSample(t, cluster, id, start.Add(20*time.Minute), ResourceCPU, CPUAmountFromCores(0.1))
		state := cluster.vpas[VpaID{Namespace: "apps", VpaName: object.Name}].AggregateStateByContainerName()[container.name]
		require.NotNil(t, state)
		assert.Equal(t, container.halfLife, state.CPUHistogramDecayHalfLife)
		checkpoint, err := state.AggregateCPUUsage.SaveToChekpoint()
		require.NoError(t, err)
		assertProfileBucketRatio(t, checkpoint, 1, 0.1, math.Exp2(-float64(20*time.Minute)/float64(container.halfLife)))
	}
}

func TestProfileAggregationCombinesPodsWithoutMixingNamespaces(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	cluster := NewClusterState(testGcPeriod)
	object := newCPUProfileVPA("apps", "profile", "*", 5*time.Minute)
	require.NoError(t, cluster.AddOrUpdateVpa(object, labels.SelectorFromSet(labels.Set{"app": "example"})))
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, namespace := range []string{"apps", "apps", "unrelated"} {
		podID := PodID{Namespace: namespace, PodName: fmt.Sprintf("pod-%d", i)}
		cluster.AddOrUpdatePod(podID, labels.Set{"app": "example", "instance": fmt.Sprint(i)}, corev1.PodRunning)
		id := ContainerID{PodID: podID, ContainerName: "app"}
		require.NoError(t, cluster.AddOrUpdateContainer(id, nil))
		addProfileSample(t, cluster, id, start, ResourceCPU, CPUAmountFromCores(1))
		addProfileSample(t, cluster, id, start.Add(20*time.Minute), ResourceCPU, CPUAmountFromCores(0.1))
	}
	state := cluster.vpas[VpaID{Namespace: "apps", VpaName: object.Name}].AggregateStateByContainerName()["app"]
	assert.Equal(t, 4, state.TotalSamplesCount, "only the two matching pods in this namespace should contribute")
	checkpoint, err := state.AggregateCPUUsage.SaveToChekpoint()
	require.NoError(t, err)
	assertProfileBucketRatio(t, checkpoint, 1, 0.1, 1.0/16.0)
}

func TestHalfLifeChangeMergesPodHistoriesWithNonAlignedReferences(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	cluster := NewClusterState(testGcPeriod)
	object := newCPUProfileVPA("apps", "profile", "*", 5*time.Minute)
	require.NoError(t, cluster.AddOrUpdateVpa(object, labels.SelectorFromSet(labels.Set{"app": "example"})))
	start := time.Date(2026, 1, 1, 6, 0, 0, 0, time.UTC)
	for i, usage := range []float64{1, 4} {
		podID := PodID{Namespace: "apps", PodName: fmt.Sprintf("pod-%d", i)}
		cluster.AddOrUpdatePod(podID, labels.Set{"app": "example", "instance": fmt.Sprint(i)}, corev1.PodRunning)
		id := ContainerID{PodID: podID, ContainerName: "app"}
		require.NoError(t, cluster.AddOrUpdateContainer(id, nil))
		addProfileSample(t, cluster, id, start.Add(time.Duration(i)*6*time.Hour), ResourceCPU, CPUAmountFromCores(usage))
	}
	object.Spec.ResourcePolicy.ContainerPolicies[0].CPUHistogramDecayHalfLife.Duration = 24 * time.Hour
	require.NoError(t, cluster.AddOrUpdateVpa(object, labels.SelectorFromSet(labels.Set{"app": "example"})))
	state := cluster.vpas[VpaID{Namespace: "apps", VpaName: object.Name}].AggregateStateByContainerName()["app"]
	assert.Equal(t, 2, state.TotalSamplesCount)
	checkpoint, err := state.AggregateCPUUsage.SaveToChekpoint()
	require.NoError(t, err)
	assertProfileBucketRatio(t, checkpoint, 1, 4, math.Exp2(-6.0/24.0))
	options := GetAggregationsConfig().CPUHistogramOptions
	assert.Equal(t, options.GetBucketStart(options.FindBucket(1)+1), state.AggregateCPUUsage.Percentile(0.4),
		"reference rounding must not bias the merged recommendation toward the newer pod")
	assert.Equal(t, options.GetBucketStart(options.FindBucket(4)+1), state.AggregateCPUUsage.Percentile(0.5))
}

func TestMemoryPeakReplacementRemainsConsistentAcrossCPUProfileChanges(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	cluster := NewClusterState(testGcPeriod)
	podID := PodID{Namespace: "apps", PodName: "pod"}
	id := ContainerID{PodID: podID, ContainerName: "app"}
	podLabels := labels.Set{"app": "example"}
	changing := newCPUProfileVPA("apps", "changing", "*", 5*time.Minute)
	unchanged := newCPUProfileVPA("apps", "unchanged", "*", time.Hour)
	for _, object := range []*vpa_types.VerticalPodAutoscaler{changing, unchanged} {
		require.NoError(t, cluster.AddOrUpdateVpa(object, podLabels.AsSelector()))
	}
	cluster.AddOrUpdatePod(podID, podLabels, corev1.PodRunning)
	require.NoError(t, cluster.AddOrUpdateContainer(id, nil))
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	addProfileSample(t, cluster, id, start, ResourceMemory, MemoryAmountFromBytes(1<<30))
	changing.Spec.ResourcePolicy.ContainerPolicies[0].CPUHistogramDecayHalfLife.Duration = 10 * time.Minute
	require.NoError(t, cluster.AddOrUpdateVpa(changing, podLabels.AsSelector()))
	addProfileSample(t, cluster, id, start.Add(time.Minute), ResourceMemory, MemoryAmountFromBytes(2<<30))
	changedState := cluster.vpas[VpaID{Namespace: "apps", VpaName: changing.Name}].AggregateStateByContainerName()["app"]
	firstPeak, err := changedState.AggregateMemoryPeaks.SaveToChekpoint()
	require.NoError(t, err)
	assert.Len(t, firstPeak.BucketWeights, 1, "the current window must contain one replacement peak, not both peaks")
	assert.GreaterOrEqual(t, changedState.AggregateMemoryPeaks.Percentile(0), float64(2<<30))
	addProfileSample(t, cluster, id, start.Add(GetAggregationsConfig().MemoryAggregationInterval+time.Minute),
		ResourceMemory, MemoryAmountFromBytes(1<<29))
	var reference *vpa_types.HistogramCheckpoint
	for _, object := range []*vpa_types.VerticalPodAutoscaler{changing, unchanged} {
		state := cluster.vpas[VpaID{Namespace: "apps", VpaName: object.Name}].AggregateStateByContainerName()["app"]
		checkpoint, err := state.AggregateMemoryPeaks.SaveToChekpoint()
		require.NoError(t, err)
		assert.Len(t, checkpoint.BucketWeights, 2, "one memory peak per aggregation interval must be retained")
		if reference == nil {
			reference = checkpoint
		} else {
			assert.Equal(t, reference, checkpoint, "changing CPU decay must not alter memory peak replacement or decay")
		}
	}
}

func TestDisabledFeatureUsesConfiguredGlobalDecayAndRejectsDuplicateSamples(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, false)
	originalConfig := GetAggregationsConfig()
	config := *originalConfig
	config.CPUHistogramDecayHalfLife = 2 * time.Hour
	InitializeAggregationsConfig(&config)
	t.Cleanup(func() { InitializeAggregationsConfig(originalConfig) })
	cluster := NewClusterState(testGcPeriod)
	object := newCPUProfileVPA(testPodID.Namespace, "ignored-profile", "*", 5*time.Minute)
	require.NoError(t, cluster.AddOrUpdateVpa(object, labels.Set(testLabels).AsSelector()))
	cluster.AddOrUpdatePod(testPodID, testLabels, corev1.PodRunning)
	require.NoError(t, cluster.AddOrUpdateContainer(testContainerID, nil))
	addProfileSample(t, cluster, testContainerID, testTimestamp, ResourceCPU, CPUAmountFromCores(1))
	addProfileSample(t, cluster, testContainerID, testTimestamp.Add(20*time.Minute), ResourceCPU, CPUAmountFromCores(0.1))
	state := cluster.vpas[VpaID{Namespace: object.Namespace, VpaName: object.Name}].AggregateStateByContainerName()[testContainerID.ContainerName]
	assert.Equal(t, 2*time.Hour, state.CPUHistogramDecayHalfLife)
	before, err := state.AggregateCPUUsage.SaveToChekpoint()
	require.NoError(t, err)
	assertProfileBucketRatio(t, before, 1, 0.1, math.Exp2(-1.0/6.0))
	for _, timestamp := range []time.Time{testTimestamp, testTimestamp.Add(20 * time.Minute)} {
		assert.Error(t, cluster.AddSample(&ContainerUsageSampleWithKey{
			Container: testContainerID,
			ContainerUsageSample: ContainerUsageSample{
				MeasureStart: timestamp, Resource: ResourceCPU, Usage: CPUAmountFromCores(10),
			},
		}), "duplicate/out-of-order samples must not be added to any aggregate")
	}
	afterState := cluster.vpas[VpaID{Namespace: object.Namespace, VpaName: object.Name}].AggregateStateByContainerName()[testContainerID.ContainerName]
	after, err := afterState.AggregateCPUUsage.SaveToChekpoint()
	require.NoError(t, err)
	assert.Equal(t, before, after)
	assert.Equal(t, 2, afterState.TotalSamplesCount)
	assert.Len(t, cluster.aggregateStateMap, 1)
}

type sampleCountingSelector struct {
	labels.Selector
	matches *int
}

func (selector sampleCountingSelector) Matches(podLabels labels.Labels) bool {
	(*selector.matches)++
	return selector.Selector.Matches(podLabels)
}

func TestFeatureDisabledSampleIngestionDoesNotScanVPAs(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, false)
	cluster := NewClusterState(testGcPeriod)
	cluster.AddOrUpdatePod(testPodID, testLabels, corev1.PodRunning)
	require.NoError(t, cluster.AddOrUpdateContainer(testContainerID, nil))
	matches := 0
	for i := range 1000 {
		id := VpaID{Namespace: testPodID.Namespace, VpaName: fmt.Sprintf("unrelated-%d", i)}
		cluster.vpas[id] = NewVpa(id, sampleCountingSelector{Selector: labels.Nothing(), matches: &matches}, time.Time{})
	}
	addProfileSample(t, cluster, testContainerID, testTimestamp, ResourceCPU, CPUAmountFromCores(0.5))
	addProfileSample(t, cluster, testContainerID, testTimestamp, ResourceMemory, MemoryAmountFromBytes(1<<30))
	addProfileSample(t, cluster, testContainerID, testTimestamp.Add(time.Minute), ResourceMemory, MemoryAmountFromBytes(2<<30))
	assert.Zero(t, matches, "feature-off ingestion and proxy getters must not call any VPA selector")
	assert.Len(t, cluster.aggregateStateMap, 1)
}

func newCPUProfileVPA(namespace, name, container string, halfLife time.Duration) *vpa_types.VerticalPodAutoscaler {
	return &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			ResourcePolicy: &vpa_types.PodResourcePolicy{ContainerPolicies: []vpa_types.ContainerResourcePolicy{{
				ContainerName: container, CPUHistogramDecayHalfLife: &metav1.Duration{Duration: halfLife},
			}}},
		},
	}
}

func addProfileSample(t *testing.T, cluster *clusterState, id ContainerID, timestamp time.Time, resource ResourceName, amount ResourceAmount) {
	t.Helper()
	require.NoError(t, cluster.AddSample(&ContainerUsageSampleWithKey{
		Container: id,
		ContainerUsageSample: ContainerUsageSample{
			MeasureStart: timestamp, Resource: resource, Usage: amount,
		},
	}))
}

func assertProfileBucketRatio(t *testing.T, checkpoint *vpa_types.HistogramCheckpoint, numerator, denominator, expected float64) {
	t.Helper()
	options := GetAggregationsConfig().CPUHistogramOptions
	numeratorWeight := checkpoint.BucketWeights[options.FindBucket(numerator)]
	denominatorWeight := checkpoint.BucketWeights[options.FindBucket(denominator)]
	require.Positive(t, numeratorWeight)
	require.Positive(t, denominatorWeight)
	assert.InDelta(t, expected, float64(numeratorWeight)/float64(denominatorWeight), 0.0002,
		"serialized CPU weights must reflect the configured half-life, within checkpoint quantization")
}
