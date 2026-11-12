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

package priority

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/ptr"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/features"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/test"
)

func TestProfileThresholdWithRealResourceDifferences(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("feature-enabled-%t", enabled), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, enabled)
			oldDefault := *defaultUpdateThreshold
			*defaultUpdateThreshold = 0.3
			t.Cleanup(func() { *defaultUpdateThreshold = oldDefault })
			for _, target := range []string{"3", "5"} {
				for _, profile := range []struct {
					name      string
					threshold *float64
					allowed   bool
				}{
					{"no-override", nil, false},
					{"aggressive", ptr.To(0.1), true},
					{"boundary", ptr.To(0.25), true},
					{"above-boundary", ptr.To(0.250001), false},
					{"conservative", ptr.To(0.5), false},
					{"zero", ptr.To(0.0), true},
				} {
					t.Run(target+"/"+profile.name, func(t *testing.T) {
						pod := test.Pod().WithName("pod").
							AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).Get()).Get()
						vpa := test.VerticalPodAutoscaler().WithContainer(containerName).
							WithTarget(target, "").WithLowerBound("1", "").WithUpperBound("8", "").Get()
						vpa.Spec.UpdatePolicy = &vpa_types.PodUpdatePolicy{PodUpdateThreshold: profile.threshold}
						actualPriority := NewProcessor().GetUpdatePriority(pod, vpa, vpa.Status.Recommendation)
						assert.InDelta(t, 0.25, actualPriority.ResourceDiff, 1e-12)
						assert.False(t, actualPriority.OutsideRecommendedRange)
						calculator := NewUpdatePriorityCalculator(vpa, nil, &test.FakeRecommendationProcessor{}, NewProcessor())
						calculator.AddPod(pod, pod.Status.StartTime.Add(*podLifetimeUpdateThreshold))
						updates := calculator.GetSortedPods(NewDefaultPodEvictionAdmission())
						if enabled && profile.allowed {
							assert.Equal(t, []*corev1.Pod{pod}, updates)
						} else {
							assert.Empty(t, updates)
						}
					})
				}
			}
		})
	}
}

func TestProfileThresholdPreservesAgeBoundsAndOOMRules(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	for _, scenario := range []struct {
		name        string
		threshold   float64
		age         time.Duration
		lowerBound  string
		target      string
		missingTime bool
		quickOOM    bool
		allowed     bool
	}{
		{name: "short-lived-in-range", threshold: 0, age: time.Minute, lowerBound: "1", target: "5"},
		{name: "missing-start-time", threshold: 0, lowerBound: "1", target: "5", missingTime: true},
		{name: "outside-bounds-bypasses-threshold-and-age", threshold: 100, age: time.Minute, lowerBound: "4.5", target: "5", allowed: true},
		{name: "quick-oom-bypasses-threshold-and-age", threshold: 100, age: 5 * time.Minute, lowerBound: "1", target: "5", quickOOM: true, allowed: true},
		{name: "quick-oom-still-requires-resource-change", threshold: 0, age: 5 * time.Minute, lowerBound: "1", target: "4", quickOOM: true},
		{name: "old-in-range-below-threshold", threshold: 0.5, age: 48 * time.Hour, lowerBound: "1", target: "5"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			pod := test.Pod().WithName("pod").
				AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).Get()).Get()
			now := pod.Status.StartTime.Add(scenario.age)
			if scenario.missingTime {
				pod.Status.StartTime = nil
			}
			if scenario.quickOOM {
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
					Name: containerName,
					LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						Reason: "OOMKilled", StartedAt: metav1.NewTime(now.Add(-2 * time.Minute)), FinishedAt: metav1.NewTime(now.Add(-time.Minute)),
					}},
				}}
			}
			vpa := test.VerticalPodAutoscaler().WithContainer(containerName).
				WithTarget(scenario.target, "").WithLowerBound(scenario.lowerBound, "").WithUpperBound("8", "").Get()
			vpa.Spec.UpdatePolicy = &vpa_types.PodUpdatePolicy{PodUpdateThreshold: &scenario.threshold}
			calculator := NewUpdatePriorityCalculator(vpa, nil, &test.FakeRecommendationProcessor{}, NewProcessor())
			calculator.AddPod(pod, now)
			updates := calculator.GetSortedPods(NewDefaultPodEvictionAdmission())
			if scenario.allowed {
				assert.Equal(t, []*corev1.Pod{pod}, updates)
			} else {
				assert.Empty(t, updates)
			}
		})
	}
}

func TestProfileThresholdUsesPodTotalsAcrossContainersAndResources(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	for _, scenario := range []struct {
		name         string
		sidecar      bool
		memoryChange bool
		expectedDiff float64
		allowed      bool
	}{
		{name: "single-container", expectedDiff: 0.25, allowed: true},
		{name: "unchanged-sidecar-dilutes-pod-cpu-change", sidecar: true, expectedDiff: 0.125},
		{name: "cpu-and-memory-differences-are-added", memoryChange: true, expectedDiff: 0.375, allowed: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			pod := test.Pod().WithName("pod").AddContainer(test.Container().WithName(containerName).
				WithCPURequest(resource.MustParse("4")).WithMemRequest(resource.MustParse("8Gi")).Get()).Get()
			vpa := test.VerticalPodAutoscaler().WithContainer(containerName).
				WithTarget("5", "8Gi").WithLowerBound("1", "1Gi").WithUpperBound("10", "20Gi").Get()
			if scenario.sidecar {
				pod.Spec.Containers = append(pod.Spec.Containers, test.Container().WithName("sidecar").
					WithCPURequest(resource.MustParse("4")).WithMemRequest(resource.MustParse("8Gi")).Get())
				vpa.Status.Recommendation.ContainerRecommendations = append(vpa.Status.Recommendation.ContainerRecommendations,
					vpa_types.RecommendedContainerResources{
						ContainerName: "sidecar",
						Target:        corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("8Gi")},
					})
			}
			if scenario.memoryChange {
				vpa.Status.Recommendation.ContainerRecommendations[0].Target[corev1.ResourceMemory] = resource.MustParse("9Gi")
			}
			vpa.Spec.UpdatePolicy = &vpa_types.PodUpdatePolicy{PodUpdateThreshold: ptr.To(0.2)}
			actual := NewProcessor().GetUpdatePriority(pod, vpa, vpa.Status.Recommendation)
			assert.InDelta(t, scenario.expectedDiff, actual.ResourceDiff, 1e-12)
			calculator := NewUpdatePriorityCalculator(vpa, nil, &test.FakeRecommendationProcessor{}, NewProcessor())
			calculator.AddPod(pod, pod.Status.StartTime.Add(*podLifetimeUpdateThreshold))
			assert.Equal(t, scenario.allowed, len(calculator.GetSortedPods(NewDefaultPodEvictionAdmission())) == 1)
		})
	}
}

func TestProfileThresholdCalculatorsDoNotLeakAcrossVPAsOrPolicyChanges(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	pod := test.Pod().WithName("pod").AddContainer(test.Container().WithName(containerName).
		WithCPURequest(resource.MustParse("4")).Get()).Get()
	vpa := test.VerticalPodAutoscaler().WithContainer(containerName).
		WithTarget("5", "").WithLowerBound("1", "").WithUpperBound("8", "").Get()
	vpa.Spec.UpdatePolicy = &vpa_types.PodUpdatePolicy{PodUpdateThreshold: ptr.To(0.1)}
	other := vpa.DeepCopy()
	other.Spec.UpdatePolicy.PodUpdateThreshold = ptr.To(0.5)
	aggressive := NewUpdatePriorityCalculator(vpa, nil, &test.FakeRecommendationProcessor{}, NewProcessor())
	conservative := NewUpdatePriorityCalculator(other, nil, &test.FakeRecommendationProcessor{}, NewProcessor())
	*vpa.Spec.UpdatePolicy.PodUpdateThreshold = 0.75
	changed := NewUpdatePriorityCalculator(vpa, nil, &test.FakeRecommendationProcessor{}, NewProcessor())
	for _, calculation := range []struct {
		calculator *UpdatePriorityCalculator
		expected   float64
		allowed    bool
	}{
		{&aggressive, 0.1, true}, {&conservative, 0.5, false}, {&changed, 0.75, false},
	} {
		assert.Equal(t, calculation.expected, calculation.calculator.config.MinChangePriority)
		calculation.calculator.AddPod(pod, pod.Status.StartTime.Add(*podLifetimeUpdateThreshold))
		assert.Equal(t, calculation.allowed, len(calculation.calculator.GetSortedPods(NewDefaultPodEvictionAdmission())) == 1)
	}
	explicit := &UpdateConfig{MinChangePriority: 0.2}
	calculator := NewUpdatePriorityCalculator(vpa, explicit, &test.FakeRecommendationProcessor{}, NewProcessor())
	require.Same(t, explicit, calculator.config, "explicit caller configuration must retain precedence")
}
