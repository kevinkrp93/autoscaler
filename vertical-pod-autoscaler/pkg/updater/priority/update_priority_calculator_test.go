/*
Copyright 2017 The Kubernetes Authors.

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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/features"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/test"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

const (
	containerName = "container1"
)

func TestProfileLifetimeControlsEligibility(t *testing.T) {
	for _, tc := range []struct {
		name        string
		enabled     bool
		lifetime    *metav1.Duration
		age         time.Duration
		diff        float64
		outside     bool
		missingTime bool
		want        int
	}{
		{name: "zero overrides default", enabled: true, lifetime: &metav1.Duration{}, diff: 0.25, want: 1},
		{name: "before boundary", enabled: true, lifetime: &metav1.Duration{Duration: 10 * time.Minute}, age: 10*time.Minute - time.Nanosecond, diff: 0.25},
		{name: "at boundary", enabled: true, lifetime: &metav1.Duration{Duration: 10 * time.Minute}, age: 10 * time.Minute, diff: 0.25, want: 1},
		{name: "absent uses global", enabled: true, age: *podLifetimeUpdateThreshold - time.Nanosecond, diff: 0.25},
		{name: "absent at global boundary", enabled: true, age: *podLifetimeUpdateThreshold, diff: 0.25, want: 1},
		{name: "disabled ignores zero", lifetime: &metav1.Duration{}, diff: 0.25},
		{name: "disabled uses global boundary", lifetime: &metav1.Duration{Duration: 48 * time.Hour}, age: *podLifetimeUpdateThreshold, diff: 0.25, want: 1},
		{name: "zero retains difference check", enabled: true, lifetime: &metav1.Duration{}, diff: 0.01},
		{name: "zero retains start time check", enabled: true, lifetime: &metav1.Duration{}, diff: 0.25, missingTime: true},
		{name: "outside range bypasses lifetime", enabled: true, lifetime: &metav1.Duration{Duration: 48 * time.Hour}, diff: 0.25, outside: true, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, tc.enabled)
			pod := test.Pod().WithName("pod").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).Get()).Get()
			now := pod.Status.StartTime.Add(tc.age)
			if tc.missingTime {
				pod.Status.StartTime = nil
			}
			vpa := test.VerticalPodAutoscaler().WithContainer(containerName).WithTarget("5", "").
				WithLowerBound("1", "").WithUpperBound("6", "").Get()
			threshold := 0.1
			vpa.Spec.UpdatePolicy = &vpa_types.PodUpdatePolicy{
				PodUpdateThreshold: &threshold, InRecommendationBoundsEvictionLifetimeThreshold: tc.lifetime,
			}
			calc := NewUpdatePriorityCalculator(vpa, nil, &test.FakeRecommendationProcessor{},
				NewFakeProcessor(map[string]PodPriority{"pod": {ResourceDiff: tc.diff, OutsideRecommendedRange: tc.outside}}))
			calc.AddPod(pod, now)
			assert.Len(t, calc.GetSortedPods(NewDefaultPodEvictionAdmission()), tc.want)
		})
	}
}

func TestProfileLifetimesAreIndependentAndCanChange(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	pod := test.Pod().WithName("pod").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).Get()).Get()
	first := test.VerticalPodAutoscaler().WithContainer(containerName).WithTarget("5", "").
		WithLowerBound("1", "").WithUpperBound("6", "").Get()
	first.Spec.UpdatePolicy = &vpa_types.PodUpdatePolicy{InRecommendationBoundsEvictionLifetimeThreshold: &metav1.Duration{}}
	second := first.DeepCopy()
	second.Spec.UpdatePolicy.InRecommendationBoundsEvictionLifetimeThreshold.Duration = 10 * time.Minute
	check := func(vpa *vpa_types.VerticalPodAutoscaler, want int) {
		t.Helper()
		calc := NewUpdatePriorityCalculator(vpa, nil, &test.FakeRecommendationProcessor{}, NewProcessor())
		calc.AddPod(pod, pod.Status.StartTime.Add(5*time.Minute))
		assert.Len(t, calc.GetSortedPods(NewDefaultPodEvictionAdmission()), want)
	}
	check(first, 1)
	check(second, 0)
	first.Spec.UpdatePolicy.InRecommendationBoundsEvictionLifetimeThreshold.Duration = time.Hour
	check(first, 0)
	check(second, 0)
	second.Spec.UpdatePolicy.InRecommendationBoundsEvictionLifetimeThreshold.Duration = time.Minute
	check(second, 1)
	check(first, 0)
}

func TestUpdatePriorityCalculatorUsesPerVPAThreshold(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	threshold := 0.45
	vpa := &vpa_types.VerticalPodAutoscaler{
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			UpdatePolicy: &vpa_types.PodUpdatePolicy{PodUpdateThreshold: &threshold},
		},
	}
	calculator := NewUpdatePriorityCalculator(vpa, nil, &test.FakeRecommendationProcessor{}, nil)
	assert.Equal(t, threshold, calculator.config.MinChangePriority)
}

func TestUpdatePriorityCalculatorUsesGlobalThresholdWhenFeatureDisabled(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, false)
	threshold := 0.45
	vpa := &vpa_types.VerticalPodAutoscaler{
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			UpdatePolicy: &vpa_types.PodUpdatePolicy{PodUpdateThreshold: &threshold},
		},
	}
	calculator := NewUpdatePriorityCalculator(vpa, nil, &test.FakeRecommendationProcessor{}, nil)
	assert.Equal(t, *defaultUpdateThreshold, calculator.config.MinChangePriority)
}

func TestProfileThresholdControlsUpdateEligibility(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	pod := test.Pod().WithName("pod").AddContainer(test.Container().WithName(containerName).
		WithCPURequest(resource.MustParse("4")).Get()).Get()
	for _, tc := range []struct {
		name        string
		threshold   float64
		wantUpdates int
	}{
		{name: "aggressive", threshold: 0.1, wantUpdates: 1},
		{name: "conservative", threshold: 0.5},
		{name: "zero", threshold: 0, wantUpdates: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vpa := test.VerticalPodAutoscaler().WithContainer(containerName).WithTarget("5", "").
				WithLowerBound("1", "").WithUpperBound("6", "").Get()
			vpa.Spec.UpdatePolicy = &vpa_types.PodUpdatePolicy{PodUpdateThreshold: &tc.threshold}
			calculator := NewUpdatePriorityCalculator(vpa, nil, &test.FakeRecommendationProcessor{},
				NewFakeProcessor(map[string]PodPriority{"pod": {ScaleUp: true, ResourceDiff: 0.25}}))
			calculator.AddPod(pod, pod.Status.StartTime.Add(24*time.Hour))
			assert.Len(t, calculator.GetSortedPods(NewDefaultPodEvictionAdmission()), tc.wantUpdates)
		})
	}
}

func TestProfileThresholdDefaultsAndExplicitConfig(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	vpa := &vpa_types.VerticalPodAutoscaler{}
	calculator := NewUpdatePriorityCalculator(vpa, nil, &test.FakeRecommendationProcessor{}, nil)
	assert.Equal(t, *defaultUpdateThreshold, calculator.config.MinChangePriority)
	threshold := 0.45
	vpa.Spec.UpdatePolicy = &vpa_types.PodUpdatePolicy{PodUpdateThreshold: &threshold}
	config := &UpdateConfig{MinChangePriority: 0.7}
	calculator = NewUpdatePriorityCalculator(vpa, config, &test.FakeRecommendationProcessor{}, nil)
	assert.Equal(t, config.MinChangePriority, calculator.config.MinChangePriority)
}

// TODO(bskiba): Refactor the SortPriority tests as a testcase list test.
func TestSortPriority(t *testing.T) {
	pod1 := test.Pod().WithName("POD1").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("2")).Get()).Get()
	pod2 := test.Pod().WithName("POD2").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).Get()).Get()
	pod3 := test.Pod().WithName("POD3").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("1")).Get()).Get()
	pod4 := test.Pod().WithName("POD4").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("3")).Get()).Get()

	vpa := test.VerticalPodAutoscaler().WithContainer(containerName).WithTarget("10", "").Get()

	priorityProcessor := NewFakeProcessor(map[string]PodPriority{
		"POD1": {ResourceDiff: 4.0},
		"POD2": {ResourceDiff: 1.5},
		"POD3": {ResourceDiff: 9.0},
		"POD4": {ResourceDiff: 2.33},
	})
	calculator := NewUpdatePriorityCalculator(vpa, nil, &test.FakeRecommendationProcessor{}, priorityProcessor)

	timestampNow := pod1.Status.StartTime.Add(time.Hour * 24)
	calculator.AddPod(pod1, timestampNow)
	calculator.AddPod(pod2, timestampNow)
	calculator.AddPod(pod3, timestampNow)
	calculator.AddPod(pod4, timestampNow)

	result := calculator.GetSortedPods(NewDefaultPodEvictionAdmission())
	assert.Exactly(t, []*apiv1.Pod{pod3, pod1, pod4, pod2}, result, "Wrong priority order")
}

func TestSortPriorityResourcesDecrease(t *testing.T) {
	pod1 := test.Pod().WithName("POD1").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).Get()).Get()
	pod2 := test.Pod().WithName("POD2").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("8")).Get()).Get()
	pod3 := test.Pod().WithName("POD3").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("10")).Get()).Get()

	vpa := test.VerticalPodAutoscaler().WithContainer(containerName).WithTarget("5", "").Get()

	priorityProcessor := NewFakeProcessor(map[string]PodPriority{
		"POD1": {ScaleUp: true, ResourceDiff: 0.25},
		"POD2": {ScaleUp: false, ResourceDiff: 0.25},
		"POD3": {ScaleUp: false, ResourceDiff: 0.5},
	})
	calculator := NewUpdatePriorityCalculator(vpa, nil, &test.FakeRecommendationProcessor{}, priorityProcessor)

	timestampNow := pod1.Status.StartTime.Add(time.Hour * 24)
	calculator.AddPod(pod1, timestampNow)
	calculator.AddPod(pod2, timestampNow)
	calculator.AddPod(pod3, timestampNow)

	// Expect the following order:
	// 1. pod1 - wants to grow by 1 unit.
	// 2. pod3 - can reclaim 5 units.
	// 3. pod2 - can reclaim 3 units.
	result := calculator.GetSortedPods(NewDefaultPodEvictionAdmission())
	assert.Exactly(t, []*apiv1.Pod{pod1, pod3, pod2}, result, "Wrong priority order")
}

func TestUpdateNotRequired(t *testing.T) {
	pod1 := test.Pod().WithName("POD1").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).Get()).Get()
	vpa := test.VerticalPodAutoscaler().WithContainer(containerName).WithTarget("4", "").Get()

	priorityProcessor := NewFakeProcessor(map[string]PodPriority{"POD1": {
		ResourceDiff: 0.0,
	}})
	calculator := NewUpdatePriorityCalculator(vpa, nil, &test.FakeRecommendationProcessor{},
		priorityProcessor)

	timestampNow := pod1.Status.StartTime.Add(time.Hour * 24)
	calculator.AddPod(pod1, timestampNow)

	result := calculator.GetSortedPods(NewDefaultPodEvictionAdmission())
	assert.Exactly(t, []*apiv1.Pod{}, result, "Pod should not be updated")
}

// TODO: add expects to fake processor
func TestUseProcessor(t *testing.T) {
	processedRecommendation := test.Recommendation().WithContainer(containerName).WithTarget("4", "10M").Get()
	recommendationProcessor := &test.RecommendationProcessorMock{}
	recommendationProcessor.On("Apply").Return(processedRecommendation, nil)

	vpa := test.VerticalPodAutoscaler().WithContainer(containerName).WithTarget("5", "5M").Get()
	pod1 := test.Pod().WithName("POD1").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).WithMemRequest(resource.MustParse("10M")).Get()).Get()

	priorityProcessor := NewFakeProcessor(map[string]PodPriority{
		"POD1": {ResourceDiff: 0.0},
	})
	calculator := NewUpdatePriorityCalculator(
		vpa, nil, recommendationProcessor, priorityProcessor)

	timestampNow := pod1.Status.StartTime.Add(time.Hour * 24)
	calculator.AddPod(pod1, timestampNow)

	result := calculator.GetSortedPods(NewDefaultPodEvictionAdmission())
	assert.Exactly(t, []*apiv1.Pod{}, result, "Pod should not be updated")
}

// Verify that a pod that lives for more than podLifetimeUpdateThreshold is
// updated if it has at least one container with the request:
// 1. outside the [MinRecommended...MaxRecommended] range or
// 2. diverging from the target by more than MinChangePriority.
func TestUpdateLonglivedPods(t *testing.T) {
	pods := []*apiv1.Pod{
		test.Pod().WithName("POD1").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).Get()).Get(),
		test.Pod().WithName("POD2").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("1")).Get()).Get(),
		test.Pod().WithName("POD3").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("8")).Get()).Get(),
	}

	// Both pods are within the recommended range.
	vpa := test.VerticalPodAutoscaler().WithContainer(containerName).
		WithTarget("5", "").
		WithLowerBound("1", "").
		WithUpperBound("6", "").Get()

	priorityProcessor := NewFakeProcessor(map[string]PodPriority{
		"POD1": {OutsideRecommendedRange: false, ScaleUp: true, ResourceDiff: 0.25},
		"POD2": {OutsideRecommendedRange: false, ScaleUp: true, ResourceDiff: 4.0},
		"POD3": {OutsideRecommendedRange: true, ScaleUp: false, ResourceDiff: 0.25},
	})

	calculator := NewUpdatePriorityCalculator(
		vpa, &UpdateConfig{MinChangePriority: 0.5}, &test.FakeRecommendationProcessor{}, priorityProcessor)

	// Pretend that the test pods started 13 hours ago.
	timestampNow := pods[0].Status.StartTime.Add(time.Hour * 13)
	for i := range 3 {
		calculator.AddPod(pods[i], timestampNow)
	}
	result := calculator.GetSortedPods(NewDefaultPodEvictionAdmission())
	assert.Exactly(t, []*apiv1.Pod{pods[1], pods[2]}, result, "Exactly POD2 and POD3 should be updated")
}

// Verify that a pod that lives for less than podLifetimeUpdateThreshold is
// updated only if the request is outside the [MinRecommended...MaxRecommended]
// range for at least one container.
func TestUpdateShortlivedPods(t *testing.T) {
	pods := []*apiv1.Pod{
		test.Pod().WithName("POD1").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).Get()).Get(),
		test.Pod().WithName("POD2").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("1")).Get()).Get(),
		test.Pod().WithName("POD3").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("10")).Get()).Get(),
	}

	// Pods 1 and 2 are within the recommended range.
	vpa := test.VerticalPodAutoscaler().WithContainer(containerName).
		WithTarget("5", "").
		WithLowerBound("1", "").
		WithUpperBound("6", "").Get()

	priorityProcessor := NewFakeProcessor(map[string]PodPriority{
		"POD1": {OutsideRecommendedRange: false, ScaleUp: true, ResourceDiff: 0.25},
		"POD2": {OutsideRecommendedRange: false, ScaleUp: true, ResourceDiff: 0.0},
		"POD3": {OutsideRecommendedRange: true, ScaleUp: false, ResourceDiff: 0.9},
	})

	calculator := NewUpdatePriorityCalculator(
		vpa, &UpdateConfig{MinChangePriority: 0.5}, &test.FakeRecommendationProcessor{}, priorityProcessor)

	// Pretend that the test pods started 11 hours ago.
	timestampNow := pods[0].Status.StartTime.Add(time.Hour * 11)
	for i := range 3 {
		calculator.AddPod(pods[i], timestampNow)
	}
	result := calculator.GetSortedPods(NewDefaultPodEvictionAdmission())
	assert.Exactly(t, []*apiv1.Pod{pods[2]}, result, "Only POD3 should be updated")
}

func TestUpdatePodWithQuickOOM(t *testing.T) {
	pod := test.Pod().WithName("POD1").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).Get()).Get()

	// Pretend that the test pod started 11 hours ago.
	timestampNow := pod.Status.StartTime.Add(time.Hour * 11)

	pod.Status.ContainerStatuses = []apiv1.ContainerStatus{
		{
			LastTerminationState: apiv1.ContainerState{
				Terminated: &apiv1.ContainerStateTerminated{
					Reason:     "OOMKilled",
					FinishedAt: metav1.NewTime(timestampNow.Add(-1 * 3 * time.Minute)),
					StartedAt:  metav1.NewTime(timestampNow.Add(-1 * 5 * time.Minute)),
				},
			},
		},
	}

	// Pod is within the recommended range.
	vpa := test.VerticalPodAutoscaler().WithContainer(containerName).
		WithTarget("5", "").
		WithLowerBound("1", "").
		WithUpperBound("6", "").Get()

	priorityProcessor := NewFakeProcessor(map[string]PodPriority{
		"POD1": {ScaleUp: true, ResourceDiff: 0.25},
	})

	calculator := NewUpdatePriorityCalculator(
		vpa, &UpdateConfig{MinChangePriority: 0.5}, &test.FakeRecommendationProcessor{}, priorityProcessor)

	calculator.AddPod(pod, timestampNow)
	result := calculator.GetSortedPods(NewDefaultPodEvictionAdmission())
	assert.Exactly(t, []*apiv1.Pod{pod}, result, "Pod should be updated")
}

func TestDontUpdatePodWithQuickOOMNoResourceChange(t *testing.T) {
	pod := test.Pod().WithName("POD1").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).WithMemRequest(resource.MustParse("8Gi")).Get()).Get()

	// Pretend that the test pod started 11 hours ago.
	timestampNow := pod.Status.StartTime.Add(time.Hour * 11)

	pod.Status.ContainerStatuses = []apiv1.ContainerStatus{
		{
			LastTerminationState: apiv1.ContainerState{
				Terminated: &apiv1.ContainerStateTerminated{
					Reason:     "OOMKilled",
					FinishedAt: metav1.NewTime(timestampNow.Add(-1 * 3 * time.Minute)),
					StartedAt:  metav1.NewTime(timestampNow.Add(-1 * 5 * time.Minute)),
				},
			},
		},
	}

	// Pod is within the recommended range.
	vpa := test.VerticalPodAutoscaler().WithContainer(containerName).
		WithTarget("4", "8Gi").
		WithLowerBound("2", "5Gi").
		WithUpperBound("5", "10Gi").Get()

	priorityProcessor := NewFakeProcessor(map[string]PodPriority{
		"POD1": {ScaleUp: true, ResourceDiff: 0.0},
	})

	calculator := NewUpdatePriorityCalculator(
		vpa, &UpdateConfig{MinChangePriority: 0.1}, &test.FakeRecommendationProcessor{}, priorityProcessor)

	calculator.AddPod(pod, timestampNow)
	result := calculator.GetSortedPods(NewDefaultPodEvictionAdmission())
	assert.Exactly(t, []*apiv1.Pod{}, result, "Pod should not be updated")
}

func TestDontUpdatePodWithOOMAfterLongRun(t *testing.T) {
	pod := test.Pod().WithName("POD1").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).Get()).Get()

	// Pretend that the test pod started 11 hours ago.
	timestampNow := pod.Status.StartTime.Add(time.Hour * 11)

	pod.Status.ContainerStatuses = []apiv1.ContainerStatus{
		{
			LastTerminationState: apiv1.ContainerState{
				Terminated: &apiv1.ContainerStateTerminated{
					Reason:     "OOMKilled",
					FinishedAt: metav1.NewTime(timestampNow.Add(-1 * 3 * time.Minute)),
					StartedAt:  metav1.NewTime(timestampNow.Add(-1 * 60 * time.Minute)),
				},
			},
		},
	}

	// Pod is within the recommended range.
	vpa := test.VerticalPodAutoscaler().WithContainer(containerName).
		WithTarget("5", "").
		WithLowerBound("1", "").
		WithUpperBound("6", "").Get()

	priorityProcessor := NewFakeProcessor(map[string]PodPriority{
		"POD1": {ScaleUp: true, ResourceDiff: 0.0},
	})
	calculator := NewUpdatePriorityCalculator(
		vpa, &UpdateConfig{MinChangePriority: 0.5}, &test.FakeRecommendationProcessor{}, priorityProcessor)

	calculator.AddPod(pod, timestampNow)
	result := calculator.GetSortedPods(NewDefaultPodEvictionAdmission())
	assert.Exactly(t, []*apiv1.Pod{}, result, "Pod shouldn't be updated")
}

func TestQuickOOM_VpaOvservedContainers(t *testing.T) {
	tests := []struct {
		name       string
		annotation map[string]string
		want       bool
	}{
		{
			name:       "no VpaOvservedContainers annotation",
			annotation: map[string]string{},
			want:       true,
		},
		{
			name:       "container listed in VpaOvservedContainers annotation",
			annotation: map[string]string{annotations.VpaObservedContainersLabel: containerName},
			want:       true,
		},
		{
			// Containers not listed in VpaOvservedContainers annotation
			// shouldn't trigger the quick OOM.
			name:       "container not listed in VpaOvservedContainers annotation",
			annotation: map[string]string{annotations.VpaObservedContainersLabel: ""},
			want:       false,
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("test case: %s", tc.name), func(t *testing.T) {
			pod := test.Pod().WithAnnotations(tc.annotation).
				WithName("POD1").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).Get()).Get()

			// Pretend that the test pod started 11 hours ago.
			timestampNow := pod.Status.StartTime.Add(time.Hour * 11)

			pod.Status.ContainerStatuses = []apiv1.ContainerStatus{
				{
					Name: containerName,
					LastTerminationState: apiv1.ContainerState{
						Terminated: &apiv1.ContainerStateTerminated{
							Reason:     "OOMKilled",
							FinishedAt: metav1.NewTime(timestampNow.Add(-1 * 3 * time.Minute)),
							StartedAt:  metav1.NewTime(timestampNow.Add(-1 * 5 * time.Minute)),
						},
					},
				},
			}

			// Pod is within the recommended range.
			vpa := test.VerticalPodAutoscaler().WithContainer(containerName).
				WithTarget("5", "").
				WithLowerBound("1", "").
				WithUpperBound("6", "").Get()

			priorityProcessor := NewFakeProcessor(map[string]PodPriority{
				"POD1": {ScaleUp: true, ResourceDiff: 0.25}})
			calculator := NewUpdatePriorityCalculator(
				vpa, &UpdateConfig{MinChangePriority: 0.5}, &test.FakeRecommendationProcessor{}, priorityProcessor)

			calculator.AddPod(pod, timestampNow)
			result := calculator.GetSortedPods(NewDefaultPodEvictionAdmission())
			isUpdate := len(result) != 0
			assert.Equal(t, tc.want, isUpdate)
		})
	}
}

func TestQuickOOM_ContainerResourcePolicy(t *testing.T) {
	scalingModeAuto := vpa_types.ContainerScalingModeAuto
	scalingModeOff := vpa_types.ContainerScalingModeOff
	tests := []struct {
		name           string
		resourcePolicy vpa_types.ContainerResourcePolicy
		want           bool
	}{
		{
			name: "ContainerScalingModeAuto",
			resourcePolicy: vpa_types.ContainerResourcePolicy{
				ContainerName: containerName,
				Mode:          &scalingModeAuto,
			},
			want: true,
		},
		{
			// Containers with ContainerScalingModeOff
			// shouldn't trigger the quick OOM.
			name: "ContainerScalingModeOff",
			resourcePolicy: vpa_types.ContainerResourcePolicy{
				ContainerName: containerName,
				Mode:          &scalingModeOff,
			},
			want: false,
		},
		{
			name: "ContainerScalingModeAuto as default",
			resourcePolicy: vpa_types.ContainerResourcePolicy{
				ContainerName: vpa_types.DefaultContainerResourcePolicy,
				Mode:          &scalingModeAuto,
			},
			want: true,
		},
		{
			// When ContainerScalingModeOff is default
			// container shouldn't trigger the quick OOM.
			name: "ContainerScalingModeOff as default",
			resourcePolicy: vpa_types.ContainerResourcePolicy{
				ContainerName: vpa_types.DefaultContainerResourcePolicy,
				Mode:          &scalingModeOff,
			},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("test case: %s", tc.name), func(t *testing.T) {
			pod := test.Pod().WithAnnotations(map[string]string{annotations.VpaObservedContainersLabel: containerName}).
				WithName("POD1").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).Get()).Get()

			// Pretend that the test pod started 11 hours ago.
			timestampNow := pod.Status.StartTime.Add(time.Hour * 11)

			pod.Status.ContainerStatuses = []apiv1.ContainerStatus{
				{
					Name: containerName,
					LastTerminationState: apiv1.ContainerState{
						Terminated: &apiv1.ContainerStateTerminated{
							Reason:     "OOMKilled",
							FinishedAt: metav1.NewTime(timestampNow.Add(-1 * 3 * time.Minute)),
							StartedAt:  metav1.NewTime(timestampNow.Add(-1 * 5 * time.Minute)),
						},
					},
				},
			}

			// Pod is within the recommended range.
			vpa := test.VerticalPodAutoscaler().WithContainer(containerName).
				WithTarget("5", "").
				WithLowerBound("1", "").
				WithUpperBound("6", "").Get()

			vpa.Spec.ResourcePolicy = &vpa_types.PodResourcePolicy{
				ContainerPolicies: []vpa_types.ContainerResourcePolicy{
					tc.resourcePolicy,
				},
			}
			priorityProcessor := NewFakeProcessor(map[string]PodPriority{
				"POD1": {ScaleUp: true, ResourceDiff: 0.25}})
			calculator := NewUpdatePriorityCalculator(
				vpa, &UpdateConfig{MinChangePriority: 0.5}, &test.FakeRecommendationProcessor{}, priorityProcessor)

			calculator.AddPod(pod, timestampNow)
			result := calculator.GetSortedPods(NewDefaultPodEvictionAdmission())
			isUpdate := len(result) != 0
			assert.Equal(t, tc.want, isUpdate)
		})
	}
}

func TestNoPods(t *testing.T) {
	calculator := NewUpdatePriorityCalculator(nil, nil, &test.FakeRecommendationProcessor{},
		NewFakeProcessor(map[string]PodPriority{}))
	result := calculator.GetSortedPods(NewDefaultPodEvictionAdmission())
	assert.Exactly(t, []*apiv1.Pod{}, result)
}

type pod1Admission struct{}

func (p *pod1Admission) LoopInit([]*apiv1.Pod, map[*vpa_types.VerticalPodAutoscaler][]*apiv1.Pod) {}
func (p *pod1Admission) Admit(pod *apiv1.Pod, recommendation *vpa_types.RecommendedPodResources) bool {
	return pod.Name == "POD1"
}
func (p *pod1Admission) CleanUp() {}

func TestAdmission(t *testing.T) {
	pod1 := test.Pod().WithName("POD1").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("2")).Get()).Get()
	pod2 := test.Pod().WithName("POD2").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("4")).Get()).Get()
	pod3 := test.Pod().WithName("POD3").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("1")).Get()).Get()
	pod4 := test.Pod().WithName("POD4").AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("3")).Get()).Get()

	vpa := test.VerticalPodAutoscaler().WithContainer(containerName).WithTarget("10", "").Get()

	priorityProcessor := NewFakeProcessor(map[string]PodPriority{
		"POD1": {ScaleUp: true, ResourceDiff: 4.0},
		"POD2": {ScaleUp: true, ResourceDiff: 1.5},
		"POD3": {ScaleUp: true, ResourceDiff: 9.0},
		"POD4": {ScaleUp: true, ResourceDiff: 2.33}})
	calculator := NewUpdatePriorityCalculator(vpa, nil,
		&test.FakeRecommendationProcessor{}, priorityProcessor)

	timestampNow := pod1.Status.StartTime.Add(time.Hour * 24)
	calculator.AddPod(pod1, timestampNow)
	calculator.AddPod(pod2, timestampNow)
	calculator.AddPod(pod3, timestampNow)
	calculator.AddPod(pod4, timestampNow)

	result := calculator.GetSortedPods(&pod1Admission{})
	assert.Exactly(t, []*apiv1.Pod{pod1}, result, "Wrong priority order")
}

func TestLessPodPriority(t *testing.T) {
	testCases := []struct {
		name        string
		prio, other PodPriority
		isLess      bool
	}{
		{
			name: "scale down more than empty",
			prio: PodPriority{
				ScaleUp:      false,
				ResourceDiff: 0.1,
			},
			other:  PodPriority{},
			isLess: false,
		}, {
			name: "scale up more than empty",
			prio: PodPriority{
				ScaleUp:      true,
				ResourceDiff: 0.1,
			},
			other:  PodPriority{},
			isLess: false,
		}, {
			name: "two scale ups",
			prio: PodPriority{
				ScaleUp:      true,
				ResourceDiff: 0.1,
			},
			other: PodPriority{
				ScaleUp:      true,
				ResourceDiff: 1.0,
			},
			isLess: true,
		}, {
			name: "two scale downs",
			prio: PodPriority{
				ScaleUp:      false,
				ResourceDiff: 0.9,
			},
			other: PodPriority{
				ScaleUp:      false,
				ResourceDiff: 0.1,
			},
			isLess: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.isLess, tc.prio.Less(tc.other))
			assert.Equal(t, !tc.isLess, tc.other.Less(tc.prio))
		})
	}
}

func TestAddPodLogs(t *testing.T) {
	testCases := []struct {
		name        string
		givenRec    *vpa_types.RecommendedPodResources
		expectedLog string
	}{
		{
			name:        "container with target and uncappedTarget",
			givenRec:    test.Recommendation().WithContainer(containerName).WithTarget("4", "10M").Get(),
			expectedLog: "container1: target: 10000k 4000m; uncappedTarget: 10000k 4000m;",
		},
		{
			name:        "container with cpu only",
			givenRec:    test.Recommendation().WithContainer(containerName).WithTarget("8", "").Get(),
			expectedLog: "container1: target: 8000m; uncappedTarget: 8000m;",
		},
		{
			name:        "container with memory only",
			givenRec:    test.Recommendation().WithContainer(containerName).WithTarget("", "10M").Get(),
			expectedLog: "container1: target: 10000k uncappedTarget: 10000k ",
		},
		{
			name: "multi-container with different resources",
			givenRec: &vpa_types.RecommendedPodResources{
				ContainerRecommendations: []vpa_types.RecommendedContainerResources{
					{
						ContainerName: "container-1",
						Target:        test.Resources("4", "10M"),
					},
					{
						ContainerName: "container-2",
						Target:        test.Resources("8", ""),
					},
					{
						ContainerName: "container-3",
						Target:        test.Resources("", "10m"),
					},
				},
			},
			expectedLog: "container-1: target: 10000k 4000m; container-2: target: 8000m; container-3: target: 1k ",
		},
		{
			name: "multi-containers with uncappedTarget",
			givenRec: &vpa_types.RecommendedPodResources{
				ContainerRecommendations: []vpa_types.RecommendedContainerResources{
					{
						ContainerName:  "container-1",
						Target:         test.Resources("4", "10M"),
						UncappedTarget: test.Resources("4", "10M"),
					},
					{
						ContainerName:  "container-2",
						Target:         test.Resources("8", ""),
						UncappedTarget: test.Resources("8", ""),
					},
					{
						ContainerName:  "container-3",
						Target:         test.Resources("", "10m"),
						UncappedTarget: test.Resources("", "10m"),
					},
				},
			},
			expectedLog: "container-1: target: 10000k 4000m; uncappedTarget: 10000k 4000m;container-2: target: 8000m; uncappedTarget: 8000m;container-3: target: 1k uncappedTarget: 1k ",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vpa := test.VerticalPodAutoscaler().WithContainer(containerName).WithTarget("10", "").Get()
			priorityProcessor := NewFakeProcessor(map[string]PodPriority{
				"POD1": {ScaleUp: true, ResourceDiff: 4.0}})
			calculator := NewUpdatePriorityCalculator(vpa, nil,
				&test.FakeRecommendationProcessor{}, priorityProcessor)

			actualLog := calculator.GetProcessedRecommendationTargets(tc.givenRec)
			assert.Equal(t, tc.expectedLog, actualLog)
		})
	}
}
