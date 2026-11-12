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

package logic

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/ptr"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	fakeclientset "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/fake"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/features"
	recommenderlogic "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/logic"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	recommenderroutines "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/routines"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/updater/priority"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/test"
)

func TestFakeClusterRunsTwoAndTenMinuteProfilesConcurrently(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	ctx := context.Background()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var workloads, vpaObjects []runtime.Object
	for _, profile := range []struct {
		name      string
		halfLife  time.Duration
		threshold float64
	}{
		{"aggressive", 2 * time.Minute, 0.1},
		{"conservative", 10 * time.Minute, 0.5},
	} {
		podLabels := map[string]string{"app": profile.name}
		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: profile.name},
			Spec:       appsv1.DeploymentSpec{Selector: &metav1.LabelSelector{MatchLabels: podLabels}},
		}
		pod := test.Pod().WithName(profile.name).AddContainer(test.Container().WithName("app").
			WithCPURequest(resource.MustParse("1")).Get()).Get()
		pod.Namespace = "apps"
		pod.Labels = podLabels
		pod.Status.StartTime = &metav1.Time{Time: time.Now().Add(-48 * time.Hour)}
		workloads = append(workloads, deployment, pod)
		vpaObjects = append(vpaObjects, &vpa_types.VerticalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: profile.name},
			Spec: vpa_types.VerticalPodAutoscalerSpec{
				TargetRef: &autoscalingv1.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: profile.name},
				UpdatePolicy: &vpa_types.PodUpdatePolicy{
					UpdateMode: ptr.To(vpa_types.UpdateModeRecreate), PodUpdateThreshold: ptr.To(profile.threshold),
				},
				ResourcePolicy: &vpa_types.PodResourcePolicy{ContainerPolicies: []vpa_types.ContainerResourcePolicy{{
					ContainerName: "*", CPUHistogramDecayHalfLife: &metav1.Duration{Duration: profile.halfLife},
					ControlledResources: ptr.To([]corev1.ResourceName{corev1.ResourceCPU}),
				}}},
			},
		})
	}
	kube := kubefake.NewClientset(workloads...)
	vpaClient := fakeclientset.NewSimpleClientset(vpaObjects...)
	cluster := model.NewClusterState(time.Minute)
	objects, err := vpaClient.AutoscalingV1().VerticalPodAutoscalers("apps").List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	for _, object := range objects.Items {
		deployment, err := kube.AppsV1().Deployments("apps").Get(ctx, object.Spec.TargetRef.Name, metav1.GetOptions{})
		require.NoError(t, err)
		selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
		require.NoError(t, err)
		require.NoError(t, cluster.AddOrUpdateVpa(&object, selector))
		pod, err := kube.CoreV1().Pods("apps").Get(ctx, object.Name, metav1.GetOptions{})
		require.NoError(t, err)
		podID := model.PodID{Namespace: "apps", PodName: pod.Name}
		cluster.AddOrUpdatePod(podID, pod.Labels, corev1.PodRunning)
		require.NoError(t, cluster.AddOrUpdateContainer(model.ContainerID{PodID: podID, ContainerName: "app"}, nil))
	}
	// Interleave the same usage stream for both workloads in the shared model.
	for i, usage := range []float64{1, 0.1} {
		for _, name := range []string{"aggressive", "conservative"} {
			require.NoError(t, cluster.AddSample(&model.ContainerUsageSampleWithKey{
				Container: model.ContainerID{PodID: model.PodID{Namespace: "apps", PodName: name}, ContainerName: "app"},
				ContainerUsageSample: model.ContainerUsageSample{
					MeasureStart: start.Add(time.Duration(i) * 8 * time.Minute), Resource: model.ResourceCPU, Usage: model.CPUAmountFromCores(usage),
				},
			}))
		}
	}
	sharedRecommender := recommenderlogic.CreatePodResourceRecommender()
	sharedUpdater := &updater{
		recommendationProcessor: &test.FakeRecommendationProcessor{},
		priorityProcessor:       priority.NewProcessor(),
		evictionAdmission:       priority.NewDefaultPodEvictionAdmission(),
	}
	type evaluation struct {
		vpa         *vpa_types.VerticalPodAutoscaler
		halfLife    time.Duration
		cpu         int64
		priority    priority.PodPriority
		wantsUpdate bool
		err         error
	}
	evaluateConcurrently := func() map[string]evaluation {
		objects, err := vpaClient.AutoscalingV1().VerticalPodAutoscalers("apps").List(ctx, metav1.ListOptions{})
		require.NoError(t, err)
		results := make(chan evaluation, len(objects.Items))
		startWorkers := make(chan struct{})
		var workers sync.WaitGroup
		for _, object := range objects.Items {
			workers.Add(1)
			go func(object *vpa_types.VerticalPodAutoscaler) {
				defer workers.Done()
				result := evaluation{vpa: object}
				defer func() { results <- result }()
				<-startWorkers
				vpa := cluster.VPAs()[model.VpaID{Namespace: object.Namespace, VpaName: object.Name}]
				if vpa == nil {
					result.err = fmt.Errorf("profile %s is missing from the shared model", object.Name)
					return
				}
				state := recommenderroutines.GetContainerNameToAggregateStateMap(vpa)
				if state["app"] == nil {
					result.err = fmt.Errorf("profile %s has no app history", object.Name)
					return
				}
				result.halfLife = state["app"].CPUHistogramDecayHalfLife
				object.Status.Recommendation = recommenderlogic.MapToListOfRecommendedContainerResources(
					sharedRecommender.GetRecommendedPodResources(state))
				if len(object.Status.Recommendation.ContainerRecommendations) != 1 {
					result.err = fmt.Errorf("profile %s did not produce exactly one container recommendation", object.Name)
					return
				}
				updated, err := vpaClient.AutoscalingV1().VerticalPodAutoscalers("apps").UpdateStatus(ctx, object, metav1.UpdateOptions{})
				if err != nil {
					result.err = fmt.Errorf("update %s recommendation: %w", object.Name, err)
					return
				}
				result.vpa = updated
				pod, err := kube.CoreV1().Pods("apps").Get(ctx, object.Name, metav1.GetOptions{})
				if err != nil {
					result.err = fmt.Errorf("get %s pod: %w", object.Name, err)
					return
				}
				result.cpu = updated.Status.Recommendation.ContainerRecommendations[0].Target.Cpu().MilliValue()
				result.priority = priority.NewProcessor().GetUpdatePriority(pod, updated, updated.Status.Recommendation)
				result.wantsUpdate = len(sharedUpdater.getPodsUpdateOrder([]*corev1.Pod{pod}, updated)) == 1
			}(object.DeepCopy())
		}
		close(startWorkers)
		workers.Wait()
		close(results)
		collected := make(map[string]evaluation)
		for result := range results {
			require.NoError(t, result.err)
			collected[result.vpa.Name] = result
		}
		require.Len(t, collected, 2)
		return collected
	}
	initial := evaluateConcurrently()
	assert.Equal(t, 2*time.Minute, initial["aggressive"].halfLife)
	assert.Equal(t, 10*time.Minute, initial["conservative"].halfLife)
	assert.Equal(t, 0.1, *initial["aggressive"].vpa.Spec.UpdatePolicy.PodUpdateThreshold)
	assert.Equal(t, 0.5, *initial["conservative"].vpa.Spec.UpdatePolicy.PodUpdateThreshold)
	assert.Less(t, initial["aggressive"].cpu, int64(200))
	assert.Greater(t, initial["conservative"].cpu, int64(1000))
	assert.True(t, initial["aggressive"].wantsUpdate)
	assert.False(t, initial["conservative"].wantsUpdate)
	for _, name := range []string{"aggressive", "conservative"} {
		result := initial[name]
		assert.False(t, result.priority.OutsideRecommendedRange, "the threshold must decide this test, not the bounds bypass")
		t.Logf("%s: half-life=%s CPU=%dm threshold=%.2f update=%t",
			name, result.halfLife, result.cpu, *result.vpa.Spec.UpdatePolicy.PodUpdateThreshold, result.wantsUpdate)
	}

	aggressiveID := model.VpaID{Namespace: "apps", VpaName: "aggressive"}
	conservativeID := model.VpaID{Namespace: "apps", VpaName: "conservative"}
	before, err := cluster.VPAs()[aggressiveID].AggregateStateByContainerName()["app"].SaveToCheckpoint()
	require.NoError(t, err)
	otherBefore, err := cluster.VPAs()[conservativeID].AggregateStateByContainerName()["app"].SaveToCheckpoint()
	require.NoError(t, err)
	changed, err := vpaClient.AutoscalingV1().VerticalPodAutoscalers("apps").Get(ctx, "aggressive", metav1.GetOptions{})
	require.NoError(t, err)
	changed.Spec.ResourcePolicy.ContainerPolicies[0].CPUHistogramDecayHalfLife.Duration = 4 * time.Minute
	changed.Spec.UpdatePolicy.PodUpdateThreshold = ptr.To(2.0)
	changed, err = vpaClient.AutoscalingV1().VerticalPodAutoscalers("apps").Update(ctx, changed, metav1.UpdateOptions{})
	require.NoError(t, err)
	deployment, err := kube.AppsV1().Deployments("apps").Get(ctx, "aggressive", metav1.GetOptions{})
	require.NoError(t, err)
	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	require.NoError(t, err)
	require.NoError(t, cluster.AddOrUpdateVpa(changed, selector))
	after, err := cluster.VPAs()[aggressiveID].AggregateStateByContainerName()["app"].SaveToCheckpoint()
	require.NoError(t, err)
	assert.Equal(t, before.CPUHistogram, after.CPUHistogram, "changing a profile must preserve the already-weighted history")
	require.NoError(t, cluster.AddSample(&model.ContainerUsageSampleWithKey{
		Container: model.ContainerID{PodID: model.PodID{Namespace: "apps", PodName: "aggressive"}, ContainerName: "app"},
		ContainerUsageSample: model.ContainerUsageSample{
			MeasureStart: start.Add(12 * time.Minute), Resource: model.ResourceCPU, Usage: model.CPUAmountFromCores(2),
		},
	}))
	updated := evaluateConcurrently()
	assert.Equal(t, 4*time.Minute, updated["aggressive"].halfLife)
	assert.Equal(t, 2.0, *updated["aggressive"].vpa.Spec.UpdatePolicy.PodUpdateThreshold)
	assert.Greater(t, updated["aggressive"].cpu, initial["aggressive"].cpu)
	assert.False(t, updated["aggressive"].priority.OutsideRecommendedRange)
	assert.False(t, updated["aggressive"].wantsUpdate, "the higher threshold must take effect on the next pass")
	assert.Equal(t, 10*time.Minute, updated["conservative"].halfLife)
	assert.Equal(t, 0.5, *updated["conservative"].vpa.Spec.UpdatePolicy.PodUpdateThreshold)
	assert.Equal(t, initial["conservative"].vpa.Status.Recommendation, updated["conservative"].vpa.Status.Recommendation)
	otherAfter, err := cluster.VPAs()[conservativeID].AggregateStateByContainerName()["app"].SaveToCheckpoint()
	require.NoError(t, err)
	assert.Equal(t, otherBefore.CPUHistogram, otherAfter.CPUHistogram, "the other workload's history must not be changed")
	assert.Equal(t, otherBefore.TotalSamplesCount, otherAfter.TotalSamplesCount)
	latest, err := cluster.VPAs()[aggressiveID].AggregateStateByContainerName()["app"].SaveToCheckpoint()
	require.NoError(t, err)
	assert.Equal(t, 3, latest.TotalSamplesCount)
	options := model.GetAggregationsConfig().CPUHistogramOptions
	lowWeight := float64(latest.CPUHistogram.BucketWeights[options.FindBucket(0.1)])
	require.Positive(t, lowWeight)
	assert.InDelta(t, 0.5, float64(latest.CPUHistogram.BucketWeights[options.FindBucket(2)])/lowWeight, 0.0002,
		"the post-change sample must use the updated four-minute half-life")
	for _, name := range []string{"aggressive", "conservative"} {
		result := updated[name]
		t.Logf("after changing aggressive only: %s half-life=%s CPU=%dm threshold=%.2f update=%t",
			name, result.halfLife, result.cpu, *result.vpa.Spec.UpdatePolicy.PodUpdateThreshold, result.wantsUpdate)
	}
}
