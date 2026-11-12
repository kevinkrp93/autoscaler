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

package checkpoint

import (
	"context"
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
	fakeclientset "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/fake"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/features"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

func TestProfileCheckpointWriterKeepsSeparateCPUWeightsAndDropsOnlyCurrentMemoryPeak(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	cluster := model.NewClusterState(time.Minute)
	start := time.Now().UTC().Add(-30 * time.Hour)
	podID := model.PodID{Namespace: "apps", PodName: "pod"}
	id := model.ContainerID{PodID: podID, ContainerName: "app"}
	podLabels := labels.Set{"app": "example"}
	halfLives := map[string]time.Duration{"aggressive": 5 * time.Minute, "conservative": 24 * time.Hour}
	for name, halfLife := range halfLives {
		object := &vpa_types.VerticalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: name},
			Spec: vpa_types.VerticalPodAutoscalerSpec{
				ResourcePolicy: &vpa_types.PodResourcePolicy{ContainerPolicies: []vpa_types.ContainerResourcePolicy{{
					ContainerName: "*", CPUHistogramDecayHalfLife: &metav1.Duration{Duration: halfLife},
				}}},
			},
		}
		require.NoError(t, cluster.AddOrUpdateVpa(object, podLabels.AsSelector()))
	}
	cluster.AddOrUpdatePod(podID, podLabels, corev1.PodRunning)
	require.NoError(t, cluster.AddOrUpdateContainer(id, nil))
	for _, sample := range []model.ContainerUsageSample{
		{MeasureStart: start, Resource: model.ResourceCPU, Usage: model.CPUAmountFromCores(1)},
		{MeasureStart: start.Add(20 * time.Minute), Resource: model.ResourceCPU, Usage: model.CPUAmountFromCores(0.1)},
		{MeasureStart: start, Resource: model.ResourceMemory, Usage: model.MemoryAmountFromBytes(1 << 30)},
		{MeasureStart: start.Add(24 * time.Hour), Resource: model.ResourceMemory, Usage: model.MemoryAmountFromBytes(2 << 30)},
	} {
		require.NoError(t, cluster.AddSample(&model.ContainerUsageSampleWithKey{Container: id, ContainerUsageSample: sample}))
	}
	client := fakeclientset.NewSimpleClientset()
	writer := NewCheckpointWriter(cluster, client.AutoscalingV1())
	writer.StoreCheckpoints(context.Background(), 1)
	checkpoints, err := client.AutoscalingV1().VerticalPodAutoscalerCheckpoints("apps").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, checkpoints.Items, 2)
	options := model.GetAggregationsConfig().CPUHistogramOptions
	for _, checkpoint := range checkpoints.Items {
		assert.Equal(t, "app", checkpoint.Spec.ContainerName)
		assert.Equal(t, 2, checkpoint.Status.TotalSamplesCount, "shared/default aggregates must not duplicate CPU samples")
		assert.Equal(t, model.SupportedCheckpointVersion, checkpoint.Status.Version)
		halfLife, exists := halfLives[checkpoint.Spec.VPAObjectName]
		require.True(t, exists)
		cpu := checkpoint.Status.CPUHistogram.BucketWeights
		require.Positive(t, cpu[options.FindBucket(0.1)])
		assert.InDelta(t, math.Exp2(-float64(20*time.Minute)/float64(halfLife)),
			float64(cpu[options.FindBucket(1)])/float64(cpu[options.FindBucket(0.1)]), 0.0002)
		restored := model.NewAggregateContainerStateWithCPUHistogramDecayHalfLife(halfLife)
		require.NoError(t, restored.LoadFromCheckpoint(&checkpoint.Status))
		assert.GreaterOrEqual(t, restored.AggregateMemoryPeaks.Percentile(1), float64(1<<30))
		assert.Less(t, restored.AggregateMemoryPeaks.Percentile(1), float64(2<<30),
			"the current memory peak must be excluded from this VPA's checkpoint")
		live := cluster.VPAs()[model.VpaID{Namespace: "apps", VpaName: checkpoint.Spec.VPAObjectName}].AggregateStateByContainerName()["app"]
		assert.GreaterOrEqual(t, live.AggregateMemoryPeaks.Percentile(1), float64(2<<30),
			"checkpoint construction must not subtract peaks from live history")
	}
}
