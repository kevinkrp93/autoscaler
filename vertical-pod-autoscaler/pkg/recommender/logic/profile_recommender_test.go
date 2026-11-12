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
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

func TestSharedRecommenderProducesDifferentCPURecommendationsForProfiles(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rising := make([]model.ContainerUsageSample, 0, 61)
	for minute := range 60 {
		rising = append(rising, model.ContainerUsageSample{
			MeasureStart: start.Add(time.Duration(minute) * time.Minute), Resource: model.ResourceCPU, Usage: model.CPUAmountFromCores(0.1),
		})
	}
	rising = append(rising, model.ContainerUsageSample{
		MeasureStart: start.Add(time.Hour), Resource: model.ResourceCPU, Usage: model.CPUAmountFromCores(1),
	})
	for _, scenario := range []struct {
		name        string
		samples     []model.ContainerUsageSample
		expectedCPU []float64
	}{
		{
			name: "falling-load",
			samples: []model.ContainerUsageSample{
				{MeasureStart: start, Resource: model.ResourceCPU, Usage: model.CPUAmountFromCores(1)},
				{MeasureStart: start.Add(20 * time.Minute), Resource: model.ResourceCPU, Usage: model.CPUAmountFromCores(0.1)},
			},
			expectedCPU: []float64{0.1, 1},
		},
		{name: "rising-burst", samples: rising, expectedCPU: []float64{1, 0.1}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			cluster := model.NewClusterState(time.Minute)
			recommender := CreatePodResourceRecommender()
			recommendations := make([]RecommendedContainerResources, 0, 2)
			for _, profile := range []struct {
				name     string
				halfLife time.Duration
			}{
				{"aggressive", 5 * time.Minute}, {"conservative", 24 * time.Hour},
			} {
				podID := model.PodID{Namespace: "apps", PodName: profile.name}
				podLabels := labels.Set{"app": profile.name}
				object := &vpa_types.VerticalPodAutoscaler{
					ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: profile.name},
					Spec: vpa_types.VerticalPodAutoscalerSpec{
						ResourcePolicy: &vpa_types.PodResourcePolicy{ContainerPolicies: []vpa_types.ContainerResourcePolicy{{
							ContainerName: "*", CPUHistogramDecayHalfLife: &metav1.Duration{Duration: profile.halfLife},
						}}},
					},
				}
				require.NoError(t, cluster.AddOrUpdateVpa(object, podLabels.AsSelector()))
				cluster.AddOrUpdatePod(podID, podLabels, corev1.PodRunning)
				id := model.ContainerID{PodID: podID, ContainerName: "app"}
				require.NoError(t, cluster.AddOrUpdateContainer(id, nil))
				for _, sample := range scenario.samples {
					require.NoError(t, cluster.AddSample(&model.ContainerUsageSampleWithKey{Container: id, ContainerUsageSample: sample}))
				}
				require.NoError(t, cluster.AddSample(&model.ContainerUsageSampleWithKey{
					Container: id,
					ContainerUsageSample: model.ContainerUsageSample{
						MeasureStart: start, Resource: model.ResourceMemory, Usage: model.MemoryAmountFromBytes(1 << 30),
					},
				}))
				state := cluster.VPAs()[model.VpaID{Namespace: "apps", VpaName: profile.name}].AggregateStateByContainerName()
				result := recommender.GetRecommendedPodResources(state)
				require.Len(t, result, 1)
				require.Contains(t, result, "app")
				require.Len(t, result["app"].Target, 2)
				recommendations = append(recommendations, result["app"])
			}
			options := model.GetAggregationsConfig().CPUHistogramOptions
			for i, usage := range scenario.expectedCPU {
				percentileCPU := model.CPUAmountFromCores(options.GetBucketStart(options.FindBucket(usage) + 1))
				expectedCPU := percentileCPU + model.ScaleResource(percentileCPU, *safetyMarginFraction)
				assert.Equal(t, expectedCPU, recommendations[i].Target[model.ResourceCPU])
			}
			assert.NotEqual(t, recommendations[0].Target[model.ResourceCPU], recommendations[1].Target[model.ResourceCPU])
			assert.Equal(t, recommendations[0].Target[model.ResourceMemory], recommendations[1].Target[model.ResourceMemory])
			assert.Equal(t, recommendations[0].LowerBound[model.ResourceMemory], recommendations[1].LowerBound[model.ResourceMemory])
			assert.Equal(t, recommendations[0].UpperBound[model.ResourceMemory], recommendations[1].UpperBound[model.ResourceMemory])
		})
	}
}
