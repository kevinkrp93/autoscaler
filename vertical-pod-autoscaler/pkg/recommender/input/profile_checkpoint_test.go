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

package input

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	fakeclientset "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/fake"
	vpainformers "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/informers/externalversions"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/features"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	controllerfetcher "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/target/controller_fetcher"
	targetmock "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/target/mock"
)

func TestCheckpointStartupRetainsOldWeightsUnderNewProfileAndAcceptsLiveSamples(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	source := model.NewAggregateContainerStateWithCPUHistogramDecayHalfLife(5 * time.Minute)
	for _, sample := range []model.ContainerUsageSample{
		{MeasureStart: start, Resource: model.ResourceCPU, Usage: model.CPUAmountFromCores(1)},
		{MeasureStart: start.Add(20 * time.Minute), Resource: model.ResourceCPU, Usage: model.CPUAmountFromCores(0.1)},
		{MeasureStart: start, Resource: model.ResourceMemory, Usage: model.MemoryAmountFromBytes(1 << 30)},
	} {
		source.AddSample(&sample)
	}
	saved, err := source.SaveToCheckpoint()
	require.NoError(t, err)
	original := saved.DeepCopy()
	object := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "profile"},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			TargetRef: &autoscalingv1.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "example"},
			ResourcePolicy: &vpa_types.PodResourcePolicy{ContainerPolicies: []vpa_types.ContainerResourcePolicy{{
				ContainerName: "*", CPUHistogramDecayHalfLife: &metav1.Duration{Duration: 10 * time.Minute},
			}}},
		},
	}
	checkpoint := &vpa_types.VerticalPodAutoscalerCheckpoint{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "profile-app"},
		Spec:       vpa_types.VerticalPodAutoscalerCheckpointSpec{VPAObjectName: "profile", ContainerName: "app"},
		Status:     *saved,
	}
	client := fakeclientset.NewSimpleClientset(object, checkpoint)
	factory := vpainformers.NewSharedInformerFactory(client, 0)
	vpas := factory.Autoscaling().V1().VerticalPodAutoscalers()
	checkpoints := factory.Autoscaling().V1().VerticalPodAutoscalerCheckpoints()
	vpas.Informer()
	checkpoints.Informer()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	factory.Start(ctx.Done())
	require.True(t, cache.WaitForCacheSync(ctx.Done(), vpas.Informer().HasSynced, checkpoints.Informer().HasSynced))
	t.Cleanup(func() {
		cancel()
		factory.Shutdown()
	})
	selector := labels.Set{"app": "example"}
	fetcher := targetmock.NewMockVpaTargetSelectorFetcher(gomock.NewController(t))
	fetcher.EXPECT().Fetch(gomock.Any()).Return(selector.AsSelector(), nil)
	cluster := model.NewClusterState(time.Minute)
	feeder := clusterStateFeeder{
		clusterState: cluster, vpaLister: vpas.Lister(), vpaCheckpointLister: checkpoints.Lister(),
		selectorFetcher: fetcher, controllerFetcher: controllerfetcher.FakeControllerFetcher{}, recommenderName: DefaultRecommenderName,
	}
	feeder.InitFromCheckpoints(ctx)
	vpa := cluster.VPAs()[model.VpaID{Namespace: "apps", VpaName: "profile"}]
	require.NotNil(t, vpa)
	require.Contains(t, vpa.ContainersInitialAggregateState, "app")
	restored := vpa.AggregateStateByContainerName()["app"]
	assert.Equal(t, 10*time.Minute, restored.CPUHistogramDecayHalfLife)
	assert.Equal(t, 2, restored.TotalSamplesCount)
	cpu, err := restored.AggregateCPUUsage.SaveToChekpoint()
	require.NoError(t, err)
	assert.Equal(t, saved.CPUHistogram.BucketWeights, cpu.BucketWeights)

	podID := model.PodID{Namespace: "apps", PodName: "pod"}
	id := model.ContainerID{PodID: podID, ContainerName: "app"}
	cluster.AddOrUpdatePod(podID, selector, corev1.PodRunning)
	require.NoError(t, cluster.AddOrUpdateContainer(id, nil))
	next := start.Add(25 * time.Minute)
	require.NoError(t, cluster.AddSample(&model.ContainerUsageSampleWithKey{
		Container: id, ContainerUsageSample: model.ContainerUsageSample{
			MeasureStart: next, Resource: model.ResourceCPU, Usage: model.CPUAmountFromCores(0.4),
		},
	}))
	combined := vpa.AggregateStateByContainerName()["app"]
	assert.Equal(t, 3, combined.TotalSamplesCount)
	cpu, err = combined.AggregateCPUUsage.SaveToChekpoint()
	require.NoError(t, err)
	options := model.GetAggregationsConfig().CPUHistogramOptions
	low := float64(cpu.BucketWeights[options.FindBucket(0.1)])
	require.Positive(t, low)
	assert.InDelta(t, 1.0/16.0, float64(cpu.BucketWeights[options.FindBucket(1)])/low, 0.0002,
		"checkpointed samples must retain their original relative weights")
	newWeight := math.Exp2(float64(next.Sub(saved.CPUHistogram.ReferenceTimestamp.Time)) / float64(10*time.Minute))
	oldWeight := math.Exp2(float64(start.Add(20*time.Minute).Sub(saved.CPUHistogram.ReferenceTimestamp.Time)) / float64(5*time.Minute))
	assert.InDelta(t, newWeight/oldWeight, float64(cpu.BucketWeights[options.FindBucket(0.4)])/low, 0.0002,
		"post-restart samples must use the new VPA half-life")
	memory, err := combined.AggregateMemoryPeaks.SaveToChekpoint()
	require.NoError(t, err)
	assert.Equal(t, saved.MemoryHistogram, *memory)
	assert.Equal(t, original, saved, "restoring or extending history must not mutate the serialized checkpoint")
}
