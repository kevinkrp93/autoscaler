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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/ptr"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/features"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/updater/priority"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/test"
)

func TestSharedUpdaterReevaluatesEachVPAThresholdOnEveryPass(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	pod := test.Pod().WithName("pod").AddContainer(test.Container().WithName("app").
		WithCPURequest(resource.MustParse("4")).Get()).Get()
	pod.Status.StartTime = &metav1.Time{Time: time.Now().Add(-48 * time.Hour)}
	aggressive := test.VerticalPodAutoscaler().WithName("aggressive").WithContainer("app").
		WithTarget("5", "").WithLowerBound("1", "").WithUpperBound("8", "").Get()
	aggressive.Spec.UpdatePolicy = &vpa_types.PodUpdatePolicy{PodUpdateThreshold: ptr.To(0.1)}
	conservative := aggressive.DeepCopy()
	conservative.Name = "conservative"
	conservative.Spec.UpdatePolicy.PodUpdateThreshold = ptr.To(0.5)
	updater := &updater{
		recommendationProcessor: &test.FakeRecommendationProcessor{},
		priorityProcessor:       priority.NewProcessor(),
		evictionAdmission:       priority.NewDefaultPodEvictionAdmission(),
	}
	pods := []*corev1.Pod{pod}
	assert.Equal(t, pods, updater.getPodsUpdateOrder(pods, aggressive))
	assert.Empty(t, updater.getPodsUpdateOrder(pods, conservative))
	aggressive.Spec.UpdatePolicy.PodUpdateThreshold = ptr.To(0.75)
	conservative.Spec.UpdatePolicy.PodUpdateThreshold = ptr.To(0.2)
	assert.Empty(t, updater.getPodsUpdateOrder(pods, aggressive), "a changed policy must be applied on the next updater pass")
	assert.Equal(t, pods, updater.getPodsUpdateOrder(pods, conservative))
}

func TestSharedUpdaterAppliesEachVPALifetimeOverride(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, true)
	pod := test.Pod().WithName("pod").AddContainer(test.Container().WithName("app").
		WithCPURequest(resource.MustParse("4")).Get()).Get()
	pod.Status.StartTime = &metav1.Time{Time: time.Now().Add(-5 * time.Minute)}
	immediate := test.VerticalPodAutoscaler().WithName("immediate").WithContainer("app").
		WithTarget("5", "").WithLowerBound("1", "").WithUpperBound("8", "").Get()
	immediate.Spec.UpdatePolicy = &vpa_types.PodUpdatePolicy{
		InRecommendationBoundsEvictionLifetimeThreshold: &metav1.Duration{Duration: 0},
	}
	defaulted := immediate.DeepCopy()
	defaulted.Name = "defaulted"
	defaulted.Spec.UpdatePolicy = nil
	updater := &updater{
		recommendationProcessor: &test.FakeRecommendationProcessor{},
		priorityProcessor:       priority.NewProcessor(),
		evictionAdmission:       priority.NewDefaultPodEvictionAdmission(),
	}
	pods := []*corev1.Pod{pod}
	assert.Equal(t, pods, updater.getPodsUpdateOrder(pods, immediate), "0s override must allow a young in-range pod")
	assert.Empty(t, updater.getPodsUpdateOrder(pods, defaulted), "VPA without override must keep the 12h global default")

	immediate.Spec.UpdatePolicy.InRecommendationBoundsEvictionLifetimeThreshold = &metav1.Duration{Duration: time.Hour}
	defaulted.Spec.UpdatePolicy = &vpa_types.PodUpdatePolicy{
		InRecommendationBoundsEvictionLifetimeThreshold: &metav1.Duration{Duration: time.Minute},
	}
	assert.Empty(t, updater.getPodsUpdateOrder(pods, immediate), "changed override must apply on the next pass")
	assert.Equal(t, pods, updater.getPodsUpdateOrder(pods, defaulted))

	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, false)
	immediate.Spec.UpdatePolicy.InRecommendationBoundsEvictionLifetimeThreshold = &metav1.Duration{Duration: 0}
	assert.Empty(t, updater.getPodsUpdateOrder(pods, immediate), "override must be ignored when PerVPAConfig is disabled")
}
