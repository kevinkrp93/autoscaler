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

package v1

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestProfileFieldsSurviveJSONAndDeepCopy(t *testing.T) {
	object := &VerticalPodAutoscaler{
		Spec: VerticalPodAutoscalerSpec{
			UpdatePolicy: &PodUpdatePolicy{PodUpdateThreshold: ptr.To(0.0), InRecommendationBoundsEvictionLifetimeThreshold: &metav1.Duration{}},
			ResourcePolicy: &PodResourcePolicy{ContainerPolicies: []ContainerResourcePolicy{{
				ContainerName: "*", CPUHistogramDecayHalfLife: &metav1.Duration{Duration: 5 * time.Minute},
			}}},
		},
	}
	encoded, err := json.Marshal(object)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"podUpdateThreshold":0`, "an explicit zero must not be omitted")
	assert.Contains(t, string(encoded), `"inRecommendationBoundsEvictionLifetimeThreshold":"0s"`)
	assert.Contains(t, string(encoded), `"cpuHistogramDecayHalfLife":"5m0s"`)
	var decoded VerticalPodAutoscaler
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.NotNil(t, decoded.Spec.UpdatePolicy.PodUpdateThreshold)
	assert.Zero(t, *decoded.Spec.UpdatePolicy.PodUpdateThreshold)
	require.NotNil(t, decoded.Spec.UpdatePolicy.InRecommendationBoundsEvictionLifetimeThreshold)
	assert.Zero(t, decoded.Spec.UpdatePolicy.InRecommendationBoundsEvictionLifetimeThreshold.Duration)
	assert.Equal(t, 5*time.Minute, decoded.Spec.ResourcePolicy.ContainerPolicies[0].CPUHistogramDecayHalfLife.Duration)
	copy := decoded.DeepCopy()
	*copy.Spec.UpdatePolicy.PodUpdateThreshold = 0.5
	copy.Spec.UpdatePolicy.InRecommendationBoundsEvictionLifetimeThreshold.Duration = time.Hour
	copy.Spec.ResourcePolicy.ContainerPolicies[0].CPUHistogramDecayHalfLife.Duration = time.Hour
	assert.Zero(t, *decoded.Spec.UpdatePolicy.PodUpdateThreshold)
	assert.Zero(t, decoded.Spec.UpdatePolicy.InRecommendationBoundsEvictionLifetimeThreshold.Duration)
	assert.Equal(t, 5*time.Minute, decoded.Spec.ResourcePolicy.ContainerPolicies[0].CPUHistogramDecayHalfLife.Duration,
		"mutating a copy must not change the object held in an informer cache")
}
