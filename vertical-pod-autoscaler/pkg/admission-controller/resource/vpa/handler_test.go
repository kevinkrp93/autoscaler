/*
Copyright 2019 The Kubernetes Authors.

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

package vpa

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/version"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/ptr"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/features"
)

const (
	cpu    = apiv1.ResourceCPU
	memory = apiv1.ResourceMemory
)

func TestValidateProfileConfiguration(t *testing.T) {
	tests := []struct {
		name      string
		enabled   bool
		halfLife  *metav1.Duration
		threshold *float64
		errorText string
	}{
		{name: "valid", enabled: true, halfLife: &metav1.Duration{Duration: 5 * time.Minute}, threshold: ptr.To(0.1)},
		{name: "zero threshold", enabled: true, threshold: ptr.To(0.0)},
		{name: "defaults with feature disabled"},
		{name: "negative threshold", enabled: true, threshold: ptr.To(-0.1), errorText: "podUpdateThreshold must"},
		{name: "zero half life", enabled: true, halfLife: &metav1.Duration{}, errorText: "cpuHistogramDecayHalfLife must"},
		{name: "negative half life", enabled: true, halfLife: &metav1.Duration{Duration: -time.Minute}, errorText: "cpuHistogramDecayHalfLife must"},
		{name: "half life feature disabled", halfLife: &metav1.Duration{Duration: time.Hour}, errorText: "PerVPAConfig is disabled"},
		{name: "threshold feature disabled", threshold: ptr.To(0.1), errorText: "PerVPAConfig is disabled"},
		{name: "zero threshold feature disabled", threshold: ptr.To(0.0), errorText: "PerVPAConfig is disabled"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, tc.enabled)
			mode := vpa_types.UpdateModeOff
			vpa := &vpa_types.VerticalPodAutoscaler{Spec: vpa_types.VerticalPodAutoscalerSpec{
				TargetRef:    &autoscalingv1.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "example"},
				UpdatePolicy: &vpa_types.PodUpdatePolicy{UpdateMode: &mode, PodUpdateThreshold: tc.threshold},
				ResourcePolicy: &vpa_types.PodResourcePolicy{ContainerPolicies: []vpa_types.ContainerResourcePolicy{
					{ContainerName: "*", CPUHistogramDecayHalfLife: tc.halfLife},
				}},
			}}
			for _, isCreate := range []bool{false, true} {
				t.Run(fmt.Sprintf("create-%t", isCreate), func(t *testing.T) {
					err := ValidateVPA(vpa, isCreate)
					if tc.errorText == "" {
						assert.NoError(t, err)
					} else {
						assert.ErrorContains(t, err, tc.errorText)
					}
				})
			}
		})
	}
}

func TestValidateVPA(t *testing.T) {
	badUpdateMode := vpa_types.UpdateMode("bad")
	validUpdateMode := vpa_types.UpdateModeOff
	badMinReplicas := int32(0)
	validMinReplicas := int32(1)
	badScalingMode := vpa_types.ContainerScalingMode("bad")
	badCPUResource := resource.MustParse("187500u")
	validScalingMode := vpa_types.ContainerScalingModeAuto
	scalingModeOff := vpa_types.ContainerScalingModeOff
	controlledValuesRequestsAndLimits := vpa_types.ContainerControlledValuesRequestsAndLimits
	inPlaceOrRecreateUpdateMode := vpa_types.UpdateModeInPlaceOrRecreate
	tests := []struct {
		name                                 string
		vpa                                  vpa_types.VerticalPodAutoscaler
		isCreate                             bool
		expectError                          error
		inPlaceOrRecreateFeatureGateDisabled bool
		PerVPAConfigDisabled                 bool
	}{
		{
			name: "empty update",
			vpa:  vpa_types.VerticalPodAutoscaler{},
		},
		{
			name:        "empty create",
			vpa:         vpa_types.VerticalPodAutoscaler{},
			isCreate:    true,
			expectError: errors.New("targetRef is required. If you're using v1beta1 version of the API, please migrate to v1"),
		},
		{
			name: "no update mode",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					UpdatePolicy: &vpa_types.PodUpdatePolicy{},
				},
			},
			expectError: errors.New("updateMode is required if UpdatePolicy is used"),
		},
		{
			name: "bad update mode",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					UpdatePolicy: &vpa_types.PodUpdatePolicy{
						UpdateMode: &badUpdateMode,
					},
				},
			},
			expectError: errors.New("unexpected UpdateMode value bad"),
		},
		{
			name: "creating VPA with InPlaceOrRecreate update mode not allowed by disabled feature gate",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					UpdatePolicy: &vpa_types.PodUpdatePolicy{
						UpdateMode: &inPlaceOrRecreateUpdateMode,
					},
				},
			},
			isCreate:                             true,
			inPlaceOrRecreateFeatureGateDisabled: true,
			expectError:                          fmt.Errorf("in order to use UpdateMode %s, you must enable feature gate %s in the admission-controller args", vpa_types.UpdateModeInPlaceOrRecreate, features.InPlaceOrRecreate),
		},
		{
			name: "updating VPA with InPlaceOrRecreate update mode allowed by disabled feature gate",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					UpdatePolicy: &vpa_types.PodUpdatePolicy{
						UpdateMode: &inPlaceOrRecreateUpdateMode,
					},
				},
			},
			isCreate:                             false,
			inPlaceOrRecreateFeatureGateDisabled: true,
			expectError:                          nil,
		},
		{
			name: "InPlaceOrRecreate update mode enabled by feature gate",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					UpdatePolicy: &vpa_types.PodUpdatePolicy{
						UpdateMode: &inPlaceOrRecreateUpdateMode,
					},
				},
			},
		},
		{
			name: "zero minReplicas",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					UpdatePolicy: &vpa_types.PodUpdatePolicy{
						MinReplicas: &badMinReplicas,
						UpdateMode:  &validUpdateMode,
					},
				},
			},
			expectError: errors.New("minReplicas has to be positive, got 0"),
		},
		{
			name: "no policy name",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					ResourcePolicy: &vpa_types.PodResourcePolicy{
						ContainerPolicies: []vpa_types.ContainerResourcePolicy{{}},
					},
				},
			},
			expectError: errors.New("containerPolicies.ContainerName is required"),
		},
		{
			name: "invalid scaling mode",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					ResourcePolicy: &vpa_types.PodResourcePolicy{
						ContainerPolicies: []vpa_types.ContainerResourcePolicy{
							{
								ContainerName: "loot box",
								Mode:          &badScalingMode,
							},
						},
					},
				},
			},
			expectError: errors.New("unexpected Mode value bad"),
		},
		{
			name: "more than one recommender",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					UpdatePolicy: &vpa_types.PodUpdatePolicy{
						UpdateMode: &validUpdateMode,
					},
					Recommenders: []*vpa_types.VerticalPodAutoscalerRecommenderSelector{
						{Name: "test1"},
						{Name: "test2"},
					},
				},
			},
			expectError: errors.New("the current version of VPA object shouldn't specify more than one recommenders"),
		},
		{
			name: "bad limits",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					ResourcePolicy: &vpa_types.PodResourcePolicy{
						ContainerPolicies: []vpa_types.ContainerResourcePolicy{
							{
								ContainerName: "loot box",
								MinAllowed: apiv1.ResourceList{
									cpu: resource.MustParse("100"),
								},
								MaxAllowed: apiv1.ResourceList{
									cpu: resource.MustParse("10"),
								},
							},
						},
					},
				},
			},
			expectError: errors.New("max resource for cpu is lower than min"),
		},
		{
			name: "bad minAllowed cpu value",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					ResourcePolicy: &vpa_types.PodResourcePolicy{
						ContainerPolicies: []vpa_types.ContainerResourcePolicy{
							{
								ContainerName: "loot box",
								MinAllowed: apiv1.ResourceList{
									cpu: resource.MustParse("187500u"),
								},
								MaxAllowed: apiv1.ResourceList{
									cpu: resource.MustParse("275m"),
								},
							},
						},
					},
				},
			},
			expectError: fmt.Errorf("minAllowed: CPU [%v] must be a whole number of milli CPUs", badCPUResource.String()),
		},
		{
			name: "bad minAllowed memory value",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					ResourcePolicy: &vpa_types.PodResourcePolicy{
						ContainerPolicies: []vpa_types.ContainerResourcePolicy{
							{
								ContainerName: "loot box",
								MinAllowed: apiv1.ResourceList{
									cpu:    resource.MustParse("1m"),
									memory: resource.MustParse("100m"),
								},
								MaxAllowed: apiv1.ResourceList{
									cpu:    resource.MustParse("275m"),
									memory: resource.MustParse("500M"),
								},
							},
						},
					},
				},
			},
			expectError: fmt.Errorf("minAllowed: memory [%v] must be a whole number of bytes", resource.MustParse("100m")),
		},
		{
			name: "bad maxAllowed cpu value",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					ResourcePolicy: &vpa_types.PodResourcePolicy{
						ContainerPolicies: []vpa_types.ContainerResourcePolicy{
							{
								ContainerName: "loot box",
								MinAllowed:    apiv1.ResourceList{},
								MaxAllowed: apiv1.ResourceList{
									cpu: resource.MustParse("187500u"),
								},
							},
						},
					},
				},
			},
			expectError: fmt.Errorf("maxAllowed: CPU [%s] must be a whole number of milli CPUs", badCPUResource.String()),
		},
		{
			name: "bad maxAllowed memory value",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					ResourcePolicy: &vpa_types.PodResourcePolicy{
						ContainerPolicies: []vpa_types.ContainerResourcePolicy{
							{
								ContainerName: "loot box",
								MinAllowed: apiv1.ResourceList{
									cpu: resource.MustParse("1m")},
								MaxAllowed: apiv1.ResourceList{
									cpu:    resource.MustParse("275m"),
									memory: resource.MustParse("500m"),
								},
							},
						},
					},
				},
			},
			expectError: fmt.Errorf("maxAllowed: memory [%v] must be a whole number of bytes", resource.MustParse("500m")),
		},
		{
			name: "scaling off with controlled values requests and limits",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					ResourcePolicy: &vpa_types.PodResourcePolicy{
						ContainerPolicies: []vpa_types.ContainerResourcePolicy{
							{
								ContainerName:    "loot box",
								Mode:             &scalingModeOff,
								ControlledValues: &controlledValuesRequestsAndLimits,
							},
						},
					},
				},
			},
			expectError: errors.New("controlledValues shouldn't be specified if container scaling mode is off"),
		},
		{
			name: "all valid",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					ResourcePolicy: &vpa_types.PodResourcePolicy{
						ContainerPolicies: []vpa_types.ContainerResourcePolicy{
							{
								ContainerName: "loot box",
								Mode:          &validScalingMode,
								MinAllowed: apiv1.ResourceList{
									cpu: resource.MustParse("10"),
								},
								MaxAllowed: apiv1.ResourceList{
									cpu: resource.MustParse("100"),
								},
							},
						},
					},
					UpdatePolicy: &vpa_types.PodUpdatePolicy{
						UpdateMode:  &validUpdateMode,
						MinReplicas: &validMinReplicas,
					},
				},
			},
		},
		{
			name: "per-vpa config active and used",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					UpdatePolicy: &vpa_types.PodUpdatePolicy{
						UpdateMode: &validUpdateMode,
					},
					ResourcePolicy: &vpa_types.PodResourcePolicy{
						ContainerPolicies: []vpa_types.ContainerResourcePolicy{
							{
								ContainerName: "loot box",
								Mode:          &validScalingMode,
								MinAllowed: apiv1.ResourceList{
									cpu: resource.MustParse("10"),
								},
								MaxAllowed: apiv1.ResourceList{
									cpu: resource.MustParse("100"),
								},
								OOMBumpUpRatio: resource.NewQuantity(2, resource.DecimalSI),
							},
						},
					},
				},
			},
			PerVPAConfigDisabled: false,
		},
		{
			name: "per-vpa config disabled and used",
			vpa: vpa_types.VerticalPodAutoscaler{
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					UpdatePolicy: &vpa_types.PodUpdatePolicy{
						UpdateMode: &validUpdateMode,
					},
					ResourcePolicy: &vpa_types.PodResourcePolicy{
						ContainerPolicies: []vpa_types.ContainerResourcePolicy{
							{
								ContainerName: "loot box",
								Mode:          &validScalingMode,
								MinAllowed: apiv1.ResourceList{
									cpu: resource.MustParse("10"),
								},
								MaxAllowed: apiv1.ResourceList{
									cpu: resource.MustParse("100"),
								},
								OOMMinBumpUp: resource.NewQuantity(2, resource.DecimalSI),
							},
						},
					},
				},
			},
			PerVPAConfigDisabled: true,
			expectError:          errors.New("per-VPA container configuration is not supported when feature flag PerVPAConfig is disabled"),
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("test case: %s", tc.name), func(t *testing.T) {
			if tc.inPlaceOrRecreateFeatureGateDisabled {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, features.MutableFeatureGate, version.MustParse("1.5"))
				featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.InPlaceOrRecreate, !tc.inPlaceOrRecreateFeatureGateDisabled)
			}
			featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.PerVPAConfig, !tc.PerVPAConfigDisabled)
			err := ValidateVPA(&tc.vpa, tc.isCreate)
			if tc.expectError == nil {
				assert.NoError(t, err)
			} else {
				if assert.Error(t, err) {
					assert.Equal(t, tc.expectError.Error(), err.Error())
				}
			}
		})
	}
}
