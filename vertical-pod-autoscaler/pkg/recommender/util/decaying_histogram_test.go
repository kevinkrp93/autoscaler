/*
Copyright 2018 The Kubernetes Authors.

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

package util

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
)

func TestMergeAfterHalfLifeChangeAlignsNonMultipleReferenceTimestamps(t *testing.T) {
	firstTime := time.Date(2026, 1, 1, 6, 0, 0, 0, time.UTC)
	secondTime := firstTime.Add(6 * time.Hour)
	for _, restored := range []bool{false, true} {
		for _, reversed := range []bool{false, true} {
			t.Run(fmt.Sprintf("checkpoint-%t/reversed-%t", restored, reversed), func(t *testing.T) {
				first := NewDecayingHistogram(testHistogramOptions, 5*time.Minute)
				first.AddSample(1, 1, firstTime)
				second := NewDecayingHistogram(testHistogramOptions, 5*time.Minute)
				second.AddSample(4, 1, secondTime)
				convert := func(source Histogram) Histogram {
					if !restored {
						return CloneDecayingHistogramWithHalfLife(source, 24*time.Hour)
					}
					checkpoint, err := source.SaveToChekpoint()
					require.NoError(t, err)
					result := NewDecayingHistogram(testHistogramOptions, 24*time.Hour)
					require.NoError(t, result.LoadFromCheckpoint(checkpoint))
					return result
				}
				left, right := convert(first), convert(second)
				if reversed {
					left, right = right, left
				}
				left.Merge(right)
				histogram := left.(*decayingHistogram)
				firstWeight := histogram.bucketWeight[testHistogramOptions.FindBucket(1)]
				secondWeight := histogram.bucketWeight[testHistogramOptions.FindBucket(4)]
				assert.Equal(t, secondTime, histogram.referenceTimestamp,
					"merge must align to the actual reference, not round it to the new half-life")
				assert.InDelta(t, math.Exp2(-6.0/24.0), firstWeight/secondWeight, 1e-12,
					"reference alignment must uniformly scale old weights by the exact fractional decay")
				assert.Equal(t, float64(2), left.Percentile(0.4))
				assert.Equal(t, float64(5), left.Percentile(0.5))
			})
		}
	}
}

func TestHalfLifeChangeRenormalizesNonAlignedReferenceWithoutWeightBias(t *testing.T) {
	firstTime := time.Date(2026, 1, 1, 6, 5, 0, 0, time.UTC)
	source := NewDecayingHistogram(testHistogramOptions, 5*time.Minute)
	source.AddSample(1, 1e40, firstTime)
	changed := CloneDecayingHistogramWithHalfLife(source, 24*time.Hour).(*decayingHistogram)
	changed.AddSample(4, 1, firstTime.Add(101*24*time.Hour))
	firstWeight := changed.bucketWeight[testHistogramOptions.FindBucket(1)]
	secondWeight := changed.bucketWeight[testHistogramOptions.FindBucket(4)]
	require.Positive(t, secondWeight)
	assert.InEpsilon(t, 1e40*math.Exp2(-101), firstWeight/secondWeight, 1e-12,
		"renormalization must retain the exact decay from the preserved fractional reference")
	assert.Equal(t, 1e40, source.(*decayingHistogram).bucketWeight[testHistogramOptions.FindBucket(1)],
		"renormalizing the copy must not mutate the source")
}

var (
	startTime = time.Unix(1234567890, 0) // Arbitrary timestamp.
)

func TestCloneDecayingHistogramWithHalfLife(t *testing.T) {
	source := NewDecayingHistogram(testHistogramOptions, time.Hour)
	source.AddSample(1, 3, startTime)
	source.AddSample(4, 2, startTime.Add(time.Hour))
	original, err := source.SaveToChekpoint()
	assert.NoError(t, err)

	clone := CloneDecayingHistogramWithHalfLife(source, 5*time.Minute)
	copied, err := clone.SaveToChekpoint()
	assert.NoError(t, err)
	assert.Equal(t, original, copied, "changing decay must retain bucket weights and reference time")

	restored := NewDecayingHistogram(testHistogramOptions, 5*time.Minute)
	assert.NoError(t, restored.LoadFromCheckpoint(original))
	timestamp := startTime.Add(2 * time.Hour)
	clone.AddSample(2, 1, timestamp)
	restored.AddSample(2, 1, timestamp)
	assert.True(t, clone.Equals(restored), "live transition must match existing checkpoint restoration")
	unchanged, err := source.SaveToChekpoint()
	assert.NoError(t, err)
	assert.Equal(t, original, unchanged, "the source histogram must not be mutated")
}

// Verifies that Percentile() returns 0.0 when called on an empty decaying histogram
// for any percentile.
func TestPercentilesEmptyDecayingHistogram(t *testing.T) {
	h := NewDecayingHistogram(testHistogramOptions, time.Hour)
	for p := -0.5; p <= 1.5; p += 0.5 {
		assert.Equal(t, 0.0, h.Percentile(p))
	}
}

// Verify that a sample with a large weight is almost entirely (but not 100%)
// decayed after sufficient amount of time elapses.
func TestSimpleDecay(t *testing.T) {
	h := NewDecayingHistogram(testHistogramOptions, time.Hour)
	// Add a sample with a very large weight.
	h.AddSample(2, 1000, startTime)
	// Add another sample 20 half life periods later. Its relative weight is
	// expected to be 2^20 * 0.001 > 1000 times larger than the first sample.
	h.AddSample(1, 1, startTime.Add(time.Hour*20))
	assert.InEpsilon(t, 2, h.Percentile(0.999), valueEpsilon)
	assert.InEpsilon(t, 3, h.Percentile(1.0), valueEpsilon)
}

// Verify that the decaying histogram behaves correctly after the decaying
// factor grows by more than 2^maxDecayExponent.
func TestLongtermDecay(t *testing.T) {
	h := NewDecayingHistogram(testHistogramOptions, time.Hour)
	// Add a sample with a very large weight.
	h.AddSample(2, 1, startTime)
	// Add another sample later, such that the relative decay factor of the
	// two samples will exceed 2^maxDecayExponent.
	h.AddSample(1, 1, startTime.Add(time.Hour*101))
	assert.InEpsilon(t, 2, h.Percentile(1.0), valueEpsilon)
}

// Verify specific values of percentiles on an example decaying histogram with
// 4 samples added with different timestamps.
func TestDecayingHistogramPercentiles(t *testing.T) {
	h := NewDecayingHistogram(testHistogramOptions, time.Hour)
	timestamp := startTime
	// Add four samples with both values and weights equal to 1, 2, 3, 4,
	// each separated by one half life period from the previous one.
	for i := 1; i <= 4; i++ {
		h.AddSample(float64(i), float64(i), timestamp)
		timestamp = timestamp.Add(time.Hour)
	}
	// The expected distribution is:
	// bucket = [1..2], weight = 1 * 2^(-3), percentiles ~  0% ... 2%
	// bucket = [2..3], weight = 2 * 2^(-2), percentiles ~  3% ... 10%
	// bucket = [3..4], weight = 3 * 2^(-1), percentiles ~ 11% ... 34%
	// bucket = [4..5], weight = 4 * 2^(-0), percentiles ~ 35% ... 100%
	assert.InEpsilon(t, 2, h.Percentile(0.00), valueEpsilon)
	assert.InEpsilon(t, 2, h.Percentile(0.02), valueEpsilon)
	assert.InEpsilon(t, 3, h.Percentile(0.03), valueEpsilon)
	assert.InEpsilon(t, 3, h.Percentile(0.10), valueEpsilon)
	assert.InEpsilon(t, 4, h.Percentile(0.11), valueEpsilon)
	assert.InEpsilon(t, 4, h.Percentile(0.34), valueEpsilon)
	assert.InEpsilon(t, 5, h.Percentile(0.35), valueEpsilon)
	assert.InEpsilon(t, 5, h.Percentile(1.00), valueEpsilon)
}

// Verifies that the decaying histogram behaves the same way as a regular
// histogram if the time is fixed and no decaying happens.
func TestNoDecay(t *testing.T) {
	h := NewDecayingHistogram(testHistogramOptions, time.Hour)
	for i := 1; i <= 4; i++ {
		h.AddSample(float64(i), float64(i), startTime)
	}
	assert.InEpsilon(t, 2, h.Percentile(0.0), valueEpsilon)
	assert.InEpsilon(t, 3, h.Percentile(0.2), valueEpsilon)
	assert.InEpsilon(t, 2, h.Percentile(0.1), valueEpsilon)
	assert.InEpsilon(t, 3, h.Percentile(0.3), valueEpsilon)
	assert.InEpsilon(t, 4, h.Percentile(0.4), valueEpsilon)
	assert.InEpsilon(t, 4, h.Percentile(0.5), valueEpsilon)
	assert.InEpsilon(t, 4, h.Percentile(0.6), valueEpsilon)
	assert.InEpsilon(t, 5, h.Percentile(0.7), valueEpsilon)
	assert.InEpsilon(t, 5, h.Percentile(0.8), valueEpsilon)
	assert.InEpsilon(t, 5, h.Percentile(0.9), valueEpsilon)
	assert.InEpsilon(t, 5, h.Percentile(1.0), valueEpsilon)
}

// Verifies that Merge() works as expected on two sample decaying histograms.
func TestDecayingHistogramMerge(t *testing.T) {
	h1 := NewDecayingHistogram(testHistogramOptions, time.Hour)
	h1.AddSample(1, 1, startTime)
	h1.AddSample(2, 1, startTime.Add(time.Hour))

	h2 := NewDecayingHistogram(testHistogramOptions, time.Hour)
	h2.AddSample(2, 1, startTime.Add(time.Hour*2))
	h2.AddSample(3, 1, startTime.Add(time.Hour))

	expected := NewDecayingHistogram(testHistogramOptions, time.Hour)
	expected.AddSample(2, 1, startTime.Add(time.Hour*2))
	expected.AddSample(2, 1, startTime.Add(time.Hour))
	expected.AddSample(3, 1, startTime.Add(time.Hour))
	expected.AddSample(1, 1, startTime)

	h1.Merge(h2)
	assert.True(t, h1.Equals(expected))
}

func TestDecayingHistogramSaveToCheckpoint(t *testing.T) {
	d := &decayingHistogram{
		histogram:          *NewHistogram(testHistogramOptions).(*histogram),
		halfLife:           time.Hour,
		referenceTimestamp: time.Time{},
	}
	d.AddSample(2, 1, startTime.Add(time.Hour*100))
	assert.NotEqual(t, d.referenceTimestamp, time.Time{})

	checkpoint, err := d.SaveToChekpoint()
	assert.NoError(t, err)
	assert.Equal(t, checkpoint.ReferenceTimestamp.Time, d.referenceTimestamp)
	// Just check that buckets are not empty, actual testing of bucketing
	// belongs to Histogram
	assert.NotEmpty(t, checkpoint.BucketWeights)
	assert.NotZero(t, checkpoint.TotalWeight)
}

func TestDecayingHistogramLoadFromCheckpoint(t *testing.T) {
	location, _ := time.LoadLocation("UTC")
	timestamp := time.Date(2018, time.January, 2, 3, 4, 5, 0, location)

	checkpoint := vpa_types.HistogramCheckpoint{
		TotalWeight: 6.0,
		BucketWeights: map[int]uint32{
			0: 1,
		},
		ReferenceTimestamp: metav1.NewTime(timestamp),
	}
	d := &decayingHistogram{
		histogram:          *NewHistogram(testHistogramOptions).(*histogram),
		halfLife:           time.Hour,
		referenceTimestamp: time.Time{},
	}
	err := d.LoadFromCheckpoint(&checkpoint)
	assert.NoError(t, err)

	assert.False(t, d.histogram.IsEmpty())
	assert.Equal(t, timestamp, d.referenceTimestamp)
}
