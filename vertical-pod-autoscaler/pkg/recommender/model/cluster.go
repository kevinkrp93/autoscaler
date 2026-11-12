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

package model

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/features"
	controllerfetcher "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/target/controller_fetcher"
	vpa_utils "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

const (
	// RecommendationMissingMaxDuration is maximum time that we accept the recommendation can be missing.
	RecommendationMissingMaxDuration = 30 * time.Minute
)

// ClusterState holds all runtime information about the cluster required for the
// VPA operations, i.e. configuration of resources (pods, containers,
// VPA objects), aggregated utilization of compute resources (CPU, memory) and
// events (container OOMs).
// All input to the VPA Recommender algorithm lives in this structure.
type ClusterState interface {
	StateMapSize() int
	AddOrUpdatePod(podID PodID, newLabels labels.Set, phase apiv1.PodPhase)
	GetContainer(containerID ContainerID) *ContainerState
	DeletePod(podID PodID)
	AddOrUpdateContainer(containerID ContainerID, request Resources) error
	AddSample(sample *ContainerUsageSampleWithKey) error
	RecordOOM(containerID ContainerID, timestamp time.Time, requestedMemory ResourceAmount) error
	AddOrUpdateVpa(apiObject *vpa_types.VerticalPodAutoscaler, selector labels.Selector) error
	DeleteVpa(vpaID VpaID) error
	MakeAggregateStateKey(pod *PodState, containerName string) AggregateStateKey
	RateLimitedGarbageCollectAggregateCollectionStates(ctx context.Context, now time.Time, controllerFetcher controllerfetcher.ControllerFetcher)
	RecordRecommendation(vpa *Vpa, now time.Time) error
	GetMatchingPods(vpa *Vpa) []PodID
	GetControllerForPodUnderVPA(ctx context.Context, pod *PodState, controllerFetcher controllerfetcher.ControllerFetcher) *controllerfetcher.ControllerKeyWithAPIVersion
	GetControllingVPA(pod *PodState) *Vpa
	VPAs() map[VpaID]*Vpa
	SetObservedVPAs([]*vpa_types.VerticalPodAutoscaler)
	ObservedVPAs() []*vpa_types.VerticalPodAutoscaler
	Pods() map[PodID]*PodState
}

type clusterState struct {
	// Pods in the cluster.
	pods map[PodID]*PodState
	// VPA objects in the cluster.
	vpas map[VpaID]*Vpa
	// VPA objects in the cluster that have no recommendation mapped to the first
	// time we've noticed the recommendation missing or last time we logged
	// a warning about it.
	// TODO consider switching to a sync.Map for emptyVPAs
	emptyVPAs map[VpaID]time.Time
	// Observed VPAs. Used to check if there are updates needed.
	observedVPAs []*vpa_types.VerticalPodAutoscaler

	// All container aggregations where the usage samples are stored.
	aggregateStateMap aggregateContainerStatesMap
	// Map with all label sets used by the aggregations. It serves as a cache
	// that allows to quickly access labels.Set corresponding to a labelSetKey.
	labelSetMap labelSetMap

	lastAggregateContainerStateGC time.Time
	gcInterval                    time.Duration

	// Mutex to protect concurrent access to maps
	mutex sync.RWMutex
}

// StateMapSize is the number of pods being tracked by the VPA
func (cluster *clusterState) StateMapSize() int {
	return len(cluster.aggregateStateMap)
}

// AggregateStateKey determines the set of containers for which the usage samples
// are kept aggregated in the model.
type AggregateStateKey interface {
	Namespace() string
	ContainerName() string
	Labels() labels.Labels
	LabelSetKey() string
	CPUHistogramDecayHalfLife() time.Duration
	OwnerVpaID() VpaID
}

// String representation of the labels.LabelSet. This is the value returned by
// labelSet.String(). As opposed to the LabelSet object, it can be used as a map key.
type labelSetKey string

// Map of label sets keyed by their string representation.
type labelSetMap map[labelSetKey]labels.Set

// AggregateContainerStatesMap is a map from AggregateStateKey to AggregateContainerState.
type aggregateContainerStatesMap map[AggregateStateKey]*AggregateContainerState

// PodState holds runtime information about a single Pod.
type PodState struct {
	// Unique id of the Pod.
	ID PodID
	// Set of labels attached to the Pod.
	labelSetKey labelSetKey
	// Containers that belong to the Pod, keyed by the container name.
	Containers map[string]*ContainerState
	// InitContainers is a list of init containers names which belong to the Pod.
	InitContainers []string
	// PodPhase describing current life cycle phase of the Pod.
	Phase apiv1.PodPhase
}

// NewClusterState returns a new clusterState with no pods.
func NewClusterState(gcInterval time.Duration) *clusterState {
	return &clusterState{
		pods:                          make(map[PodID]*PodState),
		vpas:                          make(map[VpaID]*Vpa),
		emptyVPAs:                     make(map[VpaID]time.Time),
		aggregateStateMap:             make(aggregateContainerStatesMap),
		labelSetMap:                   make(labelSetMap),
		lastAggregateContainerStateGC: time.Unix(0, 0),
		gcInterval:                    gcInterval,
	}
}

// ContainerUsageSampleWithKey holds a ContainerUsageSample together with the
// ID of the container it belongs to.
type ContainerUsageSampleWithKey struct {
	ContainerUsageSample
	Container ContainerID
}

// AddOrUpdatePod updates the state of the pod with a given PodID, if it is
// present in the cluster object. Otherwise a new pod is created and added to
// the Cluster object.
// If the labels of the pod have changed, it updates the links between the containers
// and the aggregations.
func (cluster *clusterState) AddOrUpdatePod(podID PodID, newLabels labels.Set, phase apiv1.PodPhase) {
	pod, podExists := cluster.pods[podID]
	if !podExists {
		pod = newPod(podID)
		cluster.pods[podID] = pod
	}

	newlabelSetKey := cluster.getLabelSetKey(newLabels)
	if podExists && pod.labelSetKey != newlabelSetKey {
		// This Pod is already counted in the old VPA, remove the link.
		cluster.removePodFromItsVpa(pod)
	}
	if !podExists || pod.labelSetKey != newlabelSetKey {
		pod.labelSetKey = newlabelSetKey
		// Set the links between the containers and aggregations based on the current pod labels.
		for containerName, container := range pod.Containers {
			containerID := ContainerID{PodID: podID, ContainerName: containerName}
			cluster.findOrCreateAggregateContainerState(containerID)
			container.aggregator = NewContainerStateAggregatorProxy(cluster, containerID)
		}

		cluster.addPodToItsVpa(pod)
	}
	pod.Phase = phase
}

// addPodToItsVpa increases the count of Pods associated with a VPA object.
// Does a scan similar to findOrCreateAggregateContainerState so could be optimized if needed.
func (cluster *clusterState) addPodToItsVpa(pod *PodState) {
	for _, vpa := range cluster.vpas {
		if vpa_utils.PodLabelsMatchVPA(pod.ID.Namespace, cluster.labelSetMap[pod.labelSetKey], vpa.ID.Namespace, vpa.PodSelector) {
			vpa.PodCount++
		}
	}
}

// removePodFromItsVpa decreases the count of Pods associated with a VPA object.
func (cluster *clusterState) removePodFromItsVpa(pod *PodState) {
	for _, vpa := range cluster.vpas {
		if vpa_utils.PodLabelsMatchVPA(pod.ID.Namespace, cluster.labelSetMap[pod.labelSetKey], vpa.ID.Namespace, vpa.PodSelector) {
			vpa.PodCount--
		}
	}
}

// GetContainer returns the ContainerState object for a given ContainerID or
// null if it's not present in the model.
func (cluster *clusterState) GetContainer(containerID ContainerID) *ContainerState {
	pod, podExists := cluster.pods[containerID.PodID]
	if podExists {
		container, containerExists := pod.Containers[containerID.ContainerName]
		if containerExists {
			return container
		}
	}
	return nil
}

// DeletePod removes an existing pod from the cluster.
func (cluster *clusterState) DeletePod(podID PodID) {
	pod, found := cluster.pods[podID]
	if found {
		cluster.removePodFromItsVpa(pod)
	}
	delete(cluster.pods, podID)
}

// AddOrUpdateContainer creates a new container with the given ContainerID and
// adds it to the parent pod in the clusterState object, if not yet present.
// Requires the pod to be added to the clusterState first. Otherwise an error is
// returned.
func (cluster *clusterState) AddOrUpdateContainer(containerID ContainerID, request Resources) error {
	pod, podExists := cluster.pods[containerID.PodID]
	if !podExists {
		return NewKeyError(containerID.PodID)
	}
	if container, containerExists := pod.Containers[containerID.ContainerName]; !containerExists {
		cluster.findOrCreateAggregateContainerState(containerID)
		pod.Containers[containerID.ContainerName] = NewContainerState(request, NewContainerStateAggregatorProxy(cluster, containerID))
	} else {
		// Container aleady exists. Possibly update the request.
		container.Request = request
	}
	return nil
}

// AddSample adds a new usage sample to the proper container in the clusterState
// object. Requires the container as well as the parent pod to be added to the
// clusterState first. Otherwise an error is returned.
func (cluster *clusterState) AddSample(sample *ContainerUsageSampleWithKey) error {
	pod, podExists := cluster.pods[sample.Container.PodID]
	if !podExists {
		return NewKeyError(sample.Container.PodID)
	}
	containerState, containerExists := pod.Containers[sample.Container.ContainerName]
	if !containerExists {
		return NewKeyError(sample.Container)
	}
	if !containerState.AddSample(&sample.ContainerUsageSample) {
		return errors.New("sample discarded (invalid or out of order)")
	}
	return nil
}

// RecordOOM adds info regarding OOM event in the model as an artificial memory sample.
func (cluster *clusterState) RecordOOM(containerID ContainerID, timestamp time.Time, requestedMemory ResourceAmount) error {
	pod, podExists := cluster.pods[containerID.PodID]
	if !podExists {
		return NewKeyError(containerID.PodID)
	}
	containerState, containerExists := pod.Containers[containerID.ContainerName]
	if !containerExists {
		return NewKeyError(containerID.ContainerName)
	}
	err := containerState.RecordOOM(timestamp, requestedMemory)
	if err != nil {
		return fmt.Errorf("error while recording OOM for %v, Reason: %v", containerID, err)
	}
	return nil
}

// AddOrUpdateVpa adds a new VPA with a given ID to the clusterState if it
// didn't yet exist. If the VPA already existed but had a different pod
// selector, the pod selector is updated. Updates the links between the VPA and
// all aggregations it matches.
func (cluster *clusterState) AddOrUpdateVpa(apiObject *vpa_types.VerticalPodAutoscaler, selector labels.Selector) error {
	vpaID := VpaID{Namespace: apiObject.Namespace, VpaName: apiObject.Name}
	annotationsMap := apiObject.Annotations
	conditionsMap := make(vpaConditionsMap)
	for _, condition := range apiObject.Status.Conditions {
		conditionsMap[condition.Type] = condition
	}
	var currentRecommendation *vpa_types.RecommendedPodResources
	if conditionsMap[vpa_types.RecommendationProvided].Status == apiv1.ConditionTrue {
		currentRecommendation = apiObject.Status.Recommendation
	}

	vpa, vpaExists := cluster.vpas[vpaID]
	if vpaExists && (vpa.PodSelector.String() != selector.String()) {
		// Pod selector was changed. Delete the VPA object and recreate
		// it with the new selector.
		if err := cluster.DeleteVpa(vpaID); err != nil {
			return err
		}
		vpaExists = false
	}
	if !vpaExists {
		vpa = NewVpa(vpaID, selector, apiObject.CreationTimestamp.Time)
		cluster.vpas[vpaID] = vpa
		vpa.SetResourcePolicy(apiObject.Spec.ResourcePolicy)
		vpa.SetUpdateMode(apiObject.Spec.UpdatePolicy)
		cluster.linkVpaToExistingAggregations(vpa)
		vpa.PodCount = len(cluster.GetMatchingPods(vpa))
	} else if changedContainers := vpa.changedCPUHistogramDecayContainers(apiObject.Spec.ResourcePolicy); len(changedContainers) > 0 {
		oldAggregations := make(aggregateContainerStatesMap)
		oldCheckpoints := make(ContainerNameToAggregateStateMap)
		for aggregationKey, aggregation := range vpa.aggregateContainerStates {
			if _, changed := changedContainers[aggregationKey.ContainerName()]; changed {
				oldAggregations[aggregationKey] = aggregation
				delete(vpa.aggregateContainerStates, aggregationKey)
			}
		}
		for containerName := range changedContainers {
			if checkpoint, exists := vpa.ContainersInitialAggregateState[containerName]; exists {
				oldCheckpoints[containerName] = checkpoint
				delete(vpa.ContainersInitialAggregateState, containerName)
			}
		}
		vpa.SetResourcePolicy(apiObject.Spec.ResourcePolicy)
		for containerName, checkpoint := range oldCheckpoints {
			halfLife := vpa.CPUHistogramDecayHalfLife(containerName)
			vpa.ContainersInitialAggregateState[containerName] = checkpoint.CloneWithCPUHistogramDecayHalfLife(halfLife)
		}
		for aggregationKey, aggregation := range oldAggregations {
			oldKey, ok := aggregationKey.(aggregateStateKey)
			if !ok {
				return fmt.Errorf("unexpected aggregate state key type %T", aggregationKey)
			}
			newKey := oldKey
			newKey.cpuHalfLife = vpa.CPUHistogramDecayHalfLife(oldKey.containerName)
			newAggregation := aggregation.CloneWithCPUHistogramDecayHalfLife(newKey.cpuHalfLife)
			cluster.aggregateStateMap[newKey] = newAggregation
			vpa.UseAggregationIfMatching(newKey, newAggregation)
		}
		for key, aggregation := range oldAggregations {
			if !cluster.isAggregationUsed(key) {
				aggregation.MarkNotAutoscaled()
				delete(cluster.aggregateStateMap, key)
			}
		}
	} else {
		vpa.SetResourcePolicy(apiObject.Spec.ResourcePolicy)
		vpa.SetUpdateMode(apiObject.Spec.UpdatePolicy)
	}
	vpa.TargetRef = apiObject.Spec.TargetRef
	vpa.Annotations = annotationsMap
	vpa.SetConditionsMap(conditionsMap)
	vpa.SetRecommendationDirect(currentRecommendation)
	vpa.SetUpdateMode(apiObject.Spec.UpdatePolicy)
	vpa.SetResourcePolicy(apiObject.Spec.ResourcePolicy)
	vpa.SetAPIVersion(apiObject.GetObjectKind().GroupVersionKind().Version)
	return nil
}

func (cluster *clusterState) isAggregationUsed(key AggregateStateKey) bool {
	for _, vpa := range cluster.vpas {
		if vpa.usesExactAggregation(key) {
			return true
		}
	}
	return false
}

func (cluster *clusterState) linkVpaToExistingAggregations(vpa *Vpa) {
	pending := make(aggregateContainerStatesMap)
	for key, aggregation := range cluster.aggregateStateMap {
		if vpa.matchesAggregation(key) {
			vpa.UseAggregationIfMatching(key, aggregation)
			continue
		}
		if !features.Enabled(features.PerVPAConfig) || key.OwnerVpaID() != (VpaID{}) ||
			!vpa.matchesLabels(key.Namespace(), key.ContainerName(), key.Labels()) {
			continue
		}
		newKey := key.(aggregateStateKey)
		newKey.cpuHalfLife = vpa.CPUHistogramDecayHalfLife(key.ContainerName())
		newKey.ownerVpaID = vpa.ID
		if _, exists := cluster.aggregateStateMap[newKey]; !exists {
			pending[newKey] = aggregation.CloneWithCPUHistogramDecayHalfLife(newKey.cpuHalfLife)
		}
	}
	for newKey, newAggregation := range pending {
		cluster.aggregateStateMap[newKey] = newAggregation
		vpa.UseAggregationIfMatching(newKey, newAggregation)
	}
}

// DeleteVpa removes a VPA with the given ID from the clusterState.
func (cluster *clusterState) DeleteVpa(vpaID VpaID) error {
	vpa, vpaExists := cluster.vpas[vpaID]
	if !vpaExists {
		return NewKeyError(vpaID)
	}
	delete(cluster.vpas, vpaID)
	for key, state := range vpa.aggregateContainerStates {
		if !cluster.isAggregationUsed(key) {
			state.MarkNotAutoscaled()
			if key.OwnerVpaID() == vpaID {
				delete(cluster.aggregateStateMap, key)
			}
		}
	}
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()
	delete(cluster.emptyVPAs, vpaID)
	return nil
}

func (cluster *clusterState) VPAs() map[VpaID]*Vpa {
	return cluster.vpas
}

func (cluster *clusterState) Pods() map[PodID]*PodState {
	return cluster.pods
}

func (cluster *clusterState) SetObservedVPAs(observedVPAs []*vpa_types.VerticalPodAutoscaler) {
	cluster.observedVPAs = observedVPAs
}

func (cluster *clusterState) ObservedVPAs() []*vpa_types.VerticalPodAutoscaler {
	return cluster.observedVPAs
}

func newPod(id PodID) *PodState {
	return &PodState{
		ID:         id,
		Containers: make(map[string]*ContainerState),
	}
}

// getLabelSetKey puts the given labelSet in the global labelSet map and returns a
// corresponding labelSetKey.
func (cluster *clusterState) getLabelSetKey(labelSet labels.Set) labelSetKey {
	labelSetKey := labelSetKey(labelSet.String())
	cluster.labelSetMap[labelSetKey] = labelSet
	return labelSetKey
}

// MakeAggregateStateKey returns the AggregateStateKey that should be used
// to aggregate usage samples from a container with the given name in a given pod.
func (cluster *clusterState) MakeAggregateStateKey(pod *PodState, containerName string) AggregateStateKey {
	return cluster.makeAggregateStateKey(pod, containerName, GetAggregationsConfig().CPUHistogramDecayHalfLife)
}

func (cluster *clusterState) makeAggregateStateKey(pod *PodState, containerName string, cpuHalfLife time.Duration) aggregateStateKey {
	return aggregateStateKey{
		namespace:     pod.ID.Namespace,
		containerName: containerName,
		labelSetKey:   pod.labelSetKey,
		labelSetMap:   &cluster.labelSetMap,
		cpuHalfLife:   cpuHalfLife,
	}
}

func (cluster *clusterState) aggregateStateKeyForVpa(pod *PodState, containerName string, vpa *Vpa) aggregateStateKey {
	key := cluster.makeAggregateStateKey(pod, containerName, vpa.CPUHistogramDecayHalfLife(containerName))
	if features.Enabled(features.PerVPAConfig) {
		// Isolate mutable history so A -> B -> A never substitutes another
		// VPA's weights or resurrects an old copy of this VPA's histogram.
		key.ownerVpaID = vpa.ID
	}
	return key
}

// aggregateStateKeyForContainerID returns the AggregateStateKey for the ContainerID.
// The pod with the corresponding PodID must already be present in the clusterState.
func (cluster *clusterState) aggregateStateKeyForContainerID(containerID ContainerID) AggregateStateKey {
	pod, podExists := cluster.pods[containerID.PodID]
	if !podExists {
		panic(fmt.Sprintf("Pod not present in the ClusterState: %s/%s", containerID.Namespace, containerID.PodName))
	}
	return cluster.MakeAggregateStateKey(pod, containerID.ContainerName)
}

// findOrCreateAggregateContainerState returns (possibly newly created) AggregateContainerState
// that should be used to aggregate usage samples from container with a given ID.
// The pod with the corresponding PodID must already be present in the clusterState.
func (cluster *clusterState) findOrCreateAggregateContainerState(containerID ContainerID) *AggregateContainerState {
	return cluster.findOrCreateAggregateContainerStateWithHalfLife(containerID, GetAggregationsConfig().CPUHistogramDecayHalfLife)
}

func (cluster *clusterState) findOrCreateAggregateContainerStateWithHalfLife(containerID ContainerID, cpuHalfLife time.Duration) *AggregateContainerState {
	pod, podExists := cluster.pods[containerID.PodID]
	if !podExists {
		panic(fmt.Sprintf("Pod not present in the ClusterState: %s/%s", containerID.Namespace, containerID.PodName))
	}
	return cluster.findOrCreateAggregation(cluster.makeAggregateStateKey(pod, containerID.ContainerName, cpuHalfLife))
}

func (cluster *clusterState) findOrCreateAggregation(key aggregateStateKey) *AggregateContainerState {
	aggregateContainerState, aggregateStateExists := cluster.aggregateStateMap[key]
	if !aggregateStateExists {
		aggregateContainerState = NewAggregateContainerStateWithCPUHistogramDecayHalfLife(key.cpuHalfLife)
		cluster.aggregateStateMap[key] = aggregateContainerState
		// Link the new aggregation to the existing VPAs.
		for _, vpa := range cluster.vpas {
			vpa.UseAggregationIfMatching(key, aggregateContainerState)
		}
	}
	return aggregateContainerState
}

func (cluster *clusterState) findOrCreateAggregateContainerStateForContainer(containerID ContainerID) *AggregateContainerState {
	pod := cluster.pods[containerID.PodID]
	if pod == nil || !features.Enabled(features.PerVPAConfig) {
		return cluster.findOrCreateAggregateContainerState(containerID)
	}
	podLabels := cluster.labelSetMap[pod.labelSetKey]
	for _, vpa := range cluster.vpas {
		if vpa.matchesLabels(containerID.Namespace, containerID.ContainerName, podLabels) {
			return cluster.findOrCreateAggregation(cluster.aggregateStateKeyForVpa(pod, containerID.ContainerName, vpa))
		}
	}
	return cluster.findOrCreateAggregateContainerState(containerID)
}

func (cluster *clusterState) addSampleToAggregations(containerID ContainerID, sample *ContainerUsageSample, subtract bool) {
	pod := cluster.pods[containerID.PodID]
	if pod == nil {
		return
	}
	if !features.Enabled(features.PerVPAConfig) {
		aggregation := cluster.findOrCreateAggregateContainerState(containerID)
		if subtract {
			aggregation.SubtractSample(sample)
		} else {
			aggregation.AddSample(sample)
		}
		return
	}
	podLabels := cluster.labelSetMap[pod.labelSetKey]
	keys := make(map[aggregateStateKey]struct{})
	keys[cluster.makeAggregateStateKey(pod, containerID.ContainerName, GetAggregationsConfig().CPUHistogramDecayHalfLife)] = struct{}{}
	for _, vpa := range cluster.vpas {
		if vpa.matchesLabels(containerID.Namespace, containerID.ContainerName, podLabels) {
			keys[cluster.aggregateStateKeyForVpa(pod, containerID.ContainerName, vpa)] = struct{}{}
		}
	}
	for key := range keys {
		aggregation := cluster.findOrCreateAggregation(key)
		if subtract {
			aggregation.SubtractSample(sample)
		} else {
			aggregation.AddSample(sample)
		}
	}
}

// garbageCollectAggregateCollectionStates removes obsolete AggregateCollectionStates from the clusterState.
// AggregateCollectionState is obsolete in following situations:
// 1) It has no samples and there are no more contributive pods - a pod is contributive in any of following situations:
//
//	a) It is in an active state - i.e. not PodSucceeded nor PodFailed.
//	b) Its associated controller (e.g. Deployment) still exists.
//
// 2) The last sample is too old to give meaningful recommendation (>8 days),
// 3) There are no samples and the aggregate state was created >8 days ago.
func (cluster *clusterState) garbageCollectAggregateCollectionStates(ctx context.Context, now time.Time, controllerFetcher controllerfetcher.ControllerFetcher) {
	klog.V(1).InfoS("Garbage collection of AggregateCollectionStates triggered")
	keysToDelete := make([]AggregateStateKey, 0)
	contributiveKeys := cluster.getContributiveAggregateStateKeys(ctx, controllerFetcher)
	for key, aggregateContainerState := range cluster.aggregateStateMap {
		isKeyContributive := contributiveKeys[key]
		if !isKeyContributive && aggregateContainerState.isEmpty() {
			keysToDelete = append(keysToDelete, key)
			klog.V(1).InfoS("Removing empty and not contributive AggregateCollectionState", "key", key)
			continue
		}
		if aggregateContainerState.isExpired(now) {
			keysToDelete = append(keysToDelete, key)
			klog.V(1).InfoS("Removing expired AggregateCollectionState", "key", key)
		}
	}
	for _, key := range keysToDelete {
		delete(cluster.aggregateStateMap, key)
		for _, vpa := range cluster.vpas {
			vpa.DeleteAggregation(key)
		}
	}
}

// RateLimitedGarbageCollectAggregateCollectionStates removes obsolete AggregateCollectionStates from the clusterState.
// It performs clean up only if more than `gcInterval` passed since the last time it performed a cleanup.
// AggregateCollectionState is obsolete in following situations:
// 1) It has no samples and there are no more contributive pods - a pod is contributive in any of following situations:
//
//	a) It is in an active state - i.e. not PodSucceeded nor PodFailed.
//	b) Its associated controller (e.g. Deployment) still exists.
//
// 2) The last sample is too old to give meaningful recommendation (>8 days),
// 3) There are no samples and the aggregate state was created >8 days ago.
func (cluster *clusterState) RateLimitedGarbageCollectAggregateCollectionStates(ctx context.Context, now time.Time, controllerFetcher controllerfetcher.ControllerFetcher) {
	if now.Sub(cluster.lastAggregateContainerStateGC) < cluster.gcInterval {
		return
	}
	cluster.garbageCollectAggregateCollectionStates(ctx, now, controllerFetcher)
	cluster.lastAggregateContainerStateGC = now
}

func (cluster *clusterState) getContributiveAggregateStateKeys(ctx context.Context, controllerFetcher controllerfetcher.ControllerFetcher) map[AggregateStateKey]bool {
	contributiveKeys := map[AggregateStateKey]bool{}
	for _, pod := range cluster.pods {
		// Pod is considered contributive in any of following situations:
		// 1) It is in active state - i.e. not PodSucceeded nor PodFailed.
		// 2) Its associated controller (e.g. Deployment) still exists.
		podControllerExists := cluster.GetControllerForPodUnderVPA(ctx, pod, controllerFetcher) != nil
		podActive := pod.Phase != apiv1.PodSucceeded && pod.Phase != apiv1.PodFailed
		if podActive || podControllerExists {
			for container := range pod.Containers {
				contributiveKeys[cluster.MakeAggregateStateKey(pod, container)] = true
				if features.Enabled(features.PerVPAConfig) {
					for _, vpa := range cluster.vpas {
						if vpa.matchesLabels(pod.ID.Namespace, container, cluster.labelSetMap[pod.labelSetKey]) {
							key := cluster.aggregateStateKeyForVpa(pod, container, vpa)
							contributiveKeys[key] = true
						}
					}
				}
			}
		}
	}
	return contributiveKeys
}

// RecordRecommendation marks the state of recommendation in the cluster. We
// keep track of empty recommendations and log information about them
// periodically.
func (cluster *clusterState) RecordRecommendation(vpa *Vpa, now time.Time) error {
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()
	// TODO(jkyros): Today the VPA critical sections don't try to grab the cluster mutex, but
	// if anyone ever changes it so they do we'd deadlock. This makes me a little nervous, but
	// if I don't fix this one, we'll fail the race tests
	if vpa.HasRecommendation() {
		delete(cluster.emptyVPAs, vpa.ID)
		return nil
	}
	lastLogged, ok := cluster.emptyVPAs[vpa.ID]
	if !ok {
		cluster.emptyVPAs[vpa.ID] = now
	} else {
		if lastLogged.Add(RecommendationMissingMaxDuration).Before(now) {
			cluster.emptyVPAs[vpa.ID] = now
			return fmt.Errorf("VPA %s/%s is missing recommendation for more than %v", vpa.ID.Namespace, vpa.ID.VpaName, RecommendationMissingMaxDuration)
		}
	}
	return nil
}

// GetMatchingPods returns a list of currently active pods that match the
// given VPA. Traverses through all pods in the cluster - use sparingly.
func (cluster *clusterState) GetMatchingPods(vpa *Vpa) []PodID {
	matchingPods := []PodID{}
	for podID, pod := range cluster.pods {
		if vpa_utils.PodLabelsMatchVPA(podID.Namespace, cluster.labelSetMap[pod.labelSetKey],
			vpa.ID.Namespace, vpa.PodSelector) {
			matchingPods = append(matchingPods, podID)
		}
	}
	return matchingPods
}

// GetControllerForPodUnderVPA returns controller associated with given Pod. Returns nil if Pod is not controlled by a VPA object.
func (cluster *clusterState) GetControllerForPodUnderVPA(ctx context.Context, pod *PodState, controllerFetcher controllerfetcher.ControllerFetcher) *controllerfetcher.ControllerKeyWithAPIVersion {
	controllingVPA := cluster.GetControllingVPA(pod)
	if controllingVPA != nil {
		controller := &controllerfetcher.ControllerKeyWithAPIVersion{
			ControllerKey: controllerfetcher.ControllerKey{
				Namespace: controllingVPA.ID.Namespace,
				Kind:      controllingVPA.TargetRef.Kind,
				Name:      controllingVPA.TargetRef.Name,
			},
			ApiVersion: controllingVPA.TargetRef.APIVersion,
		}
		topLevelController, _ := controllerFetcher.FindTopMostWellKnownOrScalable(ctx, controller)
		return topLevelController
	}
	return nil
}

// GetControllingVPA returns a VPA object controlling given Pod.
func (cluster *clusterState) GetControllingVPA(pod *PodState) *Vpa {
	for _, vpa := range cluster.vpas {
		if vpa_utils.PodLabelsMatchVPA(pod.ID.Namespace, cluster.labelSetMap[pod.labelSetKey],
			vpa.ID.Namespace, vpa.PodSelector) {
			return vpa
		}
	}
	return nil
}

// Implementation of the AggregateStateKey interface. It can be used as a map key.
type aggregateStateKey struct {
	namespace     string
	containerName string
	labelSetKey   labelSetKey
	cpuHalfLife   time.Duration
	ownerVpaID    VpaID
	// Pointer to the global map from labelSetKey to labels.Set.
	// Note: a pointer is used so that two copies of the same key are equal.
	labelSetMap *labelSetMap
}

// Namespace returns the namespace for the aggregateStateKey.
func (k aggregateStateKey) Namespace() string {
	return k.namespace
}

// ContainerName returns the name of the container for the aggregateStateKey.
func (k aggregateStateKey) ContainerName() string {
	return k.containerName
}

func (k aggregateStateKey) LabelSetKey() string {
	return string(k.labelSetKey)
}

func (k aggregateStateKey) CPUHistogramDecayHalfLife() time.Duration {
	return k.cpuHalfLife
}

func (k aggregateStateKey) OwnerVpaID() VpaID {
	return k.ownerVpaID
}

// Labels returns the set of labels for the aggregateStateKey.
func (k aggregateStateKey) Labels() labels.Labels {
	if k.labelSetMap == nil {
		return labels.Set{}
	}
	return (*k.labelSetMap)[k.labelSetKey]
}
