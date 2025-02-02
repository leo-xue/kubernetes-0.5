/*
Copyright 2014 Google Inc. All rights reserved.

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

package master

import (
	"sync"

	"github.com/GoogleCloudPlatform/kubernetes/pkg/api"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/api/errors"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/client"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/labels"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/registry/pod"
	"github.com/golang/glog"
)

type IPGetter interface {
	GetInstanceIP(host string) (ip string)
}

// PodCache contains both a cache of container information, as well as the mechanism for keeping
// that cache up to date.
type PodCache struct {
	ipCache       IPGetter
	containerInfo client.PodInfoGetter
	pods          pod.Registry
	// For confirming existance of a node
	nodes client.MinionInterface

	// lock protects access to all fields below
	lock sync.Mutex
	// cached pod statuses.
	podStatus map[objKey]api.PodStatus
	// nodes that we know exist. Cleared at the beginning of each
	// UpdateAllPods call.
	currentNodes map[objKey]bool
}

type objKey struct {
	namespace, name string
}

// NewPodCache returns a new PodCache which watches container information
// registered in the given PodRegistry.
// TODO(lavalamp): pods should be a client.PodInterface.
func NewPodCache(ipCache IPGetter, info client.PodInfoGetter, nodes client.MinionInterface, pods pod.Registry) *PodCache {
	return &PodCache{
		ipCache:       ipCache,
		containerInfo: info,
		pods:          pods,
		nodes:         nodes,
		currentNodes:  map[objKey]bool{},
		podStatus:     map[objKey]api.PodStatus{},
	}
}

// GetPodStatus gets the stored pod status.
func (p *PodCache) GetPodStatus(namespace, name string) (*api.PodStatus, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	value, ok := p.podStatus[objKey{namespace, name}]
	if !ok {
		return nil, client.ErrPodInfoNotAvailable
	}
	// Make a copy
	return &value, nil
}

func (p *PodCache) nodeExistsInCache(name string) (exists, cacheHit bool) {
	p.lock.Lock()
	defer p.lock.Unlock()
	exists, cacheHit = p.currentNodes[objKey{"", name}]
	return exists, cacheHit
}

// lock must *not* be held
func (p *PodCache) nodeExists(name string) bool {
	exists, cacheHit := p.nodeExistsInCache(name)
	if cacheHit {
		return exists
	}
	// TODO: suppose there's N concurrent requests for node "foo"; in that case
	// it might be useful to block all of them and only look up "foo" once.
	// (This code will make up to N lookups.) One way of doing that would be to
	// have a pool of M mutexes and require that before looking up "foo" you must
	// lock mutex hash("foo") % M.
	_, err := p.nodes.Get(name)
	exists = true
	if err != nil {
		exists = false
		if !errors.IsNotFound(err) {
			glog.Warningf("Unexpected error type verifying minion(%s) existence: %+v", name, err)
		}
	}

	p.lock.Lock()
	defer p.lock.Unlock()
	p.currentNodes[objKey{"", name}] = exists
	return exists
}

// TODO: once Host gets moved to spec, this can take a podSpec + metadata instead of an
// entire pod?
func (p *PodCache) updatePodStatus(pod *api.Pod) error {
	newStatus, err := p.computePodStatus(pod)

	p.lock.Lock()
	defer p.lock.Unlock()
	// Map accesses must be locked.
	p.podStatus[objKey{pod.Namespace, pod.Name}] = newStatus

	return err
}

// computePodStatus always returns a new status, even if it also returns a non-nil error.
// TODO: once Host gets moved to spec, this can take a podSpec + metadata instead of an
// entire pod?
func (p *PodCache) computePodStatus(pod *api.Pod) (api.PodStatus, error) {
	newStatus := pod.Status

	if pod.Status.Host == "" {
		// Not assigned.
		// or assign failed, so change return value from api.PodPending to pod.Status.Phase
		// by hbo at 2015.4.1
		// newStatus.Phase = api.PodPending (delete)
		return newStatus, nil
	}

	if !p.nodeExists(pod.Status.Host) {
		// Assigned to non-existing node.
		newStatus.Phase = api.PodFailed
		return newStatus, nil
	}

	info, err := p.containerInfo.GetPodInfo(pod.Status.Host, pod.Namespace, pod.Name)
	newStatus.HostIP = p.ipCache.GetInstanceIP(pod.Status.Host)

	if err != nil {
		// TODO (hbo)
		// Status PodUnknown is not defined, change it to PodFailed
		newStatus.Phase = api.PodFailed
	} else {
		newStatus.Info = info
		newStatus.Phase = getPhase(&pod.Spec, newStatus.Info)
		if netContainerInfo, ok := newStatus.Info["net"]; ok {
			if netContainerInfo.PodIP != "" {
				newStatus.PodIP = netContainerInfo.PodIP
			}
		}
	}
	return newStatus, err
}

func (p *PodCache) resetNodeExistenceCache() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.currentNodes = map[objKey]bool{}
}

// UpdateAllContainers updates information about all containers.
// Callers should let one call to UpdateAllContainers finish before
// calling again, or risk having new info getting clobbered by delayed
// old info.
func (p *PodCache) UpdateAllContainers() {
	p.resetNodeExistenceCache()

	ctx := api.NewContext()
	pods, err := p.pods.ListPods(ctx, labels.Everything())
	if err != nil {
		glog.Errorf("Error getting pod list: %v", err)
		return
	}

	// TODO: this algorithm is 1 goroutine & RPC per pod. With a little work,
	// it should be possible to make it 1 per *node*, which will be important
	// at very large scales. (To be clear, the goroutines shouldn't matter--
	// it's the RPCs that need to be minimized.)
	var wg sync.WaitGroup
	for i := range pods.Items {
		pod := &pods.Items[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := p.updatePodStatus(pod)
			if err != nil && err != client.ErrPodInfoNotAvailable {
				glog.Errorf("Error getting info for pod %v/%v: %v", pod.Namespace, pod.Name, err)
			}
		}()
	}
	wg.Wait()
}

// getPhase returns the phase of a pod given its container info.
// TODO(dchen1107): push this all the way down into kubelet.
func getPhase(spec *api.PodSpec, info api.PodInfo) api.PodPhase {
	if info == nil {
		return api.PodPending
	}
	running := 0
	stopped := 0
	unknown := 0
	for _, container := range spec.Containers {
		if containerStatus, ok := info[container.Name]; ok {
			if containerStatus.State.Running != nil {
				running++
			} else if containerStatus.State.Termination != nil {
				stopped++
			} else {
				unknown++
			}
		} else {
			unknown++
		}
	}
	switch {
	case running > 0 && unknown == 0:
		return api.PodRunning
	case running == 0 && stopped > 0 && unknown == 0:
		return api.PodFailed
	case running == 0 && stopped == 0 && unknown > 0:
		return api.PodPending
	default:
		return api.PodPending
	}
}
