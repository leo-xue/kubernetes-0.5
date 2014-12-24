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

package scheduler

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/GoogleCloudPlatform/kubernetes/pkg/api"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/resources"
	"github.com/golang/glog"
	"github.com/hustcat/go-lib/bitmap"
)

type genericScheduler struct {
	predicates  []FitPredicate
	prioritizer PriorityFunction
	pods        PodLister
	random      *rand.Rand
	randomLock  sync.Mutex
}

func (g *genericScheduler) Schedule(pod api.Pod, minionLister MinionLister) (SelectedMachine, error) {
	minions, err := minionLister.List()
	if err != nil {
		return SelectedMachine{"", api.Network{}, ""}, err
	} else if len(minions.Items) == 0 {
		return SelectedMachine{"", api.Network{}, ""}, fmt.Errorf("schedule MinionList is null")
	}

	filteredNodes, err := findNodesThatFit(pod, g.pods, g.predicates, minions)
	if err != nil {
		return SelectedMachine{"", api.Network{}, ""}, err
	} else if len(filteredNodes.Items) == 0 {
		return SelectedMachine{"", api.Network{}, ""}, fmt.Errorf("filtered MinionList is null")
	}

	ids := getMinionListIds(filteredNodes)
	glog.V(3).Infof("filtered Minions: %v", ids)

	index, set, err2 := g.numaCpuSelect(pod, g.pods, filteredNodes)
	if index == -1 || err2 != nil {
		return SelectedMachine{"", api.Network{}, ""}, fmt.Errorf("numaCpuSelect failed, %v", err2)
	}

	selectedMinion := filteredNodes.Items[index]

	network, err3 := allocNetwork(pod, g.pods, selectedMinion)
	if err3 != nil {
		return SelectedMachine{"", api.Network{}, ""}, err3
	}

	cpuSet := ""
	if set != nil {
		cpuSet = strings.Join(set, ",")
	}

	return SelectedMachine{
		Name:    selectedMinion.Name,
		Network: network,
		CpuSet:  cpuSet,
	}, nil
	/*
		priorityList, err := g.prioritizer(pod, g.pods, FakeMinionLister(filteredNodes))
		if err != nil {
			return "", err
		}
		if len(priorityList) == 0 {
			return "", fmt.Errorf("failed to find a fit for pod: %v", pod)
		}

		dest,_ := g.selectHost(priorityList)*/
}

func (g *genericScheduler) selectHost(priorityList HostPriorityList) (string, error) {
	if len(priorityList) == 0 {
		return "", fmt.Errorf("empty priorityList")
	}
	sort.Sort(priorityList)

	hosts := getMinHosts(priorityList)
	g.randomLock.Lock()
	defer g.randomLock.Unlock()

	ix := g.random.Int() % len(hosts)
	return hosts[ix], nil
}

func findNodesThatFit(pod api.Pod, podLister PodLister, predicates []FitPredicate, nodes api.MinionList) (api.MinionList, error) {
	filtered := []api.Minion{}
	machineToPods, err := MapPodsToMachines(podLister)
	if err != nil {
		return api.MinionList{}, err
	}
	for _, node := range nodes.Items {
		fits := true
		for _, predicate := range predicates {
			fit, err := predicate(pod, machineToPods[node.Name], node.Name)
			if err != nil {
				return api.MinionList{}, err
			}
			if !fit {
				fits = false
				break
			}
		}
		if fits {
			filtered = append(filtered, node)
		}
	}
	return api.MinionList{Items: filtered}, nil
}

func getMinHosts(list HostPriorityList) []string {
	result := []string{}
	for _, hostEntry := range list {
		if hostEntry.score == list[0].score {
			result = append(result, hostEntry.host)
		} else {
			break
		}
	}
	return result
}

// EqualPriority is a prioritizer function that gives an equal weight of one to all nodes
func EqualPriority(pod api.Pod, podLister PodLister, minionLister MinionLister) (HostPriorityList, error) {
	nodes, err := minionLister.List()
	if err != nil {
		fmt.Errorf("failed to list nodes: %v", err)
		return []HostPriority{}, err
	}

	result := []HostPriority{}
	for _, minion := range nodes.Items {
		result = append(result, HostPriority{
			host:  minion.Name,
			score: 1,
		})
	}
	return result, nil
}

func NewGenericScheduler(predicates []FitPredicate, prioritizer PriorityFunction, pods PodLister, random *rand.Rand) Scheduler {
	return &genericScheduler{
		predicates:  predicates,
		prioritizer: prioritizer,
		pods:        pods,
		random:      random,
	}
}

func (g *genericScheduler) numaCpuSelect(pod api.Pod, podLister PodLister, nodes api.MinionList) (int, []string, error) {
	var (
		cpuNodeNum       int
		numaCpuSet       []string
		numaSelectMinion int

		noNumaCpuSet       []string
		noNumaSelectMinion int
	)

	machineToPods, err := MapPodsToMachines(podLister)
	if err != nil {
		return -1, nil, err
	}

	reqCore := 0
	for ix := range pod.Spec.Containers {
		reqCore += pod.Spec.Containers[ix].Core
	}

	//no cpuset
	if reqCore == 0 {
		return 0, nil, nil
	}

	numaSelectMinion = -1
	noNumaSelectMinion = -1

	for index, minion := range nodes.Items {
		var set1 []string

		pods := machineToPods[minion.Name]

		coreNum := resources.GetIntegerResource(minion.Spec.Capacity, resources.Core, 24)
		cpuNodeNum = resources.GetIntegerResource(minion.Spec.Capacity, resources.CpuNode, 2)
		cpuMap := bitmap.NewNumaBitmapSize(uint(coreNum), cpuNodeNum)

		//get used cpu cores
		for _, pod := range pods {
			set := strings.Split(pod.Status.CpuSet, ",")
			for _, c := range set {
				coreNo, _ := strconv.Atoi(c)
				cpuMap.SetBit(uint(coreNo), 1)
			}
		}

		freeCores1 := cpuMap.Get0BitOffs()
		if len(freeCores1) < reqCore {
			continue
		} else {
			for j := 0; j < reqCore; j++ {
				off := freeCores1[j]
				set1 = append(set1, strconv.Itoa(int(off)))
				//noNumaCpuSet = append(noNumaCpuSet, strconv.Itoa(int(off)))
			}
			noNumaCpuSet = set1
			noNumaSelectMinion = index
		}

		freeCores2, err2 := cpuMap.Get0BitOffsNuma(uint(cpuNodeNum))
		if err2 != nil {
			return -1, nil, err2
		}

		for i := 0; i < cpuNodeNum; i++ {
			offs := freeCores2[i]
			if len(offs) >= reqCore {
				for j := 0; j < reqCore; j++ {
					off := offs[j]
					//cpuMap.SetBit(off, 1)
					numaCpuSet = append(numaCpuSet, strconv.Itoa(int(off)))
				}
				numaSelectMinion = index
				break
			}
		}

		if numaCpuSet != nil {
			break
		}
	} //minion.Items

	if numaCpuSet != nil {
		selectNode := nodes.Items[numaSelectMinion]
		glog.V(3).Infof("Selected Numa CPU set: %v, Minion index: %d, name: %s", numaCpuSet, numaSelectMinion, selectNode.Name)
		return numaSelectMinion, numaCpuSet, nil
	} else {
		selectNode := nodes.Items[noNumaSelectMinion]
		glog.V(3).Infof("Selected Uma CPU set: %v, Minion index: %d, name :%s", noNumaCpuSet, noNumaSelectMinion, selectNode.Name)
		return noNumaSelectMinion, noNumaCpuSet, nil
	}
}

func allocNetwork(pod api.Pod, podLister PodLister, node api.Minion) (api.Network, error) {
	var (
		network api.Network
	)

	// host, nat, none
	if pod.Spec.NetworkMode != api.PodNetworkModeBridge {
		return api.Network{Mode: pod.Spec.NetworkMode}, nil
	}

	// bridge
	machineToPods, err := MapPodsToMachines(podLister)
	if err != nil {
		return api.Network{}, err
	}

	vms := node.Spec.VMs
	pods := machineToPods[node.Name]
	for i, vm := range vms {
		used := false
		for _, pod := range pods {
			if vm.Address == pod.Status.Network.Address {
				used = true
			}
		}

		if used == false {
			network.Address = vms[i].Address
			network.Gateway = vms[i].Gateway
			network.Bridge = fmt.Sprintf("br%d", vms[i].VlanID)
			network.Mode = pod.Spec.NetworkMode
			break
		}
	}
	return network, nil
}

func getMinionListIds(nodes api.MinionList) []string {
	var ids []string
	for _, m := range nodes.Items {
		ids = append(ids, m.Name)
	}
	return ids
}
