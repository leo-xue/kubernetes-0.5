// Copyright 2014 go-dockerclient authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package docker

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/cadvisor/info"
)

func GetContainerInfo(containerID string) (*info.ContainerInfo, error) {
	cinfo := new(info.ContainerInfo)
	cinfo.Stats = make([]*info.ContainerStats, 0)
	containerStats := &info.ContainerStats{
		Timestamp: time.Now(),
		Memory: &info.MemoryStats{
			Stats: make(map[string]uint64),
		},
	}
	if err := getMemoryStats(containerID, containerStats); err != nil {
		return nil, err
	}
	cinfo.Spec.HasMemory = true
	if err := getMemorySpec(containerID, cinfo); err != nil {
		return nil, err
	}
	cinfo.Spec.HasCpu = true
	if err := getCpusetSpec(containerID, cinfo); err != nil {
		return nil, err
	}
	cinfo.Stats = append(cinfo.Stats, containerStats)

	return cinfo, nil
}

func getMemoryStats(containerID string, stats *info.ContainerStats) error {
	path := fmt.Sprintf("/cgroup/memory/docker/%s", containerID)
	// Get memory stat file
	statsFile, err := os.Open(filepath.Join(path, "memory.stat"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer statsFile.Close()
	sc := bufio.NewScanner(statsFile)
	for sc.Scan() {
		t, v, err := getCgroupParamKeyValue(sc.Text())
		if err != nil {
			return fmt.Errorf("failed to parse memory.stat (%q) - %v", sc.Text(), err)
		}
		stats.Memory.Stats[t] = v
	}
	// Get memory usage
	usage, err := getCgroupParamUint(path, "memory.usage_in_bytes")
	if err != nil {
		return fmt.Errorf("failed to parse memory.usage_in_bytes - %v", err)
	}
	stats.Memory.Usage = usage

	return nil
}

func getMemorySpec(containerID string, cinfo *info.ContainerInfo) error {
	path := fmt.Sprintf("/cgroup/memory/docker/%s", containerID)
	// Get memory limit
	limit, err := getCgroupParamUint(path, "memory.limit_in_bytes")
	if err != nil {
		return fmt.Errorf("failed to parse memory.limit_in_bytes - %v", err)
	}
	cinfo.Spec.Memory.Limit = limit
	// Get memory memsw limit
	swapLimit, err := getCgroupParamUint(path, "memory.memsw.limit_in_bytes")
	if err != nil {
		return fmt.Errorf("failed to parse memory.memsw.limit_in_bytes - %v", err)
	}
	cinfo.Spec.Memory.SwapLimit = swapLimit

	return nil
}

func getCpusetSpec(containerID string, cinfo *info.ContainerInfo) error {
	path := fmt.Sprintf("/cgroup/cpuset/docker/%s", containerID)
	// Get cpuset cpus
	cpus, err := getCgroupParamString(path, "cpuset.cpus")
	if err != nil {
		return fmt.Errorf("failed to parse cpuset.cpus - %v", err)
	}
	cinfo.Spec.Cpuset.Cpus = cpus

	return nil
}

