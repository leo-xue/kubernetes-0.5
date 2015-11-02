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

package kubelet

import (
	"fmt"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/api"
)

const ConfigSourceAnnotationKey = "kubernetes/config.source"

// PodOperation defines what changes will be made on a pod configuration.
type PodOperation int

const (
	// This is the current pod configuration
	SET PodOperation = iota
	// Pods with the given ids are new to this source
	ADD
	// Pods with the given ids have been removed from this source
	REMOVE
	// Pods with the given ids have been updated in this source
	UPDATE
)

// PodUpdate defines an operation sent on the channel. You can add or remove single services by
// sending an array of size one and Op == ADD|REMOVE (with REMOVE, only the ID is required).
// For setting the state of the system to a given state for this source configuration, set
// Pods as desired and Op to SET, which will reset the system state to that specified in this
// operation for this source channel. To remove all pods, set Pods to empty object and Op to SET.
type PodUpdate struct {
	Pods []api.BoundPod
	Op   PodOperation
}

// PodOpResult define start/stop container's result
type PodOpResult struct {
	Op       string `json:"op"`
	Code     int    `json:"code"`
	ErrorMsg string `json:"errorMsg"`
}

// PushImageParams define push image to local hub
type PushImageParams struct {
	PodID        string `json:"podID"`
	PodNamespace string `json:"podNamespace"`
	Image        string `json:"image"`
	Author       string `json:"author"`
}

type KVPair struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// PodConfig define update container config
type PodConfig struct {
	PodID          string   `json:"podID"`
	PodNamespace   string   `json:"podNamespace"`
	WriteSubsystem []KVPair `json:"writeSubsystem"`
}

type CgroupData struct {
	Group     string `json:"group,omitempty"`
	Subsystem string `json:"subsystem,omitempty"`
	Value     string `json:"value,omitempty"`
}

type CgroupResponse struct {
	Group     string `json:"group,omitempty"`
	Subsystem string `json:"subsystem,omitempty"`
	Out       string `json:"out,omitempty"`
	Status    int    `json:"status"`
}

type ApiResponse struct {
	Code     int           `json:"code"`
	ErrorMsg string        `json:"errorMsg"`
	Data     []interface{} `json:"data"`
}

// GetPodFullName returns a name that uniquely identifies a pod across all config sources.
func GetPodFullName(pod *api.BoundPod) string {
	return fmt.Sprintf("%s.%s.%s", pod.Name, pod.Namespace, pod.Annotations[ConfigSourceAnnotationKey])
}
