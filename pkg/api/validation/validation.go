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

package validation

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/GoogleCloudPlatform/kubernetes/pkg/api"
	errs "github.com/GoogleCloudPlatform/kubernetes/pkg/api/errors"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/capabilities"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/labels"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/util"
)

// ServiceLister is an abstract interface for testing.
type ServiceLister interface {
	ListServices(api.Context) (*api.ServiceList, error)
}

func validateVolumes(volumes []api.Volume) (util.StringSet, errs.ValidationErrorList) {
	allErrs := errs.ValidationErrorList{}

	allNames := util.StringSet{}
	for i := range volumes {
		vol := &volumes[i] // so we can set default values
		el := errs.ValidationErrorList{}
		if vol.Source == nil {
			// TODO: Enforce that a source is set once we deprecate the implied form.
			vol.Source = &api.VolumeSource{
				EmptyDir: &api.EmptyDir{},
			}
		}
		el = validateSource(vol.Source).Prefix("source")
		if len(vol.Name) == 0 {
			el = append(el, errs.NewFieldRequired("name", vol.Name))
		} else if !util.IsDNSLabel(vol.Name) {
			el = append(el, errs.NewFieldInvalid("name", vol.Name, ""))
		} else if allNames.Has(vol.Name) {
			el = append(el, errs.NewFieldDuplicate("name", vol.Name))
		}
		if len(el) == 0 {
			allNames.Insert(vol.Name)
		} else {
			allErrs = append(allErrs, el.PrefixIndex(i)...)
		}
	}
	return allNames, allErrs
}

func validateSource(source *api.VolumeSource) errs.ValidationErrorList {
	numVolumes := 0
	allErrs := errs.ValidationErrorList{}
	if source.HostDir != nil {
		numVolumes++
		allErrs = append(allErrs, validateHostDir(source.HostDir).Prefix("hostDirectory")...)
	}
	if source.EmptyDir != nil {
		numVolumes++
		// EmptyDirs have nothing to validate
	}
	if source.GitRepo != nil {
		numVolumes++
		allErrs = append(allErrs, validateGitRepo(source.GitRepo)...)
	}
	if source.GCEPersistentDisk != nil {
		numVolumes++
		allErrs = append(allErrs, validateGCEPersistentDisk(source.GCEPersistentDisk)...)
	}
	if numVolumes != 1 {
		allErrs = append(allErrs, errs.NewFieldInvalid("", source, "exactly 1 volume type is required"))
	}
	return allErrs
}

func validateHostDir(hostDir *api.HostDir) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}
	if hostDir.Path == "" {
		allErrs = append(allErrs, errs.NewNotFound("path", hostDir.Path))
	}
	return allErrs
}

func validateGitRepo(gitRepo *api.GitRepo) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}
	if gitRepo.Repository == "" {
		allErrs = append(allErrs, errs.NewFieldRequired("gitRepo.Repository", gitRepo.Repository))
	}
	return allErrs
}

func validateGCEPersistentDisk(PD *api.GCEPersistentDisk) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}
	if PD.PDName == "" {
		allErrs = append(allErrs, errs.NewFieldRequired("PD.PDName", PD.PDName))
	}
	if PD.FSType == "" {
		allErrs = append(allErrs, errs.NewFieldRequired("PD.FSType", PD.FSType))
	}
	if PD.Partition < 0 || PD.Partition > 255 {
		allErrs = append(allErrs, errs.NewFieldInvalid("PD.Partition", PD.Partition, ""))
	}
	return allErrs
}

var supportedPortProtocols = util.NewStringSet(string(api.ProtocolTCP), string(api.ProtocolUDP))

func validatePorts(ports []api.Port) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}

	allNames := util.StringSet{}
	for i := range ports {
		pErrs := errs.ValidationErrorList{}
		port := &ports[i] // so we can set default values
		if len(port.Name) > 0 {
			if len(port.Name) > 63 || !util.IsDNSLabel(port.Name) {
				pErrs = append(pErrs, errs.NewFieldInvalid("name", port.Name, ""))
			} else if allNames.Has(port.Name) {
				pErrs = append(pErrs, errs.NewFieldDuplicate("name", port.Name))
			} else {
				allNames.Insert(port.Name)
			}
		}
		if port.ContainerPort == 0 {
			pErrs = append(pErrs, errs.NewFieldRequired("containerPort", port.ContainerPort))
		} else if !util.IsValidPortNum(port.ContainerPort) {
			pErrs = append(pErrs, errs.NewFieldInvalid("containerPort", port.ContainerPort, ""))
		}
		if port.HostPort != 0 && !util.IsValidPortNum(port.HostPort) {
			pErrs = append(pErrs, errs.NewFieldInvalid("hostPort", port.HostPort, ""))
		}
		if len(port.Protocol) == 0 {
			port.Protocol = "TCP"
		} else if !supportedPortProtocols.Has(strings.ToUpper(string(port.Protocol))) {
			pErrs = append(pErrs, errs.NewFieldNotSupported("protocol", port.Protocol))
		}
		allErrs = append(allErrs, pErrs.PrefixIndex(i)...)
	}
	return allErrs
}

func validateEnv(vars []api.EnvVar) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}

	for i := range vars {
		vErrs := errs.ValidationErrorList{}
		ev := &vars[i] // so we can set default values
		if len(ev.Name) == 0 {
			vErrs = append(vErrs, errs.NewFieldRequired("name", ev.Name))
		}
		if !util.IsCIdentifier(ev.Name) {
			vErrs = append(vErrs, errs.NewFieldInvalid("name", ev.Name, ""))
		}
		allErrs = append(allErrs, vErrs.PrefixIndex(i)...)
	}
	return allErrs
}

func validateVolumeMounts(mounts []api.VolumeMount, volumes util.StringSet) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}

	for i := range mounts {
		mErrs := errs.ValidationErrorList{}
		mnt := &mounts[i] // so we can set default values
		if len(mnt.Name) == 0 {
			mErrs = append(mErrs, errs.NewFieldRequired("name", mnt.Name))
		} else if !volumes.Has(mnt.Name) {
			mErrs = append(mErrs, errs.NewFieldNotFound("name", mnt.Name))
		}
		if len(mnt.MountPath) == 0 {
			mErrs = append(mErrs, errs.NewFieldRequired("mountPath", mnt.MountPath))
		}
		allErrs = append(allErrs, mErrs.PrefixIndex(i)...)
	}
	return allErrs
}

// AccumulateUniquePorts runs an extraction function on each Port of each Container,
// accumulating the results and returning an error if any ports conflict.
func AccumulateUniquePorts(containers []api.Container, accumulator map[int]bool, extract func(*api.Port) int) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}

	for ci := range containers {
		cErrs := errs.ValidationErrorList{}
		ctr := &containers[ci]
		for pi := range ctr.Ports {
			port := extract(&ctr.Ports[pi])
			if port == 0 {
				continue
			}
			if accumulator[port] {
				cErrs = append(cErrs, errs.NewFieldDuplicate("port", port))
			} else {
				accumulator[port] = true
			}
		}
		allErrs = append(allErrs, cErrs.PrefixIndex(ci)...)
	}
	return allErrs
}

// checkHostPortConflicts checks for colliding Port.HostPort values across
// a slice of containers.
func checkHostPortConflicts(containers []api.Container) errs.ValidationErrorList {
	allPorts := map[int]bool{}
	return AccumulateUniquePorts(containers, allPorts, func(p *api.Port) int { return p.HostPort })
}

func validateExecAction(exec *api.ExecAction) errs.ValidationErrorList {
	allErrors := errs.ValidationErrorList{}
	if len(exec.Command) == 0 {
		allErrors = append(allErrors, errs.NewFieldRequired("command", exec.Command))
	}
	return allErrors
}

func validateHTTPGetAction(http *api.HTTPGetAction) errs.ValidationErrorList {
	allErrors := errs.ValidationErrorList{}
	if len(http.Path) == 0 {
		allErrors = append(allErrors, errs.NewFieldRequired("path", http.Path))
	}
	return allErrors
}

func validateHandler(handler *api.Handler) errs.ValidationErrorList {
	numHandlers := 0
	allErrors := errs.ValidationErrorList{}
	if handler.Exec != nil {
		numHandlers++
		allErrors = append(allErrors, validateExecAction(handler.Exec).Prefix("exec")...)
	}
	if handler.HTTPGet != nil {
		numHandlers++
		allErrors = append(allErrors, validateHTTPGetAction(handler.HTTPGet).Prefix("httpGet")...)
	}
	if numHandlers != 1 {
		allErrors = append(allErrors, errs.NewFieldInvalid("", handler, "exactly 1 handler type is required"))
	}
	return allErrors
}

func validateLifecycle(lifecycle *api.Lifecycle) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}
	if lifecycle.PostStart != nil {
		allErrs = append(allErrs, validateHandler(lifecycle.PostStart).Prefix("postStart")...)
	}
	if lifecycle.PreStop != nil {
		allErrs = append(allErrs, validateHandler(lifecycle.PreStop).Prefix("preStop")...)
	}
	return allErrs
}

func validateContainers(containers []api.Container, volumes util.StringSet) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}

	allNames := util.StringSet{}
	for i := range containers {
		cErrs := errs.ValidationErrorList{}
		ctr := &containers[i] // so we can set default values
		capabilities := capabilities.Get()
		if len(ctr.Name) == 0 {
			cErrs = append(cErrs, errs.NewFieldRequired("name", ctr.Name))
		} else if !util.IsDNSLabel(ctr.Name) {
			cErrs = append(cErrs, errs.NewFieldInvalid("name", ctr.Name, ""))
		} else if allNames.Has(ctr.Name) {
			cErrs = append(cErrs, errs.NewFieldDuplicate("name", ctr.Name))
		} else if ctr.Privileged && !capabilities.AllowPrivileged {
			cErrs = append(cErrs, errs.NewFieldForbidden("privileged", ctr.Privileged))
		} else {
			allNames.Insert(ctr.Name)
		}
		if len(ctr.Image) == 0 {
			cErrs = append(cErrs, errs.NewFieldRequired("image", ctr.Image))
		}
		if ctr.Lifecycle != nil {
			cErrs = append(cErrs, validateLifecycle(ctr.Lifecycle).Prefix("lifecycle")...)
		}
		cErrs = append(cErrs, validatePorts(ctr.Ports).Prefix("ports")...)
		cErrs = append(cErrs, validateEnv(ctr.Env).Prefix("env")...)
		cErrs = append(cErrs, validateVolumeMounts(ctr.VolumeMounts, volumes).Prefix("volumeMounts")...)
		allErrs = append(allErrs, cErrs.PrefixIndex(i)...)
	}
	// Check for colliding ports across all containers.
	// TODO(thockin): This really is dependent on the network config of the host (IP per pod?)
	// and the config of the new manifest.  But we have not specced that out yet, so we'll just
	// make some assumptions for now.  As of now, pods share a network namespace, which means that
	// every Port.HostPort across the whole pod must be unique.
	allErrs = append(allErrs, checkHostPortConflicts(containers)...)

	return allErrs
}

var supportedManifestVersions = util.NewStringSet("v1beta1", "v1beta2")

// ValidateManifest tests that the specified ContainerManifest has valid data.
// This includes checking formatting and uniqueness.  It also canonicalizes the
// structure by setting default values and implementing any backwards-compatibility
// tricks.
// TODO: replaced by ValidatePodSpec
func ValidateManifest(manifest *api.ContainerManifest) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}

	if len(manifest.Version) == 0 {
		allErrs = append(allErrs, errs.NewFieldRequired("version", manifest.Version))
	} else if !supportedManifestVersions.Has(strings.ToLower(manifest.Version)) {
		allErrs = append(allErrs, errs.NewFieldNotSupported("version", manifest.Version))
	}
	allVolumes, vErrs := validateVolumes(manifest.Volumes)
	allErrs = append(allErrs, vErrs.Prefix("volumes")...)
	allErrs = append(allErrs, validateContainers(manifest.Containers, allVolumes).Prefix("containers")...)
	allErrs = append(allErrs, validateRestartPolicy(&manifest.RestartPolicy).Prefix("restartPolicy")...)
	return allErrs
}

func validateRestartPolicy(restartPolicy *api.RestartPolicy) errs.ValidationErrorList {
	numPolicies := 0
	allErrors := errs.ValidationErrorList{}
	if restartPolicy.Always != nil {
		numPolicies++
	}
	if restartPolicy.OnFailure != nil {
		numPolicies++
	}
	if restartPolicy.Never != nil {
		numPolicies++
	}
	if numPolicies == 0 {
		restartPolicy.Always = &api.RestartPolicyAlways{}
	}
	if numPolicies > 1 {
		allErrors = append(allErrors, errs.NewFieldInvalid("", restartPolicy, "only 1 policy is allowed"))
	}
	return allErrors
}

func ValidatePodState(podState *api.PodState) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList(ValidateManifest(&podState.Manifest)).Prefix("manifest")
	return allErrs
}

// ValidatePod tests if required fields in the pod are set.
func ValidatePod(pod *api.Pod) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}
	if len(pod.Name) == 0 {
		allErrs = append(allErrs, errs.NewFieldRequired("name", pod.Name))
	}
	if !util.IsDNSSubdomain(pod.Namespace) {
		allErrs = append(allErrs, errs.NewFieldInvalid("namespace", pod.Namespace, ""))
	}
	allErrs = append(allErrs, ValidatePodSpec(&pod.Spec).Prefix("spec")...)
	allErrs = append(allErrs, validateLabels(pod.Labels)...)
	return allErrs
}

// ValidatePodSpec tests that the specified PodSpec has valid data.
// This includes checking formatting and uniqueness.  It also canonicalizes the
// structure by setting default values and implementing any backwards-compatibility
// tricks.
func ValidatePodSpec(spec *api.PodSpec) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}

	allVolumes, vErrs := validateVolumes(spec.Volumes)
	allErrs = append(allErrs, vErrs.Prefix("volumes")...)
	allErrs = append(allErrs, validateContainers(spec.Containers, allVolumes).Prefix("containers")...)
	allErrs = append(allErrs, validateRestartPolicy(&spec.RestartPolicy).Prefix("restartPolicy")...)
	allErrs = append(allErrs, validateLabels(spec.NodeSelector).Prefix("nodeSelector")...)
	return allErrs
}

func validateLabels(labels map[string]string) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}
	for k := range labels {
		if !util.IsDNS952Label(k) {
			allErrs = append(allErrs, errs.NewFieldNotSupported("label", k))
		}
	}
	return allErrs
}

// ValidatePodUpdate tests to see if the update is legal
func ValidatePodUpdate(newPod, oldPod *api.Pod) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}

	if newPod.Name != oldPod.Name {
		allErrs = append(allErrs, errs.NewFieldInvalid("name", newPod.Name, "field is immutable"))
	}

	if len(newPod.Spec.Containers) != len(oldPod.Spec.Containers) {
		allErrs = append(allErrs, errs.NewFieldInvalid("spec.containers", newPod.Spec.Containers, "may not add or remove containers"))
		return allErrs
	}
	pod := *newPod
	// Tricky, we need to copy the container list so that we don't overwrite the update
	var newContainers []api.Container
	for ix, container := range pod.Spec.Containers {
		container.Image = oldPod.Spec.Containers[ix].Image
		// Online upgrade,want to ignore cpu\memory\disk validate,
		// and scheduling can be perceived
		container.Core = oldPod.Spec.Containers[ix].Core
		container.Memory = oldPod.Spec.Containers[ix].Memory
		container.Disk = oldPod.Spec.Containers[ix].Disk
		newContainers = append(newContainers, container)
	}
	pod.Spec.Containers = newContainers
	if !reflect.DeepEqual(pod.Spec, oldPod.Spec) {
		// TODO: a better error would include all immutable fields explicitly.
		allErrs = append(allErrs, errs.NewFieldInvalid("spec.containers", newPod.Spec.Containers, "some fields are immutable"))
	}
	return allErrs
}

// ValidateService tests if required fields in the service are set.
func ValidateService(service *api.Service, lister ServiceLister, ctx api.Context) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}
	if len(service.Name) == 0 {
		allErrs = append(allErrs, errs.NewFieldRequired("name", service.Name))
	} else if !util.IsDNS952Label(service.Name) {
		allErrs = append(allErrs, errs.NewFieldInvalid("name", service.Name, ""))
	}
	if !util.IsDNSSubdomain(service.Namespace) {
		allErrs = append(allErrs, errs.NewFieldInvalid("namespace", service.Namespace, ""))
	}
	if !util.IsValidPortNum(service.Spec.Port) {
		allErrs = append(allErrs, errs.NewFieldInvalid("spec.port", service.Spec.Port, ""))
	}
	if len(service.Spec.Protocol) == 0 {
		service.Spec.Protocol = "TCP"
	} else if !supportedPortProtocols.Has(strings.ToUpper(string(service.Spec.Protocol))) {
		allErrs = append(allErrs, errs.NewFieldNotSupported("spec.protocol", service.Spec.Protocol))
	}
	if labels.Set(service.Spec.Selector).AsSelector().Empty() {
		allErrs = append(allErrs, errs.NewFieldRequired("spec.selector", service.Spec.Selector))
	}
	if service.Spec.CreateExternalLoadBalancer {
		services, err := lister.ListServices(ctx)
		if err != nil {
			allErrs = append(allErrs, errs.NewInternalError(err))
		} else {
			for i := range services.Items {
				if services.Items[i].Name != service.Name &&
					services.Items[i].Spec.CreateExternalLoadBalancer &&
					services.Items[i].Spec.Port == service.Spec.Port {
					allErrs = append(allErrs, errs.NewConflict("service", service.Name, fmt.Errorf("port: %d is already in use", service.Spec.Port)))
					break
				}
			}
		}
	}
	allErrs = append(allErrs, validateLabels(service.Labels)...)
	allErrs = append(allErrs, validateLabels(service.Spec.Selector)...)
	return allErrs
}

// ValidateReplicationController tests if required fields in the replication controller are set.
func ValidateReplicationController(controller *api.ReplicationController) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}
	if len(controller.Name) == 0 {
		allErrs = append(allErrs, errs.NewFieldRequired("name", controller.Name))
	}
	if !util.IsDNSSubdomain(controller.Namespace) {
		allErrs = append(allErrs, errs.NewFieldInvalid("namespace", controller.Namespace, ""))
	}
	allErrs = append(allErrs, ValidateReplicationControllerSpec(&controller.Spec).Prefix("spec")...)
	allErrs = append(allErrs, validateLabels(controller.Labels)...)
	return allErrs
}

// ValidateReplicationControllerSpec tests if required fields in the replication controller spec are set.
func ValidateReplicationControllerSpec(spec *api.ReplicationControllerSpec) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}

	selector := labels.Set(spec.Selector).AsSelector()
	if selector.Empty() {
		allErrs = append(allErrs, errs.NewFieldRequired("selector", spec.Selector))
	}
	if spec.Replicas < 0 {
		allErrs = append(allErrs, errs.NewFieldInvalid("replicas", spec.Replicas, ""))
	}

	if spec.Template == nil {
		allErrs = append(allErrs, errs.NewFieldRequired("template", spec.Template))
	} else {
		labels := labels.Set(spec.Template.Labels)
		if !selector.Matches(labels) {
			allErrs = append(allErrs, errs.NewFieldInvalid("template.labels", spec.Template.Labels, "selector does not match template"))
		}
		allErrs = append(allErrs, validateLabels(spec.Template.Labels).Prefix("template.labels")...)
		allErrs = append(allErrs, ValidatePodTemplateSpec(spec.Template).Prefix("template")...)
		// RestartPolicy has already been first-order validated as per ValidatePodTemplateSpec().
		if spec.Template.Spec.RestartPolicy.Always == nil {
			// TODO: should probably be Unsupported
			// TODO: api.RestartPolicy should have a String() method for nicer printing
			allErrs = append(allErrs, errs.NewFieldInvalid("template.restartPolicy", spec.Template.Spec.RestartPolicy, "must be Always"))
		}
	}
	return allErrs
}

// ValidatePodTemplateSpec validates the spec of a pod template
func ValidatePodTemplateSpec(spec *api.PodTemplateSpec) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}
	allErrs = append(allErrs, ValidatePodSpec(&spec.Spec).Prefix("spec")...)
	allErrs = append(allErrs, ValidateReadOnlyPersistentDisks(spec.Spec.Volumes).Prefix("spec.volumes")...)
	return allErrs
}

func ValidateReadOnlyPersistentDisks(volumes []api.Volume) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}
	for _, vol := range volumes {
		if vol.Source.GCEPersistentDisk != nil {
			if vol.Source.GCEPersistentDisk.ReadOnly == false {
				allErrs = append(allErrs, errs.NewFieldInvalid("GCEPersistentDisk.ReadOnly", false, "must be true"))
			}
		}
	}
	return allErrs
}

// ValidateBoundPod tests if required fields on a bound pod are set.
func ValidateBoundPod(pod *api.BoundPod) (errors []error) {
	if !util.IsDNSSubdomain(pod.Name) {
		errors = append(errors, errs.NewFieldInvalid("name", pod.Name, ""))
	}
	if !util.IsDNSSubdomain(pod.Namespace) {
		errors = append(errors, errs.NewFieldInvalid("namespace", pod.Namespace, ""))
	}
	containerManifest := &api.ContainerManifest{
		Version:       "v1beta2",
		ID:            pod.Name,
		UUID:          pod.UID,
		Containers:    pod.Spec.Containers,
		Volumes:       pod.Spec.Volumes,
		RestartPolicy: pod.Spec.RestartPolicy,
	}
	if errs := ValidateManifest(containerManifest); len(errs) != 0 {
		errors = append(errors, errs...)
	}
	return errors
}

// ValidateMinion tests if required fields in the minion are set.
func ValidateMinion(minion *api.Minion) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}
	if len(minion.Name) == 0 {
		allErrs = append(allErrs, errs.NewFieldRequired("name", minion.Name))
	}
	allErrs = append(allErrs, validateLabels(minion.Labels)...)
	return allErrs
}

// ValidateMinionUpdate tests to make sure a minion update can be applied.  Modifies oldMinion.
func ValidateMinionUpdate(oldMinion *api.Minion, minion *api.Minion) errs.ValidationErrorList {
	allErrs := errs.ValidationErrorList{}
	oldMinion.Labels = minion.Labels
	// update vms
	oldMinion.Spec.VMs = minion.Spec.VMs
	if minion.Name != oldMinion.Name {
		allErrs = append(allErrs, fmt.Errorf("pod name is being changed"))
	}

	minion.ObjectMeta = oldMinion.ObjectMeta
	if !reflect.DeepEqual(oldMinion, minion) {
		allErrs = append(allErrs, fmt.Errorf("update contains more than labels/vms changes"))
	}
	return allErrs
}
