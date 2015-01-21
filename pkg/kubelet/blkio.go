// Copyright 2015 hbohuang authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kubelet

import (
	"fmt"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/api"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strings"
)

type BlkioGroup struct {
}

const cgroupRootDir = "/cgroup/blkio/docker"

func writeFile(path, file, data string) error {
	return ioutil.WriteFile(filepath.Join(path, file), []byte(data), 0700)
}

func getPath(containerID string) string {
	return filepath.Join(cgroupRootDir, containerID)
}

func getRootfsDM(containerID string) (string, error) {
	var (
		rootfs string
		major  string
		minor  string
	)
	out, err := exec.Command("ls", "-lh", "/dev/mapper").CombinedOutput()
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if strings.Contains(line, containerID) {
			if len(fields) < 10 {
				return "", fmt.Errorf("Failed to get /dev/mapper info")
			}
			rootfs = filepath.Base(fields[10])
			break
		}
	}

	out, err = exec.Command("ls", "-lh", filepath.Join("/dev", rootfs)).CombinedOutput()
	if err != nil {
		return "", err
	}
	fields := strings.Fields(string(out))
	if len(fields) < 5 {
		return "", fmt.Errorf("Failed to get /dev/%s info", rootfs)
	}
	major = strings.Replace(fields[4], ",", "", -1)
	minor = fields[5]
	return fmt.Sprintf("%s:%s", major, minor), nil
}

func (s *BlkioGroup) SetUp(containerID string, blkio *api.Blkio) error {
	rootfs, err := getRootfsDM(containerID)
	if err != nil {
		return err
	}

	if err = writeFile(getPath(containerID), "blkio.throttle.read_bps_device", fmt.Sprintf("%s %d", rootfs, blkio.ReadBPSDevice)); err != nil {
		return err
	}

	if err = writeFile(getPath(containerID), "blkio.throttle.read_bps_device", fmt.Sprintf("%s %d", "8:0", blkio.ReadBPSDevice)); err != nil {
		return err
	}

	if err = writeFile(getPath(containerID), "blkio.throttle.write_bps_device", fmt.Sprintf("%s %d", rootfs, blkio.WriteBPSDevice)); err != nil {
		return err
	}

	if err = writeFile(getPath(containerID), "blkio.throttle.write_bps_device", fmt.Sprintf("%s %d", "8:0", blkio.WriteBPSDevice)); err != nil {
		return err
	}

	if err = writeFile(getPath(containerID), "blkio.throttle.read_iops_device", fmt.Sprintf("%s %d", rootfs, blkio.ReadIOPSDevice)); err != nil {
		return err
	}

	if err = writeFile(getPath(containerID), "blkio.throttle.read_iops_device", fmt.Sprintf("%s %d", "8:0", blkio.ReadIOPSDevice)); err != nil {
		return err
	}

	if err = writeFile(getPath(containerID), "blkio.throttle.write_iops_device", fmt.Sprintf("%s %d", rootfs, blkio.WriteIOPSDevice)); err != nil {
		return err
	}

	if err = writeFile(getPath(containerID), "blkio.throttle.write_iops_device", fmt.Sprintf("%s %d", "8:0", blkio.WriteIOPSDevice)); err != nil {
		return err
	}

	if err = writeFile(getPath(containerID), "blkio.weight_device", fmt.Sprintf("%s %d", rootfs, blkio.WeightDevice)); err != nil {
		return err
	}

	if err = writeFile(getPath(containerID), "blkio.weight_device", fmt.Sprintf("%s %d", "8:0", blkio.WeightDevice)); err != nil {
		return err
	}

	return nil
}

