package fs

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/docker/libcontainer/cgroups"
)

var (
	ErrCanNotAccess = errors.New("this subsystem can not be accessed")
)

func Set(id, driver, group, subsystem, value string) error {
	path, err := GetPath(id, driver, group, subsystem)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, []byte(value), 0700)
}

func Get(id, driver, group, subsystem string) (string, error) {
	path, err := GetPath(id, driver, group, subsystem)
	if err != nil {
		return "", err
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(string(data), "\n"), nil
}

func GetPath(id, driver, group, subsystem string) (string, error) {
	cgroupRoot, err := cgroups.FindCgroupMountpoint("cpu")
	if err != nil {
		return "", err
	}

	cgroupRoot = filepath.Dir(cgroupRoot)
	if _, err := os.Stat(cgroupRoot); err != nil {
		return "", fmt.Errorf("cgroups fs not found")
	}

	initPath, err := cgroups.GetInitCgroupDir(group)
	if err != nil {
		return "", err
	}

	path := path.Join(cgroupRoot, group, initPath, driver, id, subsystem)
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("%s not found", path)
	}
	return path, nil
}
