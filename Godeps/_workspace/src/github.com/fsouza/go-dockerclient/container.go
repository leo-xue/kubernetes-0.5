// Copyright 2014 go-dockerclient authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package docker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ListContainersOptions specify parameters to the ListContainers function.
//
// See http://goo.gl/XqtcyU for more details.
type ListContainersOptions struct {
	All    bool
	Size   bool
	Limit  int
	Since  string
	Before string
}

type APIPort struct {
	PrivatePort int64  `json:"PrivatePort,omitempty" yaml:"PrivatePort,omitempty"`
	PublicPort  int64  `json:"PublicPort,omitempty" yaml:"PublicPort,omitempty"`
	Type        string `json:"Type,omitempty" yaml:"Type,omitempty"`
	IP          string `json:"IP,omitempty" yaml:"IP,omitempty"`
}

// APIContainers represents a container.
//
// See http://goo.gl/QeFH7U for more details.
type APIContainers struct {
	ID         string    `json:"Id" yaml:"Id"`
	Image      string    `json:"Image,omitempty" yaml:"Image,omitempty"`
	Command    string    `json:"Command,omitempty" yaml:"Command,omitempty"`
	Created    int64     `json:"Created,omitempty" yaml:"Created,omitempty"`
	Status     string    `json:"Status,omitempty" yaml:"Status,omitempty"`
	Ports      []APIPort `json:"Ports,omitempty" yaml:"Ports,omitempty"`
	SizeRw     int64     `json:"SizeRw,omitempty" yaml:"SizeRw,omitempty"`
	SizeRootFs int64     `json:"SizeRootFs,omitempty" yaml:"SizeRootFs,omitempty"`
	Names      []string  `json:"Names,omitempty" yaml:"Names,omitempty"`
}

// ListContainers returns a slice of containers matching the given criteria.
//
// See http://goo.gl/XqtcyU for more details.
func (c *Client) ListContainers(opts ListContainersOptions) ([]APIContainers, error) {
	path := "/containers/json?" + queryString(opts)
	body, _, err := c.do("GET", path, nil)
	if err != nil {
		return nil, err
	}
	var containers []APIContainers
	err = json.Unmarshal(body, &containers)
	if err != nil {
		return nil, err
	}
	return containers, nil
}

// Port represents the port number and the protocol, in the form
// <number>/<protocol>. For example: 80/tcp.
type Port string

// Port returns the number of the port.
func (p Port) Port() string {
	return strings.Split(string(p), "/")[0]
}

// Proto returns the name of the protocol.
func (p Port) Proto() string {
	parts := strings.Split(string(p), "/")
	if len(parts) == 1 {
		return "tcp"
	}
	return parts[1]
}

// State represents the state of a container.
type State struct {
	Running    bool      `json:"Running,omitempty" yaml:"Running,omitempty"`
	Paused     bool      `json:"Paused,omitempty" yaml:"Paused,omitempty"`
	Pid        int       `json:"Pid,omitempty" yaml:"Pid,omitempty"`
	ExitCode   int       `json:"ExitCode,omitempty" yaml:"ExitCode,omitempty"`
	StartedAt  time.Time `json:"StartedAt,omitempty" yaml:"StartedAt,omitempty"`
	FinishedAt time.Time `json:"FinishedAt,omitempty" yaml:"FinishedAt,omitempty"`
}

// String returns the string representation of a state.
func (s *State) String() string {
	if s.Running {
		if s.Paused {
			return "paused"
		}
		return fmt.Sprintf("Up %s", time.Now().UTC().Sub(s.StartedAt))
	}
	return fmt.Sprintf("Exit %d", s.ExitCode)
}

type PortBinding struct {
	HostIp   string `json:"HostIP,omitempty" yaml:"HostIP,omitempty"`
	HostPort string `json:"HostPort,omitempty" yaml:"HostPort,omitempty"`
}

type PortMapping map[string]string

type NetworkSettings struct {
	IPAddress   string                 `json:"IPAddress,omitempty" yaml:"IPAddress,omitempty"`
	IPPrefixLen int                    `json:"IPPrefixLen,omitempty" yaml:"IPPrefixLen,omitempty"`
	Gateway     string                 `json:"Gateway,omitempty" yaml:"Gateway,omitempty"`
	Bridge      string                 `json:"Bridge,omitempty" yaml:"Bridge,omitempty"`
	PortMapping map[string]PortMapping `json:"PortMapping,omitempty" yaml:"PortMapping,omitempty"`
	Ports       map[Port][]PortBinding `json:"Ports,omitempty" yaml:"Ports,omitempty"`
}

func (settings *NetworkSettings) PortMappingAPI() []APIPort {
	var mapping []APIPort
	for port, bindings := range settings.Ports {
		p, _ := parsePort(port.Port())
		if len(bindings) == 0 {
			mapping = append(mapping, APIPort{
				PublicPort: int64(p),
				Type:       port.Proto(),
			})
			continue
		}
		for _, binding := range bindings {
			p, _ := parsePort(port.Port())
			h, _ := parsePort(binding.HostPort)
			mapping = append(mapping, APIPort{
				PrivatePort: int64(p),
				PublicPort:  int64(h),
				Type:        port.Proto(),
				IP:          binding.HostIp,
			})
		}
	}
	return mapping
}

func parsePort(rawPort string) (int, error) {
	port, err := strconv.ParseUint(rawPort, 10, 16)
	if err != nil {
		return 0, err
	}
	return int(port), nil
}

type Config struct {
	Hostname        string              `json:"Hostname,omitempty" yaml:"Hostname,omitempty"`
	Domainname      string              `json:"Domainname,omitempty" yaml:"Domainname,omitempty"`
	User            string              `json:"User,omitempty" yaml:"User,omitempty"`
	Memory          int64               `json:"Memory,omitempty" yaml:"Memory,omitempty"`
	MemorySwap      int64               `json:"MemorySwap,omitempty" yaml:"MemorySwap,omitempty"`
	CpuShares       int64               `json:"CpuShares,omitempty" yaml:"CpuShares,omitempty"`
	CpuSet          string              `json:"Cpuset,omitempty" yaml:"Cpuset,omitempty"`
	AttachStdin     bool                `json:"AttachStdin,omitempty" yaml:"AttachStdin,omitempty"`
	AttachStdout    bool                `json:"AttachStdout,omitempty" yaml:"AttachStdout,omitempty"`
	AttachStderr    bool                `json:"AttachStderr,omitempty" yaml:"AttachStderr,omitempty"`
	PortSpecs       []string            `json:"PortSpecs,omitempty" yaml:"PortSpecs,omitempty"`
	ExposedPorts    map[Port]struct{}   `json:"ExposedPorts,omitempty" yaml:"ExposedPorts,omitempty"`
	Tty             bool                `json:"Tty,omitempty" yaml:"Tty,omitempty"`
	OpenStdin       bool                `json:"OpenStdin,omitempty" yaml:"OpenStdin,omitempty"`
	StdinOnce       bool                `json:"StdinOnce,omitempty" yaml:"StdinOnce,omitempty"`
	Env             []string            `json:"Env,omitempty" yaml:"Env,omitempty"`
	Cmd             []string            `json:"Cmd,omitempty" yaml:"Cmd,omitempty"`
	Dns             []string            `json:"Dns,omitempty" yaml:"Dns,omitempty"` // For Docker API v1.9 and below only
	Image           string              `json:"Image,omitempty" yaml:"Image,omitempty"`
	Volumes         map[string]struct{} `json:"Volumes,omitempty" yaml:"Volumes,omitempty"`
	VolumesFrom     string              `json:"VolumesFrom,omitempty" yaml:"VolumesFrom,omitempty"`
	WorkingDir      string              `json:"WorkingDir,omitempty" yaml:"WorkingDir,omitempty"`
	Entrypoint      []string            `json:"Entrypoint,omitempty" yaml:"Entrypoint,omitempty"`
	NetworkDisabled bool                `json:"NetworkDisabled,omitempty" yaml:"NetworkDisabled,omitempty"`
}

type Container struct {
	ID string `json:"Id" yaml:"Id"`

	Created time.Time `json:"Created,omitempty" yaml:"Created,omitempty"`

	Path string   `json:"Path,omitempty" yaml:"Path,omitempty"`
	Args []string `json:"Args,omitempty" yaml:"Args,omitempty"`

	Config *Config `json:"Config,omitempty" yaml:"Config,omitempty"`
	State  State   `json:"State,omitempty" yaml:"State,omitempty"`
	Image  string  `json:"Image,omitempty" yaml:"Image,omitempty"`

	NetworkSettings *NetworkSettings `json:"NetworkSettings,omitempty" yaml:"NetworkSettings,omitempty"`

	SysInitPath    string `json:"SysInitPath,omitempty" yaml:"SysInitPath,omitempty"`
	ResolvConfPath string `json:"ResolvConfPath,omitempty" yaml:"ResolvConfPath,omitempty"`
	HostnamePath   string `json:"HostnamePath,omitempty" yaml:"HostnamePath,omitempty"`
	HostsPath      string `json:"HostsPath,omitempty" yaml:"HostsPath,omitempty"`
	Name           string `json:"Name,omitempty" yaml:"Name,omitempty"`
	Driver         string `json:"Driver,omitempty" yaml:"Driver,omitempty"`

	Volumes    map[string]string `json:"Volumes,omitempty" yaml:"Volumes,omitempty"`
	VolumesRW  map[string]bool   `json:"VolumesRW,omitempty" yaml:"VolumesRW,omitempty"`
	HostConfig *HostConfig       `json:"HostConfig,omitempty" yaml:"HostConfig,omitempty"`
}

// InspectContainer returns information about a container by its ID.
//
// See http://goo.gl/CxVuJ5 for more details.
func (c *Client) InspectContainer(id string) (*Container, error) {
	path := "/containers/" + id + "/json"
	body, status, err := c.do("GET", path, nil)
	if status == http.StatusNotFound {
		return nil, &NoSuchContainer{ID: id}
	}
	if err != nil {
		return nil, err
	}
	var container Container
	err = json.Unmarshal(body, &container)
	if err != nil {
		return nil, err
	}
	return &container, nil
}

// ContainerChanges returns changes in the filesystem of the given container.
//
// See http://goo.gl/QkW9sH for more details.
func (c *Client) ContainerChanges(id string) ([]Change, error) {
	path := "/containers/" + id + "/changes"
	body, status, err := c.do("GET", path, nil)
	if status == http.StatusNotFound {
		return nil, &NoSuchContainer{ID: id}
	}
	if err != nil {
		return nil, err
	}
	var changes []Change
	err = json.Unmarshal(body, &changes)
	if err != nil {
		return nil, err
	}
	return changes, nil
}

// CreateContainerOptions specify parameters to the CreateContainer function.
//
// See http://goo.gl/mErxNp for more details.
type CreateContainerOptions struct {
	Name   string
	Config *Config `qs:"-"`
}

// CreateContainer creates a new container, returning the container instance,
// or an error in case of failure.
//
// See http://goo.gl/mErxNp for more details.
func (c *Client) CreateContainer(opts CreateContainerOptions) (*Container, error) {
	path := "/containers/create?" + queryString(opts)
	body, status, err := c.do("POST", path, opts.Config)
	if status == http.StatusNotFound {
		return nil, ErrNoSuchImage
	}
	if err != nil {
		return nil, err
	}
	var container Container
	err = json.Unmarshal(body, &container)
	if err != nil {
		return nil, err
	}

	container.Name = opts.Name

	return &container, nil
}

type KeyValuePair struct {
	Key   string `json:"Key,omitempty" yaml:"Key,omitempty"`
	Value string `json:"Value,omitempty" yaml:"Value,omitempty"`
}

// RestartPolicy represents the policy for automatically restarting a container.
//
// Possible values are:
//
//   - always: the docker daemon will always restart the container
//   - on-failure: the docker daemon will restart the container on failures, at
//                 most MaximumRetryCount times
//   - no: the docker daemon will not restart the container automatically
type RestartPolicy struct {
	Name              string `json:"Name,omitempty" yaml:"Name,omitempty"`
	MaximumRetryCount int    `json:"MaximumRetryCount,omitempty" yaml:"MaximumRetryCount,omitempty"`
}

// AlwaysRestart returns a restart policy that tells the Docker daemon to
// always restart the container.
func AlwaysRestart() RestartPolicy {
	return RestartPolicy{Name: "always"}
}

// RestartOnFailure returns a restart policy that tells the Docker daemon to
// restart the container on failures, trying at most maxRetry times.
func RestartOnFailure(maxRetry int) RestartPolicy {
	return RestartPolicy{Name: "on-failure", MaximumRetryCount: maxRetry}
}

// NeverRestart returns a restart policy that tells the Docker daemon to never
// restart the container on failures.
func NeverRestart() RestartPolicy {
	return RestartPolicy{Name: "no"}
}

type HostConfig struct {
	Binds           []string               `json:"Binds,omitempty" yaml:"Binds,omitempty"`
	CapAdd          []string               `json:"CapAdd,omitempty" yaml:"CapAdd,omitempty"`
	CapDrop         []string               `json:"CapDrop,omitempty" yaml:"CapDrop,omitempty"`
	ContainerIDFile string                 `json:"ContainerIDFile,omitempty" yaml:"ContainerIDFile,omitempty"`
	LxcConf         []KeyValuePair         `json:"LxcConf,omitempty" yaml:"LxcConf,omitempty"`
	Privileged      bool                   `json:"Privileged,omitempty" yaml:"Privileged,omitempty"`
	PortBindings    map[Port][]PortBinding `json:"PortBindings,omitempty" yaml:"PortBindings,omitempty"`
	Links           []string               `json:"Links,omitempty" yaml:"Links,omitempty"`
	PublishAllPorts bool                   `json:"PublishAllPorts,omitempty" yaml:"PublishAllPorts,omitempty"`
	Dns             []string               `json:"Dns,omitempty" yaml:"Dns,omitempty"` // For Docker API v1.10 and above only
	DnsSearch       []string               `json:"DnsSearch,omitempty" yaml:"DnsSearch,omitempty"`
	VolumesFrom     []string               `json:"VolumesFrom,omitempty" yaml:"VolumesFrom,omitempty"`
	NetworkMode     string                 `json:"NetworkMode,omitempty" yaml:"NetworkMode,omitempty"`
	RestartPolicy   RestartPolicy          `json:"RestartPolicy,omitempty" yaml:"RestartPolicy,omitempty"`
}

// StartContainer starts a container, returning an error in case of failure.
//
// See http://goo.gl/iM5GYs for more details.
func (c *Client) StartContainer(id string, hostConfig *HostConfig) error {
	if hostConfig == nil {
		hostConfig = &HostConfig{}
	}
	path := "/containers/" + id + "/start"
	_, status, err := c.do("POST", path, hostConfig)
	if status == http.StatusNotFound {
		return &NoSuchContainer{ID: id}
	}
	if status == http.StatusNotModified {
		return &ContainerAlreadyRunning{ID: id}
	}
	if err != nil {
		return err
	}
	return nil
}

// StopContainer stops a container, killing it after the given timeout (in
// seconds).
//
// See http://goo.gl/EbcpXt for more details.
func (c *Client) StopContainer(id string, timeout uint) error {
	path := fmt.Sprintf("/containers/%s/stop?t=%d", id, timeout)
	_, status, err := c.do("POST", path, nil)
	if status == http.StatusNotFound {
		return &NoSuchContainer{ID: id}
	}
	if status == http.StatusNotModified {
		return &ContainerNotRunning{ID: id}
	}
	if err != nil {
		return err
	}
	return nil
}

// RestartContainer stops a container, killing it after the given timeout (in
// seconds), during the stop process.
//
// See http://goo.gl/VOzR2n for more details.
func (c *Client) RestartContainer(id string, timeout uint) error {
	path := fmt.Sprintf("/containers/%s/restart?t=%d", id, timeout)
	_, status, err := c.do("POST", path, nil)
	if status == http.StatusNotFound {
		return &NoSuchContainer{ID: id}
	}
	if err != nil {
		return err
	}
	return nil
}

// PauseContainer pauses the given container.
//
// See http://goo.gl/AM5t42 for more details.
func (c *Client) PauseContainer(id string) error {
	path := fmt.Sprintf("/containers/%s/pause", id)
	_, status, err := c.do("POST", path, nil)
	if status == http.StatusNotFound {
		return &NoSuchContainer{ID: id}
	}
	if err != nil {
		return err
	}
	return nil
}

// UnpauseContainer pauses the given container.
//
// See http://goo.gl/eBrNSL for more details.
func (c *Client) UnpauseContainer(id string) error {
	path := fmt.Sprintf("/containers/%s/unpause", id)
	_, status, err := c.do("POST", path, nil)
	if status == http.StatusNotFound {
		return &NoSuchContainer{ID: id}
	}
	if err != nil {
		return err
	}
	return nil
}

// TopResult represents the list of processes running in a container, as
// returned by /containers/<id>/top.
//
// See http://goo.gl/qu4gse for more details.
type TopResult struct {
	Titles    []string
	Processes [][]string
}

// TopContainer returns processes running inside a container
//
// See http://goo.gl/qu4gse for more details.
func (c *Client) TopContainer(id string, psArgs string) (TopResult, error) {
	var args string
	var result TopResult
	if psArgs != "" {
		args = fmt.Sprintf("?ps_args=%s", psArgs)
	}
	path := fmt.Sprintf("/containers/%s/top%s", id, args)
	body, status, err := c.do("GET", path, nil)
	if status == http.StatusNotFound {
		return result, &NoSuchContainer{ID: id}
	}
	if err != nil {
		return result, err
	}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return result, err
	}
	return result, nil
}

// KillContainerOptions represents the set of options that can be used in a
// call to KillContainer.
//
// See http://goo.gl/TFkECx for more details.
type KillContainerOptions struct {
	// The ID of the container.
	ID string `qs:"-"`

	// The signal to send to the container. When omitted, Docker server
	// will assume SIGKILL.
	Signal Signal
}

// KillContainer kills a container, returning an error in case of failure.
//
// See http://goo.gl/TFkECx for more details.
func (c *Client) KillContainer(opts KillContainerOptions) error {
	path := "/containers/" + opts.ID + "/kill" + "?" + queryString(opts)
	_, status, err := c.do("POST", path, nil)
	if status == http.StatusNotFound {
		return &NoSuchContainer{ID: opts.ID}
	}
	if err != nil {
		return err
	}
	return nil
}

// RemoveContainerOptions encapsulates options to remove a container.
//
// See http://goo.gl/ZB83ji for more details.
type RemoveContainerOptions struct {
	// The ID of the container.
	ID string `qs:"-"`

	// A flag that indicates whether Docker should remove the volumes
	// associated to the container.
	RemoveVolumes bool `qs:"v"`

	// A flag that indicates whether Docker should remove the container
	// even if it is currently running.
	Force bool
}

// RemoveContainer removes a container, returning an error in case of failure.
//
// See http://goo.gl/ZB83ji for more details.
func (c *Client) RemoveContainer(opts RemoveContainerOptions) error {
	path := "/containers/" + opts.ID + "?" + queryString(opts)
	_, status, err := c.do("DELETE", path, nil)
	if status == http.StatusNotFound {
		return &NoSuchContainer{ID: opts.ID}
	}
	if err != nil {
		return err
	}
	return nil
}

// CopyFromContainerOptions is the set of options that can be used when copying
// files or folders from a container.
//
// See http://goo.gl/rINMlw for more details.
type CopyFromContainerOptions struct {
	OutputStream io.Writer `json:"-"`
	Container    string    `json:"-"`
	Resource     string
}

// CopyFromContainer copy files or folders from a container, using a given
// resource.
//
// See http://goo.gl/rINMlw for more details.
func (c *Client) CopyFromContainer(opts CopyFromContainerOptions) error {
	if opts.Container == "" {
		return &NoSuchContainer{ID: opts.Container}
	}
	url := fmt.Sprintf("/containers/%s/copy", opts.Container)
	body, status, err := c.do("POST", url, opts)
	if status == http.StatusNotFound {
		return &NoSuchContainer{ID: opts.Container}
	}
	if err != nil {
		return err
	}
	io.Copy(opts.OutputStream, bytes.NewBuffer(body))
	return nil
}

// WaitContainer blocks until the given container stops, return the exit code
// of the container status.
//
// See http://goo.gl/J88DHU for more details.
func (c *Client) WaitContainer(id string) (int, error) {
	body, status, err := c.do("POST", "/containers/"+id+"/wait", nil)
	if status == http.StatusNotFound {
		return 0, &NoSuchContainer{ID: id}
	}
	if err != nil {
		return 0, err
	}
	var r struct{ StatusCode int }
	err = json.Unmarshal(body, &r)
	if err != nil {
		return 0, err
	}
	return r.StatusCode, nil
}

// CommitContainerOptions aggregates parameters to the CommitContainer method.
//
// See http://goo.gl/Jn8pe8 for more details.
type ChangeOptions struct {
	Includes []string
	Excludes []string
}

type CommitContainerOptions struct {
	Container  string
	Repository string `qs:"repo"`
	Tag        string
	Message    string `qs:"m"`
	Author     string
	Run        *Config `qs:"-"`
	Options    *ChangeOptions
}

// CommitContainer creates a new image from a container's changes.
//
// See http://goo.gl/Jn8pe8 for more details.
func (c *Client) CommitContainer(opts CommitContainerOptions) (*Image, error) {
	path := "/commit?" + queryString(opts)
	//	body, status, err := c.do("POST", path, opts.Run)
	body, status, err := c.do("POST", path, map[string]*ChangeOptions{"changeOptions": opts.Options})
	if status == http.StatusNotFound {
		return nil, &NoSuchContainer{ID: opts.Container}
	}
	if err != nil {
		return nil, err
	}
	var image Image
	err = json.Unmarshal(body, &image)
	if err != nil {
		return nil, err
	}
	return &image, nil
}

// AttachToContainerOptions is the set of options that can be used when
// attaching to a container.
//
// See http://goo.gl/RRAhws for more details.
type AttachToContainerOptions struct {
	Container    string    `qs:"-"`
	InputStream  io.Reader `qs:"-"`
	OutputStream io.Writer `qs:"-"`
	ErrorStream  io.Writer `qs:"-"`

	// Get container logs, sending it to OutputStream.
	Logs bool

	// Stream the response?
	Stream bool

	// Attach to stdin, and use InputStream.
	Stdin bool

	// Attach to stdout, and use OutputStream.
	Stdout bool

	// Attach to stderr, and use ErrorStream.
	Stderr bool

	// If set, after a successful connect, a sentinel will be sent and then the
	// client will block on receive before continuing.
	//
	// It must be an unbuffered channel. Using a buffered channel can lead
	// to unexpected behavior.
	Success chan struct{}

	// Use raw terminal? Usually true when the container contains a TTY.
	RawTerminal bool `qs:"-"`
}

// AttachToContainer attaches to a container, using the given options.
//
// See http://goo.gl/RRAhws for more details.
func (c *Client) AttachToContainer(opts AttachToContainerOptions) error {
	if opts.Container == "" {
		return &NoSuchContainer{ID: opts.Container}
	}
	path := "/containers/" + opts.Container + "/attach?" + queryString(opts)
	return c.hijack("POST", path, opts.Success, opts.RawTerminal, opts.InputStream, opts.ErrorStream, opts.OutputStream, nil)
}

// LogsOptions represents the set of options used when getting logs from a
// container.
//
// See http://goo.gl/rLhKSU for more details.
type LogsOptions struct {
	Container    string    `qs:"-"`
	OutputStream io.Writer `qs:"-"`
	ErrorStream  io.Writer `qs:"-"`
	Follow       bool
	Stdout       bool
	Stderr       bool
	Timestamps   bool
	Tail         string

	// Use raw terminal? Usually true when the container contains a TTY.
	RawTerminal bool `qs:"-"`
}

// Logs gets stdout and stderr logs from the specified container.
//
// See http://goo.gl/rLhKSU for more details.
func (c *Client) Logs(opts LogsOptions) error {
	if opts.Container == "" {
		return &NoSuchContainer{ID: opts.Container}
	}
	if opts.Tail == "" {
		opts.Tail = "all"
	}
	path := "/containers/" + opts.Container + "/logs?" + queryString(opts)
	return c.stream("GET", path, opts.RawTerminal, false, nil, nil, opts.OutputStream, opts.ErrorStream)
}

// ResizeContainerTTY resizes the terminal to the given height and width.
func (c *Client) ResizeContainerTTY(id string, height, width int) error {
	params := make(url.Values)
	params.Set("h", strconv.Itoa(height))
	params.Set("w", strconv.Itoa(width))
	_, _, err := c.do("POST", "/containers/"+id+"/resize?"+params.Encode(), nil)
	return err
}

// ExportContainerOptions is the set of parameters to the ExportContainer
// method.
//
// See http://goo.gl/hnzE62 for more details.
type ExportContainerOptions struct {
	ID           string
	OutputStream io.Writer
}

// ExportContainer export the contents of container id as tar archive
// and prints the exported contents to stdout.
//
// See http://goo.gl/hnzE62 for more details.
func (c *Client) ExportContainer(opts ExportContainerOptions) error {
	if opts.ID == "" {
		return &NoSuchContainer{ID: opts.ID}
	}
	url := fmt.Sprintf("/containers/%s/export", opts.ID)
	return c.stream("GET", url, true, false, nil, nil, opts.OutputStream, nil)
}

// NoSuchContainer is the error returned when a given container does not exist.
type NoSuchContainer struct {
	ID string
}

func (err *NoSuchContainer) Error() string {
	return "No such container: " + err.ID
}

// ContainerAlreadyRunning is the error returned when a given container is
// already running.
type ContainerAlreadyRunning struct {
	ID string
}

func (err *ContainerAlreadyRunning) Error() string {
	return "Container already running: " + err.ID
}

// ContainerNotRunning is the error returned when a given container is not
// running.
type ContainerNotRunning struct {
	ID string
}

func (err *ContainerNotRunning) Error() string {
	return "Container not running: " + err.ID
}

func (c *Client) UpdateContainerCgroup(id string, conf []KeyValuePair) ([]CgroupResponse, error) {
	if conf == nil {
		return nil, fmt.Errorf("params conf is nil: %v", conf)
	}
	data := make(map[string][]KeyValuePair)
	data["WriteSubsystem"] = conf
	path := "/containers/" + id + "/cgroup?w=1"
	body, status, err := c.do("POST", path, data)
	if status == http.StatusNotFound {
		return nil, &NoSuchContainer{ID: id}
	}
	if err != nil {
		return nil, err
	}
	var resp []CgroupResponse
	err = json.Unmarshal(body, &resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) UpdateContainerConfig(id string, conf []KeyValuePair) error {
	if conf == nil {
		return fmt.Errorf("params conf is nill: %v", conf)
	}
	data := make(map[string][]KeyValuePair)
	data["config"] = conf
	path := "/containers/" + id + "/set?"
	body, status, err := c.do("POST", path, data)
	if status == http.StatusNotFound {
		return &NoSuchContainer{ID: id}
	}
	if err != nil {
		return err
	}
	var response []struct {
		Key    string
		Err    string
		Status int
	}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return err
	}
	var object []string
	for _, r := range response {
		if r.Status > 0 {
			object = append(object, fmt.Sprintf("Update %s err:%s", r.Key, r.Err))
		}
	}
	if len(object) > 0 {
		return fmt.Errorf(strings.Join(object, ";"))
	}
	return nil
}
