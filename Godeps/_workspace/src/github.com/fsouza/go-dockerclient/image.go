// Copyright 2014 go-dockerclient authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package docker

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"
)

// APIImages represent an image returned in the ListImages call.
type APIImages struct {
	ID          string   `json:"Id" yaml:"Id"`
	RepoTags    []string `json:"RepoTags,omitempty" yaml:"RepoTags,omitempty"`
	Created     int64    `json:"Created,omitempty" yaml:"Created,omitempty"`
	Size        int64    `json:"Size,omitempty" yaml:"Size,omitempty"`
	VirtualSize int64    `json:"VirtualSize,omitempty" yaml:"VirtualSize,omitempty"`
	ParentId    string   `json:"ParentId,omitempty" yaml:"ParentId,omitempty"`
}

type Image struct {
	ID              string    `json:"Id" yaml:"Id"`
	Parent          string    `json:"Parent,omitempty" yaml:"Parent,omitempty"`
	Comment         string    `json:"Comment,omitempty" yaml:"Comment,omitempty"`
	Created         time.Time `json:"Created,omitempty" yaml:"Created,omitempty"`
	Container       string    `json:"Container,omitempty" yaml:"Container,omitempty"`
	ContainerConfig Config    `json:"ContainerConfig,omitempty" yaml:"ContainerConfig,omitempty"`
	DockerVersion   string    `json:"DockerVersion,omitempty" yaml:"DockerVersion,omitempty"`
	Author          string    `json:"Author,omitempty" yaml:"Author,omitempty"`
	Config          *Config   `json:"Config,omitempty" yaml:"Config,omitempty"`
	Architecture    string    `json:"Architecture,omitempty" yaml:"Architecture,omitempty"`
	Size            int64     `json:"Size,omitempty" yaml:"Size,omitempty"`
}

// ImageHistory represent a layer in an image's history returned by the
// ImageHistory call.
type ImageHistory struct {
	ID        string   `json:"Id" yaml:"Id"`
	Tags      []string `json:"Tags,omitempty" yaml:"Tags,omitempty"`
	Created   int64    `json:"Created,omitempty" yaml:"Created,omitempty"`
	CreatedBy string   `json:"CreatedBy,omitempty" yaml:"CreatedBy,omitempty"`
	Size      int64    `json:"Size,omitempty" yaml:"Size,omitempty"`
}

type ImagePre012 struct {
	ID              string    `json:"id"`
	Parent          string    `json:"parent,omitempty"`
	Comment         string    `json:"comment,omitempty"`
	Created         time.Time `json:"created"`
	Container       string    `json:"container,omitempty"`
	ContainerConfig Config    `json:"container_config,omitempty"`
	DockerVersion   string    `json:"docker_version,omitempty"`
	Author          string    `json:"author,omitempty"`
	Config          *Config   `json:"config,omitempty"`
	Architecture    string    `json:"architecture,omitempty"`
	Size            int64     `json:"size,omitempty"`
}

var (
	// ErrNoSuchImage is the error returned when the image does not exist.
	ErrNoSuchImage = errors.New("no such image")

	// ErrMissingRepo is the error returned when the remote repository is
	// missing.
	ErrMissingRepo = errors.New("missing remote repository e.g. 'github.com/user/repo'")

	// ErrMissingOutputStream is the error returned when no output stream
	// is provided to some calls, like BuildImage.
	ErrMissingOutputStream = errors.New("missing output stream")
)

// ListImages returns the list of available images in the server.
//
// See http://goo.gl/VmcR6v for more details.
func (c *Client) ListImages(all bool) ([]APIImages, error) {
	path := "/images/json?all="
	if all {
		path += "1"
	} else {
		path += "0"
	}
	body, _, err := c.do("GET", path, nil)
	if err != nil {
		return nil, err
	}
	var images []APIImages
	err = json.Unmarshal(body, &images)
	if err != nil {
		return nil, err
	}
	return images, nil
}

// ImageHistory returns the history of the image by its name or ID.
//
// See http://goo.gl/2oJmNs for more details.
func (c *Client) ImageHistory(name string) ([]ImageHistory, error) {
	body, status, err := c.do("GET", "/images/"+name+"/history", nil)
	if status == http.StatusNotFound {
		return nil, ErrNoSuchImage
	}
	if err != nil {
		return nil, err
	}
	var history []ImageHistory
	err = json.Unmarshal(body, &history)
	if err != nil {
		return nil, err
	}
	return history, nil
}

// RemoveImage removes an image by its name or ID.
//
// See http://goo.gl/znj0wM for more details.
func (c *Client) RemoveImage(name string) error {
	_, status, err := c.do("DELETE", "/images/"+name, nil)
	if status == http.StatusNotFound {
		return ErrNoSuchImage
	}
	return err
}

// InspectImage returns an image by its name or ID.
//
// See http://goo.gl/Q112NY for more details.
func (c *Client) InspectImage(name string) (*Image, error) {
	body, status, err := c.do("GET", "/images/"+name+"/json", nil)
	if status == http.StatusNotFound {
		return nil, ErrNoSuchImage
	}
	if err != nil {
		return nil, err
	}

	var image Image

	// if the caller elected to skip checking the server's version, assume it's the latest
	if c.SkipServerVersionCheck || c.expectedApiVersion.GreaterThanOrEqualTo(apiVersion_1_12) {
		err = json.Unmarshal(body, &image)
		if err != nil {
			return nil, err
		}
	} else {
		var imagePre012 ImagePre012
		err = json.Unmarshal(body, &imagePre012)
		if err != nil {
			return nil, err
		}

		image.ID = imagePre012.ID
		image.Parent = imagePre012.Parent
		image.Comment = imagePre012.Comment
		image.Created = imagePre012.Created
		image.Container = imagePre012.Container
		image.ContainerConfig = imagePre012.ContainerConfig
		image.DockerVersion = imagePre012.DockerVersion
		image.Author = imagePre012.Author
		image.Config = imagePre012.Config
		image.Architecture = imagePre012.Architecture
		image.Size = imagePre012.Size
	}

	return &image, nil
}

// PushImageOptions represents options to use in the PushImage method.
//
// See http://goo.gl/pN8A3P for more details.
type PushImageOptions struct {
	// Name of the image
	Name string

	// Tag of the image
	Tag string

	// Registry server to push the image
	Registry string

	OutputStream  io.Writer `qs:"-"`
	RawJSONStream bool      `qs:"-"`
}

// AuthConfiguration represents authentication options to use in the PushImage
// method. It represents the authentication in the Docker index server.
type AuthConfiguration struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	Email    string `json:"email,omitempty"`
}

// PushImage pushes an image to a remote registry, logging progress to w.
//
// An empty instance of AuthConfiguration may be used for unauthenticated
// pushes.
//
// See http://goo.gl/pN8A3P for more details.
func (c *Client) PushImage(opts PushImageOptions, auth AuthConfiguration) error {
	if opts.Name == "" {
		return ErrNoSuchImage
	}
	name := opts.Name
	opts.Name = ""
	path := "/images/" + name + "/push?" + queryString(&opts)
	var headers = make(map[string]string)
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(auth)

	headers["X-Registry-Auth"] = base64.URLEncoding.EncodeToString(buf.Bytes())

	return c.stream("POST", path, true, opts.RawJSONStream, headers, nil, opts.OutputStream, nil)
}

// PullImageOptions present the set of options available for pulling an image
// from a registry.
//
// See http://goo.gl/ACyYNS for more details.
type PullImageOptions struct {
	Repository    string `qs:"fromImage"`
	Registry      string
	Tag           string
	OutputStream  io.Writer `qs:"-"`
	RawJSONStream bool      `qs:"-"`
}

// PullImage pulls an image from a remote registry, logging progress to w.
//
// See http://goo.gl/ACyYNS for more details.
func (c *Client) PullImage(opts PullImageOptions, auth AuthConfiguration) error {
	if opts.Repository == "" {
		return ErrNoSuchImage
	}

	var headers = make(map[string]string)
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(auth)
	headers["X-Registry-Auth"] = base64.URLEncoding.EncodeToString(buf.Bytes())

	return c.createImage(queryString(&opts), headers, nil, opts.OutputStream, opts.RawJSONStream)
}

func (c *Client) createImage(qs string, headers map[string]string, in io.Reader, w io.Writer, rawJSONStream bool) error {
	path := "/images/create?" + qs
	return c.stream("POST", path, true, rawJSONStream, headers, in, w, nil)
}

// LoadImageOptions represents the options for LoadImage Docker API Call
//
// See http://goo.gl/Y8NNCq for more details.
type LoadImageOptions struct {
	InputStream io.Reader
}

// LoadImage imports a tarball docker image
//
// See http://goo.gl/Y8NNCq for more details.
func (c *Client) LoadImage(opts LoadImageOptions) error {
	return c.stream("POST", "/images/load", true, false, nil, opts.InputStream, nil, nil)
}

// ExportImageOptions represent the options for ExportImage Docker API call
//
// See http://goo.gl/mi6kvk for more details.
type ExportImageOptions struct {
	Name         string
	OutputStream io.Writer
}

// ExportImage exports an image (as a tar file) into the stream
//
// See http://goo.gl/mi6kvk for more details.
func (c *Client) ExportImage(opts ExportImageOptions) error {
	return c.stream("GET", fmt.Sprintf("/images/%s/get", opts.Name), true, false, nil, nil, opts.OutputStream, nil)
}

// ImportImageOptions present the set of informations available for importing
// an image from a source file or the stdin.
//
// See http://goo.gl/PhBKnS for more details.
type ImportImageOptions struct {
	Repository string `qs:"repo"`
	Source     string `qs:"fromSrc"`
	Tag        string `qs:"tag"`

	InputStream  io.Reader `qs:"-"`
	OutputStream io.Writer `qs:"-"`
}

// ImportImage imports an image from a url, a file or stdin
//
// See http://goo.gl/PhBKnS for more details.
func (c *Client) ImportImage(opts ImportImageOptions) error {
	if opts.Repository == "" {
		return ErrNoSuchImage
	}
	if opts.Source != "-" {
		opts.InputStream = nil
	}
	if opts.Source != "-" && !isURL(opts.Source) {
		f, err := os.Open(opts.Source)
		if err != nil {
			return err
		}
		b, err := ioutil.ReadAll(f)
		opts.InputStream = bytes.NewBuffer(b)
		opts.Source = "-"
	}
	return c.createImage(queryString(&opts), nil, opts.InputStream, opts.OutputStream, false)
}

// BuildImageOptions present the set of informations available for building an
// image from a tarfile with a Dockerfile in it.
//
// For more details about the Docker building process, see
// http://goo.gl/tlPXPu.
type BuildImageOptions struct {
	Name                string    `qs:"t"`
	NoCache             bool      `qs:"nocache"`
	SuppressOutput      bool      `qs:"q"`
	RmTmpContainer      bool      `qs:"rm"`
	ForceRmTmpContainer bool      `qs:"forcerm"`
	InputStream         io.Reader `qs:"-"`
	OutputStream        io.Writer `qs:"-"`
	RawJSONStream       bool      `qs:"-"`
	Remote              string    `qs:"remote"`
}

// BuildImage builds an image from a tarball's url or a Dockerfile in the input
// stream.
//
// See http://goo.gl/wRsW76 for more details.
func (c *Client) BuildImage(opts BuildImageOptions) error {
	if opts.OutputStream == nil {
		return ErrMissingOutputStream
	}
	var headers map[string]string
	if opts.Remote != "" && opts.Name == "" {
		opts.Name = opts.Remote
	}
	if opts.InputStream != nil {
		headers = map[string]string{"Content-Type": "application/tar"}
	} else if opts.Remote == "" {
		return ErrMissingRepo
	}
	return c.stream("POST", fmt.Sprintf("/build?%s",
		queryString(&opts)), true, opts.RawJSONStream, headers, opts.InputStream, opts.OutputStream, nil)
}

// TagImageOptions present the set of options to tag an image.
//
// See http://goo.gl/5g6qFy for more details.
type TagImageOptions struct {
	Repo  string
	Tag   string
	Force bool
}

// TagImage adds a tag to the image identified by the given name.
//
// See http://goo.gl/5g6qFy for more details.
func (c *Client) TagImage(name string, opts TagImageOptions) error {
	if name == "" {
		return ErrNoSuchImage
	}
	_, status, err := c.do("POST", fmt.Sprintf("/images/"+name+"/tag?%s",
		queryString(&opts)), nil)
	if status == http.StatusNotFound {
		return ErrNoSuchImage
	}

	return err
}

func isURL(u string) bool {
	p, err := url.Parse(u)
	if err != nil {
		return false
	}
	return p.Scheme == "http" || p.Scheme == "https"
}

// APIImageSearch reflect the result of a search on the dockerHub
//
// See http://goo.gl/xI5lLZ for more details.
type APIImageSearch struct {
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	IsOfficial  bool   `json:"is_official,omitempty" yaml:"is_official,omitempty"`
	IsAutomated bool   `json:"is_automated,omitempty" yaml:"is_automated,omitempty"`
	Name        string `json:"name,omitempty" yaml:"name,omitempty"`
	StarCount   int    `json:"star_count,omitempty" yaml:"star_count,omitempty"`
}

// SearchImages search the docker hub with a specific given term.
//
// See http://goo.gl/xI5lLZ for more details.
func (c *Client) SearchImages(term string) ([]APIImageSearch, error) {
	body, _, err := c.do("GET", "/images/search?term="+term, nil)
	if err != nil {
		return nil, err
	}
	var searchResult []APIImageSearch
	err = json.Unmarshal(body, &searchResult)
	if err != nil {
		return nil, err
	}
	return searchResult, nil
}

type MergeImageOptions struct {
	Container    string `qs:"container"`
	CurrentImage string `qs:"currentImage"`
	Repository    string `qs:"fromImage"`
	Registry      string
	Tag           string
	OutputStream  io.Writer `qs:"-"`
	RawJSONStream bool      `qs:"-"`
}

func (c *Client) PullImageAndApply(opts MergeImageOptions, auth AuthConfiguration) error {
	if opts.Repository == "" {
		return ErrNoSuchImage
	}
	if opts.Container == "" || opts.CurrentImage == "" {
		return fmt.Errorf("missing container ID or Image")
	}

	var headers = make(map[string]string)
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(auth)
	headers["X-Registry-Auth"] = base64.URLEncoding.EncodeToString(buf.Bytes())

	path := "/images/applypull?" + queryString(&opts)
	return c.stream("POST", path, true, opts.RawJSONStream, headers, nil, opts.OutputStream, nil)
}

func (c *Client) DiffImageAndApply(opts MergeImageOptions) error {
	if opts.Repository == "" {
		return ErrNoSuchImage
	}
	if opts.Container == "" || opts.CurrentImage == "" {
		return fmt.Errorf("missing container ID or Image")
	}

	path := "/images/applydiff?" + queryString(&opts)

	return c.stream("POST", path, true, opts.RawJSONStream, nil, nil, opts.OutputStream, nil)
}
