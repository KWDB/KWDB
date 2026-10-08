// Copyright 2019 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package localexternal

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strconv"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/blobs"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
)

const (
	// AuthParam is the query parameter for the cluster settings named
	// key in a URI.
	AuthParam          = "AUTH"
	authParamImplicit  = "implicit"
	authParamDefault   = "default"
	authParamSpecified = "specified"

	// CredentialsParam is the query parameter for the base64-encoded contents of
	// the Google Application Credentials JSON file.
	CredentialsParam = "CREDENTIALS"
)

// See SanitizeExternalStorageURI.
var redactedQueryParams = map[string]struct{}{
	CredentialsParam: {},
}

// ErrListingUnsupported is a marker for indicating listing is unsupported.
var ErrListingUnsupported = errors.New("listing is not supported")

// ExternalStorageFactory describes a factory function for ExternalStorage.
type ExternalStorageFactory func(ctx context.Context, dest roachpb.ExternalStorage) (ExternalStorage, error)

// ExternalStorageFromURIFactory describes a factory function for ExternalStorage given a URI.
type ExternalStorageFromURIFactory func(ctx context.Context, uri string) (ExternalStorage, error)

// ExternalStorage provides functions to read and write files in some storage,
// namely various cloud storage providers, for example to store backups.
// Generally an implementation is instantiated pointing to some base path or
// prefix and then gets and puts files using the various methods to interact
// with individual files contained within that path or prefix.
// However, implementations must also allow callers to provide the full path to
// a given file as the "base" path, and then read or write it with the methods
// below by simply passing an empty filename. Implementations that use stdlib's
// `filepath.Join` to concatenate their base path with the provided filename will
// find its semantics well suited to this -- it elides empty components and does
// not append surplus slashes.
type ExternalStorage interface {
	io.Closer

	// Conf should return the serializable configuration required to reconstruct
	// this ExternalStorage implementation.
	Conf() roachpb.ExternalStorage

	// ReadFile should return a Reader for requested name.
	ReadFile(ctx context.Context, basename string) (io.ReadCloser, error)

	// WriteFile should write the content to requested name.
	WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error

	// AppendWrite sends the named payload to the requested node.
	// This method will read entire content of file and send
	// it over to another node, based on the nodeID. append write.
	AppendWrite(ctx context.Context, file string, content io.ReadSeeker) error

	// ListFiles returns files that match a globs-style pattern. The returned
	// results are usually relative to the base path, meaning an ExternalStorage
	// instance can be initialized with some base path, used to query for files,
	// then pass those results to its other methods.
	//
	// As a special-case, if the passed patternSuffix is empty, the base path used
	// to initialize the storage connection is treated as a pattern. In this case,
	// as the connection is not really reusable for interacting with other files
	// and there is no clear definition of what it would mean to be relative to
	// that, the results are fully-qualified absolute URIs. The base URI is *only*
	// allowed to contain globs-patterns when the explicit patternSuffix is "".
	ListFiles(ctx context.Context, patternSuffix string) ([]string, error)

	// Delete removes the named file from the store.
	Delete(ctx context.Context, basename string) error

	// Size returns the length of the named file in bytes.
	Size(ctx context.Context, basename string) (int64, error)

	// Seek sets the offset for the next Read on file to offset.It then returns
	// an io.ReadCloser to read the contents. interpreted according to whence:
	// 0 means relative to the origin of the file, 1 means relative to the current
	// offset, and 2 means relative to the end.
	Seek(ctx context.Context, filename string, offset int64, endOffset int64, whence int) (io.ReadCloser, error)
}

// ---------------------------------------------------------------------------
// localFileStorage implements ExternalStorage using the local filesystem via
// blob client. This is the sole ExternalStorage implementation in KWDB.
// ---------------------------------------------------------------------------

type localFileStorage struct {
	cfg        roachpb.ExternalStorage_LocalFilePath // contains un-prefixed filepath -- DO NOT use for I/O ops.
	base       string                                // relative filepath prefixed with externalIODir, for I/O ops on this node.
	blobClient blobs.BlobClient                      // inter-node file sharing service
}

var _ ExternalStorage = &localFileStorage{}

// MakeLocalStorageURI converts a local path (should always be relative) to a
// valid nodelocal URI.
func MakeLocalStorageURI(path string) string {
	return fmt.Sprintf("nodelocal://01/%s", path)
}

func makeNodeLocalURIWithNodeID(nodeID roachpb.NodeID, path string) string {
	path = strings.TrimPrefix(path, "/")
	return fmt.Sprintf("nodelocal://%d/%s", nodeID, path)
}

func makeLocalStorage(
	ctx context.Context,
	cfg roachpb.ExternalStorage_LocalFilePath,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
) (ExternalStorage, error) {
	if cfg.Path == "" {
		return nil, errors.Errorf("Local storage requested but path not provided")
	}
	client, err := blobClientFactory(ctx, cfg.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create blob client")
	}
	return &localFileStorage{base: cfg.Path, cfg: cfg, blobClient: client}, nil
}

func (l *localFileStorage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{
		Provider:  roachpb.ExternalStorageProvider_LocalFile,
		LocalFile: l.cfg,
	}
}

func joinRelativePath(filePath string, file string) string {
	// Joining "." to make this a relative path.
	// This ensures path.Clean does not simplify in unexpected ways.
	return path.Join(".", filePath, file)
}

func (l *localFileStorage) WriteFile(
	ctx context.Context, basename string, content io.ReadSeeker,
) error {
	return l.blobClient.WriteFile(ctx, joinRelativePath(l.base, basename), content)
}

func (l *localFileStorage) AppendWrite(
	ctx context.Context, file string, content io.ReadSeeker,
) error {
	return l.blobClient.AppendWrite(ctx, joinRelativePath(l.base, file), content)
}

func (l *localFileStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	return l.blobClient.ReadFile(ctx, joinRelativePath(l.base, basename))
}

func (l *localFileStorage) Seek(
	ctx context.Context, filename string, offset int64, endOffset int64, whence int,
) (io.ReadCloser, error) {
	return l.blobClient.Seek(ctx, joinRelativePath(l.base, filename), offset, whence)
}

func (l *localFileStorage) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {
	pattern := l.base
	if patternSuffix != "" {
		if containsGlob(l.base) {
			return nil, errors.New("prefix cannot contain globs pattern when passing an explicit pattern")
		}
		pattern = joinRelativePath(pattern, patternSuffix)
		l.base = path.Clean(l.base)
	}

	var fileList []string
	matches, err := l.blobClient.List(ctx, pattern)
	if err != nil {
		return nil, errors.Wrap(err, "unable to match pattern provided")
	}

	for _, fileName := range matches {
		if patternSuffix != "" {
			fileList = append(fileList, strings.TrimPrefix(strings.TrimPrefix(fileName, l.base), "/"))
		} else {
			fileList = append(fileList, makeNodeLocalURIWithNodeID(l.cfg.NodeID, fileName))
		}
	}
	return fileList, nil
}

func (l *localFileStorage) Delete(ctx context.Context, basename string) error {
	return l.blobClient.Delete(ctx, joinRelativePath(l.base, basename))
}

func (l *localFileStorage) Size(ctx context.Context, basename string) (int64, error) {
	stat, err := l.blobClient.Stat(ctx, joinRelativePath(l.base, basename))
	if err != nil {
		return 0, err
	}
	return stat.Filesize, nil
}

func (*localFileStorage) Close() error {
	return nil
}

// ExternalStorageConfFromURI generates an ExternalStorage config from a URI string.
func ExternalStorageConfFromURI(path string) (roachpb.ExternalStorage, error) {
	conf := roachpb.ExternalStorage{}
	uri, err := url.Parse(path)
	if err != nil {
		return conf, err
	}
	switch uri.Scheme {
	case "nodelocal":
		if uri.Host == "" {
			return conf, errors.Errorf("host component of nodelocal URI must be a node ID")
		} else if uri.Host == "self" {
			uri.Host = "0"
		}

		nodeID, err := strconv.Atoi(uri.Host)
		if err != nil {
			return conf, errors.Errorf("host component of nodelocal URI must be a node ID: %s", path)
		}
		if nodeID == 0 {
			return conf, errors.Errorf("please enter a valid node ID (" +
				"don't use 'self' or '0' as node ID)")
		}
		conf.Provider = roachpb.ExternalStorageProvider_LocalFile
		conf.LocalFile.Path = uri.Path
		conf.LocalFile.NodeID = roachpb.NodeID(nodeID)
	default:
		return conf, errors.Errorf("unsupported storage scheme: %q", uri.Scheme)
	}
	return conf, nil
}

// ExternalStorageFromURI returns an ExternalStorage for the given URI.
func ExternalStorageFromURI(
	ctx context.Context,
	uri string,
	externalConfig base.ExternalIOConfig,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
) (ExternalStorage, error) {
	conf, err := ExternalStorageConfFromURI(uri)
	if err != nil {
		return nil, err
	}
	return MakeExternalStorage(ctx, conf, externalConfig, settings, blobClientFactory)
}

// SanitizeExternalStorageURI returns the external storage URI with with some
// secrets redacted, for use when showing these URIs in the UI, to provide some
// protection from shoulder-surfing. The param is still present -- just
// redacted -- to make it clearer that that value is indeed persisted interally.
// extraParams which should be scrubbed -- for params beyond those that the
// various clound-storage URIs supported by this package know about -- can be
// passed allowing this function to be used to scrub other URIs too (such as
// non-cloudstorage changefeed sinks).
func SanitizeExternalStorageURI(path string, extraParams []string) (string, error) {
	uri, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	if uri.Scheme == "experimental-workload" || uri.Scheme == "workload" {
		return path, nil
	}

	params := uri.Query()
	for param := range params {
		if _, ok := redactedQueryParams[param]; ok {
			params.Set(param, "redacted")
		} else {
			for _, p := range extraParams {
				if param == p {
					params.Set(param, "redacted")
				}
			}
		}
	}

	uri.RawQuery = params.Encode()
	return uri.String(), nil
}

// MakeExternalStorage creates an ExternalStorage from the given config.
func MakeExternalStorage(
	ctx context.Context,
	dest roachpb.ExternalStorage,
	conf base.ExternalIOConfig,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
) (ExternalStorage, error) {
	switch dest.Provider {
	case roachpb.ExternalStorageProvider_LocalFile:
		telemetry.Count("external-io.nodelocal")
		return makeLocalStorage(ctx, dest.LocalFile, settings, blobClientFactory)
	}
	return nil, errors.Errorf("unsupported external destination type: %s", dest.Provider.String())
}

func containsGlob(str string) bool {
	return strings.ContainsAny(str, "*?[")
}
