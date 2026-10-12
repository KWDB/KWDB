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

package blobs

import (
	"bufio"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/blobs/blobspb"
	"gitee.com/kwbasedb/kwbase/pkg/util/fileutil"
	"github.com/cockroachdb/errors"
)

// LocalStorage wraps all operations with the local file system
// that the blob service makes.
type LocalStorage struct {
	externalIODir string
}

// NewLocalStorage creates a new LocalStorage object and returns
// an error when we cannot take the absolute path of `externalIODir`.
func NewLocalStorage(externalIODir string) (*LocalStorage, error) {
	// An empty externalIODir indicates external IO is completely disabled.
	// Returning a nil *LocalStorage in this case and then hanldling `nil` in the
	// prependExternalIODir helper ensures that that is respected throughout the
	// implementation (as a failure to do so would likely fail loudly with a
	// nil-pointer dereference).
	if externalIODir == "" {
		return nil, nil
	}
	absPath, err := filepath.Abs(externalIODir)
	if err != nil {
		return nil, errors.Wrap(err, "creating LocalStorage object")
	}
	return &LocalStorage{externalIODir: absPath}, nil
}

// AppendWrite prepends IO dir to filename and append writes the content to that local file.
func (l *LocalStorage) AppendWrite(filename string, content io.Reader) error {
	fullPath, err := l.prependExternalIODir(filename)
	if err != nil {
		return err
	}
	targetDir := filepath.Dir(fullPath)
	if err = os.MkdirAll(targetDir, 0755); err != nil {
		return errors.Wrapf(err, "creating target local directory %q", targetDir)
	}
	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return errors.Wrapf(err, "open file %q failed", fullPath)
	}
	//close file handle
	defer file.Close()
	// write file, use *Writer witch data buf
	write := bufio.NewWriter(file)
	_, err = write.ReadFrom(content)
	if err != nil {
		return err
	}
	// real flush data to the file
	return write.Flush()
}

// prependExternalIODir makes `path` relative to the configured external I/O directory.
//
// Note that we purposefully only rely on the simplified cleanup
// performed by filepath.Join() - which is limited to stripping out
// occurrences of "../" - because we intendedly want to allow
// operators to "open up" their I/O directory via symlinks. Therefore,
// a full check via filepath.Abs() would be inadequate.
func (l *LocalStorage) prependExternalIODir(path string) (string, error) {
	if l == nil {
		return "", errors.Errorf("local file access is disabled")
	}
	localBase := filepath.Join(l.externalIODir, path)
	if !strings.HasPrefix(localBase, l.externalIODir) {
		return "", errors.Errorf("local file access to paths outside of external-io-dir is not allowed: %s", path)
	}
	return localBase, nil
}

// WriteFile prepends IO dir to filename and writes the content to that local file.
func (l *LocalStorage) WriteFile(filename string, content io.Reader) (err error) {
	fullPath, err := l.prependExternalIODir(filename)
	if err != nil {
		return err
	}

	targetDir := filepath.Dir(fullPath)
	if err = os.MkdirAll(targetDir, 0755); err != nil {
		return errors.Wrapf(err, "creating target local directory %q", targetDir)
	}

	// We generate the temporary file in the desired target directory.
	// This has two purposes:
	// - it avoids relying on the system-wide temporary directory, which
	//   may not be large enough to receive the file.
	// - it avoids a cross-filesystem rename in the common case.
	//   (There can still be cross-filesystem renames in very
	//   exotic edge cases, hence the use fileutil.Move below.)
	// See the explanatory comment for ioutil.TempFile to understand
	// what the "*" in the suffix means.
	tmpFile, err := ioutil.TempFile(targetDir, filepath.Base(fullPath)+"*.tmp")
	if err != nil {
		return errors.Wrap(err, "creating temporary file")
	}
	tmpFileFullName := tmpFile.Name()
	defer func() {
		if err != nil {
			// When an error occurs, we need to clean up the newly created
			// temporary file.
			_ = os.Remove(tmpFileFullName)
			//
			// TODO(someone): in the special case where an attempt is made
			// to upload to a sub-directory of the ext i/o dir for the first
			// time (MkdirAll above did create the sub-directory), and the
			// copy/rename fails, we're now left with a newly created but empty
			// sub-directory.
			//
			// We cannot safely remove that target directory here, because
			// perhaps there is another concurrent operation that is also
			// targeting it. A more principled approach could be to use a
			// mutex lock on directory accesses, and/or occasionally prune
			// empty sub-directories upon node start-ups.
		}
	}()

	// Copy the data into the temp file. We use a closure here to
	// ensure the temp file is closed after the copy is done.
	if err = func() error {
		defer tmpFile.Close()
		if _, err := io.Copy(tmpFile, content); err != nil {
			return errors.Wrapf(err, "writing to temporary file %q", tmpFileFullName)
		}
		return errors.Wrapf(tmpFile.Sync(), "flushing temporary file %q", tmpFileFullName)
	}(); err != nil {
		return err
	}

	// Finally put the file to its final location.
	return errors.Wrapf(
		fileutil.Move(tmpFileFullName, fullPath),
		"moving temporary file to final location %q", fullPath)
}

// ReadFile prepends IO dir to filename and reads the content of that local file.
func (l *LocalStorage) ReadFile(filename string) (res io.ReadCloser, err error) {
	fullPath, err := l.prependExternalIODir(filename)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = f.Close()
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return nil, errors.Errorf("expected a file but %q is a directory", fi.Name())
	}
	return f, nil
}

// List prepends IO dir to pattern and glob matches all local files against that pattern.
//
// Returned paths always use the URI-style forward slash separator, even on
// Windows: nodelocal paths originate from (and are re-joined into) URIs, whose
// path component is slash-separated regardless of the host OS.
func (l *LocalStorage) List(pattern string) ([]string, error) {
	if pattern == "" {
		return nil, errors.New("pattern cannot be empty")
	}
	fullPath, err := l.prependExternalIODir(pattern)
	if err != nil {
		return nil, err
	}
	matches, err := filepath.Glob(fullPath)
	if err != nil {
		return nil, err
	}

	var fileList []string
	for _, file := range matches {
		fileList = append(fileList, filepath.ToSlash(strings.TrimPrefix(file, l.externalIODir)))
	}
	return fileList, nil
}

// Delete prepends IO dir to filename and deletes that local file.
func (l *LocalStorage) Delete(filename string) error {
	fullPath, err := l.prependExternalIODir(filename)
	if err != nil {
		return errors.Wrap(err, "deleting file")
	}
	return os.Remove(fullPath)
}

// Stat prepends IO dir to filename and gets the Stat() of that local file.
func (l *LocalStorage) Stat(filename string) (*blobspb.BlobStat, error) {
	fullPath, err := l.prependExternalIODir(filename)
	if err != nil {
		return nil, errors.Wrap(err, "getting stat of file")
	}
	fi, err := os.Stat(fullPath)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return nil, errors.Errorf("expected a file but %q is a directory", fi.Name())
	}
	return &blobspb.BlobStat{Filesize: fi.Size()}, nil
}

// Seek sets the offset for the next Read on file to offset.It then returns
// an io.ReadCloser to read the contents. interpreted according to whence:
// 0 means relative to the origin of the file, 1 means relative to the current
// offset, and 2 means relative to the end.
func (l *LocalStorage) Seek(filename string, offset int64, whence int) (io.ReadCloser, error) {
	fullPath, err := l.prependExternalIODir(filename)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	_, err = f.Seek(offset, whence)
	if err != nil {
		return nil, err
	}
	return f, nil
}
