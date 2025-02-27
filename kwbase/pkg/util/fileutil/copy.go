// Copyright 2018 The Cockroach Authors.
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

package fileutil

import (
	"io"
	"os"
	"path/filepath"
	"strings"
)

// CopyDir recursively copies all files and directories in the from directory
// into the to directory. If the to directory does not exist, it is created.
// If the to directory already exists, its contents are overwritten.
func CopyDir(from, to string) error {
	return filepath.Walk(from, func(srcPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		destPath := strings.Replace(srcPath, from, to, 1)
		if info.IsDir() {
			return os.MkdirAll(destPath, info.Mode())
		}
		return CopyFile(srcPath, destPath)
	})
}

// CopyFile copies src to dst.
// If the target file already exists, it is overwritten.
// If the copy fails, the target file may be left in an inconsistent state.
func CopyFile(srcPath, destPath string) (err error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()
	dest, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer dest.Close()
	if _, err = io.Copy(dest, src); err != nil {
		return err
	}
	return dest.Sync()
}
