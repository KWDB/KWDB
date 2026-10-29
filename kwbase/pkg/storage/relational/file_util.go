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

package relational

import (
	"bytes"
	"io"

	"github.com/cockroachdb/pebble/vfs"
)

// SafeWriteToFile writes the byte slice to the filename, contained in dir,
// using the given fs.  It returns after both the file and the containing
// directory are synced.
func SafeWriteToFile(fs vfs.FS, dir string, filename string, b []byte) error {
	tempName := filename + ".kwdbtmp"
	f, err := fs.Create(tempName)
	if err != nil {
		return err
	}
	bReader := bytes.NewReader(b)
	if _, err = io.Copy(f, bReader); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = fs.Rename(tempName, filename); err != nil {
		return err
	}
	fdir, err := fs.OpenDir(dir)
	if err != nil {
		return err
	}
	defer fdir.Close()
	return fdir.Sync()
}
