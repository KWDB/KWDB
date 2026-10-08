// Copyright 2016 The Cockroach Authors.
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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

type storageVersion int

const (
	versionNoFile storageVersion = iota
	versionBeta20160331
	versionFileRegistry
)

const (
	versionFilename     = "KWBASEDB_VERSION"
	versionFilenameTemp = "KWBASEDB_VERSION_TEMP"
	versionMinimum      = versionNoFile
	versionCurrent      = versionFileRegistry
)

// Version stores all the version information for all stores and is used as
// the format for the version file.
type Version struct {
	Version storageVersion
}

// getVersionFilename returns the filename for the version file stored in the
// data directory.
func getVersionFilename(dir string) string {
	return filepath.Join(dir, versionFilename)
}

// getVersion returns the current on disk kwbase version from the version
// file in the passed in directory. If there is no version file yet, it
// returns 0.
func getVersion(dir string) (storageVersion, error) {
	filename := getVersionFilename(dir)
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return versionNoFile, nil
		}
		return 0, err
	}
	var ver Version
	if err := json.Unmarshal(b, &ver); err != nil {
		return 0, fmt.Errorf("version file %s is not formatted correctly; %s", filename, err)
	}
	return ver.Version, nil
}

// writeVersionFile overwrites the version file to contain the specified version.
func writeVersionFile(dir string, ver storageVersion) error {
	tempFilename := filepath.Join(dir, versionFilenameTemp)
	filename := getVersionFilename(dir)
	b, err := json.Marshal(Version{ver})
	if err != nil {
		return err
	}
	// First write to a temp file.
	if err := ioutil.WriteFile(tempFilename, b, 0644); err != nil {
		return err
	}
	// Atomically rename the file to overwrite the version file on disk.
	return os.Rename(tempFilename, filename)
}
