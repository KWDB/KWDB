// Copyright 2017 The Cockroach Authors.
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

package clusterversion

import "gitee.com/kwbasedb/kwbase/pkg/roachpb"

// VersionKey is a unique identifier for a version of KWDB.
type VersionKey int

// Version constants for KWDB.
// All versions prior to V3 are considered legacy and are always active.
// We keep only current and minimum supported versions.
//
//go:generate stringer -type=VersionKey
const (
	_ VersionKey = iota - 1 // want first named one to start at zero

	// VersionMinSupported is the minimum version supported by this binary.
	VersionMinSupported
	// Add new versions here (step one of two).
)

// versionsSingleton lists all historical versions here in chronological order.
// Only the minimum supported version and current binary version are retained.
// All intermediate CockroachDB versions (19.1 through 20.1) are always active
// and have been collapsed into VersionMinSupported.
var versionsSingleton = keyedVersions([]keyedVersion{
	{
		// VersionMinSupported is the minimum version of data supported by
		// this binary. It encompasses all previously defined versions from
		// CockroachDB 19.1 through 20.1 and KWDB V3 additions.
		Key:     VersionMinSupported,
		Version: roachpb.Version{Major: 3, Minor: 2, Unstable: 0},
	},
	// Add new versions here (step two of two).
})

var (
	// binaryMinSupportedVersion is the earliest version of data supported by
	// this binary. If this binary is started using a store marked with an older
	// version than binaryMinSupportedVersion, then the binary will exit with
	// an error.
	binaryMinSupportedVersion = VersionByKey(VersionMinSupported)
	// binaryVersion is the version of this binary.
	//
	// This is the version that a new cluster will use when created.
	binaryVersion = roachpb.Version{Major: 3, Minor: 2, Unstable: 2}
)

// VersionByKey returns the roachpb.Version for a given key.
// It is a fatal error to use an invalid key.
func VersionByKey(key VersionKey) roachpb.Version {
	return versionsSingleton.MustByKey(key)
}
