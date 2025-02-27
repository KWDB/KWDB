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

// +build !windows

//lint:file-ignore Unconvert (redundant conversions are necessary for cross-platform compatibility)

package sysutil

import (
	"fmt"
	"math"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// ProcessIdentity returns a string describing the user and group that this
// process is running as.
func ProcessIdentity() string {
	return fmt.Sprintf("uid %d euid %d gid %d egid %d",
		unix.Getuid(), unix.Geteuid(), unix.Getgid(), unix.Getegid())
}

// StatFS returns an FSInfo describing the named filesystem. It is only
// supported on Unix-like platforms.
func StatFS(path string) (*FSInfo, error) {
	var fs unix.Statfs_t
	if err := unix.Statfs(path, &fs); err != nil {
		return nil, err
	}
	// Statfs_t's fields have different types on different platforms. Our FSInfo
	// type uses int64s for all fields, so make sure the values returned by the OS
	// will fit.
	if uint64(fs.Bfree) > math.MaxInt64 ||
		uint64(fs.Bavail) > math.MaxInt64 ||
		uint64(fs.Blocks) > math.MaxInt64 ||
		uint64(fs.Bsize) > math.MaxInt64 {
		return nil, fmt.Errorf("statfs syscall returned unrepresentable value %#v", fs)
	}
	return &FSInfo{
		FreeBlocks:  int64(fs.Bfree),
		AvailBlocks: int64(fs.Bavail),
		TotalBlocks: int64(fs.Blocks),
		BlockSize:   int64(fs.Bsize),
	}, nil
}

// StatAndLinkCount wraps os.Stat, returning its result and, if the platform
// supports it, the link-count from the returned file info.
func StatAndLinkCount(path string) (os.FileInfo, int64, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return stat, 0, err
	}
	if sys := stat.Sys(); sys != nil {
		if s, ok := sys.(*syscall.Stat_t); ok {
			return stat, int64(s.Nlink), nil
		}
	}
	return stat, 0, nil
}

// IsCrossDeviceLinkErrno checks whether the given error object (as
// extracted from an *os.LinkError) is a cross-device link/rename
// error.
func IsCrossDeviceLinkErrno(errno error) bool {
	return errno == syscall.EXDEV
}
