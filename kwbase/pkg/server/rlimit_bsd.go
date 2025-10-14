// Copyright 2017 The Cockroach Authors.
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

//go:build freebsd || dragonfly
// +build freebsd dragonfly

package server

import (
	"math"

	"golang.org/x/sys/unix"
)

func setRlimitNoFile(limits *rlimit) error {
	rLimit := unix.Rlimit{Cur: int64(limits.Cur), Max: int64(limits.Max)}
	return unix.Setrlimit(unix.RLIMIT_NOFILE, &rLimit)
}

func getRlimitNoFile(limits *rlimit) error {
	var rLimit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit); err != nil {
		return err
	}
	// Some (legacy?) FreeBSD platforms had RLIMIT_INFINITY set to -1.
	if rLimit.Cur == -1 {
		limits.Cur = math.MaxUint64
	} else {
		limits.Cur = uint64(rLimit.Cur)
	}
	if rLimit.Max == -1 {
		limits.Max = math.MaxUint64
	} else {
		limits.Max = uint64(rLimit.Max)
	}
	return nil
}
