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

//go:build !stdmalloc
// +build !stdmalloc

package cli

// #cgo CPPFLAGS: -DJEMALLOC_NO_DEMANGLE
// #cgo LDFLAGS: -ljemalloc
// #cgo dragonfly freebsd LDFLAGS: -lm
// #cgo linux LDFLAGS: -lrt -lm -lpthread
//
// #include <jemalloc/jemalloc.h>
// #include <stddef.h>
//
// // Checks whether jemalloc profiling is enabled and active.
// // Returns true if profiling is enabled and active.
// // Returns false on any mallctl errors.
// bool is_profiling_enabled() {
//   bool enabled = false;
//   size_t enabledSize = sizeof(enabled);
//
//   // Check profiling flag.
//   if (je_mallctl("opt.prof", &enabled, &enabledSize, NULL, 0) != 0) {
//     return false;
//   }
//   if (!enabled) {
//     return false;
//   }
//
//   // Check prof_active flag.
//   if (je_mallctl("opt.prof_active", &enabled, &enabledSize, NULL, 0) != 0) {
//     return false;
//   }
//   return enabled;
// }
//
// // Write a heap profile to "filename". Returns true on success, false on error.
// int dump_heap_profile(const char *filename) {
//   return je_mallctl("prof.dump", NULL, NULL, &filename, sizeof(const char *));
// }
import "C"

import (
	"fmt"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/server/heapprofiler"
)

func init() {
	if C.is_profiling_enabled() {
		heapprofiler.SetJemallocHeapDumpFn(writeJemallocProfile)
	}
}

// writeJemallocProfile tells jemalloc to write a heap profile to 'filename'.
// It assumes that profiling is enabled and active.
func writeJemallocProfile(filename string) error {
	cpath := C.CString(filename)
	defer C.free(unsafe.Pointer(cpath))

	if errCode := C.dump_heap_profile(cpath); errCode != 0 {
		return fmt.Errorf("error code %d", errCode)
	}
	return nil
}
