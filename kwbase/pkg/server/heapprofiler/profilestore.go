// Copyright 2020 The Cockroach Authors.
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

package heapprofiler

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/server/dumpstore"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var (
	maxProfiles = settings.RegisterIntSetting(
		"server.mem_profile.max_profiles",
		"maximum number of profiles to be kept per ramp-up of memory usage. "+
			"A ramp-up is defined as a sequence of profiles with increasing usage.",
		5,
	)

	maxCombinedFileSize = settings.RegisterByteSizeSetting(
		"server.mem_profile.total_dump_size_limit",
		"maximum combined disk size of preserved memory profiles",
		128<<20, // 128MiB
	)
)

func init() {
	s := settings.RegisterIntSetting(
		"server.heap_profile.max_profiles", "use server.mem_profile.max_profiles instead", 5)
	s.SetRetired()

	b := settings.RegisterByteSizeSetting(
		"server.heap_profile.total_dump_size_limit",
		"use server.mem_profile.total_dump_size_limit instead",
		128<<20, // 128MiB
	)
	b.SetRetired()
}

// profileStore represents the directory where heap profiles are stored.
// It supports automatic garbage collection of old profiles.
type profileStore struct {
	*dumpstore.DumpStore
	prefix string
	st     *cluster.Settings
}

func newProfileStore(
	store *dumpstore.DumpStore, prefix string, st *cluster.Settings,
) *profileStore {
	s := &profileStore{DumpStore: store, prefix: prefix, st: st}
	return s
}

func (s *profileStore) gcProfiles(ctx context.Context, now time.Time) {
	s.GC(ctx, now, s)
}

func (s *profileStore) makeNewFileName(timestamp time.Time, curHeap int64) string {
	// We place the timestamp immediately after the (immutable) file
	// prefix to ensure that a directory listing sort also sorts the
	// profiles in timestamp order.
	fileName := fmt.Sprintf("%s.%s.%d",
		s.prefix, timestamp.Format(timestampFormat), curHeap)
	return s.GetFullPath(fileName)
}

// PreFilter is part of the dumpstore.Dumper interface.
func (s *profileStore) PreFilter(
	ctx context.Context, files []os.FileInfo, cleanupFn func(fileName string) error,
) (preserved map[int]bool, _ error) {
	maxP := maxProfiles.Get(&s.st.SV)
	preserved = s.cleanupLastRampup(ctx, files, maxP, cleanupFn)
	return
}

// CheckOwnsFile is part of the dumpstore.Dumper interface.
func (s *profileStore) CheckOwnsFile(ctx context.Context, fi os.FileInfo) bool {
	ok, _, _ := s.parseFileName(ctx, fi.Name())
	return ok
}

// cleanupLastRampup parses the filenames in files to detect the
// last ramp-up (sequence of increasing heap usage). If there
// are more than maxD entries in the last ramp-up, the fn closure
// is called for each of them.
//
// files is assumed to be sorted in chronological order already,
// oldest entry first.
//
// The preserved return value contains the indexes in files
// corresponding to the last ramp-up that were not passed to fn.
func (s *profileStore) cleanupLastRampup(
	ctx context.Context, files []os.FileInfo, maxP int64, fn func(string) error,
) (preserved map[int]bool) {
	preserved = make(map[int]bool)
	curMaxHeap := uint64(math.MaxUint64)
	numFiles := int64(0)
	for i := len(files) - 1; i >= 0; i-- {
		ok, _, curHeap := s.parseFileName(ctx, files[i].Name())
		if !ok {
			continue
		}

		if curHeap > curMaxHeap {
			// This is the end of a ramp-up sequence. We're done.
			break
		}

		// Keep the currently seen heap for the next iteration.
		curMaxHeap = curHeap

		// We saw one file.
		numFiles++

		// Did we encounter the maximum?
		if numFiles > maxP {
			// Yes: clean this up.
			if err := fn(files[i].Name()); err != nil {
				log.Warningf(ctx, "%v", err)
			}
		} else {
			// No: we preserve this file.
			preserved[i] = true
		}
	}

	return preserved
}

// parseFileName retrieves the components of a file name generated by makeNewFileName().
func (s *profileStore) parseFileName(
	ctx context.Context, fileName string,
) (ok bool, timestamp time.Time, heapUsage uint64) {
	parts := strings.Split(fileName, ".")
	const numParts = 4 /* prefix, date/time, milliseconds,  heap usage */
	if len(parts) != numParts || parts[0] != s.prefix {
		// Not for us. Silently ignore.
		return
	}
	if len(parts[2]) < 3 {
		// At some point in the v20.2 cycle the timestamps were generated
		// with format .999, which caused the trailing zeroes to be
		// omitted. During parsing, they must be present, so re-add them
		// here.
		//
		// TODO(knz): Remove this code in v21.1.
		parts[2] += "000"[:3-len(parts[2])]
	}
	maybeTimestamp := parts[1] + "." + parts[2]
	var err error
	timestamp, err = time.Parse(timestampFormat, maybeTimestamp)
	if err != nil {
		log.Warningf(ctx, "%v", errors.Wrapf(err, "%s", fileName))
		return
	}
	heapUsage, err = strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		log.Warningf(ctx, "%v", errors.Wrapf(err, "%s", fileName))
		return
	}
	ok = true
	return
}
