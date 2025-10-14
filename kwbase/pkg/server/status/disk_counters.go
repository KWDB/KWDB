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

//go:build !darwin
// +build !darwin

package status

import (
	"context"
	"time"

	"github.com/shirou/gopsutil/disk"
)

func getDiskCounters(ctx context.Context) ([]diskStats, error) {
	driveStats, err := disk.IOCountersWithContext(ctx)
	if err != nil {
		return nil, err
	}

	output := make([]diskStats, len(driveStats))
	i := 0
	for _, counters := range driveStats {
		output[i] = diskStats{
			readBytes:      int64(counters.ReadBytes),
			readCount:      int64(counters.ReadCount),
			readTime:       time.Duration(counters.ReadTime) * time.Millisecond,
			writeBytes:     int64(counters.WriteBytes),
			writeCount:     int64(counters.WriteCount),
			writeTime:      time.Duration(counters.WriteTime) * time.Millisecond,
			ioTime:         time.Duration(counters.IoTime) * time.Millisecond,
			weightedIOTime: time.Duration(counters.WeightedIO) * time.Millisecond,
			iopsInProgress: int64(counters.IopsInProgress),
		}
		i++
	}

	return output, nil
}
