// Copyright 2015 The Cockroach Authors.
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

package metric

import (
	"time"

	"github.com/codahale/hdrhistogram"
)

var _ periodic = &slidingHistogram{}

// A deprecatedWindowedHistogram is a wrapper around an
// hdrhistogram.WindowedHistogram. The caller must enforce proper
// synchronization.
type slidingHistogram struct {
	windowed *hdrhistogram.WindowedHistogram
	nextT    time.Time
	duration time.Duration
}

// newSlidingHistogram creates a new windowed HDRHistogram with the given
// parameters. Data is kept in the active window for approximately the given
// duration. See the documentation for hdrhistogram.WindowedHistogram for
// details.
func newSlidingHistogram(duration time.Duration, maxVal int64, sigFigs int) *slidingHistogram {
	if duration <= 0 {
		panic("cannot create a sliding histogram with nonpositive duration")
	}
	return &slidingHistogram{
		nextT:    now(),
		duration: duration,
		windowed: hdrhistogram.NewWindowed(histWrapNum, 0, maxVal, sigFigs),
	}
}

func (h *slidingHistogram) tick() {
	h.nextT = h.nextT.Add(h.duration / histWrapNum)
	h.windowed.Rotate()
}

func (h *slidingHistogram) nextTick() time.Time {
	return h.nextT
}

func (h *slidingHistogram) Current() *hdrhistogram.Histogram {
	maybeTick(h)
	return h.windowed.Merge()
}

func (h *slidingHistogram) RecordValue(v int64) error {
	return h.windowed.Current.RecordValue(v)
}
