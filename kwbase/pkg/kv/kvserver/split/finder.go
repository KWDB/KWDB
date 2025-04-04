// Copyright 2018 The Cockroach Authors.
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

package split

import (
	"bytes"
	"math"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
)

// Load-based splitting.
//
// - Engage split for ranges:
//  - With size exceeding min-range-bytes
//  - with reqs/s rate over a configurable threshold
// - Disengage when a range no longer meets the criteria
// - During split:
//  - Record start time
//  - Keep a sample of 10 keys
//   - Each sample contains three counters: left, right and contained.
//   - On each span, increment the left and/or right counters, depending
//     on whether the span falls entirely to the left, to the right.
//     If exactly on the key, increment neither.
//   - If the span overlaps with the key, increment the contained counter.
//   - When a sample is replaced, discard its counters.
//  - If a range is on for more than a threshold interval:
//   - Examine sample for the smallest diff between left and right counters,
//     excluding any whose counters are not sufficiently advanced;
//     If not less than some constant threshold, skip split.
//   - Use the contained counters to give lower priority to potential split
//     points that have more requests that span over it.
//   - If a desired point is reached, add range to split queue with the chosen
//     key as split key, and provide hint to scatter the replicas.

const (
	// RecordDurationThreshold is the minimum duration of time the split finder
	// will record a range for, before being ready for a split.
	RecordDurationThreshold    = 10 * time.Second // 10s
	splitKeySampleSize         = 20               // size of split key sample
	splitKeyMinCounter         = 100              // min aggregate counters before consideration
	splitKeyThreshold          = 0.25             // 25% difference between left/right counters
	splitKeyContainedThreshold = 0.50             // too many spanning queries over split point
)

type sample struct {
	key                    roachpb.Key
	left, right, contained int
}

// Finder is a structure that is used to determine the split point
// using the Reservoir Sampling method.
type Finder struct {
	startTime time.Time
	samples   [splitKeySampleSize]sample
	count     int
}

// NewFinder initiates a Finder with the given time.
func NewFinder(startTime time.Time) *Finder {
	return &Finder{
		startTime: startTime,
	}
}

// Ready checks if the Finder has been initialized with a sufficient
// sample duration.
func (f *Finder) Ready(nowTime time.Time) bool {
	return nowTime.Sub(f.startTime) > RecordDurationThreshold
}

// Record informs the Finder about where the span lies with
// regard to the keys in the samples.
func (f *Finder) Record(span roachpb.Span, intNFn func(int) int) {
	if f == nil {
		return
	}

	var idx int
	count := f.count
	f.count++
	if count < splitKeySampleSize {
		idx = count
	} else if idx = intNFn(count); idx >= splitKeySampleSize {
		// Increment all existing keys' counters.
		for i := range f.samples {
			if span.ProperlyContainsKey(f.samples[i].key) {
				f.samples[i].contained++
			} else {
				// If the split is chosen to be here and the key is on or to the left
				// of the start key of the span, we know that the request the span represents
				// - would be isolated to the right of the split point.
				// Similarly, if the split key is greater than the start key of the span
				// (and given that it is not properly contained by the span) it must mean
				// that the request the span represents would be on the left.
				if comp := bytes.Compare(f.samples[i].key, span.Key); comp <= 0 {
					f.samples[i].right++
				} else if comp > 0 {
					f.samples[i].left++
				}
			}
		}
		return
	}

	// Note we always use the start key of the span. We could
	// take the average of the byte slices, but that seems
	// unnecessarily complex for practical usage.
	f.samples[idx] = sample{key: span.Key}
}

// Key finds an appropriate split point based on the Reservoir sampling method.
// Returns a nil key if no appropriate key was found.
func (f *Finder) Key() roachpb.Key {
	if f == nil {
		return nil
	}

	var bestIdx = -1
	var bestScore float64 = 2
	for i, s := range f.samples {
		if s.left+s.right+s.contained < splitKeyMinCounter {
			continue
		}
		balanceScore := math.Abs(float64(s.left-s.right)) / float64(s.left+s.right)
		containedScore := (float64(s.contained) / float64(s.left+s.right+s.contained))
		finalScore := balanceScore + containedScore
		if balanceScore >= splitKeyThreshold ||
			containedScore >= splitKeyContainedThreshold {
			continue
		}
		if finalScore < bestScore {
			bestIdx = i
			bestScore = finalScore
		}
	}

	if bestIdx == -1 {
		return nil
	}
	return f.samples[bestIdx].key
}
