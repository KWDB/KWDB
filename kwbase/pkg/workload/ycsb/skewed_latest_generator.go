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

package ycsb

import (
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"golang.org/x/exp/rand"
)

// SkewedLatestGenerator is a random number generator that generates numbers in
// the range [iMin, iMax], but skews it towards iMax using a zipfian
// distribution.
type SkewedLatestGenerator struct {
	mu struct {
		syncutil.Mutex
		iMax    uint64
		zipfGen *ZipfGenerator
	}
}

// NewSkewedLatestGenerator constructs a new SkewedLatestGenerator with the
// given parameters. It returns an error if the parameters are outside the
// accepted range.
func NewSkewedLatestGenerator(
	rng *rand.Rand, iMin, iMax uint64, theta float64, verbose bool,
) (*SkewedLatestGenerator, error) {

	z := SkewedLatestGenerator{}
	z.mu.iMax = iMax
	zipfGen, err := NewZipfGenerator(rng, 0, iMax-iMin, theta, verbose)
	if err != nil {
		return nil, err
	}
	z.mu.zipfGen = zipfGen

	return &z, nil
}

// IncrementIMax increments iMax by count.
func (z *SkewedLatestGenerator) IncrementIMax(count uint64) error {
	z.mu.Lock()
	defer z.mu.Unlock()
	z.mu.iMax += count
	return z.mu.zipfGen.IncrementIMax(count)
}

// Uint64 returns a random Uint64 between iMin and iMax, where keys near iMax
// are most likely to be drawn.
func (z *SkewedLatestGenerator) Uint64() uint64 {
	z.mu.Lock()
	defer z.mu.Unlock()
	return z.mu.iMax - z.mu.zipfGen.Uint64()
}
