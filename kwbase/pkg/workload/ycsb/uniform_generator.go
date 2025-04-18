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

package ycsb

import (
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"golang.org/x/exp/rand"
)

// UniformGenerator is a random number generator that generates draws from a
// uniform distribution.
type UniformGenerator struct {
	iMin uint64
	mu   struct {
		syncutil.Mutex
		r    *rand.Rand
		iMax uint64
	}
}

// NewUniformGenerator constructs a new UniformGenerator with the given parameters.
// It returns an error if the parameters are outside the accepted range.
func NewUniformGenerator(rng *rand.Rand, iMin, iMax uint64) (*UniformGenerator, error) {

	z := UniformGenerator{}
	z.iMin = iMin
	z.mu.r = rng
	z.mu.iMax = iMax

	return &z, nil
}

// IncrementIMax increments iMax by count.
func (z *UniformGenerator) IncrementIMax(count uint64) error {
	z.mu.Lock()
	defer z.mu.Unlock()
	z.mu.iMax += count
	return nil
}

// Uint64 returns a random Uint64 between iMin and iMax, drawn from a uniform
// distribution.
func (z *UniformGenerator) Uint64() uint64 {
	z.mu.Lock()
	defer z.mu.Unlock()
	return (uint64)(z.mu.r.Int63n((int64)(z.mu.iMax-z.iMin+1))) + z.iMin
}
