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

package search

import (
	"math/rand"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

const pass = true
const fail = false

func TestSearchSpace(t *testing.T) {
	t.Run("fromBelow", func(t *testing.T) {
		ss := searchSpace{min: 0, max: 10}

		found, _ := ss.bound(fail, 8, 3)
		require.False(t, found)
		require.Equal(t, ss.min, 0)
		require.Equal(t, ss.max, 8)

		found, _ = ss.bound(pass, 4, 3)
		require.False(t, found)
		require.Equal(t, ss.min, 4)
		require.Equal(t, ss.max, 8)

		found, val := ss.bound(pass, 5, 3)
		require.True(t, found)
		require.Equal(t, val, 6)
		require.Equal(t, ss.min, 5)
		require.Equal(t, ss.max, 8)

		found, val = ss.bound(pass, 6, 3)
		require.True(t, found)
		require.Equal(t, val, 7)
		require.Equal(t, ss.min, 6)
		require.Equal(t, ss.max, 8)
	})

	t.Run("fromAbove", func(t *testing.T) {
		ss := searchSpace{min: 0, max: 10}

		found, _ := ss.bound(pass, 4, 3)
		require.False(t, found)
		require.Equal(t, ss.min, 4)
		require.Equal(t, ss.max, 10)

		found, _ = ss.bound(fail, 8, 3)
		require.False(t, found)
		require.Equal(t, ss.min, 4)
		require.Equal(t, ss.max, 8)

		found, val := ss.bound(fail, 7, 3)
		require.True(t, found)
		require.Equal(t, val, 5)
		require.Equal(t, ss.min, 4)
		require.Equal(t, ss.max, 7)

		found, val = ss.bound(pass, 6, 3)
		require.True(t, found)
		require.Equal(t, val, 6)
		require.Equal(t, ss.min, 6)
		require.Equal(t, ss.max, 7)
	})

	t.Run("lowPrec", func(t *testing.T) {
		ss := searchSpace{min: 0, max: 10}

		found, _ := ss.bound(fail, 8, 1)
		require.False(t, found)
		require.Equal(t, ss.min, 0)
		require.Equal(t, ss.max, 8)

		found, _ = ss.bound(fail, 3, 1)
		require.False(t, found)
		require.Equal(t, ss.min, 0)
		require.Equal(t, ss.max, 3)

		found, _ = ss.bound(pass, 1, 1)
		require.False(t, found)
		require.Equal(t, ss.min, 1)
		require.Equal(t, ss.max, 3)

		found, val := ss.bound(pass, 2, 1)
		require.True(t, found)
		require.Equal(t, val, 2)
		require.Equal(t, ss.min, 2)
		require.Equal(t, ss.max, 3)
	})

	t.Run("minimum", func(t *testing.T) {
		ss := searchSpace{min: 0, max: 10}

		found, _ := ss.bound(fail, 8, 3)
		require.False(t, found)
		require.Equal(t, ss.min, 0)
		require.Equal(t, ss.max, 8)

		found, val := ss.bound(fail, 3, 3)
		require.True(t, found)
		require.Equal(t, val, 1)
		require.Equal(t, ss.min, 0)
		require.Equal(t, ss.max, 3)
	})

	t.Run("minimumLowPrec", func(t *testing.T) {
		ss := searchSpace{min: 0, max: 10}

		found, _ := ss.bound(fail, 8, 1)
		require.False(t, found)
		require.Equal(t, ss.min, 0)
		require.Equal(t, ss.max, 8)

		found, _ = ss.bound(fail, 3, 1)
		require.False(t, found)
		require.Equal(t, ss.min, 0)
		require.Equal(t, ss.max, 3)

		found, val := ss.bound(fail, 1, 1)
		require.True(t, found)
		require.Equal(t, val, 0)
		require.Equal(t, ss.min, 0)
		require.Equal(t, ss.max, 1)
	})

	t.Run("maximum", func(t *testing.T) {
		ss := searchSpace{min: 0, max: 10}

		found, _ := ss.bound(pass, 4, 3)
		require.False(t, found)
		require.Equal(t, ss.min, 4)
		require.Equal(t, ss.max, 10)

		found, val := ss.bound(pass, 8, 3)
		require.True(t, found)
		require.Equal(t, val, 9)
		require.Equal(t, ss.min, 8)
		require.Equal(t, ss.max, 10)
	})

	t.Run("maximumLowPrec", func(t *testing.T) {
		ss := searchSpace{min: 0, max: 10}

		found, _ := ss.bound(pass, 4, 1)
		require.False(t, found)
		require.Equal(t, ss.min, 4)
		require.Equal(t, ss.max, 10)

		found, _ = ss.bound(pass, 8, 1)
		require.False(t, found)
		require.Equal(t, ss.min, 8)
		require.Equal(t, ss.max, 10)

		found, val := ss.bound(pass, 9, 1)
		require.True(t, found)
		require.Equal(t, val, 9)
		require.Equal(t, ss.min, 9)
		require.Equal(t, ss.max, 10)
	})

	// Error cases.
	t.Run("errors", func(t *testing.T) {
		ss := searchSpace{min: 0, max: 10}

		found, _ := ss.bound(pass, 4, 1)
		require.False(t, found)

		found, _ = ss.bound(fail, 8, 1)
		require.False(t, found)

		require.Panics(t, func() { ss.bound(pass, 5, 0) })  // non-positive prec
		require.Panics(t, func() { ss.bound(fail, 1, 1) })  // below bound failure
		require.Panics(t, func() { ss.bound(pass, 10, 1) }) // above bound success
	})
}

func TestBinarySearcher(t *testing.T) {
	// Looking for 66.
	bs := NewBinarySearcher(0, 100, 1)
	require.Equal(t, bs.current(), 50)

	require.Equal(t, bs.step(pass), false)
	require.Equal(t, bs.current(), 75)

	require.Equal(t, bs.step(fail), false)
	require.Equal(t, bs.current(), 62)

	require.Equal(t, bs.step(pass), false)
	require.Equal(t, bs.current(), 68)

	require.Equal(t, bs.step(fail), false)
	require.Equal(t, bs.current(), 65)

	require.Equal(t, bs.step(pass), false)
	require.Equal(t, bs.current(), 66)

	require.Equal(t, bs.step(pass), false)
	require.Equal(t, bs.current(), 67)

	require.Equal(t, bs.step(fail), true)
	require.Equal(t, bs.current(), 66)

	// Looking for 25. Should result in 26 because of precision.
	bs = NewBinarySearcher(0, 100, 3)
	res, err := bs.Search(func(i int) (bool, error) {
		return i <= 25, nil
	})
	require.Nil(t, err)
	require.Equal(t, res, 26)

	// Looking for 25. Should result in 25 because of precision.
	bs = NewBinarySearcher(0, 100, 1)
	res, err = bs.Search(func(i int) (bool, error) {
		return i <= 25, nil
	})
	require.Nil(t, err)
	require.Equal(t, res, 25)
}

func TestLineSearcher(t *testing.T) {
	// Looking for 66.
	ls := NewLineSearcher(0, 100, 20 /* start */, 2 /* stepSize */, 1)
	require.Equal(t, ls.current(), 20)

	require.Equal(t, ls.step(pass), false)
	require.Equal(t, ls.current(), 22)

	require.Equal(t, ls.step(pass), false)
	require.Equal(t, ls.current(), 26)

	require.Equal(t, ls.step(pass), false)
	require.Equal(t, ls.current(), 34)

	require.Equal(t, ls.step(pass), false)
	require.Equal(t, ls.current(), 50)

	require.Equal(t, ls.step(pass), false)
	require.Equal(t, ls.current(), 82)

	require.Equal(t, ls.step(fail), false)
	require.Equal(t, ls.current(), 66)

	require.Equal(t, ls.step(pass), false)
	require.Equal(t, ls.current(), 74)

	require.Equal(t, ls.step(fail), false)
	require.Equal(t, ls.current(), 70)

	require.Equal(t, ls.step(fail), false)
	require.Equal(t, ls.current(), 68)

	require.Equal(t, ls.step(fail), false)
	require.Equal(t, ls.current(), 67)

	require.Equal(t, ls.step(fail), true)
	require.Equal(t, ls.current(), 66)

	// Looking for 9.
	ls = NewLineSearcher(0, 100, 71 /* start */, 4 /* stepSize */, 2)
	require.Equal(t, ls.current(), 71)

	require.Equal(t, ls.step(fail), false)
	require.Equal(t, ls.current(), 67)

	require.Equal(t, ls.step(fail), false)
	require.Equal(t, ls.current(), 59)

	require.Equal(t, ls.step(fail), false)
	require.Equal(t, ls.current(), 43)

	require.Equal(t, ls.step(fail), false)
	require.Equal(t, ls.current(), 11)

	require.Equal(t, ls.step(fail), false)
	require.Equal(t, ls.current(), 1)

	require.Equal(t, ls.step(pass), false)
	require.Equal(t, ls.current(), 6)

	require.Equal(t, ls.step(pass), false)
	require.Equal(t, ls.current(), 8)

	require.Equal(t, ls.step(pass), false)
	require.Equal(t, ls.current(), 9)

	require.Equal(t, ls.step(pass), true)
	require.Equal(t, ls.current(), 10)

	// Looking for 25. Should result in 26 because of precision.
	ls = NewLineSearcher(0, 100, 67 /* start */, 2 /* stepSize */, 3)
	res, err := ls.Search(func(i int) (bool, error) {
		return i <= 25, nil
	})
	require.Nil(t, err)
	require.Equal(t, res, 26)

	// Looking for 25. Should result in 25 because of precision.
	ls = NewLineSearcher(0, 100, 92 /* start */, 2 /* stepSize */, 1)
	res, err = ls.Search(func(i int) (bool, error) {
		return i <= 25, nil
	})
	require.Nil(t, err)
	require.Equal(t, res, 25)
}

func TestIdenticalSearchers(t *testing.T) {
	const prec = 1
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	searcherFns := []func(min, max int) Searcher{
		func(min, max int) Searcher { return NewBinarySearcher(min, max, prec) },
		func(min, max int) Searcher { return NewLineSearcher(min, max, mid(min, max), 4, prec) },
	}

	const trials = 100
	for tr := 0; tr < trials; tr++ {
		min := int(rng.Int31())
		max := int(rng.Int31())
		if min == max {
			continue
		}
		if min > max {
			min, max = max, min
		}

		toFind := rng.Intn(max-min) + min
		pred := func(i int) (bool, error) {
			return i <= toFind, nil
		}

		for _, fn := range searcherFns {
			s := fn(min, max)
			val, err := s.Search(pred)
			require.Nil(t, err)
			require.Equal(t, val, toFind, "searching with %T in range [%d,%d)", s, min, max)
		}

	}
}
