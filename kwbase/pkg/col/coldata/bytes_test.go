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

package coldata

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type bytesMethod int

const (
	set bytesMethod = iota
	window
	copySlice
	appendSlice
	appendVal
)

func (m bytesMethod) String() string {
	switch m {
	case set:
		return "Set"
	case window:
		return "Window"
	case copySlice:
		return "CopySlice"
	case appendSlice:
		return "AppendSlice"
	case appendVal:
		return "AppendVal"
	default:
		panic(fmt.Sprintf("unknown bytes method %d", m))
	}
}

var bytesMethods = []bytesMethod{set, window, copySlice, appendSlice, appendVal}

// applyMethodsAndVerify applies the given methods on b1 and a reference
// [][]byte implementation and checks if the results are equal. If
// selfReferencingSources is true, this is an indication by the caller that we
// are testing an edge case where the source for copies/appends refers to the
// destination. In cases where *Bytes updates itself under the hood, we also
// update the corresponding b2Source to mirror the behavior.
func applyMethodsAndVerify(
	rng *rand.Rand,
	b1, b1Source *Bytes,
	b2, b2Source [][]byte,
	methods []bytesMethod,
	selfReferencingSources bool,
) error {
	if err := verifyEqual(b1, b2); err != nil {
		return errors.Wrap(err, "arguments should start as equal")
	}
	if err := verifyEqual(b1Source, b2Source); err != nil {
		return errors.Wrap(err, "argument sources should start as equal")
	}
	debugString := fmt.Sprintf("\ninitial:\n%s\n", b1)
	for _, m := range methods {
		n := b1.Len()
		if n != len(b2) {
			return errors.Errorf("length mismatch between flat and reference: %d != %d", n, len(b2))
		}
		sourceN := b1Source.Len()
		if sourceN != len(b2Source) {
			return errors.Errorf("length mismatch between flat and reference sources: %d != %d", sourceN, len(b2Source))
		}
		debugString += m.String()
		switch m {
		case set:
			// Can only Set starting from maxSetIndex.
			i := b1.maxSetIndex + rng.Intn(b1.Len()-b1.maxSetIndex)
			new := make([]byte, rng.Intn(16))
			rng.Read(new)
			debugString += fmt.Sprintf("(%d, %v)", i, new)
			b1.Set(i, new)
			b2[i] = new
		case window:
			start := rng.Intn(n)
			end := rng.Intn(n + 1)
			if start > end {
				end = start + 1
			}
			debugString += fmt.Sprintf("(%d, %d)", start, end)
			b1Window := b1.Window(start, end)
			b2Window := b2[start:end]
			// b1Window is not allowed to be modified, so we check explicitly whether
			// it equals the reference, and we do not update b1 and b2.
			b1Window.AssertOffsetsAreNonDecreasing(b1Window.Len())
			debugString += fmt.Sprintf("\n%s\n", b1Window)
			if err := verifyEqual(b1Window, b2Window); err != nil {
				return errors.Wrap(err, fmt.Sprintf("\ndebugString:\n%sflat:\n%sreference:\n%s", debugString, b1Window.String(), prettyByteSlice(b2Window)))
			}
			continue
		case copySlice, appendSlice:
			// Generate a length-inclusive destIdx.
			destIdx := rng.Intn(n + 1)
			srcStartIdx := rng.Intn(sourceN)
			srcEndIdx := rng.Intn(sourceN)
			if srcStartIdx > srcEndIdx {
				srcEndIdx = srcStartIdx + 1
			} else if srcStartIdx == srcEndIdx {
				// Avoid whittling down our destination slice.
				srcStartIdx = 0
				srcEndIdx = sourceN
			}
			debugString += fmt.Sprintf("(%d, %d, %d)", destIdx, srcStartIdx, srcEndIdx)
			var numNewVals int
			if m == copySlice {
				b1.CopySlice(b1Source, destIdx, srcStartIdx, srcEndIdx)
				numNewVals = copy(b2[destIdx:], b2Source[srcStartIdx:srcEndIdx])
			} else {
				b1.AppendSlice(b1Source, destIdx, srcStartIdx, srcEndIdx)
				b2 = append(b2[:destIdx], b2Source[srcStartIdx:srcEndIdx]...)
				if selfReferencingSources {
					b1Source = b1
					b2Source = b2
				}
				numNewVals = srcEndIdx - srcStartIdx
			}
			// Deep copy the copied/appended byte slices.
			b2Slice := b2[destIdx : destIdx+numNewVals]
			for i := range b2Slice {
				b2Slice[i] = append([]byte(nil), b2Slice[i]...)
			}
		case appendVal:
			v := make([]byte, 16)
			rng.Read(v)
			debugString += fmt.Sprintf("(%v)", v)
			b1.AppendVal(v)
			b2 = append(b2, v)
			if selfReferencingSources {
				b1Source = b1
				b2Source = b2
			}
		default:
			return errors.Errorf("unknown method name: %s", m)
		}
		b1.AssertOffsetsAreNonDecreasing(b1.Len())
		debugString += fmt.Sprintf("\n%s\n", b1)
		if err := verifyEqual(b1, b2); err != nil {
			return errors.Wrap(err, fmt.Sprintf("\ndebugString:\n%sflat (maxSetIdx=%d):\n%sreference:\n%s", debugString, b1.maxSetIndex, b1.String(), prettyByteSlice(b2)))
		}
	}
	return nil
}

func verifyEqual(flat *Bytes, b [][]byte) error {
	if flat.Len() != len(b) {
		return errors.Errorf("mismatched lengths %d != %d", flat.Len(), len(b))
	}
	for i := range b {
		if !bytes.Equal(b[i], flat.Get(i)) {
			return errors.Errorf("mismatch at index %d", i)
		}
	}
	return nil
}

func prettyByteSlice(b [][]byte) string {
	var builder strings.Builder
	for i := range b {
		builder.WriteString(
			fmt.Sprintf("%d: %v\n", i, b[i]),
		)
	}
	return builder.String()
}

func TestBytesRefImpl(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewPseudoRand()

	const (
		maxNumberOfCalls = 64
		maxLength        = 16
		nRuns            = 100
	)

	for nRun := 0; nRun < nRuns; nRun++ {
		n := 1 + rng.Intn(maxLength)

		flat := NewBytes(n)
		reference := make([][]byte, n)
		for i := 0; i < n; i++ {
			v := make([]byte, rng.Intn(16))
			rng.Read(v)
			flat.Set(i, append([]byte(nil), v...))
			reference[i] = append([]byte(nil), v...)
		}

		// Make a pair of sources to copy/append from. Use the destination variables
		// with a certain probability.
		sourceN := n
		flatSource := flat
		referenceSource := reference
		selfReferencingSources := true
		if rng.Float64() < 0.5 {
			selfReferencingSources = false
			sourceN = 1 + rng.Intn(maxLength)
			flatSource = NewBytes(sourceN)
			referenceSource = make([][]byte, sourceN)
			for i := 0; i < sourceN; i++ {
				v := make([]byte, rng.Intn(16))
				rng.Read(v)
				flatSource.Set(i, append([]byte(nil), v...))
				referenceSource[i] = append([]byte(nil), v...)
			}
		}

		if err := verifyEqual(flat, reference); err != nil {
			t.Fatalf("not equal: %v\nflat:\n%sreference:\n%s", err, flat, prettyByteSlice(reference))
		}

		numCalls := 1 + rng.Intn(maxNumberOfCalls)
		methods := make([]bytesMethod, 0, numCalls)
		for i := 0; i < numCalls; i++ {
			methods = append(methods, bytesMethods[rng.Intn(len(bytesMethods))])
		}
		if err := applyMethodsAndVerify(rng, flat, flatSource, reference, referenceSource, methods, selfReferencingSources); err != nil {
			t.Logf("nRun = %d\n", nRun)
			t.Fatal(err)
		}
	}
}

func TestBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("Simple", func(t *testing.T) {
		b1 := NewBytes(0)
		b1.AppendVal([]byte("hello"))
		require.Equal(t, "hello", string(b1.Get(0)))
		b1.AppendVal(nil)
		require.Equal(t, []byte{}, b1.Get(1))
		require.Equal(t, 2, b1.Len())
		// Verify that we cannot overwrite a value.
		require.Panics(
			t,
			func() { b1.Set(0, []byte("not allowed")) },
			"should be unable to overwrite value",
		)

		// However, it is legal to overwrite the last value.
		b1.Set(1, []byte("ok"))

		// If we Reset the Bytes, we can Set any index.
		b1.Reset()
		b1.Set(1, []byte("new usage"))
		// But not an index before that.
		require.Panics(
			t,
			func() { b1.Set(0, []byte("still not allowed")) },
			"should be unable to overwrite value",
		)

		// Same with Reset.
		b1.Reset()
		b1.Set(1, []byte("reset new usage"))
	})

	t.Run("Append", func(t *testing.T) {
		b1 := NewBytes(0)
		b2 := NewBytes(0)
		b2.AppendVal([]byte("source bytes value"))
		b1.AppendVal([]byte("one"))
		b1.AppendVal([]byte("two"))
		// Truncate b1.
		require.Equal(t, 2, b1.Len())
		b1.AppendSlice(b2, 0, 0, 0)
		require.Equal(t, 0, b1.Len())

		b1.AppendVal([]byte("hello again"))

		// Try appending b2 3 times. The first time will overwrite the current
		// present value in b1.
		for i := 0; i < 3; i++ {
			b1.AppendSlice(b2, i, 0, b2.Len())
			require.Equal(t, i+1, b1.Len())
			for j := 0; j <= i; j++ {
				require.Equal(t, "source bytes value", string(b1.Get(j)))
			}
		}

		b2 = NewBytes(0)
		b2.AppendVal([]byte("hello again"))
		b2.AppendVal([]byte("hello again"))
		b2.AppendVal([]byte("hello again"))
		// Try to append only a subset of the source keeping the first element of
		// b1 intact.
		b1.AppendSlice(b2, 1, 1, 2)
		require.Equal(t, 2, b1.Len())
		require.Equal(t, "source bytes value", string(b1.Get(0)))
		require.Equal(t, "hello again", string(b1.Get(1)))
	})

	t.Run("Copy", func(t *testing.T) {
		b1 := NewBytes(0)
		b2 := NewBytes(0)
		b1.AppendVal([]byte("one"))
		b1.AppendVal([]byte("two"))
		b1.AppendVal([]byte("three"))

		b2.AppendVal([]byte("source one"))
		b2.AppendVal([]byte("source two"))

		// Copy "source two" into "two"'s position. This also tests that elements
		// following the copied element are correctly shifted.
		b1.CopySlice(b2, 1, 1, 2)
		require.Equal(t, 3, b1.Len())
		require.Equal(t, "one", string(b1.Get(0)))
		require.Equal(t, "source two", string(b1.Get(1)))
		require.Equal(t, "three", string(b1.Get(2)))

		// Copy will only copy as many elements as there is capacity for. In this
		// call, the copy starts at index 2, so there is only capacity for one
		// element.
		b1.CopySlice(b2, 2, 0, b2.Len())
		require.Equal(t, "one", string(b1.Get(0)))
		require.Equal(t, "source two", string(b1.Get(1)))
		require.Equal(t, "source one", string(b1.Get(2)))

		// Set the length to 1 and  follow it with testing a full overwrite of only
		// one element.
		b1.SetLength(1)
		require.Equal(t, 1, b1.Len())
		b1.CopySlice(b2, 0, 0, b2.Len())
		require.Equal(t, 1, b1.Len())
		require.Equal(t, "source one", string(b1.Get(0)))

		// Verify a full overwrite with a non-zero source start index.
		b1.CopySlice(b2, 0, 1, b2.Len())
		require.Equal(t, 1, b1.Len())
		require.Equal(t, "source two", string(b1.Get(0)))
	})

	t.Run("Window", func(t *testing.T) {
		b1 := NewBytes(0)
		b1.AppendVal([]byte("one"))
		b1.AppendVal([]byte("two"))
		b1.AppendVal([]byte("three"))

		w := b1.Window(0, 3)
		require.NotEqual(t, unsafe.Pointer(b1), unsafe.Pointer(w), "Bytes.Window should create a new object")
		b2 := b1.Window(1, 2)
		require.Equal(t, "one", string(b1.Get(0)))
		require.Equal(t, "two", string(b1.Get(1)))
		require.Equal(t, "two", string(b2.Get(0)))

		require.Panics(t, func() { b2.AppendVal([]byte("four")) }, "appending to the window into b1 should have panicked")
	})

	t.Run("String", func(t *testing.T) {
		b1 := NewBytes(0)
		vals := [][]byte{
			[]byte("one"),
			[]byte("two"),
			[]byte("three"),
		}
		for i := range vals {
			b1.AppendVal(vals[i])
		}

		// The values should be printed using the String function.
		b1String := b1.String()
		require.True(
			t,
			strings.Contains(b1String, fmt.Sprint(vals[0])) &&
				strings.Contains(b1String, fmt.Sprint(vals[1])) &&
				strings.Contains(b1String, fmt.Sprint(vals[2])),
		)

		// A window on the bytes should only print the values included in the
		// window.
		b2String := b1.Window(1, 3).String()
		require.True(
			t,
			!strings.Contains(b2String, fmt.Sprint(vals[0])) &&
				strings.Contains(b2String, fmt.Sprint(vals[1])) &&
				strings.Contains(b2String, fmt.Sprint(vals[2])),
		)
	})

	t.Run("InvariantSimple", func(t *testing.T) {
		b1 := NewBytes(8)
		b1.Set(0, []byte("zero"))
		other := b1.Window(0, 2)
		other.AssertOffsetsAreNonDecreasing(2)

		b2 := NewBytes(8)
		b2.Set(0, []byte("zero"))
		b2.Set(2, []byte("two"))
		other = b2.Window(0, 4)
		other.AssertOffsetsAreNonDecreasing(4)
	})
}
