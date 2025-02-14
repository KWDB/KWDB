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

package roachpb

import (
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func makeSpan(s string) Span {
	parts := strings.Split(s, "-")
	if len(parts) == 2 {
		return Span{Key: Key(parts[0]), EndKey: Key(parts[1])}
	}
	return Span{Key: Key(s)}
}

func makeSpans(s string) Spans {
	var spans Spans
	if len(s) > 0 {
		for _, p := range strings.Split(s, ",") {
			spans = append(spans, makeSpan(p))
		}
	}
	return spans
}

func TestMergeSpans(t *testing.T) {
	testCases := []struct {
		spans    string
		expected string
		distinct bool
	}{
		{"", "", true},
		{"a", "a", true},
		{"a,b", "a,b", true},
		{"b,a", "a,b", true},
		{"a,a", "a", false},
		{"a-b", "a-b", true},
		{"a-b,b-c", "a-c", true},
		{"a-c,a-b", "a-c", false},
		{"a,b-c", "a,b-c", true},
		{"a,a-c", "a-c", false},
		{"a-c,b", "a-c", false},
		{"a-c,c", "a-c\x00", true},
		{"a-c,b-bb", "a-c", false},
		{"a-c,b-c", "a-c", false},
	}
	for i, c := range testCases {
		spans, distinct := MergeSpans(makeSpans(c.spans))
		expected := makeSpans(c.expected)
		if (len(expected) != 0 || len(spans) != 0) && reflect.DeepEqual(expected, spans) {
			t.Fatalf("%d: expected\n%s\n, but found:\n%s", i, expected, spans)
		}
		if c.distinct != distinct {
			t.Fatalf("%d: expected %t, but found %t", i, c.distinct, distinct)
		}
	}
}

func makeRandomParitialCovering(r *rand.Rand, maxKey int) Spans {
	var ret Spans
	for i := randutil.RandIntInRange(r, 0, maxKey); i < maxKey-1; {
		var s Span
		s.Key = encoding.EncodeVarintAscending(nil, int64(i))
		i = randutil.RandIntInRange(r, i, maxKey)
		s.EndKey = encoding.EncodeVarintAscending(nil, int64(i))
		if i < maxKey && randutil.RandIntInRange(r, 0, 10) > 5 {
			i = randutil.RandIntInRange(r, i, maxKey)
		}
		ret = append(ret, s)
	}
	return ret
}

func TestSubtractSpans(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		testCases := []struct {
			input, remove, expected string
		}{
			{"", "", ""},                               // noop.
			{"a-z", "", "a-z"},                         // noop.
			{"a-z", "a-z", ""},                         // exactly covers everything.
			{"a-z", "a-c", "c-z"},                      // covers a prefix.
			{"a-z", "t-z", "a-t"},                      // covers a suffix.
			{"a-z", "m-p", "a-m,p-z"},                  // covers a proper subspan.
			{"a-z", "a-c,t-z", "c-t"},                  // covers a prefix and suffix.
			{"f-t", "a-f,z-y, ", "f-t"},                // covers a non-covered prefix.
			{"a-b,b-c,d-m,m-z", "", "a-b,b-c,d-m,m-z"}, // noop, but with more spans.
			{"a-b,b-c,d-m,m-z", "a-b,b-c,d-m,m-z", ""}, // everything again. more spans.
			{"a-b,b-c,d-m,m-z", "a-c", "d-m,m-z"},      // subspan spanning input spans.
			{"a-c,c-e,k-m,q-v", "b-d,k-l,q-t", "a-b,d-e,l-m,t-v"},
		}
		for _, tc := range testCases {
			got := SubtractSpans(makeSpans(tc.input), makeSpans(tc.remove))
			if len(got) == 0 {
				got = nil
			}
			require.Equalf(t, makeSpans(tc.expected), got, "testcase: %q - %q", tc.input, tc.remove)
		}
	})

	t.Run("random", func(t *testing.T) {
		const iterations = 100
		for i := 0; i < iterations; i++ {
			r, s := randutil.NewPseudoRand()
			t.Logf("random seed: %d", s)
			const max = 1000
			before := makeRandomParitialCovering(r, max)
			covered := makeRandomParitialCovering(r, max)
			after := SubtractSpans(append(Spans(nil), before...), append(Spans(nil), covered...))
			for i := 0; i < max; i++ {
				k := Key(encoding.EncodeVarintAscending(nil, int64(i)))
				expected := before.ContainsKey(k) && !covered.ContainsKey(k)
				if actual := after.ContainsKey(k); actual != expected {
					t.Errorf("key %q in before? %t in remove? %t in result? %t",
						k, before.ContainsKey(k), covered.ContainsKey(k), after.ContainsKey(k),
					)
				}
			}
		}
	})
}
