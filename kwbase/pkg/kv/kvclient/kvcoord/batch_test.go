// Copyright 2015 The Cockroach Authors.
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

package kvcoord

import (
	"bytes"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestBatchPrevNext tests prev() and next()
func TestBatchPrevNext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	loc := func(s string) string {
		return string(keys.RangeDescriptorKey(roachpb.RKey(s)))
	}
	span := func(strs ...string) []roachpb.Span {
		var r []roachpb.Span
		for i, str := range strs {
			if i%2 == 0 {
				r = append(r, roachpb.Span{Key: roachpb.Key(str)})
			} else {
				r[len(r)-1].EndKey = roachpb.Key(str)
			}
		}
		return r
	}
	max, min := string(roachpb.RKeyMax), string(roachpb.RKeyMin)
	abc := span("a", "", "b", "", "c", "")
	testCases := []struct {
		spans             []roachpb.Span
		key, expFW, expBW string
	}{
		{spans: span(), key: "whatevs",
			// Sanity check for empty batch.
			expFW: max,
			expBW: min,
		},
		{spans: span("a", "b", "c", "d"), key: "c",
			// Done with `key < c`, so `c <= key` next.
			expFW: "c",
			// This is the interesting case in this test. See
			// https://gitee.com/kwbasedb/kwbase/issues/18174
			//
			// Done with `key >= c`, and nothing's in `[b, c)`, so all that's
			// left is `key < b`. Before fixing #18174 we would end up at `c`
			// which could create empty batches.
			expBW: "b",
		},
		{spans: span("a", "c", "b", ""), key: "b",
			// Done with `key < b`, so `b <= key` is next.
			expFW: "b",
			// Done with `key >= b`, but we have work to do for `b > key`.
			expBW: "b",
		},
		{spans: span("a", "c", "b", ""), key: "a",
			// Same as last one.
			expFW: "a",
			// Same as the first test case, except we drop all the way to `min`.
			expBW: min,
		},
		{spans: span("a", "c", "d", ""), key: "c",
			// Having dealt with `key < c` there's a gap which leaves us at `d <= key`.
			expFW: "d",
			// Nothing exciting: [a, c) is outstanding.
			expBW: "c",
		},
		{spans: span("a", "c\x00", "d", ""), key: "c",
			// First request overlaps `c` in both directions,
			// so that's where we stay.
			expFW: "c",
			expBW: "c",
		},
		{spans: abc, key: "b",
			// We've seen `key < b` so we still need to hit `b`.
			expFW: "b",
			// We've seen `key >= b`, so hop over the gap to `a`. Similar to the
			// first test case.
			expBW: "a",
		},
		{spans: abc, key: "b\x00",
			// No surprises.
			expFW: "c",
			expBW: "b",
		},
		{spans: abc, key: "bb",
			// Ditto.
			expFW: "c",
			expBW: "b",
		},

		// Multiple candidates. No surprises, just a sanity check.
		{spans: span("a", "b", "c", "d"), key: "e",
			expFW: max,
			expBW: "d",
		},
		{spans: span("a", "b", "c", "d"), key: "0",
			expFW: "a",
			expBW: min,
		},

		// Local keys are tricky. See keys.AddrUpperBound for a comment. The basic
		// intuition should be that /Local/a/b lives between a and a\x00 (for any b)
		// and so the smallest address-resolved range that covers /Local/a/b is [a, a\x00].
		{spans: span(loc("a"), loc("c")), key: "c",
			// We're done with any key that addresses to `< c`, and `loc(c)` isn't covered
			// by that. `loc(c)` lives between `c` and `c\x00` and so `c` is where we have
			// to start in forward mode.
			expFW: "c",
			// We're done with any key that addresses to `>=c`, `loc(c)` is the exclusive
			// end key here, so we next handle `key < c`.
			expBW: "c",
		},
		{spans: span(loc("a"), loc("c")), key: "c\x00",
			// We've dealt with everything addressing to `< c\x00`, and in particular
			// `addr(loc(c)) < c\x00`, so that span is handled and we see `max`.
			expFW: max,
			// Having dealt with `>= c\x00` we have to restart at `c\x00` itself (and not `c`!)
			// because otherwise we'd not see the local keys `/Local/c/x` which are not in `key < c`
			// but are in `key < c\x00`.
			expBW: "c\x00",
		},
		// Explanations below are an exercise for the reader, but it's very similar to above.
		{spans: span(loc("b"), ""), key: "a",
			expFW: "b",
			expBW: min,
		},
		{spans: span(loc("b"), ""), key: "b",
			expFW: "b",
			expBW: min,
		},
		{spans: span(loc("b"), ""), key: "b\x00",
			expFW: max,
			// Handled `key >= b\x00`, so next we'll have to chip away at `[KeyMin, b\x00)`. Note
			// how this doesn't return `b` which would be incorrect as `[KeyMin, b)` does not
			// contain `loc(b)`.
			expBW: "b\x00",
		},

		// Multiple candidates. No surprises, just a sanity check.
		{spans: span(loc("a"), loc("b"), loc("c"), loc("d")), key: "e",
			expFW: max,
			expBW: "d\x00",
		},
		{spans: span(loc("a"), loc("b"), loc("c"), loc("d")), key: "0",
			expFW: "a",
			expBW: min,
		},
	}

	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			var ba roachpb.BatchRequest
			for _, span := range test.spans {
				args := &roachpb.ScanRequest{}
				args.Key, args.EndKey = span.Key, span.EndKey
				ba.Add(args)
			}
			if next, err := next(ba, roachpb.RKey(test.key)); err != nil {
				t.Error(err)
			} else if !bytes.Equal(next, roachpb.Key(test.expFW)) {
				t.Errorf("next: expected %q, got %q", test.expFW, next)
			}
			if prev, err := prev(ba, roachpb.RKey(test.key)); err != nil {
				t.Error(err)
			} else if !bytes.Equal(prev, roachpb.Key(test.expBW)) {
				t.Errorf("prev: expected %q, got %q", test.expBW, prev)
			}
		})
	}
}
