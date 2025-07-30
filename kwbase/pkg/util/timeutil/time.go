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

package timeutil

import (
	"errors"
	"strings"
	"time"
)

// LibPQTimePrefix is the prefix lib/pq prints time-type datatypes with.
const LibPQTimePrefix = "0000-01-01"

// Since returns the time elapsed since t.
// It is shorthand for Now().Sub(t).
func Since(t time.Time) time.Duration {
	return Now().Sub(t)
}

// Until returns the duration until t.
// It is shorthand for t.Sub(Now()).
func Until(t time.Time) time.Duration {
	return t.Sub(Now())
}

// UnixEpoch represents the Unix epoch, January 1, 1970 UTC.
var UnixEpoch = time.Unix(0, 0).UTC()

// FromUnixMicros returns the UTC time.Time corresponding to the given Unix
// time, usec microseconds since UnixEpoch. In Go's current time.Time
// implementation, all possible values for us can be represented as a time.Time.
func FromUnixMicros(us int64) time.Time {
	return time.Unix(us/1e6, (us%1e6)*1e3).UTC()
}

// ToUnixMicros returns t as the number of microseconds elapsed since UnixEpoch.
// Fractional microseconds are rounded, half up, using time.Round. Similar to
// time.Time.UnixNano, the result is undefined if the Unix time in microseconds
// cannot be represented by an int64.
func ToUnixMicros(t time.Time) int64 {
	return t.Unix()*1e6 + int64(t.Round(time.Microsecond).Nanosecond())/1e3
}

// SubTimes return subtract between after and before
func SubTimes(after time.Time, before time.Time) int64 {
	return ToUnixMicros(after) - ToUnixMicros(before)
}

// Unix wraps time.Unix ensuring that the result is in UTC instead of Local.
func Unix(sec, nsec int64) time.Time {
	return time.Unix(sec, nsec).UTC()
}

// FromUnixMilli returns the local Time corresponding to the given Unix time,
// msec milliseconds since January 1, 1970 UTC.
func FromUnixMilli(msec int64) time.Time {
	return Unix(msec/1e3, (msec%1e3)*1e6)
}

// FromTimestamp convert timestamp and precision to time.
func FromTimestamp(ts int64, precision int32) time.Time {
	switch precision {
	case 0:
		return Unix(ts, 0)
	case 3:
		return Unix(ts/1e3, (ts%1e3)*1e6)
	case 6:
		return Unix(ts/1e6, (ts%1e6)*1e3)
	case 9:
		return Unix(ts/1e9, ts%1e9)
	default:
		panic(errors.New("precision invalid"))
	}
}

// ToUnixMilli returns t as a Unix time, the number of milliseconds elapsed since
// January 1, 1970 UTC. The result is undefined if the Unix time in
// milliseconds cannot be represented by an int64 (a date more than 292 million
// years before or after 1970). The result does not depend on the
// location associated with t.
func ToUnixMilli(t time.Time) int64 {
	return t.Unix()*1e3 + int64(t.Round(time.Millisecond).Nanosecond())/1e6
}

// SleepUntil sleeps until the given time. The current time is
// refreshed every second in case there was a clock jump
//
// untilNanos is the target time to sleep till in epoch nanoseconds
// currentTimeNanos is a function returning current time in epoch nanoseconds
func SleepUntil(untilNanos int64, currentTimeNanos func() int64) {
	for {
		d := time.Duration(untilNanos - currentTimeNanos())
		if d <= 0 {
			break
		}
		if d > time.Second {
			d = time.Second
		}
		time.Sleep(d)
	}
}

// ReplaceLibPQTimePrefix replaces unparsable lib/pq dates used for timestamps
// (0000-01-01) with timestamps that can be parsed by date libraries.
func ReplaceLibPQTimePrefix(s string) string {
	if strings.HasPrefix(s, LibPQTimePrefix) {
		return "1970-01-01" + s[len(LibPQTimePrefix):]
	}
	return s
}
