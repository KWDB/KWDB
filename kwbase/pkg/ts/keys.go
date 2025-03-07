// Copyright 2014 The Cockroach Authors.
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

package ts

import (
	"bytes"
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// Time series keys are carefully constructed to usefully sort the data in the
// key-value store for the purpose of queries. Time series data is queryied by
// providing a series name and a timespan; we therefore expose the series name
// and the collection time prominently on the key.
//
// The precise formula for a time series key is:
//
//   [system key prefix]tsd[series name][resolution][time slot][source key]
//
// The series name is an arbitrary string identifying the series, although the
// ts system may enforce naming rules at a higher level. This string is binary
// encoded in the key.
//
// The source key is a possibly empty string which identifies the source from
// which the series data was gathered. Data for a series may be gathered from
// multiple sources, which are stored separately but are sorted adjacently for
// efficient access.
//
// The resolution refers to the sample duration at which data is stored.
// Cockroach supports a fixed set of named resolutions, which are stored in the
// Resolution enumeration. This value is encoded as a VarInt in the key.
//
// Cockroach divides all data for a series into contiguous "time slots" of
// uniform length based on the "key duration" of the Resolution. For each
// series/source pair, there will be one key per slot. Slot 0 begins at unix
// epoch; the slot for a specific timestamp is found by truncating the
// timestamp to an exact multiple of the key duration, and then dividing it by
// the key duration:
//
// 		slot := (timestamp / keyDuration) // integer division

// MakeDataKey creates a time series data key for the given series name, source,
// Resolution and timestamp. The timestamp is expressed in nanoseconds since the
// epoch; it will be truncated to an exact multiple of the supplied
// Resolution's KeyDuration.
func MakeDataKey(name string, source string, r Resolution, timestamp int64) roachpb.Key {
	k := makeDataKeySeriesPrefix(name, r)

	// Normalize timestamp into a timeslot before recording.
	timeslot := timestamp / r.SlabDuration()
	k = encoding.EncodeVarintAscending(k, timeslot)
	k = append(k, source...)
	return k
}

// makeDataKeySeriesPrefix creates a key prefix for a time series at a specific
// resolution.
func makeDataKeySeriesPrefix(name string, r Resolution) roachpb.Key {
	k := append(roachpb.Key(nil), keys.TimeseriesPrefix...)
	k = encoding.EncodeBytesAscending(k, []byte(name))
	k = encoding.EncodeVarintAscending(k, int64(r))
	return k
}

// DecodeDataKey decodes a time series key into its components.
func DecodeDataKey(key roachpb.Key) (string, string, Resolution, int64, error) {
	// Detect and remove prefix.
	remainder := key
	if !bytes.HasPrefix(key, keys.TimeseriesPrefix) {
		return "", "", 0, 0, errors.Errorf("malformed time series data key %v: improper prefix", key)
	}
	remainder = remainder[len(keys.TimeseriesPrefix):]

	return decodeDataKeySuffix(remainder)
}

// decodeDataKeySuffix decodes a time series key into its components.
func decodeDataKeySuffix(key roachpb.Key) (string, string, Resolution, int64, error) {
	// Decode series name.
	remainder, name, err := encoding.DecodeBytesAscending(key, nil)
	if err != nil {
		return "", "", 0, 0, err
	}
	// Decode resolution.
	remainder, resolutionInt, err := encoding.DecodeVarintAscending(remainder)
	if err != nil {
		return "", "", 0, 0, err
	}
	resolution := Resolution(resolutionInt)
	// Decode timestamp.
	remainder, timeslot, err := encoding.DecodeVarintAscending(remainder)
	if err != nil {
		return "", "", 0, 0, err
	}
	timestamp := timeslot * resolution.SlabDuration()
	// The remaining bytes are the source.
	source := remainder

	return string(name), string(source), resolution, timestamp, nil
}

func prettyPrintKey(key roachpb.Key) string {
	name, source, resolution, timestamp, err := decodeDataKeySuffix(key)
	if err != nil {
		// Not a valid timeseries key, fall back to doing the best we can to display
		// it.
		return encoding.PrettyPrintValue(nil /* dirs */, key, "/")
	}
	return fmt.Sprintf("/%s/%s/%s/%s", name, source, resolution,
		timeutil.Unix(0, timestamp).Format(time.RFC3339Nano))
}

func init() {
	keys.PrettyPrintTimeseriesKey = prettyPrintKey
}
