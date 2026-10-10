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

package relational

import (
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
)

func serializeMergeInputs(sources ...roachpb.InternalTimeSeriesData) ([][]byte, error) {
	// Wrap each proto in an inlined MVCC value, and marshal each wrapped value
	// to bytes. This is the format required by the engine.
	srcBytes := make([][]byte, 0, len(sources))
	var val roachpb.Value
	for _, src := range sources {
		if err := val.SetProto(&src); err != nil {
			return nil, err
		}
		bytes, err := protoutil.Marshal(&enginepb.MVCCMetadata{
			RawBytes: val.RawBytes,
		})
		if err != nil {
			return nil, err
		}
		srcBytes = append(srcBytes, bytes)
	}
	return srcBytes, nil
}

func deserializeMergeOutput(mergedBytes []byte) (roachpb.InternalTimeSeriesData, error) {
	// Unmarshal merged bytes and extract the time series value within.
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(mergedBytes, &meta); err != nil {
		return roachpb.InternalTimeSeriesData{}, err
	}
	mergedTS, err := MakeValue(meta).GetTimeseries()
	if err != nil {
		return roachpb.InternalTimeSeriesData{}, err
	}
	return mergedTS, nil
}

// MergeInternalTimeSeriesData exports the engine's C++ merge logic for
// InternalTimeSeriesData to higher level packages. This is intended primarily
// for consumption by high level testing of time series functionality.
// If mergeIntoNil is true, then the initial state of the merge is taken to be
// 'nil' and the first operand is merged into nil. If false, the first operand
// is taken to be the initial state of the merge.
// If usePartialMerge is true, the operands are merged together using a partial
// merge operation first, and are then merged in to the initial state. This
// can combine with mergeIntoNil: the initial state is either 'nil' or the first
// operand.
func MergeInternalTimeSeriesData(
	mergeIntoNil, usePartialMerge bool, sources ...roachpb.InternalTimeSeriesData,
) (roachpb.InternalTimeSeriesData, error) {
	// Merge every element into a nil byte slice, one at a time.
	var mergedBytes []byte
	srcBytes, err := serializeMergeInputs(sources...)
	if err != nil {
		return roachpb.InternalTimeSeriesData{}, err
	}
	if !mergeIntoNil {
		mergedBytes = srcBytes[0]
		srcBytes = srcBytes[1:]
	}
	if usePartialMerge {
		partialBytes := srcBytes[0]
		srcBytes = srcBytes[1:]
		for _, bytes := range srcBytes {
			partialBytes, err = goPartialMerge(partialBytes, bytes)
			if err != nil {
				return roachpb.InternalTimeSeriesData{}, err
			}
		}
		srcBytes = [][]byte{partialBytes}
	}
	for _, bytes := range srcBytes {
		mergedBytes, err = goMerge(mergedBytes, bytes)
		if err != nil {
			return roachpb.InternalTimeSeriesData{}, err
		}
	}
	return deserializeMergeOutput(mergedBytes)
}
