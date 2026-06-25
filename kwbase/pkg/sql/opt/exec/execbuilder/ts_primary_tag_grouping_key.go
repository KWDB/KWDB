// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT.
// See the Mulan PSL v2 for more details.

package execbuilder

import "encoding/binary"

// AppendTSPrimaryTagGroupingKeyBytes appends one primary-tag component to an
// in-memory grouping key. The key is only for priTagValMap grouping; it is not
// the payload primaryTag bytes and is not the KV primary tag key.
func AppendTSPrimaryTagGroupingKeyBytes(dst []byte, value []byte) []byte {
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(value)))
	dst = append(dst, lenBuf[:n]...)
	return append(dst, value...)
}

// AppendTSPrimaryTagGroupingKeyString appends one primary-tag component to an
// in-memory grouping key. The key is only for priTagValMap grouping; it is not
// the payload primaryTag bytes and is not the KV primary tag key.
func AppendTSPrimaryTagGroupingKeyString(dst []byte, value string) []byte {
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(value)))
	dst = append(dst, lenBuf[:n]...)
	return append(dst, value...)
}
