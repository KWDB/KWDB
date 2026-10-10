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

package enginepb_test

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
)

func TestFormatMVCCMetadata(t *testing.T) {
	txnID, err := uuid.FromBytes([]byte("ת\x0f^\xe4-Fؽ\xf7\x16\xe4\xf9\xbe^\xbe"))
	if err != nil {
		t.Fatal(err)
	}
	ts := hlc.Timestamp{Logical: 1}
	tmeta := &enginepb.TxnMeta{
		Key:            roachpb.Key("a"),
		ID:             txnID,
		Epoch:          1,
		WriteTimestamp: ts,
		MinTimestamp:   ts,
	}
	val1 := roachpb.Value{}
	val1.SetString("foo")
	val2 := roachpb.Value{}
	val2.SetString("bar")
	val3 := roachpb.Value{}
	val3.SetString("baz")
	meta := &enginepb.MVCCMetadata{
		Txn:       tmeta,
		Timestamp: hlc.LegacyTimestamp(ts),
		KeyBytes:  123,
		ValBytes:  456,
		RawBytes:  val1.RawBytes,
		IntentHistory: []enginepb.MVCCMetadata_SequencedIntent{
			{Sequence: 11, Value: val2.RawBytes},
			{Sequence: 22, Value: val3.RawBytes},
		},
	}

	const expStr = `txn={id=d7aa0f5e key="a" pri=0.00000000 epo=1 ts=0,1 min=0,1 seq=0}` +
		` ts=0,1 del=false klen=123 vlen=456 rawlen=8 nih=2`

	if str := meta.String(); str != expStr {
		t.Errorf(
			"expected meta: %s\n"+
				"got:          %s",
			expStr, str)
	}

	const expV = `txn={id=d7aa0f5e key="a" pri=0.00000000 epo=1 ts=0,1 min=0,1 seq=0}` +
		` ts=0,1 del=false klen=123 vlen=456 raw=/BYTES/foo ih={{11 /BYTES/bar}{22 /BYTES/baz}}`

	if str := fmt.Sprintf("%+v", meta); str != expV {
		t.Errorf(
			"expected meta: %s\n"+
				"got:           %s",
			expV, str)
	}
}

func TestTxnSeqIsIgnored(t *testing.T) {
	type s = enginepb.TxnSeq
	type r = enginepb.IgnoredSeqNumRange
	mr := func(a, b s) r {
		return r{Start: a, End: b}
	}

	testData := []struct {
		list       []r
		ignored    []s
		notIgnored []s
	}{
		{[]r{}, nil, []s{0, 1, 10}},
		{[]r{mr(1, 1)}, []s{1}, []s{0, 2, 10}},
		{[]r{mr(1, 1), mr(2, 3)}, []s{1, 2, 3}, []s{0, 4, 10}},
		{[]r{mr(1, 2), mr(4, 8), mr(9, 10)}, []s{1, 2, 5, 10}, []s{0, 3, 11}},
		{[]r{mr(0, 10)}, []s{0, 1, 2, 3, 10}, []s{11, 100}},
	}

	for _, tc := range testData {
		for _, ign := range tc.ignored {
			assert.True(t, enginepb.TxnSeqIsIgnored(ign, tc.list))
		}
		for _, notIgn := range tc.notIgnored {
			assert.False(t, enginepb.TxnSeqIsIgnored(notIgn, tc.list))
		}
	}
}
