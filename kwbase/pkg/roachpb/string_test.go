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

package roachpb_test

import (
	"fmt"
	"testing"

	// Hook up the pretty printer.
	_ "gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

func TestTransactionString(t *testing.T) {
	txnID, err := uuid.FromBytes([]byte("ת\x0f^\xe4-Fؽ\xf7\x16\xe4\xf9\xbe^\xbe"))
	if err != nil {
		t.Fatal(err)
	}
	txn := roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			Key:            roachpb.Key("foo"),
			ID:             txnID,
			Epoch:          2,
			WriteTimestamp: hlc.Timestamp{WallTime: 20, Logical: 21},
			MinTimestamp:   hlc.Timestamp{WallTime: 10, Logical: 11},
			Priority:       957356782,
			Sequence:       15,
		},
		Name:          "name",
		Status:        roachpb.COMMITTED,
		LastHeartbeat: hlc.Timestamp{WallTime: 10, Logical: 11},
		ReadTimestamp: hlc.Timestamp{WallTime: 30, Logical: 31},
		MaxTimestamp:  hlc.Timestamp{WallTime: 40, Logical: 41},
	}
	expStr := `"name" meta={id=d7aa0f5e key="foo" pri=44.58039917 epo=2 ts=0.000000020,21 min=0.000000010,11 seq=15}` +
		` lock=true stat=COMMITTED rts=0.000000030,31 wto=false max=0.000000040,41`

	if str := txn.String(); str != expStr {
		t.Errorf(
			"expected txn: %s\n"+
				"got:          %s",
			expStr, str)
	}
}

func TestBatchRequestString(t *testing.T) {
	br := roachpb.BatchRequest{}
	txn := roachpb.MakeTransaction(
		"test",
		nil, /* baseKey */
		roachpb.NormalUserPriority,
		hlc.Timestamp{}, // now
		0,               // maxOffsetNs
	)
	br.Txn = &txn
	for i := 0; i < 100; i++ {
		var ru roachpb.RequestUnion
		ru.MustSetInner(&roachpb.GetRequest{})
		br.Requests = append(br.Requests, ru)
	}
	var ru roachpb.RequestUnion
	ru.MustSetInner(&roachpb.EndTxnRequest{})
	br.Requests = append(br.Requests, ru)

	e := fmt.Sprintf(`[txn: %s], Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), ... 76 skipped ..., Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), EndTxn(commit:false tsflex:false) [/Min] `,
		br.Txn.Short())
	if e != br.String() {
		t.Fatalf("e = %s\nv = %s", e, br.String())
	}
}
