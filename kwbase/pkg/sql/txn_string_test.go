// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sql

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestTxnTypeString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name string
		val  txnType
		want string
	}{
		{"implicitTxn", implicitTxn, "implicitTxn"},
		{"explicitTxn", explicitTxn, "explicitTxn"},
		{"invalid negative", txnType(-1), "txnType(-1)"},
		{"invalid out of range", txnType(2), "txnType(2)"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.val.String(); got != tc.want {
				t.Errorf("txnType(%d).String() = %q, want %q", tc.val, got, tc.want)
			}
		})
	}
}

func TestTxnEventString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name string
		val  txnEvent
		want string
	}{
		{"noEvent", noEvent, "noEvent"},
		{"txnStart", txnStart, "txnStart"},
		{"txnCommit", txnCommit, "txnCommit"},
		{"txnRollback", txnRollback, "txnRollback"},
		{"txnRestart", txnRestart, "txnRestart"},
		{"invalid negative", txnEvent(-1), "txnEvent(-1)"},
		{"invalid out of range", txnEvent(5), "txnEvent(5)"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.val.String(); got != tc.want {
				t.Errorf("txnEvent(%d).String() = %q, want %q", tc.val, got, tc.want)
			}
		})
	}
}
