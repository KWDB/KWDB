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

package physicalplan_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
)

// makeNodeDesc creates a minimal NodeDescriptor for tests that only need a NodeID.
func makeNodeDesc(nodeID int) *roachpb.NodeDescriptor {
	return &roachpb.NodeDescriptor{NodeID: roachpb.NodeID(nodeID)}
}

// mkSpan creates a simple roachpb.Span from two strings.
func mkSpan(start, end string) roachpb.Span {
	return roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)}
}

// skipIfShort is a test helper to skip long-running tests when -short is set.
func skipIfShort(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
}
