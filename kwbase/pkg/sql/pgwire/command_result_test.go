// Copyright 2018 The Cockroach Authors.
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

package pgwire

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestSupportsPGWireResultsIsTrue pins the value the TS engine's pg-encode
// short circuit is gated on: a pgwire connection is the one writer that can
// forward pre-encoded PostgreSQL wire bytes to its client verbatim, so this
// must stay true. mysqlwire's commandResult pins the opposite value
// (TestSupportsPGWireResultsIsFalse) for the same reason — flipping either
// one is collateral damage to that short circuit.
func TestSupportsPGWireResultsIsTrue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r := &commandResult{}
	if !r.SupportsPGWireResults() {
		t.Error("a pgwire connection can forward PostgreSQL wire bytes")
	}
}
