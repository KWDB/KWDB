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

package gossip

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestGossipStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ss := ServerStatus{
		ConnStatus: []ConnStatus{
			{NodeID: 1, Address: "localhost:1234", AgeNanos: 17e9},
			{NodeID: 4, Address: "localhost:4567", AgeNanos: 18e9},
		},
		MaxConns: 3,
		MetricSnap: MetricSnap{
			BytesReceived: 1000,
			BytesSent:     2000,
			InfosReceived: 10,
			InfosSent:     20,
			ConnsRefused:  17,
		},
	}
	if exp, act := `gossip server (2/3 cur/max conns, infos 20/10 sent/received, bytes 2000B/1000B sent/received, refused 17 conns)
  1: localhost:1234 (17s)
  4: localhost:4567 (18s)
`, ss.String(); exp != act {
		t.Errorf("expected:\n%q\ngot:\n%q", exp, act)
	}

	cs := ClientStatus{
		ConnStatus: []OutgoingConnStatus{
			{
				ConnStatus: ss.ConnStatus[0],
				MetricSnap: MetricSnap{BytesReceived: 77, BytesSent: 88, InfosReceived: 11, InfosSent: 22},
			},
		},
		MaxConns: 3,
	}
	if exp, act := `gossip client (1/3 cur/max conns)
  1: localhost:1234 (17s: infos 22/11 sent/received, bytes 88B/77B sent/received)
`, cs.String(); exp != act {
		t.Errorf("expected:\n%q\ngot:\n%q", exp, act)
	}

}
