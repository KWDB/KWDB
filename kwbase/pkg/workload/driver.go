// Copyright 2017 The Cockroach Authors.
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

package workload

import (
	gosql "database/sql"
	"database/sql/driver"
	"strings"
	"sync/atomic"

	"github.com/lib/pq"
)

// kwbaseDriver is a wrapper around lib/pq which provides for round-robin
// load balancing amongst a list of URLs. The name passed to Open() is a space
// separated list of "postgres" URLs to connect to.
//
// Note that the round-robin load balancing can lead to imbalances in
// connections across the cluster. This is currently only suitable for
// simplistic setups where nodes in the cluster are stable and do not go up and
// down.
type kwbaseDriver struct {
	idx uint32
}

func (d *kwbaseDriver) Open(name string) (driver.Conn, error) {
	urls := strings.Split(name, " ")
	i := atomic.AddUint32(&d.idx, 1) - 1
	return pq.Open(urls[i%uint32(len(urls))])
}

func init() {
	gosql.Register("kwbase", &kwbaseDriver{})
}
