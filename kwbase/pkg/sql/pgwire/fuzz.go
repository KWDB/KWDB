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

//go:build gofuzz
// +build gofuzz

package pgwire

import (
	"context"
	"io"
	"io/ioutil"
	"net"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
)

func FuzzServeConn(data []byte) int {
	s := MakeServer(
		log.AmbientContext{},
		&base.Config{},
		&cluster.Settings{},
		sql.MemoryMetrics{},
		&mon.BytesMonitor{},
		time.Minute,
		&sql.ExecutorConfig{
			Settings: &cluster.Settings{},
		},
	)

	// Fake a connection using a pipe.
	srv, client := net.Pipe()
	go func() {
		// Write the fuzz data to the connection and close.
		_, _ = client.Write(data)
		_ = client.Close()
	}()
	go func() {
		// Discard all data sent from the server.
		_, _ = io.Copy(ioutil.Discard, client)
	}()
	err := s.ServeConn(context.Background(), srv)
	if err != nil {
		return 0
	}
	return 1
}
