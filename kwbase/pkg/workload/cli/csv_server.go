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

package cli

import (
	"fmt"
	"net/http"
	"net/http/pprof"

	"gitee.com/kwbasedb/kwbase/pkg/workload"
	"github.com/spf13/cobra"
)

var csvServerCmd = SetCmdDefaults(&cobra.Command{
	Use:   `csv-server`,
	Short: `serves csv table data through an HTTP interface`,
	Args:  cobra.NoArgs,
	RunE:  runCSVServer,
})

var port *int

func init() {
	port = csvServerCmd.Flags().Int(`port`, 8081, `The port to bind to`)
	AddSubCmd(func(_ bool) *cobra.Command { return csvServerCmd })
}

func runCSVServer(_ *cobra.Command, _ []string) error {
	mux := workload.CSVMux(workload.Registered())

	// Cribbed straight from pprof's `init()` method. See:
	// https://golang.org/src/net/http/pprof/pprof.go
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	s := &http.Server{
		Addr:    fmt.Sprintf(`:%d`, *port),
		Handler: mux,
	}
	fmt.Printf("Listening on %s\n", s.Addr)
	return s.ListenAndServe()
}
