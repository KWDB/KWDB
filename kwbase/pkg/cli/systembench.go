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
	"strconv"

	"gitee.com/kwbasedb/kwbase/pkg/cli/systembench"
	"github.com/spf13/cobra"
)

// An seqWriteBench command runs I/O benchmarks on kwbase.
var seqWriteBench = &cobra.Command{
	Use:   "seqwrite",
	Short: "Runs the sequential disk write benchmark.",
	Long: `
Runs the sequential disk write benchmark.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(RunSeqWriteBench),
}

// A cpuBench command runs CPU benchmarks on kwbase.
var cpuBench = &cobra.Command{
	Use:   "cpu",
	Short: "Runs the prime finding cpu benchmark.",
	Long: `
Runs the prime finding cpu benchmark.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(RunCPUBench),
}

// A networkBench command runs network benchmarks on kwbase.
var networkBench = &cobra.Command{
	Use:   "network",
	Short: "Runs network benchmarks.",
	Long: `
Runs network benchmarks..
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(RunNetworkBench),
}

// RunSeqWriteBench runs a sequential write I/O benchmark.
func RunSeqWriteBench(cmd *cobra.Command, args []string) error {
	iOOpts := systembench.DiskOptions{
		Concurrency: systemBenchCtx.concurrency,
		Duration:    systemBenchCtx.duration,
		Dir:         systemBenchCtx.tempDir,

		Type:         systembench.SeqWriteTest,
		WriteSize:    systemBenchCtx.writeSize,
		SyncInterval: systemBenchCtx.syncInterval,
	}
	return systembench.Run(iOOpts)
}

// RunCPUBench runs the prime finding cpu benchmark.
func RunCPUBench(cmd *cobra.Command, args []string) error {
	cpuOptions := systembench.CPUOptions{
		Concurrency: systemBenchCtx.concurrency,
		Duration:    systemBenchCtx.duration,

		Type: systembench.CPUPrimeTest,
	}
	return systembench.RunCPU(cpuOptions)
}

// RunNetworkBench runs the network benchmark.
func RunNetworkBench(cmd *cobra.Command, args []string) error {
	if networkBenchCtx.server {
		serverOptions := systembench.ServerOptions{
			Port: strconv.Itoa(networkBenchCtx.port),
		}
		return systembench.RunServer(serverOptions)
	}

	clientOptions := systembench.ClientOptions{
		Concurrency: systemBenchCtx.concurrency,
		Duration:    systemBenchCtx.duration,

		Addresses:   networkBenchCtx.addresses,
		LatencyMode: networkBenchCtx.latency,
	}
	return systembench.RunClient(clientOptions)
}

var systemBenchCmds = []*cobra.Command{
	seqWriteBench,
	cpuBench,
	networkBench,
}

var systemBenchCmd = &cobra.Command{
	Use:   "systembench [command]",
	Short: "Run systembench",
	Long: `
Run kwbase hardware benchmarks, for options use --help.`,
	RunE: usageAndErr,
}

func init() {
	systemBenchCmd.AddCommand(systemBenchCmds...)
}
