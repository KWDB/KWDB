// Copyright 2018 The Cockroach Authors.
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

package main

func registerTests(r *testRegistry) {
	// Helpful shell pipeline to generate the list below:
	//
	// grep -h -E 'func register[^(]+\(.*testRegistry\) {' pkg/cmd/roachtest/*.go | grep -E -o 'register[^(]+' | grep -E -v '^register(Tests|Benchmarks)$' | grep -v '^\w*Bench$' | sort -f | awk '{printf "\t%s(r)\n", $0}'

	registerAcceptance(r)
	registerActiveRecord(r)
	registerAutoUpgrade(r)
	registerClockJumpTests(r)
	registerClockMonotonicTests(r)
	registerDiskStalledDetection(r)
	registerDjango(r)
	registerElectionAfterRestart(r)
	registerEncryption(r)
	registerFlowable(r)
	registerFollowerReads(r)
	registerGopg(r)
	registerGossip(r)
	registerHibernate(r)
	registerInconsistency(r)
	registerJepsen(r)
	registerLibPQ(r)
	registerNamespaceUpgrade(r)
	registerPebble(r)
	registerPgjdbc(r)
	registerPgx(r)
	registerPsycopg(r)
	registerQuitAllNodes(r)
	registerQuitTransfersLeases(r)
	registerRestore(r)
	registerScaleData(r)
	registerSecondaryIndexesMultiVersionCluster(r)
	registerSQLAlchemy(r)
	registerSQLSmith(r)
	registerSyncTest(r)
	registerSysbench(r)
	registerTypeORM(r)
}

func registerBenchmarks(r *testRegistry) {
}
