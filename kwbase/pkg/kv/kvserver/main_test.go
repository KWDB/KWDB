// Copyright 2015 The Cockroach Authors.
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

package kvserver_test

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/security/securitytest"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

//go:generate ../../util/leaktest/add-leaktest.sh *_test.go

func init() {
	security.SetAssetLoader(securitytest.EmbeddedAssets)
}

var verifyBelowRaftProtos bool

func TestMain(m *testing.M) {
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)

	// Create a set of all protos we believe to be marshaled downstream of raft.
	// After the tests are run, we'll subtract the encountered protos from this
	// set.
	notBelowRaftProtos := make(map[reflect.Type]struct{}, len(belowRaftGoldenProtos))
	for typ := range belowRaftGoldenProtos {
		notBelowRaftProtos[typ] = struct{}{}
	}

	// Before running the tests, enable instrumentation that tracks protos which
	// are marshaled downstream of raft.
	stopTrackingAndGetTypes := kvserver.TrackRaftProtos()

	code := m.Run()

	// Only do this verification if the associated test was run. Without this
	// condition, the verification here would spuriously fail when running a
	// small subset of tests e.g. as we often do with `stress`.
	if verifyBelowRaftProtos {
		failed := false
		// Retrieve all the observed downstream-of-raft protos and confirm that they
		// are all present in our expected set.
		for _, typ := range stopTrackingAndGetTypes() {
			if _, ok := belowRaftGoldenProtos[typ]; ok {
				delete(notBelowRaftProtos, typ)
			} else {
				failed = true
				fmt.Printf("%s: missing fixture! Please adjust belowRaftGoldenProtos if necessary\n", typ)
			}
		}

		// Confirm that our expected set is now empty; we don't want to cement any
		// protos needlessly.
		// Two exceptions: these are sometimes observed below raft, sometimes not.
		// It supposedly depends on whether any test server runs for long enough to
		// write internal time series.
		delete(notBelowRaftProtos, reflect.TypeOf(&roachpb.InternalTimeSeriesData{}))
		delete(notBelowRaftProtos, reflect.TypeOf(&enginepb.MVCCMetadataSubsetForMergeSerialization{}))
		for typ := range notBelowRaftProtos {
			failed = true
			fmt.Printf("%s: not observed below raft!\n", typ)
		}

		// Make sure our error messages make it out.
		if failed && code == 0 {
			code = 1
		}
	}

	os.Exit(code)
}
