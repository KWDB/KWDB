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

package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/stateloader"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDebugCheckStore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	// Number of nodes. Increasing this will make the test flaky as written
	// because it relies on finding r1 on n1.
	const n = 3

	clusterArgs := base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{},
	}
	var storePaths []string
	for i := 0; i < n; i++ {
		args := base.TestServerArgs{}
		args.ScanMaxIdleTime = time.Millisecond
		args.ScanMaxIdleTime = time.Millisecond
		storeID := roachpb.StoreID(i + 1)
		path := filepath.Join(baseDir, fmt.Sprintf("s%d", storeID))
		storePaths = append(storePaths, path)
		args.StoreSpecs = []base.StoreSpec{{Path: path}}
		clusterArgs.ServerArgsPerNode[i] = args
	}

	// Start the cluster, wait for full replication, stop the cluster.
	func() {
		tc := testcluster.StartTestCluster(t, n, clusterArgs)
		defer tc.Stopper().Stop(ctx)
		require.NoError(t, tc.WaitForFullReplication())
	}()

	check := func(dir string) (string, error) {
		var buf strings.Builder
		err := checkStoreRangeStats(ctx, dir, func(args ...interface{}) {
			fmt.Fprintln(&buf, args...)
		})
		return buf.String(), err
	}

	// Should not error out randomly.
	for _, dir := range storePaths {
		out, err := check(dir)
		require.NoError(t, err, dir)
		require.Contains(t, out, "total stats", dir)
	}

	// Introduce a stats divergence on s1.
	func() {
		eng, err := relational.NewDefaultEngine(
			10<<20, /* 10mb */
			base.StorageConfig{
				Dir:       storePaths[0],
				MustExist: true,
			})
		require.NoError(t, err)
		defer eng.Close()
		sl := stateloader.Make(1)
		ms, err := sl.LoadMVCCStats(ctx, eng)
		require.NoError(t, err)
		ms.ContainsEstimates = 0
		ms.LiveBytes++
		require.NoError(t, sl.SetMVCCStats(ctx, eng, &ms))
	}()

	// The check should now fail on s1.
	{
		const s = "stats inconsistency"
		out, err := check(storePaths[0])
		require.Error(t, err)
		require.Contains(t, out, s)
		require.Contains(t, out, "total stats")
	}
}
