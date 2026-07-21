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

package sql

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestTsServerSingleNode is used for unit testing of stream, pipe, and publication.
// Since currently only one TestServer can be started after launching TsEngine,
// all TsServer-related tests need to be centralized this Test.
func TestTsServerSingleNode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	path := filepath.Join(baseDir, fmt.Sprintf("test_ts_server%d", 1))
	args := base.TestServerArgs{
		StoreSpecs:    []base.StoreSpec{{Path: path}},
		CatchCoreDump: true,
	}
	s, db, _ := serverutils.StartServer(t, args)

	defer s.Stopper().Stop(ctx)

	StreamTest(t, db)
	PipeTest(t, db)
	PubTest(t, db)
}
