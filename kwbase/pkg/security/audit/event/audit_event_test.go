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

package event_test

import (
	"context"
	"os"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/security/securitytest"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

func TestMain(m *testing.M) {
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}

func TestAuditEvent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// start test server
	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	//prepare audit
	createAuditStmt := "create audit stmttest ON DATABASE FOR CREATE TO root;"
	enableAuditStmt := "ALTER AUDIT stmttest enable;"
	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"

	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec("CREATE DATABASE test"); err != nil {
		t.Fatal(err)
	}
}
