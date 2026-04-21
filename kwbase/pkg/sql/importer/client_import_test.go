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

package importer_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// TestDropDatabaseCascadeDuringImportsFails ensures that dropping a database
// while an IMPORT is ongoing fails with an error. This is critical because
// otherwise we may end up with orphaned table descriptors. See #48589 for
// more details.
func TestDropDatabaseCascadeDuringImportsFails(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	args := base.TestClusterArgs{}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)

	tc.WaitForNodeLiveness(t)
	require.NoError(t, tc.WaitForFullReplication())

	db := tc.ServerConn(0)
	runner := sqlutils.MakeSQLRunner(db)

	// Use some names that need quoting to ensure that the error quoting is correct.
	const dbName, tableName = `"fooBarBaz"`, `"foo bar"`
	runner.Exec(t, `CREATE DATABASE `+dbName)

	mkServer := func(method string, handler func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == method {
				handler(w, r)
			}
		}))
	}

	// Let's start an import into this table of ours.
	allowResponse := make(chan struct{})
	var gotRequestOnce sync.Once
	gotRequest := make(chan struct{})
	srv := mkServer("GET", func(w http.ResponseWriter, r *http.Request) {
		gotRequestOnce.Do(func() { close(gotRequest) })
		select {
		case <-allowResponse:
		case <-ctx.Done(): // Deal with test failures.
		}
		_, _ = w.Write([]byte("1,asdfasdfasdfasdf"))
	})
	defer srv.Close()

	importErrCh := make(chan error, 1)
	go func() {
		_, err := db.Exec(`IMPORT TABLE `+dbName+"."+tableName+
			` (k INT, v STRING) CSV DATA ($1)`, srv.URL)
		importErrCh <- err
	}()
	select {
	case <-gotRequest:
	case err := <-importErrCh:
		t.Fatalf("err %v", err)
	}

	_, err := db.Exec(`DROP DATABASE "fooBarBaz" CASCADE`)
	require.Regexp(t, `cannot drop a database with OFFLINE tables, ensure `+
		dbName+`\.public\.`+tableName+` is dropped or made public before dropping`+
		` database `+dbName, err)
	pgErr := new(pq.Error)
	require.True(t, errors.As(err, &pgErr))
	require.Equal(t, pgcode.ObjectNotInPrerequisiteState, string(pgErr.Code))

	close(allowResponse)
	require.NoError(t, <-importErrCh)
	runner.Exec(t, `DROP DATABASE `+dbName+` CASCADE`)
}
