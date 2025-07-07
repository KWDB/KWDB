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

package tests_test

import (
	"context"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestInitialKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const keysPerDesc = 2
	const nonDescKeys = 9

	ms := sqlbase.MakeMetadataSchema(zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef())
	kv, _ /* splits */ := ms.GetInitialValues(clusterversion.TestingClusterVersion)
	expected := nonDescKeys + keysPerDesc*ms.SystemDescriptorCount()
	if actual := len(kv); actual != expected {
		t.Fatalf("Wrong number of initial sql kv pairs: %d, wanted %d", actual, expected)
	}

	// Add an additional table.
	sqlbase.SystemAllowedPrivileges[keys.MaxReservedDescID] = privilege.List{privilege.ALL}
	desc, err := sql.CreateTestTableDescriptor(
		context.TODO(),
		keys.SystemDatabaseID,
		keys.MaxReservedDescID,
		"CREATE TABLE system.x (val INT8 PRIMARY KEY)",
		sqlbase.NewDefaultPrivilegeDescriptor(),
	)
	if err != nil {
		t.Fatal(err)
	}
	ms.AddDescriptor(keys.SystemDatabaseID, &desc)
	kv, _ /* splits */ = ms.GetInitialValues(clusterversion.TestingClusterVersion)
	expected = nonDescKeys + keysPerDesc*ms.SystemDescriptorCount()
	if actual := len(kv); actual != expected {
		t.Fatalf("Wrong number of initial sql kv pairs: %d, wanted %d", actual, expected)
	}

	// Verify that IDGenerator value is correct.
	found := false
	var idgenkv roachpb.KeyValue
	for _, v := range kv {
		if v.Key.Equal(keys.DescIDGenerator) {
			idgenkv = v
			found = true
			break
		}
	}

	if !found {
		t.Fatal("Could not find descriptor ID generator in initial key set")
	}
	// Expect 2 non-reserved IDs to have been allocated.
	i, err := idgenkv.Value.GetInt()
	if err != nil {
		t.Fatal(err)
	}
	if a, e := i, int64(keys.MinUserDescID); a != e {
		t.Fatalf("Expected next descriptor ID to be %d, was %d", e, a)
	}
}

// TestSystemTableLiterals compares the result of evaluating the `CREATE TABLE`
// statement strings that describe each system table with the TableDescriptor
// literals that are actually used at runtime. This ensures we can use the hand-
// written literals instead of having to evaluate the `CREATE TABLE` statements
// before initialization and with limited SQL machinery bootstraped, while still
// confident that the result is the same as if `CREATE TABLE` had been run.
//
// This test may also be useful when writing a new system table:
// adding the new schema along with a trivial, empty TableDescriptor literal
// will print the expected proto which can then be used to replace the empty
// one (though pruning the explicit zero values may make it more readable).
func TestSystemTableLiterals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testcase struct {
		id     sqlbase.ID
		schema string
		pkg    sqlbase.TableDescriptor
	}

	for _, test := range []testcase{
		{keys.NamespaceTableID, sqlbase.NamespaceTableSchema, sqlbase.NamespaceTable},
		{keys.DescriptorTableID, sqlbase.DescriptorTableSchema, sqlbase.DescriptorTable},
		{keys.UsersTableID, sqlbase.UsersTableSchema, sqlbase.UsersTable},
		{keys.ZonesTableID, sqlbase.ZonesTableSchema, sqlbase.ZonesTable},
		{keys.LeaseTableID, sqlbase.LeaseTableSchema, sqlbase.LeaseTable},
		{keys.AuditsTableID, sqlbase.AuditsTableSchema, sqlbase.AuditsTable},
		//{keys.EventLogTableID, sqlbase.EventLogTableSchema, sqlbase.EventLogTable},
		{keys.RangeEventTableID, sqlbase.RangeEventTableSchema, sqlbase.RangeEventTable},
		{keys.UITableID, sqlbase.UITableSchema, sqlbase.UITable},
		{keys.JobsTableID, sqlbase.JobsTableSchema, sqlbase.JobsTable},
		{keys.SettingsTableID, sqlbase.SettingsTableSchema, sqlbase.SettingsTable},
		{keys.WebSessionsTableID, sqlbase.WebSessionsTableSchema, sqlbase.WebSessionsTable},
		{keys.TableStatisticsTableID, sqlbase.TableStatisticsTableSchema, sqlbase.TableStatisticsTable},
		{keys.LocationsTableID, sqlbase.LocationsTableSchema, sqlbase.LocationsTable},
		{keys.RoleMembersTableID, sqlbase.RoleMembersTableSchema, sqlbase.RoleMembersTable},
		{keys.CommentsTableID, sqlbase.CommentsTableSchema, sqlbase.CommentsTable},
		{keys.ProtectedTimestampsMetaTableID, sqlbase.ProtectedTimestampsMetaTableSchema, sqlbase.ProtectedTimestampsMetaTable},
		{keys.ProtectedTimestampsRecordsTableID, sqlbase.ProtectedTimestampsRecordsTableSchema, sqlbase.ProtectedTimestampsRecordsTable},
		{keys.RoleOptionsTableID, sqlbase.RoleOptionsTableSchema, sqlbase.RoleOptionsTable},
		{keys.StatementBundleChunksTableID, sqlbase.StatementBundleChunksTableSchema, sqlbase.StatementBundleChunksTable},
		{keys.StatementDiagnosticsRequestsTableID, sqlbase.StatementDiagnosticsRequestsTableSchema, sqlbase.StatementDiagnosticsRequestsTable},
		{keys.StatementDiagnosticsTableID, sqlbase.StatementDiagnosticsTableSchema, sqlbase.StatementDiagnosticsTable},
		{keys.ScheduledJobsTableID, sqlbase.ScheduledJobsTableSchema, sqlbase.ScheduledJobsTable},
		{keys.UDRTableID, sqlbase.UDRTableSchema, sqlbase.UDRTable},
	} {
		privs := *test.pkg.Privileges
		gen, err := sql.CreateTestTableDescriptor(
			context.TODO(),
			keys.SystemDatabaseID,
			test.id,
			test.schema,
			&privs,
		)
		if err != nil {
			t.Fatalf("test: %+v, err: %v", test, err)
		}
		require.NoError(t, gen.ValidateTable())

		if !proto.Equal(&test.pkg, &gen) {
			diff := strings.Join(pretty.Diff(&test.pkg, &gen), "\n")
			t.Errorf("%s table descriptor generated from CREATE TABLE statement does not match "+
				"hardcoded table descriptor:\n%s", test.pkg.Name, diff)
		}
	}
}
