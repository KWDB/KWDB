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

package infos_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/infos"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

func TestNodeInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clusterID := uuid.MakeV4()
	nodeInfo := &infos.NodeInfo{
		NodeID:    1,
		LastUp:    1234567890,
		ClusterID: clusterID,
	}

	// Test String() method
	str := nodeInfo.String()
	if str == "" {
		t.Error("NodeInfo.String() should return non-empty string")
	}
}

func TestDatabaseInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test with all fields
	dbInfo := &infos.DatabaseInfo{
		DatabaseName:        "test_db",
		NewName:             "new_test_db",
		Statement:           "CREATE DATABASE test_db",
		User:                "root",
		DroppedTableObjects: []string{"table1", "table2"},
	}

	str := dbInfo.String()
	if str == "" {
		t.Error("DatabaseInfo.String() should return non-empty string")
	}

	// Test without optional fields
	dbInfo2 := &infos.DatabaseInfo{
		DatabaseName: "test_db",
		Statement:    "CREATE DATABASE test_db",
		User:         "root",
	}

	str2 := dbInfo2.String()
	if str2 == "" {
		t.Error("DatabaseInfo.String() should return non-empty string")
	}
}

func TestSchemaInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	schemaInfo := &infos.SchemaInfo{
		SchemaName: "test_schema",
		Statement:  "CREATE SCHEMA test_schema",
		User:       "root",
	}

	str := schemaInfo.String()
	if str == "" {
		t.Error("SchemaInfo.String() should return non-empty string")
	}
}

func TestTableInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tableInfo := &infos.TableInfo{
		TableName: "test_table",
		Statement: "CREATE TABLE test_table (id INT)",
		User:      "root",
	}

	str := tableInfo.String()
	if str == "" {
		t.Error("TableInfo.String() should return non-empty string")
	}
}

func TestCommentInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	comment := "This is a comment"
	commentInfo := &infos.CommentInfo{
		Name:      "test_object",
		Statement: "COMMENT ON TABLE test_table IS 'This is a comment'",
		User:      "root",
		Comment:   &comment,
	}

	str := commentInfo.String()
	if str == "" {
		t.Error("CommentInfo.String() should return non-empty string")
	}
}

func TestIndexInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	indexInfo := &infos.IndexInfo{
		TableName: "test_table",
		IndexName: "idx_id",
		Statement: "CREATE INDEX idx_id ON test_table (id)",
		User:      "root",
	}

	str := indexInfo.String()
	if str == "" {
		t.Error("IndexInfo.String() should return non-empty string")
	}
}

func TestViewInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	viewInfo := &infos.ViewInfo{
		ViewName:  "test_view",
		Statement: "CREATE VIEW test_view AS SELECT * FROM test_table",
		User:      "root",
	}

	str := viewInfo.String()
	if str == "" {
		t.Error("ViewInfo.String() should return non-empty string")
	}
}

func TestSequenceInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sequenceInfo := &infos.SequenceInfo{
		SequenceName: "test_seq",
		Statement:    "CREATE SEQUENCE test_seq",
		User:         "root",
	}

	str := sequenceInfo.String()
	if str == "" {
		t.Error("SequenceInfo.String() should return non-empty string")
	}
}

func TestFunctionInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	functionInfo := &infos.FunctionInfo{
		FunctionName: "test_func",
		Statement:    "CREATE FUNCTION test_func() RETURNS INT AS 'SELECT 1'",
		User:         "root",
	}

	str := functionInfo.String()
	if str == "" {
		t.Error("FunctionInfo.String() should return non-empty string")
	}
}

func TestTriggerInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	triggerInfo := &infos.TriggerInfo{
		TriggerName: "test_trigger",
		TableName:   "test_table",
		Statement:   "CREATE TRIGGER test_trigger AFTER INSERT ON test_table FOR EACH ROW EXECUTE PROCEDURE test_func()",
		User:        "root",
	}

	str := triggerInfo.String()
	if str == "" {
		t.Error("TriggerInfo.String() should return non-empty string")
	}
}

func TestJobInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	jobInfo := &infos.JobInfo{
		Statement: "BACKUP DATABASE test_db TO 's3://bucket'",
		User:      "root",
	}

	str := jobInfo.String()
	if str == "" {
		t.Error("JobInfo.String() should return non-empty string")
	}
}

func TestUserInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test with username
	userInfo := &infos.UserInfo{
		Username:  "test_user",
		Statement: "CREATE USER test_user",
		User:      "root",
	}

	str := userInfo.String()
	if str == "" {
		t.Error("UserInfo.String() should return non-empty string")
	}

	// Test with role name
	userInfo2 := &infos.UserInfo{
		RoleName:  "test_role",
		Statement: "CREATE ROLE test_role",
		User:      "root",
	}

	str2 := userInfo2.String()
	if str2 == "" {
		t.Error("UserInfo.String() should return non-empty string")
	}
}

func TestPrivilegeInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	privilegeInfo := &infos.PrivilegeInfo{
		Statement: "GRANT SELECT ON test_table TO test_user",
		User:      "root",
	}

	str := privilegeInfo.String()
	if str == "" {
		t.Error("PrivilegeInfo.String() should return non-empty string")
	}
}

func TestQueryInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	queryInfo := &infos.QueryInfo{
		Statement: "SELECT * FROM test_table",
		User:      "test_user",
	}

	str := queryInfo.String()
	if str == "" {
		t.Error("QueryInfo.String() should return non-empty string")
	}
}

func TestDumpLoadInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dumpLoadInfo := &infos.DumpLoadInfo{
		Statement: "IMPORT INTO test_table FROM 's3://bucket/data.csv'",
		User:      "root",
	}

	str := dumpLoadInfo.String()
	if str == "" {
		t.Error("DumpLoadInfo.String() should return non-empty string")
	}
}

func TestFlashBackInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	flashBackInfo := &infos.FlashBackInfo{
		Statement: "FLASHBACK TABLE test_table TO AS OF SYSTEM TIME '2023-01-01 00:00:00'",
		User:      "root",
	}

	str := flashBackInfo.String()
	if str == "" {
		t.Error("FlashBackInfo.String() should return non-empty string")
	}
}

func TestCDCInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cdcInfo := &infos.CDCInfo{
		Statement: "CREATE CHANGEFEED FOR test_table INTO 'kafka://localhost:9092'",
		User:      "root",
	}

	str := cdcInfo.String()
	if str == "" {
		t.Error("CDCInfo.String() should return non-empty string")
	}
}

func TestTransactionInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	transactionInfo := &infos.TransactionInfo{
		Statement: "BEGIN TRANSACTION",
		User:      "test_user",
	}

	str := transactionInfo.String()
	if str == "" {
		t.Error("TransactionInfo.String() should return non-empty string")
	}
}

func TestStatisticsInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	statisticsInfo := &infos.StatisticsInfo{
		TableName: "test_table",
		Statement: "ANALYZE test_table",
		User:      "root",
	}

	str := statisticsInfo.String()
	if str == "" {
		t.Error("StatisticsInfo.String() should return non-empty string")
	}
}

func TestScheduledInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	scheduledInfo := &infos.ScheduledInfo{
		ScheduledName: "test_schedule",
		ScheduledID:   123,
		Statement:     "CREATE SCHEDULE test_schedule CRON '0 0 * * *' 'BACKUP DATABASE test_db TO 's3://bucket'",
		User:          "root",
	}

	str := scheduledInfo.String()
	if str == "" {
		t.Error("ScheduledInfo.String() should return non-empty string")
	}
}

func TestSessionInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sessionInfo := &infos.SessionInfo{
		Statement: "SET session_timezone = 'UTC'",
		User:      "test_user",
	}

	str := sessionInfo.String()
	if str == "" {
		t.Error("SessionInfo.String() should return non-empty string")
	}
}

func TestAuditInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	auditInfo := &infos.AuditInfo{
		Statement: "CREATE AUDIT test_audit ON TABLE FOR ALL TO root",
		User:      "root",
	}

	str := auditInfo.String()
	if str == "" {
		t.Error("AuditInfo.String() should return non-empty string")
	}
}

func TestSetClusterSettingInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	setClusterSettingInfo := &infos.SetClusterSettingInfo{
		SettingName: "audit.enabled",
		Value:       "true",
		Statement:   "SET CLUSTER SETTING audit.enabled = true",
		User:        "root",
	}

	str := setClusterSettingInfo.String()
	if str == "" {
		t.Error("SetClusterSettingInfo.String() should return non-empty string")
	}
}

func TestSetZoneConfigInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	setZoneConfigInfo := &infos.SetZoneConfigInfo{
		Target:    "DATABASE test_db",
		Statement: "ALTER DATABASE test_db CONFIGURE ZONE USING num_replicas = 3",
		User:      "root",
	}

	str := setZoneConfigInfo.String()
	if str == "" {
		t.Error("SetZoneConfigInfo.String() should return non-empty string")
	}
}
