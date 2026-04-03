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

package object_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target/object"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestNewDatabaseMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dbMetric := object.NewDatabaseMetric(ctx)

	if dbMetric == nil {
		t.Error("NewDatabaseMetric should return non-nil DatabaseOperationMetric")
	}
	if dbMetric.Object == nil {
		t.Error("DatabaseOperationMetric.Object should be non-nil")
	}
	if dbMetric.Object.Name != "opera_db_count" {
		t.Errorf("DatabaseOperationMetric.Object.Name = %q, want %q", dbMetric.Object.Name, "opera_db_count")
	}
	if dbMetric.Object.Level != target.StmtLevel {
		t.Errorf("DatabaseOperationMetric.Object.Level = %v, want %v", dbMetric.Object.Level, target.StmtLevel)
	}
	if dbMetric.Object.ObjMetrics == nil {
		t.Error("DatabaseOperationMetric.Object.ObjMetrics should be non-nil")
	}
}

func TestNewTableMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tableMetric := object.NewTableMetric(ctx)

	if tableMetric == nil {
		t.Error("NewTableMetric should return non-nil TableOperationMetric")
	}
	if tableMetric.Object == nil {
		t.Error("TableOperationMetric.Object should be non-nil")
	}
	if tableMetric.Object.Name != "opera_tb_count" {
		t.Errorf("TableOperationMetric.Object.Name = %q, want %q", tableMetric.Object.Name, "opera_tb_count")
	}
	if tableMetric.Object.Level != target.StmtLevel {
		t.Errorf("TableOperationMetric.Object.Level = %v, want %v", tableMetric.Object.Level, target.StmtLevel)
	}
	if tableMetric.Object.ObjMetrics == nil {
		t.Error("TableOperationMetric.Object.ObjMetrics should be non-nil")
	}
}

func TestTableOperationMetricGetAuditLevel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tableMetric := object.NewTableMetric(ctx)

	// Test operations that should return ObjectLevel
	objectLevelOps := []target.OperationType{
		target.Select,
		target.Insert,
		target.Update,
		target.Upsert,
		target.Delete,
		target.MergeInto,
	}

	for _, op := range objectLevelOps {
		if tableMetric.GetAuditLevel(op) != target.ObjectLevel {
			t.Errorf("TableOperationMetric.GetAuditLevel(%v) should return ObjectLevel", op)
		}
	}

	// Test operations that should return StmtLevel
	stmtLevelOps := []target.OperationType{
		target.Create,
		target.Alter,
		target.Drop,
		target.Truncate,
		target.Flashback,
	}

	for _, op := range stmtLevelOps {
		if tableMetric.GetAuditLevel(op) != target.StmtLevel {
			t.Errorf("TableOperationMetric.GetAuditLevel(%v) should return StmtLevel", op)
		}
	}
}

func TestNewNodeMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	nodeMetric := object.NewNodeMetric(ctx)

	if nodeMetric == nil {
		t.Error("NewNodeMetric should return non-nil NodeOperationMetric")
	}
	if nodeMetric.Object == nil {
		t.Error("NodeOperationMetric.Object should be non-nil")
	}
}

func TestNewClusterMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	clusterMetric := object.NewClusterMetric(ctx)

	if clusterMetric == nil {
		t.Error("NewClusterMetric should return non-nil ClusterOperationMetric")
	}
	if clusterMetric.Object == nil {
		t.Error("ClusterOperationMetric.Object should be non-nil")
	}
}

func TestNewClusterSettingMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	clusterSettingMetric := object.NewClusterSettingMetric(ctx)

	if clusterSettingMetric == nil {
		t.Error("NewClusterSettingMetric should return non-nil ClusterSettingOperationMetric")
	}
	if clusterSettingMetric.Object == nil {
		t.Error("ClusterSettingOperationMetric.Object should be non-nil")
	}
}

func TestNewStoreMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeMetric := object.NewStoreMetric(ctx)

	if storeMetric == nil {
		t.Error("NewStoreMetric should return non-nil StoreOperationMetric")
	}
	if storeMetric.Object == nil {
		t.Error("StoreOperationMetric.Object should be non-nil")
	}
}

func TestNewConnMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	connectMetric := object.NewConnMetric(ctx)

	if connectMetric == nil {
		t.Error("NewConnectMetric should return non-nil ConnectOperationMetric")
	}
	if connectMetric.Object == nil {
		t.Error("ConnectOperationMetric.Object should be non-nil")
	}
}

func TestNewSchemaMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	schemaMetric := object.NewSchemaMetric(ctx)

	if schemaMetric == nil {
		t.Error("NewSchemaMetric should return non-nil SchemaOperationMetric")
	}
	if schemaMetric.Object == nil {
		t.Error("SchemaOperationMetric.Object should be non-nil")
	}
}

func TestNewIndexMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	indexMetric := object.NewIndexMetric(ctx)

	if indexMetric == nil {
		t.Error("NewIndexMetric should return non-nil IndexOperationMetric")
	}
	if indexMetric.Object == nil {
		t.Error("IndexOperationMetric.Object should be non-nil")
	}
}

func TestNewViewMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	viewMetric := object.NewViewMetric(ctx)

	if viewMetric == nil {
		t.Error("NewViewMetric should return non-nil ViewOperationMetric")
	}
	if viewMetric.Object == nil {
		t.Error("ViewOperationMetric.Object should be non-nil")
	}
}

func TestNewSequenceMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	sequenceMetric := object.NewSequenceMetric(ctx)

	if sequenceMetric == nil {
		t.Error("NewSequenceMetric should return non-nil SequenceOperationMetric")
	}
	if sequenceMetric.Object == nil {
		t.Error("SequenceOperationMetric.Object should be non-nil")
	}
}

func TestNewFunctionMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	functionMetric := object.NewFunctionMetric(ctx)

	if functionMetric == nil {
		t.Error("NewFunctionMetric should return non-nil FunctionOperationMetric")
	}
	if functionMetric.Object == nil {
		t.Error("FunctionOperationMetric.Object should be non-nil")
	}
}

func TestNewProcedureMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	procedureMetric := object.NewProcedureMetric(ctx)

	if procedureMetric == nil {
		t.Error("NewProcedureMetric should return non-nil ProcedureOperationMetric")
	}
	if procedureMetric.Object == nil {
		t.Error("ProcedureOperationMetric.Object should be non-nil")
	}
}

func TestNewTriggerMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	triggerMetric := object.NewTriggerMetric(ctx)

	if triggerMetric == nil {
		t.Error("NewTriggerMetric should return non-nil TriggerOperationMetric")
	}
	if triggerMetric.Object == nil {
		t.Error("TriggerOperationMetric.Object should be non-nil")
	}
}

func TestNewUserMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	userMetric := object.NewUserMetric(ctx)

	if userMetric == nil {
		t.Error("NewUserMetric should return non-nil UserOperationMetric")
	}
	if userMetric.Object == nil {
		t.Error("UserOperationMetric.Object should be non-nil")
	}
}

func TestNewRoleMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	roleMetric := object.NewRoleMetric(ctx)

	if roleMetric == nil {
		t.Error("NewRoleMetric should return non-nil RoleOperationMetric")
	}
	if roleMetric.Object == nil {
		t.Error("RoleOperationMetric.Object should be non-nil")
	}
}

func TestNewPrivilegeMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	privilegeMetric := object.NewPrivilegeMetric(ctx)

	if privilegeMetric == nil {
		t.Error("NewPrivilegeMetric should return non-nil PrivilegeOperationMetric")
	}
	if privilegeMetric.Object == nil {
		t.Error("PrivilegeOperationMetric.Object should be non-nil")
	}
}

func TestNewAuditMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	auditMetric := object.NewAuditMetric(ctx)

	if auditMetric == nil {
		t.Error("NewAuditMetric should return non-nil AuditOperationMetric")
	}
	if auditMetric.Object == nil {
		t.Error("AuditOperationMetric.Object should be non-nil")
	}
}

func TestNewRangeMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	rangeMetric := object.NewRangeMetric(ctx)

	if rangeMetric == nil {
		t.Error("NewRangeMetric should return non-nil RangeOperationMetric")
	}
	if rangeMetric.Object == nil {
		t.Error("RangeOperationMetric.Object should be non-nil")
	}
}

func TestNewChangeFeedMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	changeFeedMetric := object.NewChangeFeedMetric(ctx)

	if changeFeedMetric == nil {
		t.Error("NewChangeFeedMetric should return non-nil ChangeFeedOperationMetric")
	}
	if changeFeedMetric.Object == nil {
		t.Error("ChangeFeedOperationMetric.Object should be non-nil")
	}
}

func TestNewQueryMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	queryMetric := object.NewQueryMetric(ctx)

	if queryMetric == nil {
		t.Error("NewQueryMetric should return non-nil QueryOperationMetric")
	}
	if queryMetric.Object == nil {
		t.Error("QueryOperationMetric.Object should be non-nil")
	}
}

func TestNewJobMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	jobMetric := object.NewJobMetric(ctx)

	if jobMetric == nil {
		t.Error("NewJobMetric should return non-nil JobOperationMetric")
	}
	if jobMetric.Object == nil {
		t.Error("JobOperationMetric.Object should be non-nil")
	}
}

func TestNewScheduleMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	scheduleMetric := object.NewScheduleMetric(ctx)

	if scheduleMetric == nil {
		t.Error("NewScheduleMetric should return non-nil ScheduleOperationMetric")
	}
	if scheduleMetric.Object == nil {
		t.Error("ScheduleOperationMetric.Object should be non-nil")
	}
}

func TestNewSessionMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	sessionMetric := object.NewSessionMetric(ctx)

	if sessionMetric == nil {
		t.Error("NewSessionMetric should return non-nil SessionOperationMetric")
	}
	if sessionMetric.Object == nil {
		t.Error("SessionOperationMetric.Object should be non-nil")
	}
}

func TestMetricMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dbMetric := object.NewDatabaseMetric(ctx)

	// Test that the embedded Object methods work
	if dbMetric.GetAuditLevel(target.Create) != target.StmtLevel {
		t.Error("DatabaseOperationMetric.GetAuditLevel should return StmtLevel")
	}

	metrics := dbMetric.RegisterMetric()
	if metrics == nil {
		t.Error("DatabaseOperationMetric.RegisterMetric should return non-nil map")
	}

	if !dbMetric.Metric(target.Create) {
		t.Error("DatabaseOperationMetric.Metric should return true for valid operation")
	}

	count := dbMetric.Count(target.Create)
	if count == 0 {
		t.Error("DatabaseOperationMetric.Count should return at least 1 after Metric() is called")
	}
}
