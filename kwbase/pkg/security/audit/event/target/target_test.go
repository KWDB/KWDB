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

package target_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestAuditLevelType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test AuditLevelType constants
	if target.UnknownLevel != -1 {
		t.Errorf("UnknownLevel = %d, want %d", target.UnknownLevel, -1)
	}

	if target.SystemLevel != 0 {
		t.Errorf("SystemLevel = %d, want %d", target.SystemLevel, 0)
	}

	if target.StmtLevel != 1 {
		t.Errorf("StmtLevel = %d, want %d", target.StmtLevel, 1)
	}

	if target.ObjectLevel != 2 {
		t.Errorf("ObjectLevel = %d, want %d", target.ObjectLevel, 2)
	}
}

func TestAuditObjectType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test some AuditObjectType constants
	testCases := []struct {
		objType  target.AuditObjectType
		expected string
	}{
		{target.ObjectNode, "NODE"},
		{target.ObjectCluster, "CLUSTER"},
		{target.ObjectDatabase, "DATABASE"},
		{target.ObjectTable, "TABLE"},
		{target.ObjectUser, "USER"},
		{target.ObjectRole, "ROLE"},
	}

	for _, tc := range testCases {
		if string(tc.objType) != tc.expected {
			t.Errorf("ObjectType %v = %q, want %q", tc.objType, string(tc.objType), tc.expected)
		}
	}
}

func TestOperationType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test some OperationType constants
	testCases := []struct {
		opType   target.OperationType
		expected string
	}{
		{target.Login, "LOGIN"},
		{target.Logout, "LOGOUT"},
		{target.Create, "CREATE"},
		{target.Alter, "ALTER"},
		{target.Drop, "DROP"},
		{target.Grant, "GRANT"},
		{target.Revoke, "REVOKE"},
		{target.Select, "SELECT"},
		{target.Insert, "INSERT"},
		{target.Update, "UPDATE"},
		{target.Delete, "DELETE"},
	}

	for _, tc := range testCases {
		if string(tc.opType) != tc.expected {
			t.Errorf("OperationType %v = %q, want %q", tc.opType, string(tc.opType), tc.expected)
		}
	}
}

func TestCreateMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test CreateMetric
	metric := target.CreateMetric(target.ObjectDatabase, target.Create)
	if metric == nil {
		t.Error("CreateMetric should return non-nil ObjectMetrics")
	}
	if metric.CatchCount == nil {
		t.Error("ObjectMetrics.CatchCount should be non-nil")
	}
}

func TestCreateMetricMap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test CreateMetricMap
	metricMap := target.CreateMetricMap(target.ObjectDatabase)
	if metricMap == nil {
		t.Error("CreateMetricMap should return non-nil map")
	}
	if len(metricMap) == 0 {
		t.Error("CreateMetricMap should return non-empty map")
	}

	// Test that expected operations are present
	expectedOps := []target.OperationType{target.Create, target.Drop, target.Alter, target.Flashback, target.Import, target.Export}
	for _, op := range expectedOps {
		if _, exists := metricMap[op]; !exists {
			t.Errorf("CreateMetricMap should include operation %v", op)
		}
	}
}

func TestObjectMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// Create a test Object
	obj := &target.Object{
		Ctx:        ctx,
		Name:       "test_object",
		Level:      target.ObjectLevel,
		ObjMetrics: target.CreateMetricMap(target.ObjectDatabase),
	}

	// Test GetAuditLevel
	if obj.GetAuditLevel(target.Create) != target.ObjectLevel {
		t.Errorf("Object.GetAuditLevel() should return ObjectLevel")
	}

	// Test RegisterMetric
	metrics := obj.RegisterMetric()
	if metrics == nil {
		t.Error("Object.RegisterMetric() should return non-nil map")
	}

	// Test Metric
	if !obj.Metric(target.Create) {
		t.Error("Object.Metric() should return true for valid operation")
	}
	if obj.Metric(target.Execute) {
		t.Error("Object.Metric() should return false for invalid operation")
	}

	// Test Count
	count := obj.Count(target.Create)
	if count == 0 {
		t.Error("Object.Count() should return at least 1 after Metric() is called")
	}
	if obj.Count(target.Execute) != 0 {
		t.Error("Object.Count() should return 0 for invalid operation")
	}

	// Test nil Object methods
	nilObj := (*target.Object)(nil)
	if nilObj.GetAuditLevel(target.Create) != target.UnknownLevel {
		t.Errorf("nil Object.GetAuditLevel() should return UnknownLevel")
	}
	if nilObj.RegisterMetric() != nil {
		t.Errorf("nil Object.RegisterMetric() should return nil")
	}
}

func TestAuditObjectOperationMap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test that some audit objects have expected operations
	testCases := []struct {
		objType     target.AuditObjectType
		expectedOps []target.OperationType
	}{
		{
			target.ObjectUser,
			[]target.OperationType{target.Create, target.Alter, target.Drop},
		},
		{
			target.ObjectRole,
			[]target.OperationType{target.Create, target.Drop, target.Grant, target.Revoke, target.Alter},
		},
		{
			target.ObjectTable,
			[]target.OperationType{target.Create, target.Drop, target.Alter, target.Flashback, target.Import, target.Export, target.Truncate, target.Select, target.Insert, target.Delete, target.Update},
		},
	}

	for _, tc := range testCases {
		// Use CreateMetricMap to get the operations for the object type
		metricMap := target.CreateMetricMap(tc.objType)
		if len(metricMap) == 0 {
			t.Errorf("CreateMetricMap should return non-empty map for %v", tc.objType)
			continue
		}

		// Check that all expected operations are present
		for _, expectedOp := range tc.expectedOps {
			if _, exists := metricMap[expectedOp]; !exists {
				t.Errorf("CreateMetricMap for %v should include operation %v", tc.objType, expectedOp)
			}
		}
	}
}
