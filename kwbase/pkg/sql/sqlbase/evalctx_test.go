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

package sqlbase_test

import (
	"context"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
)

func TestDummySequenceOperators(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	so := &sqlbase.DummySequenceOperators{}

	// Test ParseQualifiedTableName
	tn, err := so.ParseQualifiedTableName("table_name")
	if err == nil {
		t.Error("Expected error for ParseQualifiedTableName, got nil")
	}
	if err != nil && !isSequenceOpError(err) {
		t.Errorf("Expected sequence operation error, got %v", err)
	}
	if tn != nil {
		t.Errorf("Expected nil TableName, got %v", tn)
	}

	// Test ResolveTableName
	tableID, err := so.ResolveTableName(ctx, &tree.TableName{})
	if err == nil {
		t.Error("Expected error for ResolveTableName, got nil")
	}
	if err != nil && !isSequenceOpError(err) {
		t.Errorf("Expected sequence operation error, got %v", err)
	}
	if tableID != 0 {
		t.Errorf("Expected table ID 0, got %v", tableID)
	}

	// Test LookupSchema
	found, meta, err := so.LookupSchema(ctx, "db", "schema")
	if err == nil {
		t.Error("Expected error for LookupSchema, got nil")
	}
	if err != nil && !isSequenceOpError(err) {
		t.Errorf("Expected sequence operation error, got %v", err)
	}
	if found {
		t.Error("Expected found to be false")
	}
	if meta != nil {
		t.Errorf("Expected nil SchemaMeta, got %v", meta)
	}

	// Test IncrementSequence
	seqVal, err := so.IncrementSequence(ctx, &tree.TableName{})
	if err == nil {
		t.Error("Expected error for IncrementSequence, got nil")
	}
	if err != nil && !isSequenceOpError(err) {
		t.Errorf("Expected sequence operation error, got %v", err)
	}
	if seqVal != 0 {
		t.Errorf("Expected sequence value 0, got %v", seqVal)
	}

	// Test GetLatestValueInSessionForSequence
	latestVal, err := so.GetLatestValueInSessionForSequence(ctx, &tree.TableName{})
	if err == nil {
		t.Error("Expected error for GetLatestValueInSessionForSequence, got nil")
	}
	if err != nil && !isSequenceOpError(err) {
		t.Errorf("Expected sequence operation error, got %v", err)
	}
	if latestVal != 0 {
		t.Errorf("Expected latest value 0, got %v", latestVal)
	}

	// Test SetSequenceValue
	err = so.SetSequenceValue(ctx, &tree.TableName{}, 100, true)
	if err == nil {
		t.Error("Expected error for SetSequenceValue, got nil")
	}
	if err != nil && !isSequenceOpError(err) {
		t.Errorf("Expected sequence operation error, got %v", err)
	}
}

func TestDummyEvalPlanner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	ep := &sqlbase.DummyEvalPlanner{}

	// Test IsInternalSQL
	isInternal := ep.IsInternalSQL()
	if isInternal {
		t.Error("Expected IsInternalSQL to return false")
	}

	// Test ParseQualifiedTableName - this should work since it delegates to parser
	tn, err := ep.ParseQualifiedTableName("public.table_name")
	if err != nil {
		t.Errorf("Expected no error for ParseQualifiedTableName, got %v", err)
	}
	if tn == nil {
		t.Error("Expected non-nil TableName")
	}

	// Test LookupSchema
	found, meta, err := ep.LookupSchema(ctx, "db", "schema")
	if err == nil {
		t.Error("Expected error for LookupSchema, got nil")
	}
	// if !isScalarOpError(err) {
	// 	t.Errorf("Expected scalar operation error, got %v", err)
	// }
	if found {
		t.Error("Expected found to be false")
	}
	if meta != nil {
		t.Errorf("Expected nil SchemaMeta, got %v", meta)
	}

	// Test ResolveTableName
	tableID, err := ep.ResolveTableName(ctx, &tree.TableName{})
	if err == nil {
		t.Error("Expected error for ResolveTableName, got nil")
	}
	// if !isScalarOpError(err) {
	// 	t.Errorf("Expected scalar operation error, got %v", err)
	// }
	if tableID != 0 {
		t.Errorf("Expected table ID 0, got %v", tableID)
	}

	// Test ParseType
	typ, err := ep.ParseType("INT")
	if err == nil {
		t.Error("Expected error for ParseType, got nil")
	}
	// if !isScalarOpError(err) {
	// 	t.Errorf("Expected scalar operation error, got %v", err)
	// }
	if typ != nil {
		t.Errorf("Expected nil type, got %v", typ)
	}

	// Test MakeNewPlanAndRunForTsInsert - this should not return an error
	var param tree.TSInsertSelectParam // Use the actual type
	count, err := ep.MakeNewPlanAndRunForTsInsert(ctx, nil, param)
	if err != nil {
		t.Errorf("Expected no error for MakeNewPlanAndRunForTsInsert, got %v", err)
	}
	if count != 0 {
		t.Errorf("Expected count 0, got %v", count)
	}

	// Test GetStmt
	stmt := ep.GetStmt()
	if stmt != "" {
		t.Errorf("Expected empty stmt, got %v", stmt)
	}

	// Test EvalSubquery
	datum, err := ep.EvalSubquery(&tree.Subquery{})
	if err == nil {
		t.Error("Expected error for EvalSubquery, got nil")
	}
	// if !isScalarOpError(err) {
	// 	t.Errorf("Expected scalar operation error, got %v", err)
	// }
	if datum != nil {
		t.Errorf("Expected nil datum, got %v", datum)
	}

	// Test ExecutorConfig
	config := ep.ExecutorConfig()
	if config != nil {
		t.Errorf("Expected nil config, got %v", config)
	}
}

func TestDummyPrivilegedAccessor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	ep := &sqlbase.DummyPrivilegedAccessor{}

	// Test LookupNamespaceID
	id, found, err := ep.LookupNamespaceID(ctx, 123, "name")
	if err == nil {
		t.Error("Expected error for LookupNamespaceID, got nil")
	}
	// if !isScalarOpError(err) {
	// 	t.Errorf("Expected scalar operation error, got %v", err)
	// }
	if id != 0 {
		t.Errorf("Expected ID 0, got %v", id)
	}
	if found {
		t.Error("Expected found to be false")
	}

	// Test LookupZoneConfigByNamespaceID
	bytes, found, err := ep.LookupZoneConfigByNamespaceID(ctx, 123)
	if err == nil {
		t.Error("Expected error for LookupZoneConfigByNamespaceID, got nil")
	}
	// if !isScalarOpError(err) {
	// 	t.Errorf("Expected scalar operation error, got %v", err)
	// }
	if bytes != "" {
		t.Errorf("Expected empty bytes, got %v", bytes)
	}
	if found {
		t.Error("Expected found to be false")
	}
}

func TestDummySessionAccessor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	ep := &sqlbase.DummySessionAccessor{}

	// Test GetSessionVar
	exists, value, err := ep.GetSessionVar(ctx, "var", true)
	if err == nil {
		t.Error("Expected error for GetSessionVar, got nil")
	}
	// if !isScalarOpError(err) {
	// 	t.Errorf("Expected scalar operation error, got %v", err)
	// }
	if exists {
		t.Error("Expected exists to be false")
	}
	if value != "" {
		t.Errorf("Expected empty value, got %v", value)
	}

	// Test GetUserDefinedVar
	var exists2 bool
	var value2 interface{}
	var err2 error
	exists2, value2, err2 = ep.GetUserDefinedVar(ctx, "var", true)
	if err2 == nil {
		t.Error("Expected error for GetUserDefinedVar, got nil")
	}
	// if !isScalarOpError(err2) {
	// 	t.Errorf("Expected scalar operation error, got %v", err2)
	// }
	if exists2 {
		t.Error("Expected exists to be false")
	}
	if value2 != nil {
		t.Errorf("Expected nil value, got %v", value2)
	}

	// Test SetSessionVar
	err3 := ep.SetSessionVar(ctx, "var", "value")
	if err3 == nil {
		t.Error("Expected error for SetSessionVar, got nil")
	}
	// if !isScalarOpError(err3) {
	// 	t.Errorf("Expected scalar operation error, got %v", err3)
	// }

	// Test HasAdminRole
	hasRole, err4 := ep.HasAdminRole(ctx)
	if err4 == nil {
		t.Error("Expected error for HasAdminRole, got nil")
	}
	// if !isScalarOpError(err4) {
	// 	t.Errorf("Expected scalar operation error, got %v", err4)
	// }
	if hasRole {
		t.Error("Expected hasRole to be false")
	}
}

func TestDummyClientNoticeSender(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	c := &sqlbase.DummyClientNoticeSender{}

	// Test SendClientNotice - this should not panic or return an error
	err := func() (err interface{}) {
		defer func() {
			if r := recover(); r != nil {
				err = r
			}
		}()
		c.SendClientNotice(ctx, errors.New("test notice"))
		return nil
	}()

	if err != nil {
		t.Errorf("SendClientNotice caused panic: %v", err)
	}
}

func TestDummyTsDBAccessor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tac := &sqlbase.DummyTsDBAccessor{}

	// Test GetRangeRowCountFromNode
	count, err := tac.GetRangeRowCountFromNode(ctx, 123, 456)
	if err == nil {
		t.Error("Expected error for GetRangeRowCountFromNode, got nil")
	}
	// if !isScalarOpError(err) {
	// 	t.Errorf("Expected scalar operation error, got %v", err)
	// }
	if count != 0 {
		t.Errorf("Expected count 0, got %v", count)
	}

	// Test RelocateRange
	err = tac.RelocateRange(ctx, 123, 456, 789)
	if err == nil {
		t.Error("Expected error for RelocateRange, got nil")
	}
	// if !isScalarOpError(err) {
	// 	t.Errorf("Expected scalar operation error, got %v", err)
	// }

	// Test GetRangeDebugInfo
	info, err := tac.GetRangeDebugInfo(ctx, 123)
	if err == nil {
		t.Error("Expected error for GetRangeDebugInfo, got nil")
	}
	// if !isScalarOpError(err) {
	// 	t.Errorf("Expected scalar operation error, got %v", err)
	// }
	if info != nil {
		t.Errorf("Expected nil info, got %v", info)
	}

	// Test GetProblemRangesInfo
	info, err = tac.GetProblemRangesInfo(ctx)
	if err == nil {
		t.Error("Expected error for GetProblemRangesInfo, got nil")
	}
	// if !isScalarOpError(err) {
	// 	t.Errorf("Expected scalar operation error, got %v", err)
	// }
	if info != nil {
		t.Errorf("Expected nil info, got %v", info)
	}
}

// Helper function to check if an error is a scalar operation error
func isScalarOpError(err error) bool {
	pgErr, ok := errors.UnwrapAll(err).(*pgerror.Error)
	if !ok {
		return false
	}
	return pgErr.Code == string(pgcode.ScalarOperationCannotRunWithoutFullSessionContext)
}

// Helper function to check if an error is a sequence operation error
func isSequenceOpError(err error) bool {
	unimplErr := unimplemented.NewWithIssue(42508, "")
	expectedMsg := unimplErr.Error()
	return err != nil && errors.Is(errors.UnwrapAll(err), unimplErr) ||
		(err != nil && expectedMsg != "" && strings.Contains(err.Error(), expectedMsg))
}
