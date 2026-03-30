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
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// MockNodeFormatter implements tree.NodeFormatter for testing purposes
type MockNodeFormatter struct {
	name string
}

func (m MockNodeFormatter) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(m.name)
}

func TestNewTransactionAbortedError(t *testing.T) {
	// Test with custom message
	customMsg := "custom error message"
	err := sqlbase.NewTransactionAbortedError(customMsg)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.InFailedSQLTransaction {
		t.Errorf("Expected code %s, got %s", pgcode.InFailedSQLTransaction, pgErr.Code)
	}

	expectedMsg := customMsg + ": " + "current transaction is aborted, commands ignored until end of transaction block"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}

	// Test without custom message
	err = sqlbase.NewTransactionAbortedError("")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr = pgerror.Flatten(err)
	if pgErr.Code != pgcode.InFailedSQLTransaction {
		t.Errorf("Expected code %s, got %s", pgcode.InFailedSQLTransaction, pgErr.Code)
	}

	expectedMsg = "current transaction is aborted, commands ignored until end of transaction block"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewTransactionCommittedError(t *testing.T) {
	err := sqlbase.NewTransactionCommittedError()
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.InvalidTransactionState {
		t.Errorf("Expected code %s, got %s", pgcode.InvalidTransactionState, pgErr.Code)
	}

	expectedMsg := "current transaction is committed, commands ignored until end of transaction block"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewNonNullViolationError(t *testing.T) {
	columnName := "test_column"
	err := sqlbase.NewNonNullViolationError(columnName)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.NotNullViolation {
		t.Errorf("Expected code %s, got %s", pgcode.NotNullViolation, pgErr.Code)
	}

	expectedMsg := `null value in column "test_column" violates not-null constraint`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewInvalidSchemaDefinitionError(t *testing.T) {
	innerErr := errors.New("parse error")
	err := sqlbase.NewInvalidSchemaDefinitionError(innerErr)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.InvalidSchemaDefinition {
		t.Errorf("Expected code %s, got %s", pgcode.InvalidSchemaDefinition, pgErr.Code)
	}

	if !strings.Contains(pgErr.Message, "parse error") {
		t.Errorf("Expected message to contain inner error, got %q", pgErr.Message)
	}
}

func TestNewUnsupportedSchemaUsageError(t *testing.T) {
	schemaName := "invalid_schema"
	err := sqlbase.NewUnsupportedSchemaUsageError(schemaName)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.InvalidSchemaName {
		t.Errorf("Expected code %s, got %s", pgcode.InvalidSchemaName, pgErr.Code)
	}

	expectedMsg := `unsupported schema specification: "invalid_schema"`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewCCLRequiredError(t *testing.T) {
	innerErr := errors.New("feature requires CCL")
	err := sqlbase.NewCCLRequiredError(innerErr)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.CCLRequired {
		t.Errorf("Expected code %s, got %s", pgcode.CCLRequired, pgErr.Code)
	}

	if !strings.Contains(pgErr.Message, "feature requires CCL") {
		t.Errorf("Expected message to contain inner error, got %q", pgErr.Message)
	}
}

func TestIsCCLRequiredError(t *testing.T) {
	innerErr := errors.New("feature requires CCL")
	cclErr := sqlbase.NewCCLRequiredError(innerErr)

	if !sqlbase.IsCCLRequiredError(cclErr) {
		t.Error("Expected IsCCLRequiredError to return true for CCL required error")
	}

	otherErr := sqlbase.NewNonNullViolationError("test")
	if sqlbase.IsCCLRequiredError(otherErr) {
		t.Error("Expected IsCCLRequiredError to return false for non-CCL required error")
	}
}

func TestNewUndefinedDatabaseError(t *testing.T) {
	dbName := "nonexistent_db"
	err := sqlbase.NewUndefinedDatabaseError(dbName)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.InvalidCatalogName {
		t.Errorf("Expected code %s, got %s", pgcode.InvalidCatalogName, pgErr.Code)
	}

	expectedMsg := `database "nonexistent_db" does not exist`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewUndefinedSchemaError(t *testing.T) {
	schemaName := "nonexistent_schema"
	err := sqlbase.NewUndefinedSchemaError(schemaName)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.InvalidCatalogName {
		t.Errorf("Expected code %s, got %s", pgcode.InvalidCatalogName, pgErr.Code)
	}

	expectedMsg := `schema "nonexistent_schema" does not exist`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewUndefinedTableError(t *testing.T) {
	tableName := "nonexistent_table"
	err := sqlbase.NewUndefinedTableError(tableName)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.InvalidCatalogName {
		t.Errorf("Expected code %s, got %s", pgcode.InvalidCatalogName, pgErr.Code)
	}

	expectedMsg := `table "nonexistent_table" does not exist`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewUndefinedTagError(t *testing.T) {
	tagName := "nonexistent_tag"
	err := sqlbase.NewUndefinedTagError(tagName)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.UndefinedObject {
		t.Errorf("Expected code %s, got %s", pgcode.UndefinedObject, pgErr.Code)
	}

	expectedMsg := `tag "nonexistent_tag" does not exist`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestTSUnsupportedError(t *testing.T) {
	opName := "CREATE INDEX"
	err := sqlbase.TSUnsupportedError(opName)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.FeatureNotSupported {
		t.Errorf("Expected code %s, got %s", pgcode.FeatureNotSupported, pgErr.Code)
	}

	expectedMsg := "CREATE INDEX is not supported in timeseries table"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestTemplateUnsupportedError(t *testing.T) {
	opName := "ALTER TABLE"
	err := sqlbase.TemplateUnsupportedError(opName)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.FeatureNotSupported {
		t.Errorf("Expected code %s, got %s", pgcode.FeatureNotSupported, pgErr.Code)
	}

	expectedMsg := `unsupported feature in template table: "ALTER TABLE"`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestUnsupportedDeleteConditionError(t *testing.T) {
	errMsg := "complex condition not supported"
	err := sqlbase.UnsupportedDeleteConditionError(errMsg)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.FeatureNotSupported {
		t.Errorf("Expected code %s, got %s", pgcode.FeatureNotSupported, pgErr.Code)
	}

	expectedMsg := "unsupported conditions found in the deletion operation: complex condition not supported"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestUnsupportedUpdateConditionError(t *testing.T) {
	errMsg := "complex condition not supported"
	err := sqlbase.UnsupportedUpdateConditionError(errMsg)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.FeatureNotSupported {
		t.Errorf("Expected code %s, got %s", pgcode.FeatureNotSupported, pgErr.Code)
	}

	expectedMsg := "unsupported conditions in update: complex condition not supported"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestTemplateAndInstanceUnsupportedError(t *testing.T) {
	opName := "DROP COLUMN"
	err := sqlbase.TemplateAndInstanceUnsupportedError(opName)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.FeatureNotSupported {
		t.Errorf("Expected code %s, got %s", pgcode.FeatureNotSupported, pgErr.Code)
	}

	expectedMsg := "unsupported feature in template and instance table: DROP COLUMN"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestIntOutOfRangeError(t *testing.T) {
	typ := types.Int
	colName := "test_col"
	err := sqlbase.IntOutOfRangeError(typ, colName)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.NumericValueOutOfRange {
		t.Errorf("Expected code %s, got %s", pgcode.NumericValueOutOfRange, pgErr.Code)
	}

	expectedMsg := "integer out of range for type INT8 (column test_col)"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewTSNameOutOfLengthError(t *testing.T) {
	err := sqlbase.NewTSNameOutOfLengthError("table", "very_long_name", 10)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.InvalidName {
		t.Errorf("Expected code %s, got %s", pgcode.InvalidName, pgErr.Code)
	}

	expectedMsg := `ts table name "very_long_name" exceeds max length (10)`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewTSNameInvalidError(t *testing.T) {
	err := sqlbase.NewTSNameInvalidError("invalid-name!")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.InvalidName {
		t.Errorf("Expected code %s, got %s", pgcode.InvalidName, pgErr.Code)
	}

	expectedMsg := "invalid name: invalid-name!, naming of time series objects only supports letters, numbers and symbols"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewTSColInvalidError(t *testing.T) {
	err := sqlbase.NewTSColInvalidError("invalid-col!")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.InvalidName {
		t.Errorf("Expected code %s, got %s", pgcode.InvalidName, pgErr.Code)
	}

	expectedMsg := "invalid name: invalid-col!, naming of column/tag in timeseries table only supports letters, numbers and symbols"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestUnsupportedTSExplicitTxnError(t *testing.T) {
	err := sqlbase.UnsupportedTSExplicitTxnError()
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.FeatureNotSupported {
		t.Errorf("Expected code %s, got %s", pgcode.FeatureNotSupported, pgErr.Code)
	}

	expectedMsg := "TS DDL statement is not supported in explicit transaction"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewUndefinedRelationError(t *testing.T) {
	node := MockNodeFormatter{name: "test_relation"}
	err := sqlbase.NewUndefinedRelationError(node)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.UndefinedTable {
		t.Errorf("Expected code %s, got %s", pgcode.UndefinedTable, pgErr.Code)
	}

	expectedMsg := `relation "test_relation" does not exist`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewUndefinedProcedureError(t *testing.T) {
	node := MockNodeFormatter{name: "test_procedure"}
	err := sqlbase.NewUndefinedProcedureError(node)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.UndefinedObject {
		t.Errorf("Expected code %s, got %s", pgcode.UndefinedObject, pgErr.Code)
	}

	expectedMsg := `procedure "test_procedure" does not exist`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewUndefinedTriggerError(t *testing.T) {
	node := MockNodeFormatter{name: "test_trigger"}
	err := sqlbase.NewUndefinedTriggerError(node)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.UndefinedObject {
		t.Errorf("Expected code %s, got %s", pgcode.UndefinedObject, pgErr.Code)
	}

	expectedMsg := `trigger "test_trigger" does not exist`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewUndefinedColumnError(t *testing.T) {
	err := sqlbase.NewUndefinedColumnError("test_column")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.UndefinedColumn {
		t.Errorf("Expected code %s, got %s", pgcode.UndefinedColumn, pgErr.Code)
	}

	expectedMsg := `column "test_column" does not exist`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewUndefinedVarColumnError(t *testing.T) {
	err := sqlbase.NewUndefinedVarColumnError("test_var")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.UndefinedColumn {
		t.Errorf("Expected code %s, got %s", pgcode.UndefinedColumn, pgErr.Code)
	}

	expectedMsg := `variable "test_var" does not exist`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewDatabaseAlreadyExistsError(t *testing.T) {
	err := sqlbase.NewDatabaseAlreadyExistsError("existing_db")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.DuplicateDatabase {
		t.Errorf("Expected code %s, got %s", pgcode.DuplicateDatabase, pgErr.Code)
	}

	expectedMsg := `database "existing_db" already exists`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewRelationAlreadyExistsError(t *testing.T) {
	err := sqlbase.NewRelationAlreadyExistsError("existing_rel")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.DuplicateRelation {
		t.Errorf("Expected code %s, got %s", pgcode.DuplicateRelation, pgErr.Code)
	}

	expectedMsg := `relation "existing_rel" already exists`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestIsRelationAlreadyExistsError(t *testing.T) {
	existingRelErr := sqlbase.NewRelationAlreadyExistsError("test")

	if !sqlbase.IsRelationAlreadyExistsError(existingRelErr) {
		t.Error("Expected IsRelationAlreadyExistsError to return true for relation already exists error")
	}

	otherErr := sqlbase.NewNonNullViolationError("test")
	if sqlbase.IsRelationAlreadyExistsError(otherErr) {
		t.Error("Expected IsRelationAlreadyExistsError to return false for non-relation already exists error")
	}
}

func TestNewWrongObjectTypeError(t *testing.T) {
	node := MockNodeFormatter{name: "test_obj"}
	err := sqlbase.NewWrongObjectTypeError(node, "table")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.WrongObjectType {
		t.Errorf("Expected code %s, got %s", pgcode.WrongObjectType, pgErr.Code)
	}

	expectedMsg := `"test_obj" is not a table`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewSyntaxError(t *testing.T) {
	err := sqlbase.NewSyntaxError("syntax error message")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.Syntax {
		t.Errorf("Expected code %s, got %s", pgcode.Syntax, pgErr.Code)
	}

	expectedMsg := "syntax error message"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewDependentObjectError(t *testing.T) {
	err := sqlbase.NewDependentObjectError("dependent object error message")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.DependentObjectsStillExist {
		t.Errorf("Expected code %s, got %s", pgcode.DependentObjectsStillExist, pgErr.Code)
	}

	expectedMsg := "dependent object error message"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewDependentObjectErrorWithHint(t *testing.T) {
	err := sqlbase.NewDependentObjectErrorWithHint("dependent object error message", "try dropping dependencies first")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.DependentObjectsStillExist {
		t.Errorf("Expected code %s, got %s", pgcode.DependentObjectsStillExist, pgErr.Code)
	}

	expectedMsg := "dependent object error message"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}

	// Check if the hint is present
	hint := errors.FlattenHints(err)
	if len(hint) == 0 {
		t.Errorf("Expected hint to be present, got none")
	} else {
		// Convert the hint to string for comparison
		hintStr := string(hint)
		if !strings.Contains(hintStr, "try dropping dependencies first") {
			t.Errorf("Expected hint to contain 'try dropping dependencies first', got %v", hintStr)
		}
	}
}

func TestNewRangeUnavailableError(t *testing.T) {
	origErr := errors.New("original error")
	nodeIDs := []roachpb.NodeID{1, 2, 3}
	err := sqlbase.NewRangeUnavailableError(10, origErr, nodeIDs...)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.RangeUnavailable {
		t.Errorf("Expected code %s, got %s", pgcode.RangeUnavailable, pgErr.Code)
	}

	expectedMsg := "key range id:10 is unavailable; missing nodes: [1 2 3]. Original error: original error"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewWindowInAggError(t *testing.T) {
	err := sqlbase.NewWindowInAggError()
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.Grouping {
		t.Errorf("Expected code %s, got %s", pgcode.Grouping, pgErr.Code)
	}

	expectedMsg := "window functions are not allowed in aggregate"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewAggInAggError(t *testing.T) {
	err := sqlbase.NewAggInAggError()
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.Grouping {
		t.Errorf("Expected code %s, got %s", pgcode.Grouping, pgErr.Code)
	}

	expectedMsg := "aggregate function calls cannot be nested"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestQueryCanceledError(t *testing.T) {
	err := sqlbase.QueryCanceledError
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.QueryCanceled {
		t.Errorf("Expected code %s, got %s", pgcode.QueryCanceled, pgErr.Code)
	}

	expectedMsg := "query execution canceled"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestQueryTimeoutError(t *testing.T) {
	err := sqlbase.QueryTimeoutError
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.QueryCanceled {
		t.Errorf("Expected code %s, got %s", pgcode.QueryCanceled, pgErr.Code)
	}

	expectedMsg := "query execution canceled due to statement timeout"
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestIsOutOfMemoryError(t *testing.T) {
	outOfMemErr := pgerror.New(pgcode.OutOfMemory, "out of memory")

	if !sqlbase.IsOutOfMemoryError(outOfMemErr) {
		t.Error("Expected IsOutOfMemoryError to return true for out of memory error")
	}

	otherErr := sqlbase.NewNonNullViolationError("test")
	if sqlbase.IsOutOfMemoryError(otherErr) {
		t.Error("Expected IsOutOfMemoryError to return false for non-out of memory error")
	}
}

func TestIsUndefinedColumnError(t *testing.T) {
	undefinedColErr := sqlbase.NewUndefinedColumnError("test")

	if !sqlbase.IsUndefinedColumnError(undefinedColErr) {
		t.Error("Expected IsUndefinedColumnError to return true for undefined column error")
	}

	otherErr := sqlbase.NewNonNullViolationError("test")
	if sqlbase.IsUndefinedColumnError(otherErr) {
		t.Error("Expected IsUndefinedColumnError to return false for non-undefined column error")
	}
}

func TestNewAlterTSTableError(t *testing.T) {
	err := sqlbase.NewAlterTSTableError("test_table")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.ObjectInUse {
		t.Errorf("Expected code %s, got %s", pgcode.ObjectInUse, pgErr.Code)
	}

	expectedMsg := `table "test_table" is being modified. Please wait for success`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewCreateTSTableError(t *testing.T) {
	err := sqlbase.NewCreateTSTableError("test_table")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.ObjectInUse {
		t.Errorf("Expected code %s, got %s", pgcode.ObjectInUse, pgErr.Code)
	}

	expectedMsg := `table "test_table" is being created. Please wait for success`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewDropTSTableError(t *testing.T) {
	err := sqlbase.NewDropTSTableError("test_table")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.ObjectInUse {
		t.Errorf("Expected code %s, got %s", pgcode.ObjectInUse, pgErr.Code)
	}

	expectedMsg := `table "test_table" is being dropped. Please wait for success`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}

func TestNewDropTSDBError(t *testing.T) {
	err := sqlbase.NewDropTSDBError("test_db")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	pgErr := pgerror.Flatten(err)
	if pgErr.Code != pgcode.ObjectInUse {
		t.Errorf("Expected code %s, got %s", pgcode.ObjectInUse, pgErr.Code)
	}

	expectedMsg := `database "test_db" is being dropped. Please wait for success`
	if !strings.Contains(pgErr.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got %q", expectedMsg, pgErr.Message)
	}
}
