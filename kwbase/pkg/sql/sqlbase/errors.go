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

package sqlbase

import (
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

const (
	// DuplicateUpsertErrText is error text used when a row is modified twice by
	// an upsert statement.
	DuplicateUpsertErrText = "UPSERT or INSERT...ON CONFLICT command cannot affect row a second time"

	txnAbortedMsg = "current transaction is aborted, commands ignored " +
		"until end of transaction block"
	txnCommittedMsg = "current transaction is committed, commands ignored " +
		"until end of transaction block"
)

// NewTransactionAbortedError creates an error for trying to run a command in
// the context of transaction that's in the aborted state. Any statement other
// than ROLLBACK TO SAVEPOINT will return this error.
func NewTransactionAbortedError(customMsg string) error {
	if customMsg != "" {
		return pgerror.Newf(
			pgcode.InFailedSQLTransaction, "%s: %s", customMsg, txnAbortedMsg)
	}
	return pgerror.New(pgcode.InFailedSQLTransaction, txnAbortedMsg)
}

// NewTransactionCommittedError creates an error that signals that the SQL txn
// is in the COMMIT_WAIT state and that only a COMMIT statement will be accepted.
func NewTransactionCommittedError() error {
	return pgerror.New(pgcode.InvalidTransactionState, txnCommittedMsg)
}

// NewNonNullViolationError creates an error for a violation of a non-NULL constraint.
func NewNonNullViolationError(columnName string) error {
	return pgerror.Newf(pgcode.NotNullViolation, "null value in column %q violates not-null constraint", columnName)
}

// NewInvalidSchemaDefinitionError creates an error for an invalid schema
// definition such as a schema definition that doesn't parse.
func NewInvalidSchemaDefinitionError(err error) error {
	return pgerror.WithCandidateCode(err, pgcode.InvalidSchemaDefinition)
}

// NewUnsupportedSchemaUsageError creates an error for an invalid
// schema use, e.g. mydb.someschema.tbl.
func NewUnsupportedSchemaUsageError(name string) error {
	return pgerror.Newf(pgcode.InvalidSchemaName,
		"unsupported schema specification: %q", name)
}

// NewCCLRequiredError creates an error for when a CCL feature is used in an OSS
// binary.
func NewCCLRequiredError(err error) error {
	return pgerror.WithCandidateCode(err, pgcode.CCLRequired)
}

// IsCCLRequiredError returns whether the error is a CCLRequired error.
func IsCCLRequiredError(err error) bool {
	return errHasCode(err, pgcode.CCLRequired)
}

// NewUndefinedDatabaseError creates an error that represents a missing database.
func NewUndefinedDatabaseError(name string) error {
	// Postgres will return an UndefinedTable error on queries that go to a "relation"
	// that does not exist (a query to a non-existent table or database), but will
	// return an InvalidCatalogName error when connecting to a database that does
	// not exist. We've chosen to return this code for all cases where the error cause
	// is a missing database.
	return pgerror.Newf(
		pgcode.InvalidCatalogName, "database %q does not exist", name)
}

// NewUndefinedSchemaError creates an error that represents a missing schema.
func NewUndefinedSchemaError(name string) error {
	return pgerror.Newf(
		pgcode.InvalidCatalogName, "schema %q does not exist", name)
}

// NewUndefinedTableError creates an error that represents a missing table.
func NewUndefinedTableError(name string) error {
	return pgerror.Newf(
		pgcode.InvalidCatalogName, "table %q does not exist", name)
}

// NewUndefinedTagError creates an error that represents a missing tag.
func NewUndefinedTagError(name string) error {
	return pgerror.Newf(
		pgcode.UndefinedObject, "tag %q does not exist", name)
}

// TSUnsupportedError returns an error if feature is not supported under timeseries scenario
func TSUnsupportedError(opName string) error {
	return pgerror.Newf(pgcode.FeatureNotSupported, "%s is not supported in timeseries table", opName)
}

// TemplateUnsupportedError returns an error if feature is not supported for template table
func TemplateUnsupportedError(opName string) error {
	return pgerror.Newf(pgcode.FeatureNotSupported, "unsupported feature in template table: %q", opName)
}

// UnsupportedDeleteConditionError returns an error if filter for delete is not supported
func UnsupportedDeleteConditionError(errMessage string) error {
	return pgerror.Newf(pgcode.FeatureNotSupported, "unsupported conditions found in the deletion operation: %s", errMessage)
}

// UnsupportedUpdateConditionError returns an error if filter for update is not supported
func UnsupportedUpdateConditionError(errMessage string) error {
	return pgerror.Newf(pgcode.FeatureNotSupported, "unsupported conditions in update: %s", errMessage)
}

// TemplateAndInstanceUnsupportedError returns an error if feature is not supported for template and instance table
func TemplateAndInstanceUnsupportedError(opName string) error {
	return pgerror.Newf(pgcode.FeatureNotSupported, "unsupported feature in template and instance table: %s", opName)
}

// IntOutOfRangeError returns an error if type int out of range
func IntOutOfRangeError(typ *types.T, name string) error {
	return pgerror.Newf(pgcode.NumericValueOutOfRange, "integer out of range for type %s (column %s)", typ.SQLString(), name)
}

// NewTSNameOutOfLengthError creates an error that name of timeseries object is out of length.
func NewTSNameOutOfLengthError(typ, name string, length int) error {
	return pgerror.Newf(
		pgcode.InvalidName, "ts %s name \"%s\" exceeds max length (%d)", typ, name, length)
}

// NewTSNameInvalidError creates an error indicates that name of ts object contains invalid character.
func NewTSNameInvalidError(name string) error {
	return pgerror.Newf(pgcode.InvalidName, "invalid name: %s, naming of time series objects only supports letters, numbers and symbols", name)
}

// NewTSColInvalidError creates an error indicates that colName/tagName of ts table contains invalid character.
func NewTSColInvalidError(name string) error {
	return pgerror.Newf(
		pgcode.InvalidName,
		"invalid name: %s, naming of column/tag in timeseries table only supports letters, numbers and symbols",
		name)
}

// UnsupportedTSExplicitTxnError returns error when there is an explicit transaction in TS mode.
func UnsupportedTSExplicitTxnError() error {
	return pgerror.New(pgcode.FeatureNotSupported, "TS DDL statement is not supported in explicit transaction")
}

// NewUndefinedRelationError creates an error that represents a missing database table or view.
func NewUndefinedRelationError(name tree.NodeFormatter) error {
	return pgerror.Newf(pgcode.UndefinedTable,
		"relation %q does not exist", tree.ErrString(name))
}

// NewUndefinedProcedureError creates an error that represents a missing procedure.
func NewUndefinedProcedureError(name tree.NodeFormatter) error {
	return pgerror.Newf(pgcode.UndefinedObject,
		"procedure %q does not exist", tree.ErrString(name))
}

// NewUndefinedTriggerError creates an error that represents a missing trigger.
func NewUndefinedTriggerError(name tree.NodeFormatter) error {
	return pgerror.Newf(pgcode.UndefinedObject,
		"trigger %q does not exist", tree.ErrString(name))
}

// NewUndefinedColumnError creates an error that represents a missing database column.
func NewUndefinedColumnError(name string) error {
	return pgerror.Newf(pgcode.UndefinedColumn, "column %q does not exist", name)
}

// NewUndefinedVarColumnError creates an error that represents a missing variable column.
func NewUndefinedVarColumnError(name string) error {
	return pgerror.Newf(pgcode.UndefinedColumn, "variable %q does not exist", name)
}

// NewDatabaseAlreadyExistsError creates an error for a preexisting database.
func NewDatabaseAlreadyExistsError(name string) error {
	return pgerror.Newf(pgcode.DuplicateDatabase, "database %q already exists", name)
}

// NewRelationAlreadyExistsError creates an error for a preexisting relation.
func NewRelationAlreadyExistsError(name string) error {
	return pgerror.Newf(pgcode.DuplicateRelation, "relation %q already exists", name)
}

// IsRelationAlreadyExistsError checks whether this is an error for a preexisting relation.
func IsRelationAlreadyExistsError(err error) bool {
	return errHasCode(err, pgcode.DuplicateRelation)
}

// NewWrongObjectTypeError creates a wrong object type error.
func NewWrongObjectTypeError(name tree.NodeFormatter, desiredObjType string) error {
	return pgerror.Newf(pgcode.WrongObjectType, "%q is not a %s",
		tree.ErrString(name), desiredObjType)
}

// NewSyntaxError creates a syntax error.
func NewSyntaxError(msg string) error {
	return pgerror.New(pgcode.Syntax, msg)
}

// NewDependentObjectError creates a dependent object error.
func NewDependentObjectError(msg string) error {
	return pgerror.New(pgcode.DependentObjectsStillExist, msg)
}

// NewDependentObjectErrorWithHint creates a dependent object error with a hint
func NewDependentObjectErrorWithHint(msg string, hint string) error {
	err := pgerror.New(pgcode.DependentObjectsStillExist, msg)
	return errors.WithHint(err, hint)
}

// NewRangeUnavailableError creates an unavailable range error.
func NewRangeUnavailableError(
	rangeID roachpb.RangeID, origErr error, nodeIDs ...roachpb.NodeID,
) error {
	return pgerror.Newf(pgcode.RangeUnavailable,
		"key range id:%d is unavailable; missing nodes: %s. Original error: %v",
		rangeID, nodeIDs, origErr)
}

// NewWindowInAggError creates an error for the case when a window function is
// nested within an aggregate function.
func NewWindowInAggError() error {
	return pgerror.New(pgcode.Grouping,
		"window functions are not allowed in aggregate")
}

// NewAggInAggError creates an error for the case when an aggregate function is
// contained within another aggregate function.
func NewAggInAggError() error {
	return pgerror.New(pgcode.Grouping, "aggregate function calls cannot be nested")
}

// QueryCanceledError is an error representing query cancellation.
var QueryCanceledError = pgerror.New(
	pgcode.QueryCanceled, "query execution canceled")

// QueryTimeoutError is an error representing a query timeout.
var QueryTimeoutError = pgerror.New(
	pgcode.QueryCanceled, "query execution canceled due to statement timeout")

// IsOutOfMemoryError checks whether this is an out of memory error.
func IsOutOfMemoryError(err error) bool {
	return errHasCode(err, pgcode.OutOfMemory)
}

// IsUndefinedColumnError checks whether this is an undefined column error.
func IsUndefinedColumnError(err error) bool {
	return errHasCode(err, pgcode.UndefinedColumn)
}

func errHasCode(err error, code ...string) bool {
	pgCode := pgerror.GetPGCode(err)
	for _, c := range code {
		if pgCode == c {
			return true
		}
	}
	return false
}

// NewAlterTSTableError creates an error when ts table is being modified.
func NewAlterTSTableError(name string) error {
	return pgerror.Newf(pgcode.ObjectInUse, "table \"%s\" is being modified. Please wait for success", name)
}

// NewCreateTSTableError creates an error when ts table is being created.
func NewCreateTSTableError(name string) error {
	return pgerror.Newf(pgcode.ObjectInUse, "table \"%s\" is being created. Please wait for success", name)
}

// NewDropTSTableError creates an error when ts table is being dropped.
func NewDropTSTableError(name string) error {
	return pgerror.Newf(pgcode.ObjectInUse, "table \"%s\" is being dropped. Please wait for success", name)
}

// NewDropTSDBError creates an error when ts database is being dropped.
func NewDropTSDBError(name string) error {
	return pgerror.Newf(pgcode.ObjectInUse, "database \"%s\" is being dropped. Please wait for success", name)
}
