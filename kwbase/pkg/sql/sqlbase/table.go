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
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
	"golang.org/x/text/language"
)

// Max length of fixed and indefinite length type.
const (
	TSMaxFixedLen         = 1024
	TSMaxVariableLen      = 65534
	TSMaxVariableTupleLen = 255
	TSBOStringMaxLen      = 1023
)

// TableTypeMap identify what type the table is.
type TableTypeMap map[tree.TableType]int

// SanitizeVarFreeExpr verifies that an expression is valid, has the correct
// type and contains no variable expressions. It returns the type-checked and
// constant-folded expression.
func SanitizeVarFreeExpr(
	expr tree.Expr,
	expectedType *types.T,
	context string,
	semaCtx *tree.SemaContext,
	allowImpure bool,
	tsTypeCheck bool,
	colName string,
) (tree.TypedExpr, error) {
	if tree.ContainsVars(expr) {
		return nil, pgerror.Newf(pgcode.Syntax,
			"variable sub-expressions are not allowed in %s", context)
	}

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called from another context
	// which uses the properties field.
	defer semaCtx.Properties.Restore(semaCtx.Properties)

	// Ensure that the expression doesn't contain special functions.
	flags := tree.RejectSpecial
	if !allowImpure {
		flags |= tree.RejectImpureFunctions
	}
	semaCtx.Properties.Require(context, flags)
	var typedExpr tree.TypedExpr
	var err error
	if tsTypeCheck {
		switch v := expr.(type) {
		case *tree.NumVal:
			typedExpr, err = v.TSTypeCheck(colName, expectedType)
			if err != nil {
				return nil, err
			}
			return typedExpr, nil
		case *tree.StrVal:
			typedExpr, err = (*v).TSTypeCheck(colName, expectedType, semaCtx)
			if err != nil {
				return nil, err
			}
			return typedExpr, nil
		default:
			typedExpr, err = tree.TypeCheck(expr, semaCtx, expectedType)
			if err != nil {
				return nil, err
			}
		}
	} else {
		typedExpr, err = tree.TypeCheck(expr, semaCtx, expectedType)
		if err != nil {
			return nil, err
		}
	}

	actualType := typedExpr.ResolvedType()
	if !expectedType.Equivalent(actualType) && typedExpr != tree.DNull {
		// The expression must match the column type exactly unless it is a constant
		// NULL value.
		return nil, fmt.Errorf("expected %s expression to have type %s, but '%s' has type %s",
			context, expectedType, expr, actualType)
	}
	return typedExpr, nil
}

// ValidateColumnDefType returns an error if the type of a column definition is
// not valid. It is checked when a column is created or altered.
func ValidateColumnDefType(t *types.T, tableType tree.TableType) error {
	switch t.Family() {
	case types.StringFamily, types.CollatedStringFamily:
		if t.Family() == types.CollatedStringFamily {
			if _, err := language.Parse(t.Locale()); err != nil {
				return pgerror.Newf(pgcode.Syntax, `invalid locale %s`, t.Locale())
			}
		}

	case types.DecimalFamily:
		switch {
		case t.Precision() == 0 && t.Scale() > 0:
			// TODO (seif): Find right range for error message.
			return errors.New("invalid NUMERIC precision 0")
		case t.Precision() < t.Scale():
			return fmt.Errorf("NUMERIC scale %d must be between 0 and precision %d",
				t.Scale(), t.Precision())
		}

	case types.ArrayFamily:
		if t.ArrayContents().Family() == types.ArrayFamily {
			// Nested arrays are not supported as a column type.
			return errors.Errorf("nested array unsupported as column type: %s", t.String())
		}
		if err := types.CheckArrayElementType(t.ArrayContents()); err != nil {
			return err
		}
		return ValidateColumnDefType(t.ArrayContents(), tableType)
	case types.TimeFamily, types.TimeTZFamily, types.IntervalFamily, types.TimestampFamily, types.TimestampTZFamily:
		switch tableType {
		case tree.RelationalTable:
			if t.Precision() > 6 {
				return pgerror.Newf(pgcode.InvalidColumnDefinition, "precision %d out of range", t.Precision())
			}
		case tree.TimeseriesTable, tree.TemplateTable, tree.InstanceTable:
			switch t.Precision() {
			case 3, 6, 9:
				// These precisions are OK.
			default:
				return pgerror.Newf(pgcode.InvalidColumnDefinition, "precision %d out of range", t.Precision())
			}
		}
	case types.BitFamily, types.IntFamily, types.FloatFamily, types.BoolFamily, types.BytesFamily, types.DateFamily,
		types.INetFamily, types.JsonFamily, types.OidFamily, types.UuidFamily:
		// These types are OK.

	default:
		return pgerror.Newf(pgcode.InvalidTableDefinition,
			"value type %s cannot be used for table columns", t.String())
	}

	return nil
}

// MakeColumnDefDescs creates the column descriptor for a column, as well as the
// index descriptor if the column is a primary key or unique.
//
// If the column type *may* be SERIAL (or SERIAL-like), it is the
// caller's responsibility to call sql.processSerialInColumnDef() and
// sql.doCreateSequence() before MakeColumnDefDescs() to remove the
// SERIAL type and replace it with a suitable integer type and default
// expression.
//
// semaCtx can be nil if no default expression is used for the
// column.
//
// The DEFAULT expression is returned in TypedExpr form for analysis (e.g. recording
// sequence dependencies).
func MakeColumnDefDescs(
	d *tree.ColumnTableDef, semaCtx *tree.SemaContext, tableType tree.TableType,
) (*ColumnDescriptor, *IndexDescriptor, tree.TypedExpr, error) {
	if d.IsSerial {
		// To the reader of this code: if control arrives here, this means
		// the caller has not suitably called processSerialInColumnDef()
		// prior to calling MakeColumnDefDescs. The dependent sequences
		// must be created, and the SERIAL type eliminated, prior to this
		// point.
		return nil, nil, nil, pgerror.New(pgcode.FeatureNotSupported,
			"SERIAL cannot be used in this context")
	}

	if len(d.CheckExprs) > 0 {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, nil, nil, errors.New("unexpected column CHECK constraint")
	}
	if d.HasFKConstraint() {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, nil, nil, errors.New("unexpected column REFERENCED constraint")
	}

	col := &ColumnDescriptor{
		Name:     string(d.Name),
		Nullable: d.Nullable.Nullability != tree.NotNull && !d.PrimaryKey.IsPrimaryKey,
	}

	// Validate and assign column type.
	err := ValidateColumnDefType(d.Type, tree.RelationalTable)
	if err != nil {
		return nil, nil, nil, err
	}
	// bytes(n)/varbytes(n) are not allowed in relational mode.
	if d.Type.TypeEngine() == types.TIMESERIES.Mask() {
		return nil, nil, nil, pgerror.Newf(
			pgcode.WrongObjectType, "column %s: unsupported column type %s with length in relational table",
			d.Name, d.Type.Name())
	}
	col.Type = *d.Type

	var typedExpr tree.TypedExpr
	var isFuncDefault bool
	if d.HasDefaultExpr() {
		// Verify the default expression type is compatible with the column type
		// and does not contain invalid functions.
		var err error
		if typedExpr, err = SanitizeVarFreeExpr(
			d.DefaultExpr.Expr, d.Type, "DEFAULT", semaCtx, true /* allowImpure */, false, "",
		); err != nil {
			return nil, nil, nil, err
		}
		if funcExpr, ok := typedExpr.(*tree.FuncExpr); ok {
			isFuncDefault = true
			if strings.ToLower(funcExpr.Func.String()) == "unique_rowid" {
				actualType := funcExpr.ResolvedType()
				if !actualType.Equal(*d.Type) {
					return nil, nil, nil, errors.Newf("The type of the default expression does not match the type of the column (column %s)", col.Name)
				}
			}
		}

		// Keep the type checked expression so that the type annotation gets
		// properly stored, only if the default expression is not NULL.
		// Otherwise we want to keep the default expression nil.
		// todo: wsh, temporarily do not display type annotation for constant default value
		if typedExpr != tree.DNull {
			if isFuncDefault {
				d.DefaultExpr.Expr = typedExpr
				s := tree.Serialize(d.DefaultExpr.Expr)
				col.DefaultExpr = &s
			} else {
				s := tree.Serialize(d.DefaultExpr.Expr)
				col.DefaultExpr = &s
			}
		}
	}

	if d.IsComputed() {
		s := tree.Serialize(d.Computed.Expr)
		col.ComputeExpr = &s
	}

	var idx *IndexDescriptor
	if d.PrimaryKey.IsPrimaryKey || d.Unique {
		if !d.PrimaryKey.Sharded {
			idx = &IndexDescriptor{
				Unique:           true,
				ColumnNames:      []string{string(d.Name)},
				ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
			}
		} else {
			buckets, err := tree.EvalShardBucketCount(d.PrimaryKey.ShardBuckets)
			if err != nil {
				return nil, nil, nil, err
			}
			shardColName := GetShardColumnName([]string{string(d.Name)}, buckets)
			idx = &IndexDescriptor{
				Unique:           true,
				ColumnNames:      []string{shardColName, string(d.Name)},
				ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC, IndexDescriptor_ASC},
				Sharded: ShardedDescriptor{
					IsSharded:    true,
					Name:         shardColName,
					ShardBuckets: buckets,
					ColumnNames:  []string{string(d.Name)},
				},
			}
		}
		if d.UniqueConstraintName != "" {
			idx.Name = string(d.UniqueConstraintName)
		}
	}

	return col, idx, typedExpr, nil
}

// UpdateTimeColPrecision update time column precision.
// When tableType is relational, the type is not updated.
// When tableType is time_series and the precision of the time type is 0 and the precision is not specified,
// the precision is updated to 3.
func UpdateTimeColPrecision(oldTyp *types.T, tableType tree.TableType) *types.T {
	newTyp := oldTyp
	var precision int32
	switch tableType {
	case tree.TimeseriesTable, tree.TemplateTable, tree.InstanceTable:
		precision = 3
	default:
		return newTyp
	}
	if oldTyp.InternalType.Precision == 0 && !oldTyp.InternalType.TimePrecisionIsSet {
		switch oldTyp.Oid() {
		case oid.T_time:
			newTyp = types.MakeTime(precision)
		case oid.T_timetz:
			newTyp = types.MakeTimeTZ(precision)
		case oid.T_timestamp:
			newTyp = types.MakeTimestamp(precision)
		case oid.T_timestamptz:
			newTyp = types.MakeTimestampTZ(precision)
		case oid.T__time:
			n := types.MakeTime(precision)
			newTyp = types.MakeArray(n)
		case oid.T__timetz:
			n := types.MakeTimeTZ(precision)
			newTyp = types.MakeArray(n)
		case oid.T__timestamp:
			n := types.MakeTimestamp(precision)
			newTyp = types.MakeArray(n)
		case oid.T__timestamptz:
			n := types.MakeTimestampTZ(precision)
			newTyp = types.MakeArray(n)
		}
	}
	return newTyp
}

// MaxTSColumnNameLength represents the maximum length of timeseries table column name.
const MaxTSColumnNameLength = 128

// ContainsNonAlphaNumSymbol determines whether the string contains characters
// other than letters, numbers, and symbols. If yes, return true.
func ContainsNonAlphaNumSymbol(s string) bool {
	regex := regexp.MustCompile(`[^a-zA-Z0-9[:punct:]\s]`)
	return regex.MatchString(s)
}

// MakeTSColumnDefDescs creates a column descriptor of ts table.
func MakeTSColumnDefDescs(
	name string,
	typ *types.T,
	nullable bool,
	sde bool,
	columnType ColumnType,
	defaultExpr tree.Expr,
	semaCtx *tree.SemaContext,
) (*ColumnDescriptor, tree.TypedExpr, error) {
	// Parameter validation for ts column.
	if len(name) > MaxTSColumnNameLength {
		return nil, nil, NewTSNameOutOfLengthError("column", name, MaxTSColumnNameLength)
	}
	if ContainsNonAlphaNumSymbol(name) {
		return nil, nil, NewTSColInvalidError(name)
	}
	if typ.Width() == 0 {
		switch typ.Oid() {
		case oid.T_bpchar:
			typ = types.MakeChar(1)
		case types.T_nchar:
			typ = types.MakeNChar(1)
		case oid.T_bytea:
			typ = types.MakeBytes(1, typ.TypeEngine())
		case types.T_varbytea:
			typ = types.MakeVarBytes(254, typ.TypeEngine())
		case types.T_geometry:
			typ = types.MakeGeometry(1023)
		case oid.T_varchar:
			typ = types.MakeVarChar(254, typ.TypeEngine())
		case types.T_nvarchar:
			typ = types.MakeNVarChar(63)
		}
	}
	col := &ColumnDescriptor{
		Name:     name,
		Nullable: nullable,
		TsCol:    TSCol{ColumnType: columnType},
	}

	var typedExpr tree.TypedExpr
	var isFuncDefault bool
	if defaultExpr != nil {
		// Verify the default expression type is compatible with the column type
		// and does not contain invalid functions.
		var err error
		if typedExpr, err = SanitizeVarFreeExpr(
			defaultExpr, typ, "DEFAULT", semaCtx, true /* allowImpure */, true, col.Name,
		); err != nil {
			return nil, nil, err
		}

		if typedExpr == tree.DNull && !col.Nullable {
			colTyp := "tag"
			if columnType == ColumnType_TYPE_DATA {
				colTyp = "column"
			}
			return nil, nil, pgerror.Newf(pgcode.InvalidColumnDefinition,
				"default value NULL violates NOT NULL constraint on %s %s", colTyp, col.Name)
		}

		if funcExpr, ok := typedExpr.(*tree.FuncExpr); ok {
			isFuncDefault = true
			switch typ.Oid() {
			case oid.T_timestamp, oid.T_timestamptz:
				if strings.ToLower(funcExpr.Func.String()) != "now" {
					return nil, nil, pgerror.Newf(pgcode.InvalidColumnDefinition,
						"column %s with type %s can only be set now() as default function", col.Name, typ.SQLString())
				}
			default:
				return nil, nil, pgerror.Newf(pgcode.FeatureNotSupported,
					"column %s with type %s can only be set constant as default value", col.Name, typ.SQLString())
			}
		}
		if typedExpr != tree.DNull {
			if isFuncDefault {
				defaultExpr = typedExpr
				s := tree.Serialize(defaultExpr)
				col.DefaultExpr = &s
			} else {
				s := tree.Serialize(defaultExpr)
				col.DefaultExpr = &s
			}
		}
	}

	// Validate and assign column type.
	typ = UpdateTimeColPrecision(typ, tree.TimeseriesTable)
	err := ValidateColumnDefType(typ, tree.TimeseriesTable)
	if err != nil {
		return nil, nil, err
	}
	col.Type = *typ
	storageType := GetTSDataType(typ)
	if storageType == DataType_UNKNOWN {
		errMsg := "tag"

		if columnType == ColumnType_TYPE_DATA {
			errMsg = "column"
		}
		errType := typ.String()
		switch typ.Oid() {
		case oid.T_varchar:
			errType = "char/character varying"
		}
		return nil, nil, pgerror.Newf(
			pgcode.WrongObjectType, "column %s: unsupported %s type %s in timeseries table", col.Name, errMsg, errType)
	}
	col.TsCol.StorageType = storageType
	stLen := GetStorageLenForFixedLenTypes(storageType)
	if stLen != 0 {
		// we are done for non-string data type, return now
		col.TsCol.StorageLen = uint64(stLen)
		return col, typedExpr, nil
	}
	// for string types, the length needs to be calculated
	typeWidth := typ.Width()
	if storageType == DataType_NCHAR || storageType == DataType_NVARCHAR {
		typeWidth = typ.Width() * 4
	}
	if typeWidth == 0 {
		// in case we cannot derive len from input type
		switch storageType {
		case DataType_CHAR:
			col.TsCol.StorageLen = 1
		case DataType_BYTES:
			col.TsCol.StorageLen = 3
		case DataType_NCHAR:
			col.TsCol.StorageLen = 4
		case DataType_VARCHAR, DataType_NVARCHAR, DataType_VARBYTES:
			col.TsCol.StorageLen = TSMaxVariableTupleLen
		}
	} else {
		// we can derive length from input type
		switch storageType {
		// char or nchar
		case DataType_CHAR, DataType_NCHAR:
			if typeWidth < TSMaxFixedLen {
				col.TsCol.StorageLen = uint64(typeWidth) + 1
			} else if typ.Oid() == types.T_geometry {
				return nil, nil, pgerror.Newf(pgcode.WrongObjectType, "column %s: unsupported width of %s", col.Name, typ.Name())
			} else {
				return nil, nil, pgerror.Newf(pgcode.WrongObjectType, "column %s: unsupported width of %s", col.Name, storageType.String())
			}
		case DataType_BYTES:
			if typeWidth < TSMaxFixedLen {
				col.TsCol.StorageLen = uint64(typeWidth) + 2
			} else {
				return nil, nil, pgerror.Newf(pgcode.WrongObjectType, "column %s: unsupported width of %s", col.Name, storageType.String())
			}
		// varchar, nvarchar, varbytes
		case DataType_VARCHAR, DataType_NVARCHAR:
			if typeWidth <= TSMaxVariableLen {
				col.TsCol.StorageLen = uint64(typeWidth + 1)
			} else {
				return nil, nil, pgerror.Newf(pgcode.WrongObjectType, "column %s: unsupported width of %s", col.Name, storageType.String())
			}
		case DataType_VARBYTES:
			if typeWidth <= TSMaxVariableLen {
				col.TsCol.StorageLen = uint64(typeWidth)
			} else {
				return nil, nil, pgerror.Newf(pgcode.WrongObjectType, "column %s: unsupported width of %s", col.Name, storageType.String())
			}
		}
	}
	// dict encoding
	if sde && col.TsCol.StorageLen <= TSBOStringMaxLen {
		switch storageType {
		case DataType_CHAR:
			col.TsCol.StorageType = DataType_SDECHAR
		case DataType_VARCHAR:
			col.TsCol.StorageType = DataType_SDEVARCHAR
		default:
			// empty
		}
	}

	switch storageType {
	case DataType_VARCHAR, DataType_NVARCHAR, DataType_VARBYTES:
		col.TsCol.VariableLengthType = StorageIndependentPage
	default:
		col.TsCol.VariableLengthType = StorageTuple
	}

	return col, typedExpr, nil
}

// GetShardColumnName generates a name for the hidden shard column to be used to create a
// hash sharded index.
func GetShardColumnName(colNames []string, buckets int32) string {
	// We sort the `colNames` here because we want to avoid creating a duplicate shard
	// column if one already exists for the set of columns in `colNames`.
	sort.Strings(colNames)
	return strings.Join(
		append(append([]string{`kwdb_internal`}, colNames...), fmt.Sprintf(`shard_%v`, buckets)), `_`,
	)
}

// EncodeColumns is a version of EncodePartialIndexKey that takes ColumnIDs and
// directions explicitly. WARNING: unlike EncodePartialIndexKey, EncodeColumns
// appends directly to keyPrefix.
func EncodeColumns(
	columnIDs []ColumnID,
	directions directions,
	colMap map[ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	key = keyPrefix
	for colIdx, id := range columnIDs {
		val := findColumnValue(id, colMap, values)
		if val == tree.DNull {
			containsNull = true
		}

		dir, err := directions.get(colIdx)
		if err != nil {
			return nil, containsNull, err
		}

		if key, err = EncodeTableKey(key, val, dir); err != nil {
			return nil, containsNull, err
		}
	}
	return key, containsNull, nil
}

// GetColumnTypes returns the types of the columns with the given IDs.
func GetColumnTypes(desc *TableDescriptor, columnIDs []ColumnID) ([]types.T, error) {
	types := make([]types.T, len(columnIDs))
	for i, id := range columnIDs {
		col, err := desc.FindActiveColumnByID(id)
		if err != nil {
			return nil, err
		}
		types[i] = col.Type
	}
	return types, nil
}

// ConstraintType is used to identify the type of a constraint.
type ConstraintType string

const (
	// ConstraintTypePK identifies a PRIMARY KEY constraint.
	ConstraintTypePK ConstraintType = "PRIMARY KEY"
	// ConstraintTypeFK identifies a FOREIGN KEY constraint.
	ConstraintTypeFK ConstraintType = "FOREIGN KEY"
	// ConstraintTypeUnique identifies a FOREIGN constraint.
	ConstraintTypeUnique ConstraintType = "UNIQUE"
	// ConstraintTypeCheck identifies a CHECK constraint.
	ConstraintTypeCheck ConstraintType = "CHECK"
)

// ConstraintDetail describes a constraint.
type ConstraintDetail struct {
	Kind        ConstraintType
	Columns     []string
	Details     string
	Unvalidated bool

	// Only populated for PK and Unique Constraints.
	Index *IndexDescriptor

	// Only populated for FK Constraints.
	FK              *ForeignKeyConstraint
	ReferencedTable *TableDescriptor

	// Only populated for Check Constraints.
	CheckConstraint *TableDescriptor_CheckConstraint
}

type tableLookupFn func(ID) (*TableDescriptor, error)

// GetConstraintInfo returns a summary of all constraints on the table.
func (desc *TableDescriptor) GetConstraintInfo(
	ctx context.Context, txn *kv.Txn,
) (map[string]ConstraintDetail, error) {
	var tableLookup tableLookupFn
	if txn != nil {
		tableLookup = func(id ID) (*TableDescriptor, error) {
			return GetTableDescFromID(ctx, txn, id)
		}
	}
	return desc.collectConstraintInfo(tableLookup)
}

// GetConstraintInfoWithLookup returns a summary of all constraints on the
// table using the provided function to fetch a TableDescriptor from an ID.
func (desc *TableDescriptor) GetConstraintInfoWithLookup(
	tableLookup tableLookupFn,
) (map[string]ConstraintDetail, error) {
	return desc.collectConstraintInfo(tableLookup)
}

// CheckUniqueConstraints returns a non-nil error if a descriptor contains two
// constraints with the same name.
func (desc *TableDescriptor) CheckUniqueConstraints() error {
	_, err := desc.collectConstraintInfo(nil)
	return err
}

// if `tableLookup` is non-nil, provide a full summary of constraints, otherwise just
// check that constraints have unique names.
func (desc *TableDescriptor) collectConstraintInfo(
	tableLookup tableLookupFn,
) (map[string]ConstraintDetail, error) {
	info := make(map[string]ConstraintDetail)

	// Indexes provide PK, Unique and FK constraints.
	indexes := desc.AllNonDropIndexes()
	for _, index := range indexes {
		if index.ID == desc.PrimaryIndex.ID {
			if _, ok := info[index.Name]; ok {
				return nil, pgerror.Newf(pgcode.DuplicateObject,
					"duplicate constraint name: %q", index.Name)
			}
			colHiddenMap := make(map[ColumnID]bool, len(desc.Columns))
			for i := range desc.Columns {
				col := &desc.Columns[i]
				colHiddenMap[col.ID] = col.Hidden
			}
			// Don't include constraints against only hidden columns.
			// This prevents the auto-created rowid primary key index from showing up
			// in show constraints.
			hidden := true
			for _, id := range index.ColumnIDs {
				if !colHiddenMap[id] {
					hidden = false
					break
				}
			}
			if hidden {
				continue
			}
			detail := ConstraintDetail{Kind: ConstraintTypePK}
			detail.Columns = index.ColumnNames
			detail.Index = index
			info[index.Name] = detail
		} else if index.Unique {
			if _, ok := info[index.Name]; ok {
				return nil, pgerror.Newf(pgcode.DuplicateObject,
					"duplicate constraint name: %q", index.Name)
			}
			detail := ConstraintDetail{Kind: ConstraintTypeUnique}
			detail.Columns = index.ColumnNames
			detail.Index = index
			info[index.Name] = detail
		}
	}

	fks := desc.AllActiveAndInactiveForeignKeys()
	for _, fk := range fks {
		if _, ok := info[fk.Name]; ok {
			return nil, pgerror.Newf(pgcode.DuplicateObject,
				"duplicate constraint name: %q", fk.Name)
		}
		detail := ConstraintDetail{Kind: ConstraintTypeFK}
		// Constraints in the Validating state are considered Unvalidated for this purpose
		detail.Unvalidated = fk.Validity != ConstraintValidity_Validated
		var err error
		detail.Columns, err = desc.NamesForColumnIDs(fk.OriginColumnIDs)
		if err != nil {
			return nil, err
		}
		detail.FK = fk

		if tableLookup != nil {
			other, err := tableLookup(fk.ReferencedTableID)
			if err != nil {
				return nil, errors.NewAssertionErrorWithWrappedErrf(err,
					"error resolving table %d referenced in foreign key",
					log.Safe(fk.ReferencedTableID))
			}
			referencedColumnNames, err := other.NamesForColumnIDs(fk.ReferencedColumnIDs)
			if err != nil {
				return nil, err
			}
			detail.Details = fmt.Sprintf("%s.%v", other.Name, referencedColumnNames)
			detail.ReferencedTable = other
		}
		info[fk.Name] = detail
	}

	for _, c := range desc.AllActiveAndInactiveChecks() {
		if _, ok := info[c.Name]; ok {
			return nil, pgerror.Newf(pgcode.DuplicateObject,
				"duplicate constraint name: %q", c.Name)
		}
		detail := ConstraintDetail{Kind: ConstraintTypeCheck}
		// Constraints in the Validating state are considered Unvalidated for this purpose
		detail.Unvalidated = c.Validity != ConstraintValidity_Validated
		detail.CheckConstraint = c
		detail.Details = c.Expr
		if tableLookup != nil {
			colsUsed, err := c.ColumnsUsed(desc)
			if err != nil {
				return nil, errors.NewAssertionErrorWithWrappedErrf(err,
					"error computing columns used in check constraint %q", c.Name)
			}
			for _, colID := range colsUsed {
				col, err := desc.FindColumnByID(colID)
				if err != nil {
					return nil, errors.NewAssertionErrorWithWrappedErrf(err,
						"error finding column %d in table %s", log.Safe(colID), desc.Name)
				}
				detail.Columns = append(detail.Columns, col.Name)
			}
		}
		info[c.Name] = detail
	}
	return info, nil
}

// IsValidOriginIndex returns whether the index can serve as an origin index for a foreign
// key constraint with the provided set of originColIDs.
func (idx *IndexDescriptor) IsValidOriginIndex(originColIDs ColumnIDs) bool {
	return ColumnIDs(idx.ColumnIDs).HasPrefix(originColIDs)
}

// IsValidReferencedIndex returns whether the index can serve as a referenced index for a foreign
// key constraint with the provided set of referencedColumnIDs.
func (idx *IndexDescriptor) IsValidReferencedIndex(referencedColIDs ColumnIDs) bool {
	return idx.Unique && ColumnIDs(idx.ColumnIDs).Equals(referencedColIDs)
}

// FindFKReferencedIndex finds the first index in the supplied referencedTable
// that can satisfy a foreign key of the supplied column ids.
func FindFKReferencedIndex(
	referencedTable *TableDescriptor, referencedColIDs ColumnIDs,
) (*IndexDescriptor, error) {
	// Search for a unique index on the referenced table that matches our foreign
	// key columns.
	if referencedTable.PrimaryIndex.IsValidReferencedIndex(referencedColIDs) {
		return &referencedTable.PrimaryIndex, nil
	}
	// If the PK doesn't match, find the index corresponding to the referenced column.
	for i := range referencedTable.Indexes {
		idx := &referencedTable.Indexes[i]
		if idx.IsValidReferencedIndex(referencedColIDs) {
			return idx, nil
		}
	}
	return nil, pgerror.Newf(
		pgcode.ForeignKeyViolation,
		"there is no unique constraint matching given keys for referenced table %s",
		referencedTable.Name,
	)
}

// FindFKOriginIndex finds the first index in the supplied originTable
// that can satisfy an outgoing foreign key of the supplied column ids.
func FindFKOriginIndex(
	originTable *TableDescriptor, originColIDs ColumnIDs,
) (*IndexDescriptor, error) {
	// Search for an index on the origin table that matches our foreign
	// key columns.
	if originTable.PrimaryIndex.IsValidOriginIndex(originColIDs) {
		return &originTable.PrimaryIndex, nil
	}
	// If the PK doesn't match, find the index corresponding to the origin column.
	for i := range originTable.Indexes {
		idx := &originTable.Indexes[i]
		if idx.IsValidOriginIndex(originColIDs) {
			return idx, nil
		}
	}
	return nil, pgerror.Newf(
		pgcode.ForeignKeyViolation,
		"there is no index matching given keys for referenced table %s",
		originTable.Name,
	)
}

// ConditionalGetTableDescFromTxn validates that the supplied TableDescriptor
// matches the one currently stored in kv. This simulates a CPut and returns a
// ConditionFailedError on mismatch. We don't directly use CPut with protos
// because the marshaling is not guaranteed to be stable and also because it's
// sensitive to things like missing vs default values of fields.
func ConditionalGetTableDescFromTxn(
	ctx context.Context, txn *kv.Txn, expectation *TableDescriptor,
) (*roachpb.Value, error) {
	key := MakeDescMetadataKey(expectation.ID)
	existingKV, err := txn.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	var existing *Descriptor
	if existingKV.Value != nil {
		existing = &Descriptor{}
		if err := existingKV.Value.GetProto(existing); err != nil {
			return nil, errors.Wrapf(err,
				"decoding current table descriptor value for id: %d", expectation.ID)
		}
		existing.Table(existingKV.Value.Timestamp)
	}
	wrapped := WrapDescriptor(expectation)
	if !existing.Equal(wrapped) {
		return nil, &roachpb.ConditionFailedError{ActualValue: existingKV.Value}
	}
	return existingKV.Value, nil
}

// GetTableDescriptorWithErr works the same as GetTableDescriptor but returns an error
// message instead of downtime when an error occurs.
func GetTableDescriptorWithErr(
	kvDB *kv.DB, database string, table string,
) (*TableDescriptor, error) {
	// log.VEventf(context.TODO(), 2, "GetTableDescriptor %q %q", database, table)
	// testutil, so we pass settings as nil for both database and table name keys.
	dKey := NewDatabaseKey(database)
	ctx := context.TODO()
	gr, err := kvDB.Get(ctx, dKey.Key())
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		return nil, errors.Newf("database %s missing", database)
	}
	dbDescID := ID(gr.ValueInt())
	if dbDescID == InvalidID {
		return nil, NewDropTSDBError(database)
	}

	tKey := NewPublicTableKey(dbDescID, table)
	gr, err = kvDB.Get(ctx, tKey.Key())
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		return nil, errors.Newf("table %s missing", table)
	}

	descKey := MakeDescMetadataKey(ID(gr.ValueInt()))
	desc := &Descriptor{}
	ts, err := kvDB.GetProtoTs(ctx, descKey, desc)
	if err != nil || (*desc == Descriptor{}) {
		log.Fatalf(ctx, "proto with id %d missing. err: %v", gr.ValueInt(), err)
	}
	tableDesc := desc.Table(ts)
	if tableDesc == nil {
		return nil, errors.Newf("table %s missing", table)
	}
	err = tableDesc.MaybeFillInDescriptor(ctx, kvDB)
	if err != nil {
		log.Fatalf(ctx, "failure to fill in descriptor. err: %v", err)
	}
	return tableDesc, nil
}

// GetTSDataType returns types.T mapped datatype.
func GetTSDataType(typ *types.T) DataType {
	if typ.InternalType.TypeEngine != 0 && !typ.IsTypeEngineSet(types.TIMESERIES) {
		return DataType_UNKNOWN
	}
	switch typ.Name() {
	case "timestamp":
		switch typ.Precision() {
		case 0, 3:
			return DataType_TIMESTAMP
		case 6:
			return DataType_TIMESTAMP_MICRO
		case 9:
			return DataType_TIMESTAMP_NANO
		default:
			return DataType_TIMESTAMP
		}
		return DataType_TIMESTAMP
	case "int2":
		return DataType_SMALLINT
	case "int4":
		return DataType_INT
	case "int":
		return DataType_BIGINT
	case "float4", "decimal":
		return DataType_FLOAT
	case "float":
		return DataType_DOUBLE
	case "bool":
		return DataType_BOOL
	case "char", "geometry":
		return DataType_CHAR
	case "nchar":
		return DataType_NCHAR
	case "varchar":
		return DataType_VARCHAR
	case "nvarchar":
		return DataType_NVARCHAR
	case "varbytes":
		return DataType_VARBYTES
	case "timestamptz":
		switch typ.Precision() {
		case 0, 3:
			return DataType_TIMESTAMPTZ
		case 6:
			return DataType_TIMESTAMPTZ_MICRO
		case 9:
			return DataType_TIMESTAMPTZ_NANO
		default:
			return DataType_TIMESTAMPTZ
		}
		return DataType_TIMESTAMPTZ
	default:
		return DataType_UNKNOWN
	}
}

// GetStorageLenForFixedLenTypes returns KColumn's storage length
func GetStorageLenForFixedLenTypes(dataType DataType) uint32 {
	switch dataType {
	case DataType_TIMESTAMP, DataType_TIMESTAMP_MICRO, DataType_TIMESTAMP_NANO,
		DataType_TIMESTAMPTZ, DataType_TIMESTAMPTZ_MICRO, DataType_TIMESTAMPTZ_NANO:
		return 8
	case DataType_BOOL:
		return 1
	case DataType_SMALLINT:
		return 2
	case DataType_INT:
		return 4
	case DataType_BIGINT:
		return 8
	case DataType_FLOAT:
		return 4
	case DataType_DOUBLE:
		return 8
	default:
		return 0
	}
}

// Insert insert value in map.
func (t TableTypeMap) Insert(tableType tree.TableType) {
	v, ok := t[tableType]
	if ok {
		t[tableType] = v + 1
	} else {
		t[tableType] = 1
	}
}

// IncludeTSTable check if there has TS table.
func (t TableTypeMap) IncludeTSTable() bool {
	_, ok1 := t[tree.TemplateTable]
	_, ok2 := t[tree.InstanceTable]
	_, ok3 := t[tree.TimeseriesTable]
	if ok1 || ok2 || ok3 {
		return true
	}
	return false
}

// HasMultiTSTable check if there has mutil TS table.
func (t TableTypeMap) HasMultiTSTable() bool {
	v1, _ := t[tree.TemplateTable]
	v2, _ := t[tree.InstanceTable]
	v3, _ := t[tree.TimeseriesTable]
	if v1+v2+v3 > 1 {
		return true
	}
	return false
}

// HasStable check if there has a SUPER_TABLE.
func (t TableTypeMap) HasStable() bool {
	_, ok := t[tree.TemplateTable]
	return ok
}

// HasCtable check if there has a CHILD_TABLE.
func (t TableTypeMap) HasCtable() bool {
	_, ok := t[tree.InstanceTable]
	return ok
}

// HasGtable check if there has a TIMESERIES_TABLE.
func (t TableTypeMap) HasGtable() bool {
	_, ok := t[tree.TimeseriesTable]
	return ok
}

// HasRtable check if there has a RELATIONAL_TABLE.
func (t TableTypeMap) HasRtable() bool {
	_, ok := t[tree.RelationalTable]
	return ok
}

// GetTableDescriptorUseTxn works the same as GetTableDescriptor
// but use the specified txn.
func GetTableDescriptorUseTxn(
	txn *kv.Txn, database string, schema string, table string,
) (*TableDescriptor, error) {
	// log.VEventf(context.TODO(), 2, "GetTableDescriptor %q %q", database, table)
	// testutil, so we pass settings as nil for both database and table name keys.
	dKey := NewDatabaseKey(database)
	ctx := context.TODO()
	gr, err := txn.Get(ctx, dKey.Key())
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		return nil, errors.Errorf("database %s missing", database)
	}
	dbDescID := ID(gr.ValueInt())
	if dbDescID == InvalidID {
		return nil, NewDropTSDBError(database)
	}
	scKey := NewSchemaKey(dbDescID, schema)
	gr, err = txn.Get(ctx, scKey.Key())
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		return nil, errors.Errorf("schema %s missing", schema)
	}
	scDescID := ID(gr.ValueInt())
	tKey := NewTableKey(dbDescID, scDescID, table)
	gr, err = txn.Get(ctx, tKey.Key())
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		return nil, errors.Errorf("table %s missing", table)
	}

	descKey := MakeDescMetadataKey(ID(gr.ValueInt()))
	desc := &Descriptor{}
	ts, err := txn.GetProtoTs(ctx, descKey, desc)
	if err != nil || (*desc == Descriptor{}) {
		return nil, errors.Errorf("proto with id %d missing. err: %v", gr.ValueInt(), err)
	}
	tableDesc := desc.Table(ts)
	if tableDesc == nil {
		return nil, errors.Newf("table %s missing", table)
	}
	err = tableDesc.MaybeFillInDescriptor(ctx, txn)
	if err != nil {
		return nil, errors.Errorf("failure to fill in descriptor. err: %v", err)
	}
	return tableDesc, nil
}
