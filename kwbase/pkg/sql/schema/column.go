// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package schema

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/lib/pq/oid"
)

// dummyColumnItem is used in MakeCheckConstraint to construct an expression
// that can be both type-checked and examined for variable expressions.
type dummyColumnItem struct {
	typ *types.T
	// name is only used for error-reporting.
	name tree.Name
}

// String implements the Stringer interface.
func (d *dummyColumnItem) String() string {
	return tree.AsString(d)
}

// Format implements the NodeFormatter interface.
func (d *dummyColumnItem) Format(ctx *tree.FmtCtx) {
	d.name.Format(ctx)
}

// Walk implements the Expr interface.
func (d *dummyColumnItem) Walk(_ tree.Visitor) tree.Expr {
	return d
}

// TypeCheck implements the Expr interface.
func (d *dummyColumnItem) TypeCheck(_ *tree.SemaContext, desired *types.T) (tree.TypedExpr, error) {
	return d, nil
}

// Eval implements the TypedExpr interface.
func (*dummyColumnItem) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic("dummyColumnItem.Eval() is undefined")
}

// ResolvedType implements the TypedExpr interface.
func (d *dummyColumnItem) ResolvedType() *types.T {
	return d.typ
}

// CheckTSColValidity checks the column options in DDL statements for TimeSeries tables
func CheckTSColValidity(d *tree.ColumnTableDef) error {
	makeTsErr := func(msg string) error {
		return pgerror.Newf(pgcode.FeatureNotSupported, "%s is not supported in timeseries table", msg)
	}
	if d.Type.Family() == types.DecimalFamily || d.Type.Oid() == oid.T_bytea {
		return pgerror.Newf(pgcode.WrongObjectType, "column %s: unsupported column type %s in timeseries table", d.Name, d.Type.Name())
	}
	if d.Type.Family() == types.CollatedStringFamily || d.Type.Oid() == types.T_citext {
		return pgerror.Newf(pgcode.WrongObjectType, "column %s: unsupported column type %s in timeseries table", d.Name, d.Type.Name())
	}

	if d.IsSerial {
		return makeTsErr("serial column")
	}
	if d.PrimaryKey.IsPrimaryKey {
		return makeTsErr("primary key")
	}
	if d.Unique {
		return makeTsErr("unique constraint")
	}
	//if d.HasDefaultExpr() {
	//	return makeTsErr("default Expr")
	//}
	if len(d.CheckExprs) > 0 {
		// Should never happen since `HoistConstraints` moves these to table level
		return makeTsErr("check constraint")
	}
	if d.HasFKConstraint() {
		// Should never happen since `HoistConstraints` moves these to table level
		return makeTsErr("referenced constraint")
	}
	if d.IsComputed() {
		return makeTsErr("computed column")
	}
	return nil
}

// ValidateComputedColumn checks that a computed column satisfies a number of
// validity constraints, for instance, that it typechecks.
func ValidateComputedColumn(
	desc *sqlbase.MutableTableDescriptor, d *tree.ColumnTableDef, semaCtx *tree.SemaContext,
) error {
	if d.HasDefaultExpr() {
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"computed column %s cannot have default values", d.Name,
		)
	}

	dependencies := make(map[sqlbase.ColumnID]struct{})
	// First, check that no column in the expression is a computed column.
	if err := iterColDescriptorsInExpr(desc, d.Computed.Expr, func(c *sqlbase.ColumnDescriptor) error {
		if c.IsComputed() {
			return pgerror.Newf(pgcode.InvalidTableDefinition,
				"computed column %s cannot reference other computed columns", d.Name)
		}
		dependencies[c.ID] = struct{}{}

		return nil
	}); err != nil {
		return err
	}

	// TODO(justin,bram): allow depending on columns like this. We disallow it
	// for now because cascading changes must hook into the computed column
	// update path.
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		for _, id := range fk.OriginColumnIDs {
			if _, ok := dependencies[id]; !ok {
				// We don't depend on this column.
				continue
			}
			for _, action := range []sqlbase.ForeignKeyReference_Action{
				fk.OnDelete,
				fk.OnUpdate,
			} {
				switch action {
				case sqlbase.ForeignKeyReference_CASCADE,
					sqlbase.ForeignKeyReference_SET_NULL,
					sqlbase.ForeignKeyReference_SET_DEFAULT:
					return pgerror.New(pgcode.InvalidTableDefinition,
						"computed columns cannot reference non-restricted FK columns")
				}
			}
		}
	}

	// Replace column references with typed dummies to allow typechecking.
	replacedExpr, _, err := ReplaceVars(desc, d.Computed.Expr)
	if err != nil {
		return err
	}

	if _, err := sqlbase.SanitizeVarFreeExpr(
		replacedExpr, d.Type, "computed column", semaCtx, false /* allowImpure */, false, string(d.Name),
	); err != nil {
		return err
	}

	return nil
}
