// Copyright 2020 The Cockroach Authors.
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

// Package schema is to avoid a circular dependency on pkg/sql/row and pkg/sql.
// It can be removed once schemas can be resolved to a protobuf.
package schema

import (
	"bytes"
	"context"
	"fmt"
	"go/constant"
	"strconv"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// ResolveNameByID resolves a schema's name based on db and schema id.
// TODO(sqlexec): this should return the descriptor instead if given an ID.
// Instead, we have to rely on a scan of the kv table.
// TODO(sqlexec): this should probably be cached.
func ResolveNameByID(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID, schemaID sqlbase.ID,
) (string, error) {
	// Fast-path for public schema, to avoid hot lookups.
	if schemaID == keys.PublicSchemaID {
		return string(tree.PublicSchemaName), nil
	}
	schemas, err := GetForDatabase(ctx, txn, dbID)
	if err != nil {
		return "", err
	}
	if schema, ok := schemas[schemaID]; ok {
		return schema, nil
	}
	return "", errors.Newf("unable to resolve schema id %d for db %d", schemaID, dbID)
}

// GetForDatabase looks up and returns all available
// schema ids to names for a given database.
func GetForDatabase(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID,
) (map[sqlbase.ID]string, error) {
	log.Eventf(ctx, "fetching all schema descriptor IDs for %d", dbID)

	nameKey := sqlbase.NewSchemaKey(dbID, "" /* name */).Key()
	kvs, err := txn.Scan(ctx, nameKey, nameKey.PrefixEnd(), 0 /* maxRows */)
	if err != nil {
		return nil, err
	}

	// Always add public schema ID.
	// TODO(solon): This can be removed in 20.2, when this is always written.
	// In 20.1, in a migrating state, it may be not included yet.
	ret := make(map[sqlbase.ID]string, len(kvs)+1)
	ret[sqlbase.ID(keys.PublicSchemaID)] = tree.PublicSchema

	for _, kv := range kvs {
		id := sqlbase.ID(kv.ValueInt())
		if _, ok := ret[id]; ok {
			continue
		}
		_, _, name, err := sqlbase.DecodeNameMetadataKey(kv.Key)
		if err != nil {
			return nil, err
		}
		ret[id] = name
	}
	return ret, nil
}

// RemoveFKForBackReferenceFromTable edits the supplied originTableDesc to
// remove the foreign key constraint that corresponds to the supplied
// backreference, which is a member of the supplied referencedTableDesc.
func RemoveFKForBackReferenceFromTable(
	originTableDesc *sqlbase.MutableTableDescriptor,
	backref *sqlbase.ForeignKeyConstraint,
	referencedTableDesc *sqlbase.TableDescriptor,
) error {
	matchIdx := -1
	for i, fk := range originTableDesc.OutboundFKs {
		if fk.ReferencedTableID == referencedTableDesc.ID && fk.Name == backref.Name {
			// We found a match! We want to delete it from the list now.
			matchIdx = i
			break
		}
	}
	if matchIdx == -1 {
		// There was no match: no back reference in the referenced table that
		// matched the foreign key constraint that we were trying to delete.
		// This really shouldn't happen...
		return errors.AssertionFailedf("there was no foreign key constraint "+
			"for backreference %v on table %q", backref, originTableDesc.Name)
	}
	// Delete our match.
	originTableDesc.OutboundFKs = append(
		originTableDesc.OutboundFKs[:matchIdx],
		originTableDesc.OutboundFKs[matchIdx+1:]...)
	return nil
}

// RemoveFKBackReferenceFromTable edits the supplied referencedTableDesc to
// remove the foreign key backreference that corresponds to the supplied fk,
// which is a member of the supplied originTableDesc.
func RemoveFKBackReferenceFromTable(
	referencedTableDesc *sqlbase.MutableTableDescriptor,
	fkName string,
	originTableDesc *sqlbase.TableDescriptor,
) error {
	matchIdx := -1
	for i, backref := range referencedTableDesc.InboundFKs {
		if backref.OriginTableID == originTableDesc.ID && backref.Name == fkName {
			// We found a match! We want to delete it from the list now.
			matchIdx = i
			break
		}
	}
	if matchIdx == -1 {
		// There was no match: no back reference in the referenced table that
		// matched the foreign key constraint that we were trying to delete.
		// This really shouldn't happen...
		return errors.AssertionFailedf("there was no foreign key backreference "+
			"for constraint %q on table %q", fkName, originTableDesc.Name)
	}
	// Delete our match.
	referencedTableDesc.InboundFKs = append(referencedTableDesc.InboundFKs[:matchIdx], referencedTableDesc.InboundFKs[matchIdx+1:]...)
	return nil
}

// MaybeCreateAndAddShardCol adds a new hidden computed shard column (or its mutation) to
// `desc`, if one doesn't already exist for the given index column set and number of shard
// buckets.
func MaybeCreateAndAddShardCol(
	shardBuckets int, desc *sqlbase.MutableTableDescriptor, colNames []string, isNewTable bool,
) (col *sqlbase.ColumnDescriptor, created bool, err error) {
	shardCol, err := makeShardColumnDesc(colNames, shardBuckets)
	if err != nil {
		return nil, false, err
	}
	existingShardCol, dropped, err := desc.FindColumnByName(tree.Name(shardCol.Name))
	if err == nil && !dropped {
		// TODO(ajwerner): In what ways is existingShardCol allowed to differ from
		// the newly made shardCol? Should there be some validation of
		// existingShardCol?
		return existingShardCol, false, nil
	}
	columnIsUndefined := sqlbase.IsUndefinedColumnError(err)
	if err != nil && !columnIsUndefined {
		return nil, false, err
	}
	if columnIsUndefined || dropped {
		if isNewTable {
			desc.AddColumn(shardCol)
		} else {
			desc.AddColumnMutation(shardCol, sqlbase.DescriptorMutation_ADD)
		}
		created = true
	}
	return shardCol, created, nil
}

// makeShardColumnDesc returns a new column descriptor for a hidden computed shard column
// based on all the `colNames`.
func makeShardColumnDesc(colNames []string, buckets int) (*sqlbase.ColumnDescriptor, error) {
	col := &sqlbase.ColumnDescriptor{
		Hidden:   true,
		Nullable: false,
		Type:     *types.Int4,
	}
	col.Name = sqlbase.GetShardColumnName(colNames, int32(buckets))
	col.ComputeExpr = makeHashShardComputeExpr(colNames, buckets)
	return col, nil
}

// makeHashShardComputeExpr creates the serialized computed expression for a hash shard
// column based on the column names and the number of buckets. The expression will be
// of the form:
//
//	mod(fnv32(colNames[0]::STRING)+fnv32(colNames[1])+...,buckets)
func makeHashShardComputeExpr(colNames []string, buckets int) *string {
	unresolvedFunc := func(funcName string) tree.ResolvableFunctionReference {
		return tree.ResolvableFunctionReference{
			FunctionReference: &tree.UnresolvedName{
				NumParts: 1,
				Parts:    tree.NameParts{funcName},
			},
		}
	}
	hashedColumnExpr := func(colName string) tree.Expr {
		return &tree.FuncExpr{
			Func: unresolvedFunc("fnv32"),
			Exprs: tree.Exprs{
				// NB: We have created the hash shard column as NOT NULL so we need
				// to coalesce NULLs into something else. There's a variety of different
				// reasonable choices here. We could pick some outlandish value, we
				// could pick a zero value for each type, or we can do the simple thing
				// we do here, however the empty string seems pretty reasonable. At worst
				// we'll have a collision for every combination of NULLable string
				// columns. That seems just fine.
				&tree.CoalesceExpr{
					Name: "COALESCE",
					Exprs: tree.Exprs{
						&tree.CastExpr{
							Type: types.String,
							Expr: &tree.ColumnItem{ColumnName: tree.Name(colName)},
						},
						tree.NewDString(""),
					},
				},
			},
		}
	}

	// Construct an expression which is the sum of all of the casted and hashed
	// columns.
	var expr tree.Expr
	for i := len(colNames) - 1; i >= 0; i-- {
		c := colNames[i]
		if expr == nil {
			expr = hashedColumnExpr(c)
		} else {
			expr = &tree.BinaryExpr{
				Left:     hashedColumnExpr(c),
				Operator: tree.Plus,
				Right:    expr,
			}
		}
	}
	str := tree.Serialize(&tree.FuncExpr{
		Func: unresolvedFunc("mod"),
		Exprs: tree.Exprs{
			expr,
			tree.NewDInt(tree.DInt(buckets)),
		},
	})
	return &str
}

// GenerateMaybeDuplicateNameForCheckConstraint generates a name, the given check
// constraint expression, which may already be taken by another object in the table
// descriptor.
func GenerateMaybeDuplicateNameForCheckConstraint(
	desc *sqlbase.MutableTableDescriptor, expr tree.Expr,
) (string, error) {
	var nameBuf bytes.Buffer
	nameBuf.WriteString("check")

	if err := iterColDescriptorsInExpr(desc, expr, func(c *sqlbase.ColumnDescriptor) error {
		nameBuf.WriteByte('_')
		nameBuf.WriteString(c.Name)
		return nil
	}); err != nil {
		return "", err
	}
	return nameBuf.String(), nil
}

// GenerateNameForCheckConstraint generates a unique name for the given check constraint.
func GenerateNameForCheckConstraint(
	desc *sqlbase.MutableTableDescriptor, expr tree.Expr, inuseNames map[string]struct{},
) (string, error) {

	name, err := GenerateMaybeDuplicateNameForCheckConstraint(desc, expr)
	if err != nil {
		return "", err
	}
	// If generated name isn't unique, attempt to add a number to the end to
	// get a unique name.
	if _, ok := inuseNames[name]; ok {
		i := 1
		for {
			appended := fmt.Sprintf("%s%d", name, i)
			if _, ok := inuseNames[appended]; !ok {
				name = appended
				break
			}
			i++
		}
	}
	if inuseNames != nil {
		inuseNames[name] = struct{}{}
	}

	return name, nil
}

// MakeShardCheckConstraintDef creates
func MakeShardCheckConstraintDef(
	desc *sqlbase.MutableTableDescriptor, buckets int, shardCol *sqlbase.ColumnDescriptor,
) (*tree.CheckConstraintTableDef, error) {
	values := &tree.Tuple{}
	for i := 0; i < buckets; i++ {
		const negative = false
		values.Exprs = append(values.Exprs, tree.NewNumVal(
			constant.MakeInt64(int64(i)),
			strconv.Itoa(i),
			negative))
	}
	return &tree.CheckConstraintTableDef{
		Expr: &tree.ComparisonExpr{
			Operator: tree.In,
			Left: &tree.ColumnItem{
				ColumnName: tree.Name(shardCol.Name),
			},
			Right: values,
		},
		Hidden: true,
	}, nil
}

func iterColDescriptorsInExpr(
	desc *sqlbase.MutableTableDescriptor, rootExpr tree.Expr, f func(*sqlbase.ColumnDescriptor) error,
) error {
	_, err := tree.SimpleVisit(rootExpr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			// Not a VarName, don't do anything to this node.
			return true, expr, nil
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return false, nil, err
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return true, expr, nil
		}

		col, dropped, err := desc.FindColumnByName(c.ColumnName)
		if err != nil || dropped {
			return false, nil, pgerror.Newf(pgcode.InvalidTableDefinition,
				"column %q not found, referenced in %q",
				c.ColumnName, rootExpr)
		}

		if err := f(col); err != nil {
			return false, nil, err
		}
		return false, expr, err
	})

	return err
}

// ReplaceVars replaces the occurrences of column names in an expression with
// dummies containing their type, so that they may be typechecked. It returns
// this new expression tree alongside a set containing the ColumnID of each
// column seen in the expression.
func ReplaceVars(
	desc *sqlbase.MutableTableDescriptor, expr tree.Expr,
) (tree.Expr, map[sqlbase.ColumnID]struct{}, error) {
	colIDs := make(map[sqlbase.ColumnID]struct{})
	newExpr, err := tree.SimpleVisit(expr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			// Not a VarName, don't do anything to this node.
			return true, expr, nil
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return false, nil, err
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return true, expr, nil
		}

		col, dropped, err := desc.FindColumnByName(c.ColumnName)
		if err != nil || dropped {
			return false, nil, fmt.Errorf("column %q not found for constraint %q",
				c.ColumnName, expr.String())
		}
		colIDs[col.ID] = struct{}{}
		// Convert to a dummy node of the correct type.
		return false, &dummyColumnItem{typ: &col.Type, name: c.ColumnName}, nil
	})
	return newExpr, colIDs, err
}
