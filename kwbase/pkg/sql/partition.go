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
// See the Mulan PSL v2 for more details

package sql

import (
	"context"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// partitionVisitor is a tree.Visitor that replaces minvalue and maxvalue
type partitionVisitor struct{}

// VisitPre implements the tree.Visitor interface.
func (v partitionVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	return true, expr
}

// VisitPost implements the tree.Visitor interface.
func (partitionVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// NewPartitioningDescriptor creates a PartitioningDescriptor from a
// tree.PartitionBy.
// For TsTable, partition only support HashPoint and because ts table eDistribute, table partition is due to table preDistribute
// For RlTable, partition only support table, not support index
// Parameters:
//   - ctx: Context object for managing request lifecycle
//   - evalCtx: Expression evaluation context for processing partition expressions
//   - tableDesc: Table descriptor containing complete table metadata
//   - indexDesc: Index descriptor specifying the index structure to partition
//   - partBy: Partition definition object containing partition type and rules
//
// Returns:
//   - sqlbase.PartitioningDescriptor: Constructed partitioning descriptor
//   - error: Returns error if partition definition is invalid or conflicts with index structure
func NewPartitioningDescriptor(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.MutableTableDescriptor,
	indexDesc *sqlbase.IndexDescriptor,
	partBy *tree.PartitionBy,
) (sqlbase.PartitioningDescriptor, error) {
	var partDesc sqlbase.PartitioningDescriptor
	if partBy == nil {
		return partDesc, nil
	}
	if tableDesc.IsTSTable() {
		if partBy.HashPoint == nil {
			return partDesc, nil
		}
		for _, hashPoint := range partBy.HashPoint {
			if len(hashPoint.HashPoints) != 0 {
				partDesc.HashPoint = append(partDesc.HashPoint, sqlbase.PartitioningDescriptor_HashPoint{
					Name:       hashPoint.Name.String(),
					HashPoints: hashPoint.HashPoints,
				})
			} else {
				if hashPoint.From >= hashPoint.To {
					return partDesc, errors.Errorf("from point must smaller than to point")
				}
				partDesc.HashPoint = append(partDesc.HashPoint, sqlbase.PartitioningDescriptor_HashPoint{
					Name:      hashPoint.Name.String(),
					FromPoint: hashPoint.From,
					ToPoint:   hashPoint.To,
				})
			}
		}
	} else {
		partDesc.NumColumns = uint32(len(partBy.Fields))
		partitioningString := func() string {
			partCols := append([]string(nil), indexDesc.ColumnNames[:0]...)
			for _, p := range partBy.Fields {
				partCols = append(partCols, string(p))
			}
			return strings.Join(partCols, ", ")
		}
		var cols []sqlbase.ColumnDescriptor
		for i := 0; i < len(partBy.Fields); i++ {
			// Gets column descriptor and verifies column name consistency
			col, err := tableDesc.FindActiveColumnByName(indexDesc.ColumnNames[i])
			if err != nil {
				return partDesc, err
			}
			cols = append(cols, *col)
			if string(partBy.Fields[i]) != col.Name {
				n := i + 1
				return partDesc, pgerror.Newf(pgcode.Syntax,
					"declared partition columns (%s) do not match first %d columns in index being partitioned (%s)",
					partitioningString(), n, strings.Join(indexDesc.ColumnNames[:n], ", "))
			}
		}
		// Sets hash partitioning flag
		if partBy.IsHash {
			partDesc.IsHash = true
		}
		// Builds list partitioning descriptors
		for _, l := range partBy.List {
			p := sqlbase.PartitioningDescriptor_List{
				Name: string(l.Name),
			}
			// Encodes partition key values and handles subpartitions
			for _, expr := range l.Exprs {
				encodedTuple, err := encodePartitionTupleValues(evalCtx, expr, cols)
				if err != nil {
					return partDesc, errors.Wrapf(err, "PARTITION %s", p.Name)
				}
				p.Values = append(p.Values, encodedTuple)
			}
			partDesc.List = append(partDesc.List, p)
		}
		// Builds range partitioning descriptors
		for _, r := range partBy.Range {
			p := sqlbase.PartitioningDescriptor_Range{
				Name: string(r.Name),
			}
			var err error
			// Encodes range start value
			p.FromInclusive, err = encodePartitionTupleValues(evalCtx, &tree.Tuple{Exprs: r.From}, cols)
			if err != nil {
				return partDesc, errors.Wrapf(err, "PARTITION %s", p.Name)
			}
			// Encodes range end value
			p.ToExclusive, err = encodePartitionTupleValues(evalCtx, &tree.Tuple{Exprs: r.To}, cols)
			if err != nil {
				return partDesc, errors.Wrapf(err, "PARTITION %s", p.Name)
			}
			partDesc.Range = append(partDesc.Range, p)
		}
	}
	return partDesc, nil
}

// encodePartitionTupleValues encodes a tuple of partition key values.
func encodePartitionTupleValues(
	evalCtx *tree.EvalContext, maybeTuple tree.Expr, cols []sqlbase.ColumnDescriptor,
) ([]byte, error) {
	// Convert input expression to a tuple if it isn't already one
	maybeTuple, _ = tree.WalkExpr(partitionVisitor{}, maybeTuple)

	tuple, ok := maybeTuple.(*tree.Tuple)
	if !ok {
		tuple = &tree.Tuple{Exprs: []tree.Expr{maybeTuple}}
	}

	// Ensure the number of values matches the number of partition columns
	if len(tuple.Exprs) != len(cols) {
		return nil, errors.Errorf("partition has %d columns but %d values were supplied",
			len(cols), len(tuple.Exprs))
	}

	var value, scratch []byte
	for i, expr := range tuple.Exprs {
		expr = tree.StripParens(expr)
		// Validate expression is variable-free and compatible with column type
		var semaCtx tree.SemaContext
		typedExpr, err := sqlbase.SanitizeVarFreeExpr(expr, &cols[i].Type, "partition",
			&semaCtx, false /* allowImpure */, false, cols[i].Name)
		if err != nil {
			return nil, err
		}

		// Evaluate the expression to a datum
		datum, err := typedExpr.Eval(evalCtx)
		if err != nil {
			return nil, errors.Wrap(err, typedExpr.String())
		}

		// Encode the value using table value encoding
		value, err = sqlbase.EncodeTableValue(
			value, sqlbase.ColumnID(encoding.NoColumnID), datum, scratch,
		)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}
