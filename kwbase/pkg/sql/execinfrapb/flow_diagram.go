// Copyright 2018 The Cockroach Authors.
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

package execinfrapb

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
	"github.com/gogo/protobuf/types"
)

type diagramCellType interface {
	// summary produces a title and an arbitrary number of lines that describe a
	// "cell" in a diagram node (input sync, processor core, or output router).
	summary() (title string, details []string)
}

func (ord *Ordering) diagramString() string {
	var buf bytes.Buffer
	for i, c := range ord.Columns {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, "@%d", c.ColIdx+1)
		if c.Direction == Ordering_Column_DESC {
			buf.WriteByte('-')
		} else {
			buf.WriteByte('+')
		}
	}
	return buf.String()
}

func colListStr(cols []uint32) string {
	var buf bytes.Buffer
	for i, c := range cols {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, "@%d", c+1)
	}
	return buf.String()
}

// summary implements the diagramCellType interface.
func (*NoopCoreSpec) summary() (string, []string) {
	return "No-op", []string{}
}

// summary implements the diagramCellType interface.
func (*TSNoopSpec) summary() (string, []string) {
	return "No-op", []string{}
}

// summary implements the diagramCellType interface.
func (mts *MetadataTestSenderSpec) summary() (string, []string) {
	return "MetadataTestSender", []string{mts.ID}
}

// summary implements the diagramCellType interface.
func (*MetadataTestReceiverSpec) summary() (string, []string) {
	return "MetadataTestReceiver", []string{}
}

// summary implements the diagramCellType interface.
func (*TsInsertProSpec) summary() (string, []string) {
	return "TsInsert", []string{}
}

// summary implements the diagramCellType interface.
func (*TsCreateTableProSpec) summary() (string, []string) {
	return "TsCreateTable", []string{}
}

// summary implements the diagramCellType interface.
func (*TsProSpec) summary() (string, []string) {
	return "TsProSpec", []string{}
}

// summary implements the diagramCellType interface.
func (*TsDeleteProSpec) summary() (string, []string) {
	return "TsDelete", []string{}
}

// summary implements the diagramCellType interface.
func (*TsAlterProSpec) summary() (string, []string) {
	return "TsAlter", []string{}
}

// summary implements the diagramCellType interface.
func (*TsTagUpdateProSpec) summary() (string, []string) {
	return "TsTagUpdate", []string{}
}

// summary implements the diagramCellType interface.
func (v *ValuesCoreSpec) summary() (string, []string) {
	var bytes uint64
	for _, b := range v.RawBytes {
		bytes += uint64(len(b))
	}
	detail := fmt.Sprintf("%s (%d chunks)", humanize.IBytes(bytes), len(v.RawBytes))
	return "Values", []string{detail}
}

// summary implements the diagramCellType interface.
func (t *TsInsertSelSpec) summary() (string, []string) {
	var buffer bytes.Buffer
	for i, col := range t.Cols {
		if i == 0 {
			buffer.WriteString(fmt.Sprintf("@%v", col))
		} else {
			buffer.WriteString(fmt.Sprintf(", @%v", col))
		}
	}
	var tableType string
	tableType = tree.TableTypeName(tree.TableType(t.TableType))

	details := []string{
		fmt.Sprintf("TableID: %v", t.TargetTableId),
		fmt.Sprintf("DBID: %v", t.DbId),
		fmt.Sprintf("TableName:%v", t.ChildName),
		fmt.Sprintf("TableType:%v", tableType),
		fmt.Sprintf("InsertCols: %v", buffer.String()),
	}

	return "TSInsertSelect", details
}

// summary implements the diagramCellType interface.
func (m *TSSynchronizerSpec) summary() (string, []string) {
	details := []string{
		fmt.Sprintf("Degree: %v", m.Degree),
	}

	return "TSSynchronizer", details
}

// summary implements the diagramCellType interface.
func (a *AggregatorSpec) summary() (string, []string) {
	details := make([]string, 0, len(a.Aggregations)+1)
	if len(a.GroupCols) > 0 {
		details = append(details, colListStr(a.GroupCols))
	}
	if len(a.OrderedGroupCols) > 0 {
		details = append(details, fmt.Sprintf("Ordered: %s", colListStr(a.OrderedGroupCols)))
	} else if a.GroupWindowId >= 0 {
		details = append(details, fmt.Sprint("GroupWindowForceOrdered: true"))
	}

	for _, agg := range a.Aggregations {
		var buf bytes.Buffer
		buf.WriteString(agg.Func.String())
		buf.WriteByte('(')

		if agg.Distinct {
			buf.WriteString("DISTINCT ")
		}
		buf.WriteString(colListStr(agg.ColIdx))
		for _, c := range agg.Arguments {
			buf.WriteByte(',')
			fmt.Fprintf(&buf, "%s", c)
		}
		buf.WriteByte(')')
		if agg.FilterColIdx != nil {
			fmt.Fprintf(&buf, " FILTER @%d", *agg.FilterColIdx+1)
		}

		details = append(details, buf.String())
	}
	if a.AggPushDown {
		details = append(details, fmt.Sprintf("AggPushDown: %v", a.AggPushDown))
	}

	return "Aggregator", details
}

func indexDetail(desc *sqlbase.TableDescriptor, indexIdx uint32) string {
	index := "primary"
	if indexIdx > 0 {
		index = desc.Indexes[indexIdx-1].Name
	}
	return fmt.Sprintf("%s@%s", desc.Name, index)
}

// getTSSpanStr get ts span string
func getTSSpanStr(src []TsSpan) string {
	var spanStr strings.Builder
	spanStr.WriteString("Spans: ")
	spanStr.WriteString(fmt.Sprintf("%v - %v", src[0].FromTimeStamp, src[0].ToTimeStamp))

	if len(src) > 1 {
		spanStr.WriteString(fmt.Sprintf(" and %d other", len(src)-1))
	}

	if len(src) > 2 {
		spanStr.WriteString("s") // pluralize the 'other'
	}

	return spanStr.String()
}

// summary implements the diagramCellType interface.
func (tr *TableReaderSpec) summary() (string, []string) {
	details := []string{indexDetail(&tr.Table, tr.IndexIdx)}

	if len(tr.Spans) > 0 {
		// only show the first span
		idx, _, _ := tr.Table.FindIndexByIndexIdx(int(tr.IndexIdx))
		valDirs := sqlbase.IndexKeyValDirs(idx)

		var spanStr strings.Builder
		spanStr.WriteString("Spans: ")
		spanStr.WriteString(sqlbase.PrettySpan(valDirs, tr.Spans[0].Span, 2))

		if len(tr.Spans) > 1 {
			spanStr.WriteString(fmt.Sprintf(" and %d other", len(tr.Spans)-1))
		}

		if len(tr.Spans) > 2 {
			spanStr.WriteString("s") // pluralize the 'other'
		}

		details = append(details, spanStr.String())
	}

	return "TableReader", details
}

// summary implements the diagramCellType interface.
func (tr *TSReaderSpec) summary() (string, []string) {
	details := []string{
		fmt.Sprintf("TableID: %v", tr.TableID),
	}

	if tr.OffsetOpt {
		details = append(details, "Offset optimize")
	}

	if tr.OrderedScan {
		details = append(details, fmt.Sprintf("Ordered scan: %v", tr.OrderedScan))
	}

	if len(tr.TsSpans) > 0 {
		details = append(details, getTSSpanStr(tr.TsSpans))
	}

	if tr.Aggregator != nil {
		name, res := tr.Aggregator.summary()
		details = append(details, name+":")
		details = append(details, res...)
	}

	if tr.AggregatorPost != nil {
		res := tr.AggregatorPost.summary()
		details = append(details, "AggregatorPost:")
		details = append(details, res...)
	}
	if tr.Sorter != nil {
		res := tr.Sorter.diagramString()
		details = append(details, "Sorter:")
		details = append(details, res)
	}

	if tr.Reverse != nil {
		details = append(details, fmt.Sprintf("Ordered direction: %v", *tr.Reverse))
	}

	return "TableReader", details
}

// summary implements the diagramCellType interface.
func (tr *TSTagReaderSpec) summary() (string, []string) {
	var res []string
	res = append(res, fmt.Sprintf("TableID: %v", tr.TableID))
	res = append(res, fmt.Sprintf("mode: %v", tr.AccessMode.String()))
	if tr.OnlyTag {
		res = append(res, "OnlyScanTag")
	}
	for _, val := range tr.PrimaryTags {
		res = append(res, fmt.Sprintf("ptag [%v]: %v", val.Colid, val.TagValues))
	}
	if len(tr.TagIndexIDs) > 0 {
		res = append(res, fmt.Sprintf("UnionType: %v", tr.UnionType))
		for _, val := range tr.TagIndexIDs {
			res = append(res, fmt.Sprintf("TagIndexIDs  %v", val))
		}
		for _, tagIndexes := range tr.TagIndexes {
			for _, val := range tagIndexes.TagValues {
				res = append(res, fmt.Sprintf("TagIndex [%v]: %v", val.Colid, val.TagValues))
			}
		}
	}

	return "TagReader", res
}

// summary implements the diagramCellType interface.
func (ts *TSStatisticReaderSpec) summary() (string, []string) {
	details := []string{}
	if len(ts.TsSpans) > 0 {
		details = append(details, getTSSpanStr(ts.TsSpans))
	}
	details = append(details, fmt.Sprintf("tableID %d", ts.TableID))
	for idx := range ts.AggTypes {
		var buf bytes.Buffer
		buf.WriteString(AggregatorSpec_Func_name[ts.AggTypes[idx]])
		buf.WriteByte('(')
		for i, val := range ts.ParamIdx[idx].Param {
			if i > 0 {
				buf.WriteString(",")
			}
			switch val.Typ {
			case TSStatisticReaderSpec_ParamInfo_colID:
				buf.WriteString(colListStr([]uint32{uint32(val.Value)}))
			case TSStatisticReaderSpec_ParamInfo_const:
				buf.WriteString(fmt.Sprintf("%v", val.Value))
			}
		}
		buf.WriteByte(')')

		details = append(details, fmt.Sprintf("Agg[%d]: %v", idx, buf.String()))
	}
	if ts.Scalar {
		details = append(details, "scalar")
	}
	if ts.LastRowOpt {
		details = append(details, "LastRowOpt")
	}
	return "StatisticReader", details
}

// summary implements the diagramCellType interface.
func (jr *JoinReaderSpec) summary() (string, []string) {
	details := make([]string, 0, 4)
	if jr.Type != sqlbase.InnerJoin {
		details = append(details, joinTypeDetail(jr.Type))
	}
	details = append(details, indexDetail(&jr.Table, jr.IndexIdx))
	if jr.LookupColumns != nil {
		details = append(details, fmt.Sprintf("Lookup join on: %s", colListStr(jr.LookupColumns)))
	}
	if !jr.OnExpr.Empty() {
		details = append(details, fmt.Sprintf("ON %s", jr.OnExpr))
	}
	return "JoinReader", details
}

func joinTypeDetail(joinType sqlbase.JoinType) string {
	typeStr := strings.Replace(joinType.String(), "_", " ", -1)
	if joinType == sqlbase.IntersectAllJoin || joinType == sqlbase.ExceptAllJoin {
		return fmt.Sprintf("Type: %s", typeStr)
	}
	return fmt.Sprintf("Type: %s JOIN", typeStr)
}

// summary implements the diagramCellType interface.
func (hj *HashJoinerSpec) summary() (string, []string) {
	name := "HashJoiner"
	if hj.Type.IsSetOpJoin() {
		name = "HashSetOp"
	}

	details := make([]string, 0, 4)

	if hj.Type != sqlbase.InnerJoin {
		details = append(details, joinTypeDetail(hj.Type))
	}
	if len(hj.LeftEqColumns) > 0 {
		details = append(details, fmt.Sprintf(
			"left(%s)=right(%s)",
			colListStr(hj.LeftEqColumns), colListStr(hj.RightEqColumns),
		))
	}
	if !hj.OnExpr.Empty() {
		details = append(details, fmt.Sprintf("ON %s", hj.OnExpr))
	}
	if hj.MergedColumns {
		details = append(details, fmt.Sprintf("Merged columns: %d", len(hj.LeftEqColumns)))
	}

	return name, details
}

// summary implements the diagramCellType interface.
func (blj *BatchLookupJoinerSpec) summary() (string, []string) {
	name := "BatchLookupJoiner"
	if blj.Type.IsSetOpJoin() {
		name = "BatchLookupJoinHashSetOp"
	}

	details := make([]string, 0, 4)

	if blj.Type != sqlbase.InnerJoin {
		details = append(details, joinTypeDetail(blj.Type))
	}
	if len(blj.LeftEqColumns) > 0 {
		details = append(details, fmt.Sprintf(
			"left(%s)=right(%s)",
			colListStr(blj.LeftEqColumns), colListStr(blj.RightEqColumns),
		))
	}
	if !blj.OnExpr.Empty() {
		details = append(details, fmt.Sprintf("ON %s", blj.OnExpr))
	}
	if blj.MergedColumns {
		details = append(details, fmt.Sprintf("Merged columns: %d", len(blj.LeftEqColumns)))
	}

	return name, details
}

func orderedJoinDetails(
	joinType sqlbase.JoinType, left, right Ordering, onExpr Expression,
) []string {
	details := make([]string, 0, 3)

	if joinType != sqlbase.InnerJoin {
		details = append(details, joinTypeDetail(joinType))
	}
	details = append(details, fmt.Sprintf(
		"left(%s)=right(%s)", left.diagramString(), right.diagramString(),
	))

	if !onExpr.Empty() {
		details = append(details, fmt.Sprintf("ON %s", onExpr))
	}

	return details
}

// summary implements the diagramCellType interface.
func (mj *MergeJoinerSpec) summary() (string, []string) {
	name := "MergeJoiner"
	if mj.Type.IsSetOpJoin() {
		name = "MergeSetOp"
	}
	return name, orderedJoinDetails(mj.Type, mj.LeftOrdering, mj.RightOrdering, mj.OnExpr)
}

// summary implements the diagramCellType interface.
func (irj *InterleavedReaderJoinerSpec) summary() (string, []string) {
	// As of right now, we only plan InterleaveReaderJoiner with two
	// tables.
	tables := irj.Tables[:2]
	details := make([]string, 0, len(tables)*6+3)
	for i, table := range tables {
		// left or right label
		var tableLabel string
		if i == 0 {
			tableLabel = "Left"
		} else if i == 1 {
			tableLabel = "Right"
		}
		details = append(details, tableLabel)
		// table@index name
		details = append(details, indexDetail(&table.Desc, table.IndexIdx))
		// Post process (filters, projections, renderExprs, limits/offsets)
		details = append(details, table.Post.summaryWithPrefix(fmt.Sprintf("%s ", tableLabel))...)
	}
	details = append(details, "Joiner")
	details = append(
		details, orderedJoinDetails(irj.Type, tables[0].Ordering, tables[1].Ordering, irj.OnExpr)...,
	)
	return "InterleaveReaderJoiner", details
}

// summary implements the diagramCellType interface.
func (zj *ZigzagJoinerSpec) summary() (string, []string) {
	name := "ZigzagJoiner"
	tables := zj.Tables
	details := make([]string, 0, len(tables)+1)
	for i, table := range tables {
		details = append(details, fmt.Sprintf(
			"Side %d: %s", i, indexDetail(&table, zj.IndexOrdinals[i]),
		))
	}
	if !zj.OnExpr.Empty() {
		details = append(details, fmt.Sprintf("ON %s", zj.OnExpr))
	}
	return name, details
}

// summary implements the diagramCellType interface.
func (s *SorterSpec) summary() (string, []string) {
	details := []string{s.OutputOrdering.diagramString()}
	if s.OrderingMatchLen != 0 {
		details = append(details, fmt.Sprintf("match len: %d", s.OrderingMatchLen))
	}
	return "Sorter", details
}

// summary implements the diagramCellType interface.
func (bf *BackfillerSpec) summary() (string, []string) {
	details := []string{
		bf.Table.Name,
		fmt.Sprintf("Type: %s", bf.Type.String()),
	}
	return "Backfiller", details
}

// summary implements the diagramCellType interface.
func (d *DistinctSpec) summary() (string, []string) {
	details := []string{
		colListStr(d.DistinctColumns),
	}
	if len(d.OrderedColumns) > 0 {
		details = append(details, fmt.Sprintf("Ordered: %s", colListStr(d.OrderedColumns)))
	}
	return "Distinct", details
}

// summary implements the diagramCellType interface.
func (o *OrdinalitySpec) summary() (string, []string) {
	return "Ordinality", []string{}
}

// summary implements the diagramCellType interface.
func (d *ProjectSetSpec) summary() (string, []string) {
	var details []string
	for _, expr := range d.Exprs {
		details = append(details, expr.String())
	}
	return "ProjectSet", details
}

// summary implements the diagramCellType interface.
func (s *SamplerSpec) summary() (string, []string) {
	details := []string{fmt.Sprintf("SampleSize: %d", s.SampleSize)}
	for _, sk := range s.Sketches {
		details = append(details, fmt.Sprintf("Stat: %s", colListStr(sk.Columns)))
	}

	return "Sampler", details
}

// summary implements the diagramCellType interface.
func (s *TSSamplerSpec) summary() (string, []string) {
	details := []string{fmt.Sprintf("SampleSize: %d", *s.SampleSize)}
	for _, sk := range s.Sketches {
		var columns []uint32
		columns = sk.ColIdx
		details = append(details, fmt.Sprintf("Stat: %s", colListStr(columns)))
	}

	return "TsSampler", details
}

// summary implements the diagramCellType interface.
func (s *SampleAggregatorSpec) summary() (string, []string) {
	details := []string{
		fmt.Sprintf("SampleSize: %d", s.SampleSize),
	}
	for _, sk := range s.Sketches {
		s := fmt.Sprintf("Stat: %s", colListStr(sk.Columns))
		if sk.GenerateHistogram {
			s = fmt.Sprintf("%s (%d buckets)", s, sk.HistogramMaxBuckets)
		}
		details = append(details, s)
	}

	return "SampleAggregator", details
}

func (is *InputSyncSpec) summary(showTypes bool) (string, []string) {
	typs := make([]string, 0, len(is.ColumnTypes)+1)
	if showTypes {
		for _, typ := range is.ColumnTypes {
			typs = append(typs, typ.Name())
		}
	}
	switch is.Type {
	case InputSyncSpec_UNORDERED:
		return "unordered", typs
	case InputSyncSpec_ORDERED:
		return "ordered", append(typs, is.Ordering.diagramString())
	default:
		return "unknown", []string{}
	}
}

// summary implements the diagramCellType interface.
func (r *LocalPlanNodeSpec) summary() (string, []string) {
	return fmt.Sprintf("local %s %d", *r.Name, *r.RowSourceIdx), []string{}
}

// summary implements the diagramCellType interface.
func (r *OutputRouterSpec) summary() (string, []string) {
	switch r.Type {
	case OutputRouterSpec_PASS_THROUGH:
		return "", []string{}
	case OutputRouterSpec_MIRROR:
		return "mirror", []string{}
	case OutputRouterSpec_BY_HASH:
		return "by hash", []string{colListStr(r.HashColumns)}
	case OutputRouterSpec_BY_RANGE:
		return "by range", []string{}
	default:
		return "unknown", []string{}
	}
}

// summary implements the diagramCellType interface.
func (post *PostProcessSpec) summary() []string {
	return post.summaryWithPrefix("")
}

// summary implements the diagramCellType interface.
func (post *TSPostProcessSpec) summary() []string {
	var res []string
	if post.Filter != "" {
		res = append(res, fmt.Sprintf("Filter: %s", post.Filter))
	}

	if post.Projection {
		outputColumns := "None"
		if len(post.OutputColumns) > 0 {
			outputColumns = colListStr(post.OutputColumns)
		}
		res = append(res, fmt.Sprintf("Out: %s", outputColumns))
	}

	if len(post.Renders) > 0 {
		var buf bytes.Buffer
		buf.WriteString("Render: ")
		for i, expr := range post.Renders {
			if i > 0 {
				buf.WriteString(", ")
			}
			// Remove any spaces in the expression (makes things more compact
			// and it's easier to visually separate expressions).
			buf.WriteString(strings.Replace(expr, " ", "", -1))
		}
		res = append(res, buf.String())
	}

	for _, val := range post.OutputTypes {
		res = append(res, fmt.Sprintf("outtype: %s", val.String()))
	}
	if post.Limit != 0 || post.Offset != 0 {
		var buf bytes.Buffer
		if post.Limit != 0 {
			fmt.Fprintf(&buf, "Limit %d", post.Limit)
		}
		if post.Offset != 0 {
			if buf.Len() != 0 {
				buf.WriteByte(' ')
			}
			fmt.Fprintf(&buf, "Offset %d", post.Offset)
		}
		res = append(res, buf.String())
	}
	return res
}

// prefix is prepended to every line outputted to disambiguate processors
// (namely InterleavedReaderJoiner) that have multiple PostProcessors.
func (post *PostProcessSpec) summaryWithPrefix(prefix string) []string {
	var res []string
	if !post.Filter.Empty() {
		res = append(res, fmt.Sprintf("%sFilter: %s", prefix, post.Filter))
	}
	if post.Projection {
		outputColumns := "None"
		if len(post.OutputColumns) > 0 {
			outputColumns = colListStr(post.OutputColumns)
		}
		res = append(res, fmt.Sprintf("%sOut: %s", prefix, outputColumns))
	}
	if len(post.RenderExprs) > 0 {
		var buf bytes.Buffer
		buf.WriteString(fmt.Sprintf("%sRender: ", prefix))
		for i, expr := range post.RenderExprs {
			if i > 0 {
				buf.WriteString(", ")
			}
			// Remove any spaces in the expression (makes things more compact
			// and it's easier to visually separate expressions).
			buf.WriteString(strings.Replace(expr.String(), " ", "", -1))
		}
		res = append(res, buf.String())
	}
	if post.Limit != 0 || post.Offset != 0 {
		var buf bytes.Buffer
		if post.Limit != 0 {
			fmt.Fprintf(&buf, "%sLimit %d", prefix, post.Limit)
		}
		if post.Offset != 0 {
			if buf.Len() != 0 {
				buf.WriteByte(' ')
			}
			fmt.Fprintf(&buf, "%sOffset %d", prefix, post.Offset)
		}
		res = append(res, buf.String())
	}
	return res
}

// summary implements the diagramCellType interface.
func (c *ReadImportDataSpec) summary() (string, []string) {
	ss := make([]string, 0, len(c.Uri))
	for _, s := range c.Uri {
		ss = append(ss, s)
	}
	return "ReadImportData", ss
}

// summary implements the diagramCellType interface.
func (s *CSVWriterSpec) summary() (string, []string) {
	return "CSVWriter", []string{s.Destination}
}

// summary implements the diagramCellType interface.
func (s *BulkRowWriterSpec) summary() (string, []string) {
	return "BulkRowWriterSpec", []string{}
}

// summary implements the diagramCellType interface.
func (w *WindowerSpec) summary() (string, []string) {
	details := make([]string, 0, len(w.WindowFns))
	if len(w.PartitionBy) > 0 {
		details = append(details, fmt.Sprintf("PARTITION BY: %s", colListStr(w.PartitionBy)))
	}
	for _, windowFn := range w.WindowFns {
		var buf bytes.Buffer
		if windowFn.Func.WindowFunc != nil {
			buf.WriteString(windowFn.Func.WindowFunc.String())
		} else {
			buf.WriteString(windowFn.Func.AggregateFunc.String())
		}
		buf.WriteByte('(')
		buf.WriteString(colListStr(windowFn.ArgsIdxs))
		buf.WriteByte(')')
		if len(windowFn.Ordering.Columns) > 0 {
			buf.WriteString(" (ORDER BY ")
			buf.WriteString(windowFn.Ordering.diagramString())
			buf.WriteByte(')')
		}
		details = append(details, buf.String())
	}

	return "Windower", details
}

// summary implements the diagramCellType interface.
func (s *ChangeAggregatorSpec) summary() (string, []string) {
	var details []string
	for _, watch := range s.Watches {
		details = append(details, watch.Span.String())
	}
	return "ChangeAggregator", details
}

// summary implements the diagramCellType interface.
func (s *ChangeFrontierSpec) summary() (string, []string) {
	return "ChangeFrontier", []string{}
}

type diagramCell struct {
	Title   string   `json:"title"`
	Details []string `json:"details"`
}

type diagramProcessor struct {
	NodeIdx int           `json:"nodeIdx"`
	Inputs  []diagramCell `json:"inputs"`
	Core    diagramCell   `json:"core"`
	Outputs []diagramCell `json:"outputs"`
	StageID int32         `json:"stage"`

	processorID int32
}

type diagramEdge struct {
	SourceProc   int      `json:"sourceProc"`
	SourceOutput int      `json:"sourceOutput"`
	DestProc     int      `json:"destProc"`
	DestInput    int      `json:"destInput"`
	Stats        []string `json:"stats,omitempty"`

	streamID StreamID
}

// FlowDiagram is a plan diagram that can be made into a URL.
type FlowDiagram interface {
	// ToURL generates the json data for a flow diagram and a URL which encodes the
	// diagram.
	ToURL() (string, url.URL, error)

	// AddSpans adds stats extracted from the input spans to the diagram.
	AddSpans([]tracing.RecordedSpan, []string)

	// MakeDistsqlJSON construct json in explain analyze
	MakeDistsqlJSON(url string) (string, error)
}

type diagramData struct {
	SQL        string             `json:"sql"`
	NodeNames  []string           `json:"nodeNames"`
	Processors []diagramProcessor `json:"processors"`
	Edges      []diagramEdge      `json:"edges"`

	flowID FlowID
}

type diagramDataDist struct {
	SQL        string             `json:"sql"`
	NodeNames  []string           `json:"nodeNames"`
	Processors []diagramProcessor `json:"processors"`
	Edges      []diagramEdge      `json:"edges"`

	flowID     FlowID
	EncodePlan string `json:"encodePlan"`
}

var _ FlowDiagram = &diagramData{}

// ToURL implements the FlowDiagram interface.
func (d diagramData) ToURL() (string, url.URL, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(d); err != nil {
		return "", url.URL{}, err
	}
	return encodeJSONToURL(buf)
}

// MakeDistsqlJSON construct json in explain analyze
func (d diagramData) MakeDistsqlJSON(url string) (string, error) {
	da := diagramDataDist{d.SQL, d.NodeNames, d.Processors, d.Edges, d.flowID, url}
	json, err := json.MarshalIndent(da, "\n", "  ")
	if err != nil {
		return "", err
	}
	return string(json), nil
}

// AddSpans implements the FlowDiagram interface.
func (d *diagramData) AddSpans(spans []tracing.RecordedSpan, responseSpan []string) {
	processorStats, streamStats := extractStatsFromSpans(d.flowID, spans)
	for i := range d.Processors {
		if statDetails, ok := processorStats[int(d.Processors[i].processorID)]; ok {
			d.Processors[i].Core.Details = append(d.Processors[i].Core.Details, statDetails...)
		} else if d.Processors[i].processorID == -1 {
			d.Processors[i].Core.Details = append(d.Processors[i].Core.Details, responseSpan...)
		}
	}
	for i := range d.Edges {
		d.Edges[i].Stats = streamStats[int(d.Edges[i].streamID)]
	}
}

func generateDiagramData(
	sql string, flows []FlowSpec, nodeNames []string, showInputTypes bool,
) (FlowDiagram, error) {
	d := &diagramData{
		SQL:       sql,
		NodeNames: nodeNames,
	}
	if len(flows) > 0 {
		d.flowID = flows[0].FlowID
		for i := 1; i < len(flows); i++ {
			if flows[i].FlowID != d.flowID {
				return nil, errors.AssertionFailedf("flow ID mismatch within a diagram")
			}
		}
	}

	// inPorts maps streams to their "destination" attachment point. Only DestProc
	// and DestInput are set in each diagramEdge value.
	inPorts := make(map[StreamID]diagramEdge)
	syncResponseNode := -1

	i := 0
	pIdx := 0
	for n := range flows {
		if len(flows[n].TsProcessors) != 0 {
			for _, p := range flows[n].TsProcessors {
				proc := diagramProcessor{NodeIdx: len(flows) + i}
				proc.Core.Title, proc.Core.Details = p.Core.GetValue().(diagramCellType).summary()
				proc.Core.Title += fmt.Sprintf("/%d", p.ProcessorID)
				proc.processorID = p.ProcessorID
				proc.Core.Details = append(proc.Core.Details, p.Post.summary()...)

				if len(p.Input) > 1 || (len(p.Input) == 1 && len(p.Input[0].Streams) > 1) {
					proc.Inputs = make([]diagramCell, len(p.Input))
					for i, s := range p.Input {
						proc.Inputs[i].Title, proc.Inputs[i].Details = s.summary(showInputTypes)
					}
				} else {
					proc.Inputs = []diagramCell{}
				}

				// Add entries in the map for the inputs.
				for i, input := range p.Input {
					val := diagramEdge{
						DestProc: pIdx,
					}
					if len(proc.Inputs) > 0 {
						val.DestInput = i + 1
					}
					for _, stream := range input.Streams {
						inPorts[stream.StreamID] = val
					}
				}

				for _, r := range p.Output {
					for _, o := range r.Streams {
						if o.Type == StreamEndpointType_SYNC_RESPONSE {
							if syncResponseNode != -1 && syncResponseNode != n {
								return nil, errors.Errorf("multiple nodes with SyncResponse")
							}
							syncResponseNode = n
						}
					}
				}

				// We need explicit routers if we have multiple outputs, or if the one
				// output has multiple input streams.
				if len(p.Output) > 1 || (len(p.Output) == 1 && len(p.Output[0].Streams) > 1) {
					proc.Outputs = make([]diagramCell, len(p.Output))
					for i, r := range p.Output {
						proc.Outputs[i].Title, proc.Outputs[i].Details = r.summary()
					}
				} else {
					proc.Outputs = []diagramCell{}
				}
				d.Processors = append(d.Processors, proc)
				pIdx++
			}
			i++
		}

		for _, p := range flows[n].Processors {
			proc := diagramProcessor{NodeIdx: n}
			proc.Core.Title, proc.Core.Details = p.Core.GetValue().(diagramCellType).summary()
			proc.Core.Title += fmt.Sprintf("/%d", p.ProcessorID)
			proc.processorID = p.ProcessorID
			proc.Core.Details = append(proc.Core.Details, p.Post.summary()...)

			// We need explicit synchronizers if we have multiple inputs, or if the
			// one input has multiple input streams.
			if len(p.Input) > 1 || (len(p.Input) == 1 && len(p.Input[0].Streams) > 1) {
				proc.Inputs = make([]diagramCell, len(p.Input))
				for i, s := range p.Input {
					proc.Inputs[i].Title, proc.Inputs[i].Details = s.summary(showInputTypes)
				}
			} else {
				proc.Inputs = []diagramCell{}
			}

			// Add entries in the map for the inputs.
			for i, input := range p.Input {
				val := diagramEdge{
					DestProc: pIdx,
				}
				if len(proc.Inputs) > 0 {
					val.DestInput = i + 1
				}
				for _, stream := range input.Streams {
					inPorts[stream.StreamID] = val
				}
			}

			for _, r := range p.Output {
				for _, o := range r.Streams {
					if o.Type == StreamEndpointType_SYNC_RESPONSE {
						if syncResponseNode != -1 && syncResponseNode != n {
							return nil, errors.Errorf("multiple nodes with SyncResponse")
						}
						syncResponseNode = n
					}
				}
			}

			// We need explicit routers if we have multiple outputs, or if the one
			// output has multiple input streams.
			if len(p.Output) > 1 || (len(p.Output) == 1 && len(p.Output[0].Streams) > 1) {
				proc.Outputs = make([]diagramCell, len(p.Output))
				for i, r := range p.Output {
					proc.Outputs[i].Title, proc.Outputs[i].Details = r.summary()
				}
			} else {
				proc.Outputs = []diagramCell{}
			}
			proc.StageID = p.StageID
			d.Processors = append(d.Processors, proc)
			pIdx++
		}
	}
	i = 0
	if syncResponseNode != -1 {
		d.Processors = append(d.Processors, diagramProcessor{
			NodeIdx: syncResponseNode,
			Core:    diagramCell{Title: "Response", Details: []string{}},
			Inputs:  []diagramCell{},
			Outputs: []diagramCell{},
			// When generating stats, spans are mapped from processor ID in the span
			// tags to processor ID in the diagram data. To avoid clashing with
			// the processor with ID 0, assign an impossible processorID.
			processorID: -1,
		})
	}

	// Produce the edges.
	pIdx = 0
	for n := range flows {
		if len(flows[n].TsProcessors) != 0 {
			for _, p := range flows[n].TsProcessors {
				for i, output := range p.Output {
					srcOutput := 0
					if len(d.Processors[pIdx].Outputs) > 0 {
						srcOutput = i + 1
					}
					for _, o := range output.Streams {
						edge := diagramEdge{
							SourceProc:   pIdx,
							SourceOutput: srcOutput,
							streamID:     o.StreamID,
						}
						if o.Type == StreamEndpointType_SYNC_RESPONSE {
							edge.DestProc = len(d.Processors) - 1
						} else {
							to, ok := inPorts[o.StreamID]
							if !ok {
								return nil, errors.Errorf("stream %d has no destination", o.StreamID)
							}
							edge.DestProc = to.DestProc
							edge.DestInput = to.DestInput
						}
						d.Edges = append(d.Edges, edge)
					}
				}
				pIdx++
			}
		}
		for _, p := range flows[n].Processors {
			for i, output := range p.Output {
				srcOutput := 0
				if len(d.Processors[pIdx].Outputs) > 0 {
					srcOutput = i + 1
				}
				for _, o := range output.Streams {
					edge := diagramEdge{
						SourceProc:   pIdx,
						SourceOutput: srcOutput,
						streamID:     o.StreamID,
					}
					if o.Type == StreamEndpointType_SYNC_RESPONSE {
						edge.DestProc = len(d.Processors) - 1
					} else {
						to, ok := inPorts[o.StreamID]
						if !ok {
							return nil, errors.Errorf("stream %d has no destination", o.StreamID)
						}
						edge.DestProc = to.DestProc
						edge.DestInput = to.DestInput
					}
					d.Edges = append(d.Edges, edge)
				}
			}
			pIdx++
		}
	}

	return d, nil
}

// GeneratePlanDiagram generates the data for a flow diagram. There should be
// one FlowSpec per node. The function assumes that StreamIDs are unique across
// all flows.
func GeneratePlanDiagram(
	sql string, flows map[roachpb.NodeID]*FlowSpec, showInputTypes bool,
) (FlowDiagram, error) {
	// We sort the flows by node because we want the diagram data to be
	// deterministic.
	nodeIDs := make([]int, 0, len(flows))
	for n := range flows {
		nodeIDs = append(nodeIDs, int(n))
	}
	sort.Ints(nodeIDs)

	flowSlice := make([]FlowSpec, len(nodeIDs))
	nodeNames := make([]string, len(nodeIDs))
	for i, nVal := range nodeIDs {
		n := roachpb.NodeID(nVal)
		flowSlice[i] = *flows[n]
		nodeNames[i] = n.String()
	}
	for n := range flowSlice {
		if len(flowSlice[n].TsProcessors) != 0 {
			nodeNames = append(nodeNames, "agent"+nodeNames[n])
		}
	}

	return generateDiagramData(sql, flowSlice, nodeNames, showInputTypes)
}

// GeneratePlanDiagramURL generates the json data for a flow diagram and a
// URL which encodes the diagram. There should be one FlowSpec per node. The
// function assumes that StreamIDs are unique across all flows.
func GeneratePlanDiagramURL(
	sql string, flows map[roachpb.NodeID]*FlowSpec, showInputTypes bool,
) (string, url.URL, error) {
	d, err := GeneratePlanDiagram(sql, flows, showInputTypes)
	if err != nil {
		return "", url.URL{}, err
	}
	return d.ToURL()
}

func encodeJSONToURL(json bytes.Buffer) (string, url.URL, error) {
	var compressed bytes.Buffer
	jsonStr := json.String()

	encoder := base64.NewEncoder(base64.URLEncoding, &compressed)
	compressor := zlib.NewWriter(encoder)
	if _, err := json.WriteTo(compressor); err != nil {
		return "", url.URL{}, err
	}
	if err := compressor.Close(); err != nil {
		return "", url.URL{}, err
	}
	if err := encoder.Close(); err != nil {
		return "", url.URL{}, err
	}
	url := url.URL{
		Scheme:   "https",
		Host:     "",
		Path:     "distsqlplan/decode.html",
		Fragment: compressed.String(),
	}
	return jsonStr, url, nil
}

// extractStatsFromSpans extracts stats from spans tagged with a processor id
// and returns a map from that processor id to a slice of stat descriptions
// that can be added to a plan.
func extractStatsFromSpans(
	flowID FlowID, spans []tracing.RecordedSpan,
) (processorStats, streamStats map[int][]string) {
	processorStats = make(map[int][]string)
	streamStats = make(map[int][]string)
	for _, span := range spans {
		// The trace can contain spans from multiple flows; make sure we select the
		// right ones.
		if fid, ok := span.Tags[FlowIDTagKey]; !ok || fid != flowID.String() {
			continue
		}

		var id string
		var stats map[int][]string

		// Get the processor or stream id for this span. If neither exists, this
		// span doesn't belong to a processor or stream.
		if pid, ok := span.Tags[ProcessorIDTagKey]; ok {
			id = pid
			stats = processorStats
		} else if sid, ok := span.Tags[StreamIDTagKey]; ok {
			id = sid
			stats = streamStats
		} else {
			continue
		}

		var da types.DynamicAny
		if err := types.UnmarshalAny(span.Stats, &da); err != nil {
			continue
		}
		if dss, ok := da.Message.(DistSQLSpanStats); ok {
			i, err := strconv.Atoi(id)
			if err != nil {
				continue
			}
			if span.Operation == sqlbase.TsTableReaderProcName {
				for k, v := range dss.TsStatsForQueryPlan() {
					stats[int(k)] = v
				}
			} else {
				stats[i] = append(stats[i], dss.StatsForQueryPlan()...)
			}
		}
	}
	return processorStats, streamStats
}

// DisplayFlowSpec construct massage of AE processor and ME processor.
// return massage and the flow whether a query flow.
func DisplayFlowSpec(flow FlowSpec) (string, bool) {
	var buf bytes.Buffer
	isQuery := false

	// construct message of AE processor.
	if flow.TsProcessors != nil {
		isQuery = true
		buf.WriteString("=====AE:=====\n")
	}
	for _, v := range flow.TsProcessors {
		res := getSpecMessage(
			v.Core.GetValue().(diagramCellType),
			strings.Join(v.Post.summary(), ","),
			v.Input,
			v.Output,
			v.ProcessorID)
		buf.WriteString(res)
	}

	// construct message of ME processor.
	buf.WriteString("=====ME:=====\n")
	for _, v := range flow.Processors {
		if v.Core.TableReader != nil {
			isQuery = true
		}
		core, ok := v.Core.GetValue().(diagramCellType)
		if ok {
			res := getSpecMessage(
				core,
				strings.Join(v.Post.summary(), ","),
				v.Input,
				v.Output,
				v.ProcessorID)
			buf.WriteString(res)
		}
	}

	res := buf.String()
	return res, isQuery
}

// getSpecMessage construct massage of processor.
// core is the processor, postDetail is the post of processor
// inputSyncSpec are the input routers of a  processor
// outputRouterSpec are the output routers of a processor
// processorID is the id of processor
func getSpecMessage(
	core diagramCellType,
	postDetail string,
	inputSyncSpec []InputSyncSpec,
	outputRouterSpec []OutputRouterSpec,
	processorID int32,
) string {
	var buf bytes.Buffer
	title, details := core.summary()
	detail := strings.Join(details, ",")
	buf.WriteString(title + ":\n")
	buf.WriteString("    " + detail + "\n")
	buf.WriteString("--" + title + "_Post:\n")
	buf.WriteString("    " + postDetail + "\n")
	buf.WriteString(fmt.Sprintf("--ProcessorID: %v\n", processorID))
	var input, output []string
	if inputSyncSpec != nil {
		for _, i := range inputSyncSpec {
			for _, s := range i.Streams {
				input = append(input, strconv.Itoa(int(s.StreamID)))
			}
		}
	}
	if outputRouterSpec != nil {
		for _, o := range outputRouterSpec {
			for k, s := range o.Streams {
				output = append(output, strconv.Itoa(int(s.StreamID)))
				if s.Type == StreamEndpointType_SYNC_RESPONSE && k == len(o.Streams)-1 {
					output = append(output, fmt.Sprintf("   type: RESPONSE"))
				}
				if s.Type == StreamEndpointType_REMOTE && k == len(o.Streams)-1 {
					output = append(output, fmt.Sprintf("   type: REMOTE"))
				}
			}
		}
	}
	if input != nil {
		buf.WriteString(fmt.Sprintf("--Input: %v\n", strings.Join(input, ",")))
	}
	if output != nil {
		buf.WriteString(fmt.Sprintf("--Onput: %v\n", strings.Join(output, ",")))
	}
	return buf.String()
}
