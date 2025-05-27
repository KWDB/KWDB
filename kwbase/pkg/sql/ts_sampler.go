// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	"sort"
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"github.com/pkg/errors"
)

var tsSpecPool = sync.Pool{
	New: func() interface{} {
		return &execinfrapb.TSSamplerSpec{}
	},
}

// initTsSamplerConfiguration initializes the configuration for the TsSampler.
// It checks if statistics are requested, prepares column configurations for scanning,
// and sorts these configurations for efficient processing.
func initTsSamplerConfiguration(
	planCtx *PlanningCtx, desc *sqlbase.ImmutableTableDescriptor, reqStats []requestedStat,
) (TsSamplerConfig, error) {
	if len(reqStats) == 0 {
		return TsSamplerConfig{}, errors.New("no statistics requested in create statistics")
	}

	var colCfg scanColumnsConfig
	var tableColSet util.FastIntSet
	for _, stat := range reqStats {
		for _, col := range stat.columns {
			if !tableColSet.Contains(int(col)) {
				tableColSet.Add(int(col))
				colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(col))
			}
		}
	}

	// Sort the wanted columns by column ID in ascending order
	sort.Slice(colCfg.wantedColumns, func(i, j int) bool {
		return colCfg.wantedColumns[i] < colCfg.wantedColumns[j]
	})

	tsSamplerCfg := TsSamplerConfig{
		PlanCtx:    planCtx,
		TabDesc:    desc,
		ColCfg:     colCfg,
		SampleSize: histogramSamples,
		ReqStats:   reqStats,
	}

	return tsSamplerCfg, nil
}

// NewTsSamplerSpec returns a new TsSamplerSpec.
func NewTsSamplerSpec() *execinfrapb.TSSamplerSpec {
	return tsSpecPool.Get().(*execinfrapb.TSSamplerSpec)
}

// createTsScanNode is used to init a tsScanNode
func createTsScanNode(tsSamplerCfg TsSamplerConfig) *tsScanNode {
	tsScan := &tsScanNode{
		Table: &optTable{
			desc:     tsSamplerCfg.TabDesc,
			rawStats: nil,
			zone:     nil,
		},
		resultColumns: tsSamplerCfg.StatsCols,
		hashNum:       tsSamplerCfg.TabDesc.TsTable.HashNum,
	}

	// Add meta id, because of the physical ID of a column will change due to adding or deleting columns
	for _, statsCol := range tsSamplerCfg.StatsCols {
		tsScan.ScanSource.Add(tsSamplerCfg.TabDesc.ColumnIdxMap()[statsCol.PGAttributeNum] + 1)
	}

	for _, colType := range tsSamplerCfg.ColumnTypes {
		switch colType {
		case uint32(sqlbase.ColumnType_TYPE_DATA):
			tsScan.AccessMode = execinfrapb.TSTableReadMode_metaTable
		case uint32(sqlbase.ColumnType_TYPE_TAG), uint32(sqlbase.ColumnType_TYPE_PTAG):
			tsScan.AccessMode = execinfrapb.TSTableReadMode_tableTableMeta
			break
		default:
			tsScan.AccessMode = execinfrapb.TSTableReadMode_metaTable
		}
	}

	return tsScan
}

// setupColumnConfiguration gets the desired column description
func setupColumnConfiguration(
	tsSamplerCfg *TsSamplerConfig,
) (map[sqlbase.ColumnID]int, util.FastIntSet, error) {
	// Get the descriptor of statistic column
	neededCols, err := getNeededColumns(*tsSamplerCfg)
	if err != nil {
		return nil, util.FastIntSet{}, err
	}

	// Set column index mapping
	colIdxMap := make(map[sqlbase.ColumnID]int, len(neededCols))
	for i, c := range neededCols {
		colIdxMap[c.ID] = i
	}

	valNeededForCol := util.FastIntSet{}
	if len(neededCols) > 0 {
		valNeededForCol.AddRange(0, len(neededCols)-1)
	}

	// Set statistics column
	statsCols, statsColsTypes, columnTypes := setupStatsColumns(neededCols, tsSamplerCfg.TabDesc)
	tsSamplerCfg.StatsCols = statsCols
	tsSamplerCfg.StatsColsTypes = statsColsTypes
	tsSamplerCfg.ColumnTypes = columnTypes
	return colIdxMap, valNeededForCol, nil
}

// initTsSamplerSpec initializes the specifications for time-series sampling based on the provided configuration.
// It returns a pointer to a TSSamplerSpec and any error encountered during the initialization.
func initTsSamplerSpec(
	tsConfig TsSamplerConfig, colIdxMap map[sqlbase.ColumnID]int,
) (*execinfrapb.TSSamplerSpec, error) {
	s := NewTsSamplerSpec()
	*s = execinfrapb.TSSamplerSpec{
		Sketches:   make([]execinfrapb.SketchInfo, len(tsConfig.ReqStats)),
		TableId:    uint64(tsConfig.TabDesc.ID),
		SampleSize: &tsConfig.SampleSize,
	}

	// Initialize time-series columns and a mapping from column ID to index and type.
	tsCols := make([]sqlbase.TSCol, 0)
	tsColMap := make(map[sqlbase.ColumnID]tsColIndex, 0)

	// Iterate over columns to identify and append non-tag columns.
	for i := 0; i < len(tsConfig.TabDesc.Columns); i++ {
		col := &tsConfig.TabDesc.Columns[i]
		if !col.IsTagCol() {
			tsCols = append(tsCols, col.TsCol)
			tsColMap[col.ID] = tsColIndex{idx: len(tsCols) - 1, colType: col.TsCol.ColumnType}
		}
	}

	// Append tag columns after non-tag columns to maintain a consistent ordering.
	for i := 0; i < len(tsConfig.TabDesc.Columns); i++ {
		col := &tsConfig.TabDesc.Columns[i]
		if col.IsTagCol() {
			tsCols = append(tsCols, col.TsCol)
			tsColMap[col.ID] = tsColIndex{idx: len(tsCols) - 1, colType: col.TsCol.ColumnType}
		}
	}
	// Populate sketches based on the column metadata.
	for i := range tsConfig.ReqStats {
		columnIDxs := make([]uint32, 0, len(tsConfig.ReqStats[i].columns))
		columnTypes := make([]uint32, 0, len(tsConfig.ReqStats[i].columnsTypes))

		// Map columns to their internal sketch representations.
		for _, v1 := range tsConfig.ReqStats[i].columns {
			columnIDxs = append(columnIDxs, uint32(colIdxMap[v1]))
			columnTypes = append(columnTypes, uint32(tsColMap[v1].colType))
		}

		// Set up each sketch with properties from the configuration.
		s.Sketches[i] = execinfrapb.SketchInfo{
			// By default, there is only one algorithm
			SketchType:        execinfrapb.SketchMethod_HLL_PLUS_PLUS,
			GenerateHistogram: tsConfig.ReqStats[i].histogram,
			ColIdx:            columnIDxs,
			ColTypes:          columnTypes,
			HasAllPTag:        tsConfig.ReqStats[i].hasAllPTag,
			SortedHistogram:   tsConfig.ReqStats[i].sortedHistogram,
		}
	}
	return s, nil
}

var supportedSketchTypes = map[execinfrapb.SketchMethod]struct{}{
	// The code currently hardcodes the use of this single type of sketch
	// (which avoids the extra complexity until we actually have multiple types).
	execinfrapb.SketchMethod_HLL_PLUS_PLUS: {},
}

// updateOutput sets TsSamplerConfig and Processor
func updateOutput(tsConfig *TsSamplerConfig, p *PhysicalPlan) {
	// total sample columns
	resultColumns := appendExtraColumns(tsConfig.StatsCols, tsConfig.TabDesc)

	outputColumnsType := make([]types.T, len(resultColumns))
	outputColumnsIDx := make([]uint32, len(resultColumns))
	planToStreamColMap := make([]int, len(resultColumns))

	for i, col := range resultColumns {
		outputColumnsType[i] = *col.Typ
		outputColumnsIDx[i] = uint32(i)
		planToStreamColMap[i] = i
	}

	tsConfig.ResultCols = resultColumns

	// Set the scan cols and output types for ts sampler
	post := execinfrapb.TSPostProcessSpec{
		OutputColumns: outputColumnsIDx,
		OutputTypes:   make([]types.Family, len(outputColumnsType)),
	}

	// Set output types
	for i, typ := range outputColumnsType {
		post.OutputTypes[i] = typ.InternalType.Family
	}

	// The final stage of setting up the physical plan.
	p.SetLastStageTSPost(post, outputColumnsType)

	// up-to-date stream
	p.PlanToStreamColMap = planToStreamColMap
}

// getNeededColumns gets the descriptor of the object columns
func getNeededColumns(tsConfig TsSamplerConfig) ([]sqlbase.ColumnDescriptor, error) {
	nonTagCols := make([]sqlbase.ColumnDescriptor, 0) // Store non-tag columns
	tagCols := make([]sqlbase.ColumnDescriptor, 0)    // Store tag columns
	for _, wc := range tsConfig.ColCfg.wantedColumns {
		var c *sqlbase.ColumnDescriptor
		var err error
		if id := sqlbase.ColumnID(wc); tsConfig.ColCfg.visibility == publicColumns {
			c, err = tsConfig.TabDesc.FindActiveColumnByID(id)
		} else {
			c, _, err = tsConfig.TabDesc.FindReadableColumnByID(id)
		}
		if err != nil {
			return nil, err
		}

		if c.IsTagCol() {
			tagCols = append(tagCols, *c)
		} else {
			nonTagCols = append(nonTagCols, *c)
		}
	}
	// Merge the non-tag columns and tag columns into a single slice, with the non-tag columns first and the tag columns last
	neededCols := append(nonTagCols, tagCols...)

	return neededCols, nil
}

// setupStatsColumns sets result columns
func setupStatsColumns(
	neededCols []sqlbase.ColumnDescriptor, desc *sqlbase.ImmutableTableDescriptor,
) (sqlbase.ResultColumns, []types.T, []uint32) {
	statsCols := make(sqlbase.ResultColumns, 0, len(neededCols))
	statsColsTypes := make([]types.T, 0, len(neededCols))
	columnTypes := make([]uint32, 0, len(neededCols))
	for i := range neededCols {
		// Convert the ColumnDescriptor to ResultColumn.
		colDesc := &neededCols[i]
		typ := &colDesc.Type
		hidden := colDesc.Hidden
		statsCols = append(
			statsCols,
			sqlbase.ResultColumn{
				Name:           colDesc.Name,
				Typ:            typ,
				Hidden:         hidden,
				TableID:        desc.GetID(),
				PGAttributeNum: colDesc.ID,
				TypeModifier:   typ.TypeModifier(),
			},
		)
		statsColsTypes = append(statsColsTypes, colDesc.Type)
		columnTypes = append(columnTypes, uint32(colDesc.TsCol.ColumnType))
	}
	return statsCols, statsColsTypes, columnTypes
}

// appendExtraColumns appends other sample columns
func appendExtraColumns(
	resCols sqlbase.ResultColumns, desc *sqlbase.ImmutableTableDescriptor,
) sqlbase.ResultColumns {
	resultColumns := make(sqlbase.ResultColumns, 0, len(resCols)+5)
	resultColumns = append(resultColumns, resCols...)
	// The TsSampler outputs the original columns plus a rank column and four sketch columns.
	rankCol := sqlbase.ResultColumn{
		Name:           "rankCol",
		Typ:            types.Int,
		TableID:        desc.GetID(),
		PGAttributeNum: resCols[0].PGAttributeNum + 1,
		TypeModifier:   types.Int.TypeModifier(),
	}
	sketchIdxCol := sqlbase.ResultColumn{
		Name:           "sketchIdxCol",
		Typ:            types.Int,
		TableID:        desc.GetID(),
		PGAttributeNum: resCols[0].PGAttributeNum + 2,
		TypeModifier:   types.Int.TypeModifier(),
	}
	numRowsCol := sqlbase.ResultColumn{
		Name:           "numRowsCol",
		Typ:            types.Int,
		TableID:        desc.GetID(),
		PGAttributeNum: resCols[0].PGAttributeNum + 3,
		TypeModifier:   types.Int.TypeModifier(),
	}
	numNullsCol := sqlbase.ResultColumn{
		Name:           "numNullsCol",
		Typ:            types.Int,
		TableID:        desc.GetID(),
		PGAttributeNum: resCols[0].PGAttributeNum + 4,
		TypeModifier:   types.Int.TypeModifier(),
	}
	sketchCol := sqlbase.ResultColumn{
		Name:           "sketchCol",
		Typ:            types.Bytes,
		TableID:        desc.GetID(),
		PGAttributeNum: resCols[0].PGAttributeNum + 5,
		TypeModifier:   types.Int.TypeModifier(),
	}
	bucketIDCol := sqlbase.ResultColumn{
		Name:           "bucketIDCol",
		Typ:            types.Int,
		TableID:        desc.GetID(),
		PGAttributeNum: resCols[0].PGAttributeNum + 6,
		TypeModifier:   types.Int.TypeModifier(),
	}
	bucketNumRowsCol := sqlbase.ResultColumn{
		Name:           "bucketNumRowsCol",
		Typ:            types.Int,
		TableID:        desc.GetID(),
		PGAttributeNum: resCols[0].PGAttributeNum + 7,
		TypeModifier:   types.Int.TypeModifier(),
	}
	resultColumns = append(resultColumns, rankCol)
	resultColumns = append(resultColumns, sketchIdxCol)
	resultColumns = append(resultColumns, numRowsCol)
	resultColumns = append(resultColumns, numNullsCol)
	resultColumns = append(resultColumns, sketchCol)
	resultColumns = append(resultColumns, bucketIDCol)
	resultColumns = append(resultColumns, bucketNumRowsCol)
	return resultColumns
}
