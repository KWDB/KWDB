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

package cdcpb

// CDCTableInfo stores the table information used by CDC task.
type CDCTableInfo struct {
	// The ID of table.
	ID uint64 `json:"id,omitempty"`
	// The database name of the table.
	Database string `json:"database,omitempty"`
	// The schema name of the table.
	Schema string `json:"schema,omitempty"`
	// The name of the table.
	Table string `json:"table,omitempty"`
	// The list of columns to be captured in the table.
	ColNames []string `json:"column_names,omitempty"`
	// The types list of columns to be captured in the table.
	ColTypes []string `json:"column_types,omitempty"`
	// The ID list of columns to be captured in the table.
	ColIDs []uint32 `json:"column_ids,omitempty"`
	// The filter to be captured in the table.
	Filter string `json:"filter,omitempty"`
	// The primary tag list to be captured in the table.
	PrimaryTagCols []string `json:"primary_tag_cols,omitempty"`
	// The ts column name of the table.
	TsColumnName string `json:"ts_column_name,omitempty"`
	// The low-watermark of the table.
	LowWatermark int64 `json:"lowWatermark,omitempty"`
	// Is it necessary to retrieve tags during the CDC process.
	NeedNormalTag bool `json:"need_normal_tag,omitempty"`
	// The table is ts table.
	IsTsTable bool `json:"is_ts_table,omitempty"`
}
