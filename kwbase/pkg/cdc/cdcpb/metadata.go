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

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	kjson "gitee.com/kwbasedb/kwbase/pkg/util/json"
	"github.com/pkg/errors"
)

// FormatType indicates the cdc data format
type FormatType int32

const (
	// FormatSQL indicates the value format is query.
	FormatSQL FormatType = 0
	// FormatPGBindBinary indicates the value format is binary.
	FormatPGBindBinary FormatType = 1
	// FormatPGBindText indicates the value format is plain text.
	FormatPGBindText FormatType = 2
	// FormatJSON indicates the value format is json.
	FormatJSON FormatType = 3

	// InvalidWatermark indicates the invalid watermark.
	InvalidWatermark = 0
	// CDCInternalTypeUnknown indicates the unknown type.
	CDCInternalTypeUnknown = 0

	// EventHeartbeat indicates the heartbeat event.
	EventHeartbeat = "heartbeat"
	// EventSnapshot indicates the history data event.
	EventSnapshot = "snapshot"
	// EventInsert indicates the realtime insert data event.
	EventInsert = "insert"
	// EventUpdate indicates the realtime update data event.
	EventUpdate = "update"
	// EventDelete indicates the realtime delete data event.
	EventDelete = "delete"
	// EventDDL indicates the realtime ddl event.
	EventDDL = "ddl"
	// EventAll indicates the realtime all event.
	EventAll = "all"

	// historicalSnapshotMaxLimit is the threshold for historical data volume, beyond which it needs to be batched.
	historicalSnapshotMaxLimit = 1000000
	// historicalDefaultRowWidth is default row width, beyond which it needs to be batched.
	historicalDefaultRowWidth = 400

	// OperationWrong is error operation.
	OperationWrong = 0
	// OperationInsert is insert operation.
	OperationInsert = 1
	// OperationUpdateNormalTag is update tag operation.
	OperationUpdateNormalTag = 2
	// OperationDeleteTag is delete tag operation.
	OperationDeleteTag = 3
	// OperationDeleteMetric is delete data operation.
	OperationDeleteMetric = 4
)

// TsCDCMaxActiveNumber indicates the max number of running cdc task
var TsCDCMaxActiveNumber = settings.RegisterPublicValidatedIntSetting(
	"ts.cdc.max_active_number",
	"the max number of running cdc task",
	10,
	func(v int64) error {
		if v < 1 {
			return errors.Errorf("cannot set %s to a value < 1: %d", "ts.cdc.max_active_number", v)
		}
		return nil
	},
)

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
	// IsStar is true if the object(s) specified by pipe, stream or publication
	// is star(*) of table,  table name, table list, or database.
	IsStar bool `json:"is_star"`
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
	// The normal tag list to be captured in the table.
	NormalTagCols []string `json:"normal_tag_cols,omitempty"`
	// The ts column name of the table.
	TsColumnName string `json:"ts_column_name,omitempty"`
	// The ts column precision of the table.
	TsColumnPrecision int32 `json:"ts_column_precision,omitempty"`
	// The low-watermark of the table.
	LowWatermark int64 `json:"low_watermark,omitempty"`
	// Is it necessary to retrieve tags during the CDC process.
	NeedNormalTag bool `json:"need_normal_tag,omitempty"`
	// The table is ts table.
	IsTsTable bool `json:"is_ts_table,omitempty"`
}

// GetFullName formats and returns the full name of table in database_name.schema_name.table_name.
func (t *CDCTableInfo) GetFullName() string {
	return fmt.Sprintf("%s.%s.%s", t.Database, t.Schema, t.Table)
}

// GetFullTableName checks database name, schema name, table name, and return full table name.
// Return error if t has no Database and Schema.
// Return full table name if t at least has one of Database and Schema.
// The legal format of full table name is database.table, schema.table, and database.schema.table.
func (t *CDCTableInfo) GetFullTableName(evalCtx *tree.EvalContext) (string, error) {
	if len(t.Table) == 0 {
		return "", errors.Errorf("at least one table in param is empty")
	}
	isDatabaseEmpty := len(t.Database) == 0
	isSchemaEmpty := len(t.Schema) == 0
	if isDatabaseEmpty && isSchemaEmpty {
		return "", errors.Errorf("table %s in param has no database and schema", t.Table)
	}
	var fullTableName strings.Builder
	if !isDatabaseEmpty {
		fullTableName.WriteString(t.Database)
		fullTableName.WriteString(".")
	} else {
		fullTableName.WriteString(evalCtx.SessionData.Database)
		fullTableName.WriteString(".")
	}
	if !isSchemaEmpty {
		fullTableName.WriteString(t.Schema)
		fullTableName.WriteString(".")
	} else {
		fullTableName.WriteString("public")
		fullTableName.WriteString(".")
	}
	fullTableName.WriteString(t.Table)

	return fullTableName.String(), nil
}

// RunInfo records information of running pipe
type RunInfo struct {
	JobID        int64  `json:"job_id"`
	StartTime    string `json:"start_time,omitempty"`
	EndTime      string `json:"end_time,omitempty"`
	ErrorMessage string `json:"message,omitempty"`
}

// MarshalRunInfo marshals running job info to json.
func MarshalRunInfo(runInfo []RunInfo) (kjson.JSON, error) {
	info, err := json.Marshal(runInfo)
	if err != nil {
		return nil, err
	}
	return kjson.FromString(string(info)), nil
}

// UnmarshalRunInfo unmarshal json to running job info.
func UnmarshalRunInfo(runInfo kjson.JSON) ([]RunInfo, error) {
	var para []RunInfo
	str, err := runInfo.AsText()
	if err != nil {
		return para, err
	}
	if err := json.Unmarshal([]byte(*str), &para); err != nil {
		return para, err
	}
	return para, nil
}

// CalculateBatchRows calculate batch size of rows from column types.
func CalculateBatchRows(colTypes []string) int64 {
	var rowWidth, batchRows int64
	for _, typ := range colTypes {
		if strings.Contains(typ, "(") {
			matches := strings.Split(typ, "(")
			if len(matches) > 1 {
				colWidth, _ := strconv.Atoi(strings.Trim(matches[1], ")"))
				rowWidth += int64(colWidth)
			}
		} else {
			rowWidth += 8
		}
	}

	if rowWidth == 0 {
		rowWidth = historicalDefaultRowWidth
	}

	batchRows = historicalSnapshotMaxLimit * historicalDefaultRowWidth / rowWidth

	return batchRows
}

// GetFullTABLE returns full table path.
func (m *CDCTable) GetFullTABLE() string {
	var fullPath strings.Builder
	if m.Database != "" {
		fullPath.WriteString(m.Database)
		fullPath.WriteString(".")
	}

	if m.Schema != "" {
		fullPath.WriteString(m.Schema)
		fullPath.WriteString(".")
	}

	fullPath.WriteString(m.Table)

	return fullPath.String()
}
