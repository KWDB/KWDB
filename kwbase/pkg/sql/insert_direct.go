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

package sql

import (
	"context"
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec/execbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/lib/pq/oid"
	"github.com/paulsmith/gogeos/geos"
)

// DirectInsertTable is related struct of ts tables in insert_direct
type DirectInsertTable struct {
	DbID, TabID uint32
	HashNum     uint64
	ColsDesc    []sqlbase.ColumnDescriptor
	Tname       *tree.TableName
	Desc        tree.NameList
	TableType   tree.TableType
}

// DatumConverter is a function type for converting raw value to tree.Datum
type DatumConverter func(ptCtx tree.ParseTimeContext, column *sqlbase.ColumnDescriptor, valueType parser.TokenType, rawValue string) (tree.Datum, error)

// DirectInsert is related struct in insert_direct
type DirectInsert struct {
	RowNum, ColNum             int
	IDMap, PosMap              []int
	ColIndexs                  map[int]int
	DefIndexs                  map[int]int
	PArgs                      execbuilder.PayloadArgs
	PrettyCols, PrimaryTagCols []*sqlbase.ColumnDescriptor
	Dcs                        []*sqlbase.ColumnDescriptor
	PayloadNodeMap             map[int]*sqlbase.PayloadForDistTSInsert
	InputValues                []tree.Datums
	DirectTimes                directTimes
	ColConverters              []DatumConverter // cached converters for each column
}

const (
	errUnsupportedType = "unsupported input type relation \"%s\" (column %s)"
	errOutOfRange      = "integer \"%s\" out of range for type %s (column %s)"
	errInvalidValue    = "value '%s' is invalid for type %s (column %s)"
	errValueOutofRange = "value '%s' out of range for type %s (column %s)"
	errTooLong         = "value '%s' too long for type %s (column %s)"
)

const (
	// HandleSimpleStart means insertdirecrt starts.
	HandleSimpleStart = iota
	// ParseStart means parse starts.
	ParseStart
	// ParseEnd means parse ends.
	ParseEnd
	// CreateInsertNodeStart means construct starts.
	CreateInsertNodeStart
	// CreateInsertNodeEnd means construct ends.
	CreateInsertNodeEnd
	// PlanAndRunStart means planning starts.
	PlanAndRunStart
	// PlanAndRunEnd means planning ends.
	PlanAndRunEnd
	// OtherStart means other starts.
	OtherStart
	// OtherEnd means other ends.
	OtherEnd

	// sessionNum must be listed last so that it can be used to
	// define arrays sufficiently large to hold all the other values.
	sessionNum
)

type directTimes [sessionNum]time.Time

// getPrepareInputValues convert data format form Bind to []tree.Datums.
func getPrepareInputValues(
	ptCtx tree.ParseTimeContext, bindCmd *BindStmt, inferredTypes []oid.Oid, di *DirectInsert,
) ([]tree.Datums, error) {
	qArgFormatCodes := bindCmd.ArgFormatCodes
	if len(qArgFormatCodes) == 1 && di.RowNum > 1 {
		fmtCode := qArgFormatCodes[0]
		qArgFormatCodes = make([]pgwirebase.FormatCode, di.RowNum)
		for i := range qArgFormatCodes {
			qArgFormatCodes[i] = fmtCode
		}
	}
	rowNum := di.RowNum / di.ColNum
	outputValues := make([]tree.Datums, rowNum)
	row := 0
	col := 0
	for i, arg := range bindCmd.Args {
		if outputValues[row] == nil {
			outputValues[row] = make(tree.Datums, di.ColNum)
		}
		k := tree.PlaceholderIdx(i)
		t := inferredTypes[i]
		if arg == nil {
			// nil indicates a NULL argument value.
			outputValues[row][col] = tree.DNull
		} else {
			d, err := pgwirebase.DecodeOidDatum(ptCtx, t, qArgFormatCodes[i], arg)
			if err != nil {
				return nil, pgerror.Wrapf(err, pgcode.ProtocolViolation,
					"error in argument for %s", k)
			}
			outputValues[row][col] = d
		}
		col++
		if col%di.ColNum == 0 {
			col = 0
			row++
		}
	}

	return outputValues, nil
}

// BuildPerNodePayloads is used to PerNodePayloads
func BuildPerNodePayloads(
	payloadNodeMap map[int]*sqlbase.PayloadForDistTSInsert,
	nodeID []roachpb.NodeID,
	payload []byte,
	priTagRowIdx []int,
	primaryTagKey roachpb.Key,
	hashNum uint64,
) {
	payloadInfo := &sqlbase.SinglePayloadInfo{
		Payload:       payload,
		RowNum:        uint32(len(priTagRowIdx)),
		PrimaryTagKey: primaryTagKey,
		HashNum:       hashNum,
	}

	nodeIDInt := int(nodeID[0])

	if val, ok := payloadNodeMap[nodeIDInt]; ok {
		val.PerNodePayloads = append(val.PerNodePayloads, payloadInfo)
	} else {
		payloadNodeMap[nodeIDInt] = &sqlbase.PayloadForDistTSInsert{
			NodeID:          nodeID[0],
			PerNodePayloads: []*sqlbase.SinglePayloadInfo{payloadInfo},
		}
	}
}

// NumofInsertDirect id used to calculate colNum, insertLength, rowNum
func NumofInsertDirect(
	ins *tree.Insert, colsDesc *[]sqlbase.ColumnDescriptor, stmts parser.Statements, di *DirectInsert,
) int {
	stmt := &stmts[0].Insertdirectstmt

	di.ColNum = len(ins.Columns)
	if ins.IsNoSchema {
		di.ColNum /= 3
	}

	if di.ColNum == 0 {
		di.ColNum = len(*colsDesc)
	}

	di.RowNum = int(stmt.RowsAffected)
	return len(stmt.InsertValues)
}

// BuildRowBytesForPrepareTsInsert builds rows for PrepareTsInsert efficiently
func BuildRowBytesForPrepareTsInsert(
	ptCtx tree.ParseTimeContext,
	Args [][]byte,
	Dit DirectInsertTable,
	di *DirectInsert,
	evalCtx tree.EvalContext,
	table *sqlbase.ImmutableTableDescriptor,
	nodeID roachpb.NodeID,
	rowTimestamps []int64,
	cfg *ExecutorConfig,
) error {
	rowNum := di.RowNum / di.ColNum

	// Pre-allocate all required buffers
	rowBytes, dataOffset, independentOffset, err := preAllocateDataRowBytes(rowNum, &di.Dcs)
	if err != nil {
		return err
	}

	// Process rows sequentially with optimized memory usage
	bitmapOffset := execbuilder.DataLenSize
	for i := 0; i < rowNum; i++ {
		Payload := rowBytes[i]
		offset := dataOffset
		varDataOffset := independentOffset

		baseIdx := i * di.ColNum
		for j, col := range di.PrettyCols {
			colIdx := di.ColIndexs[int(col.ID)]

			if col.IsTagCol() {
				continue
			}

			argIdx := baseIdx + colIdx

			dataColIdx := j - di.PArgs.PTagNum - di.PArgs.AllTagNum
			isLastDataCol := dataColIdx == di.PArgs.DataColNum-1

			curColLength := execbuilder.VarColumnSize
			if int(col.TsCol.VariableLengthType) == sqlbase.StorageTuple {
				curColLength = int(col.TsCol.StorageLen)
			}
			// deal with NULL value
			if colIdx < 0 || Args[argIdx] == nil {
				if !col.Nullable {
					return sqlbase.NewNonNullViolationError(col.Name)
				}
				Payload[bitmapOffset+dataColIdx/8] |= 1 << (dataColIdx % 8)
				offset += curColLength

				if isLastDataCol {
					binary.LittleEndian.PutUint32(Payload[0:], uint32(varDataOffset-execbuilder.DataLenSize))
					rowBytes[i] = Payload[:varDataOffset]
				}
				continue
			}
			arg := Args[argIdx]
			argLen := len(arg)
			switch col.Type.Oid() {
			case oid.T_varchar:
				vardataOffset := varDataOffset - bitmapOffset
				binary.LittleEndian.PutUint32(Payload[offset:], uint32(vardataOffset))

				addSize := argLen + execbuilder.VarDataLenSize + 1 // \0
				if varDataOffset+addSize > len(Payload) {
					newPayload := make([]byte, len(Payload)+addSize)
					copy(newPayload, Payload)
					Payload = newPayload
				}
				binary.LittleEndian.PutUint16(Payload[varDataOffset:], uint16(argLen+1)) // \0
				copy(Payload[varDataOffset+execbuilder.VarDataLenSize:], arg)
				varDataOffset += addSize

			case types.T_nvarchar, oid.T_varbytea:
				vardataOffset := varDataOffset - bitmapOffset
				binary.LittleEndian.PutUint32(Payload[offset:], uint32(vardataOffset))

				addSize := argLen + execbuilder.VarDataLenSize
				if varDataOffset+addSize > len(Payload) {
					newPayload := make([]byte, len(Payload)+addSize)
					copy(newPayload, Payload)
					Payload = newPayload
				}
				binary.LittleEndian.PutUint16(Payload[varDataOffset:], uint16(argLen))
				copy(Payload[varDataOffset+execbuilder.VarDataLenSize:], arg)
				varDataOffset += addSize

			case oid.T_bool:
				copy(Payload[offset:offset+argLen], arg)

			case oid.T_bytea:
				copy(Payload[offset+2:offset+2+argLen], arg)

			default:
				if argLen > curColLength {
					argLen = curColLength
				}
				copy(Payload[offset:offset+argLen], arg)
			}
			offset += curColLength

			if isLastDataCol {
				binary.LittleEndian.PutUint32(Payload[0:], uint32(varDataOffset-execbuilder.DataLenSize))
				rowBytes[i] = Payload[:varDataOffset]
			}
		}
	}

	// Build primary tag value map with pre-allocated capacity
	priTagValMap := make(map[string][]int, 10)
	var keyBuilder strings.Builder
	keyBuilder.Grow(64)

	colIndexes := make([]int, 0, len(di.PrimaryTagCols))
	for _, col := range di.PrimaryTagCols {
		colIndexes = append(colIndexes, di.ColIndexs[int(col.ID)])
	}

	for i := 0; i < rowNum; i++ {
		keyBuilder.Reset()
		for _, idx := range colIndexes {
			keyBuilder.Write(Args[i*di.ColNum+idx])
		}
		key := keyBuilder.String()
		priTagValMap[key] = append(priTagValMap[key], i)
	}

	// Pre-allocate payloads slice
	allPayloads := make([]*sqlbase.SinglePayloadInfo, 0, len(priTagValMap))

	for _, priTagRowIdx := range priTagValMap {
		payload, _, err := execbuilder.BuildPreparePayloadForTsInsert(
			&evalCtx,
			evalCtx.Txn,
			nil,
			priTagRowIdx[:1],
			di.PArgs.PrettyCols[:di.PArgs.PTagNum+di.PArgs.AllTagNum],
			di.ColIndexs,
			di.PArgs,
			Dit.DbID,
			Dit.TabID,
			Dit.HashNum,
			Args,
			di.ColNum,
		)
		if err != nil {
			return err
		}

		hashPoints := sqlbase.DecodeHashPointFromPayload(payload)
		primaryTagKey := sqlbase.MakeTsPrimaryTagKey(table.ID, hashPoints)
		if !evalCtx.StartSinglenode {
			groupLen := len(priTagRowIdx)
			groupBytes := make([][]byte, groupLen)
			valueSize := int32(0)

			for i, idx := range priTagRowIdx {
				groupBytes[i] = rowBytes[idx]
				valueSize += int32(len(groupBytes[i]))
			}

			hashNum := Dit.HashNum
			allPayloads = append(allPayloads, &sqlbase.SinglePayloadInfo{
				Payload:       payload,
				RowNum:        uint32(groupLen),
				PrimaryTagKey: primaryTagKey,
				RowBytes:      groupBytes,
				StartKey:      sqlbase.MakeTsRangeKey(table.ID, uint64(hashPoints[0]), hashNum),
				EndKey:        sqlbase.MakeTsRangeKey(table.ID, uint64(hashPoints[0])+1, hashNum),
				ValueSize:     valueSize,
				HashNum:       hashNum,
			})
		} else {
			valueSize := int32(0)
			rowNum := uint32(0)
			for _, idx := range priTagRowIdx {
				valueSize += int32(len(rowBytes[idx]))
				rowNum++
			}

			payloadSize := int32(len(payload)) + valueSize + 4
			payloadBytes := make([]byte, payloadSize)
			copy(payloadBytes, payload)
			offset := len(payload)
			binary.LittleEndian.PutUint32(payloadBytes[offset:], uint32(valueSize))
			offset += 4
			for _, idx := range priTagRowIdx {
				copy(payloadBytes[offset:], rowBytes[idx])
				offset += len(rowBytes[idx])
			}

			binary.LittleEndian.PutUint32(payloadBytes[38:], uint32(rowNum))

			hashNum := Dit.HashNum
			allPayloads = append(allPayloads, &sqlbase.SinglePayloadInfo{
				Payload:       payloadBytes,
				RowNum:        uint32(len(priTagRowIdx)),
				PrimaryTagKey: primaryTagKey,
				HashNum:       hashNum,
			})
		}
	}

	di.PayloadNodeMap[int(evalCtx.NodeID)] = &sqlbase.PayloadForDistTSInsert{
		NodeID:          nodeID,
		PerNodePayloads: allPayloads,
	}
	di.PayloadNodeMap[int(evalCtx.NodeID)].CDCData = BuildCDCDataForDirectInsert(
		&evalCtx, uint64(table.ID), table.Columns, di.InputValues, di.ColIndexs, cfg.CDCCoordinator)
	return nil
}

// TsprepareTypeCheck performs args conversion based on input type and column type
func TsprepareTypeCheck(
	ptCtx tree.ParseTimeContext,
	Args [][]byte,
	inferTypes []oid.Oid,
	ArgFormatCodes []pgwirebase.FormatCode,
	cols *[]sqlbase.ColumnDescriptor,
	di DirectInsert,
) ([][][]byte, []int64, error) {
	rowNum := di.RowNum / di.ColNum
	rowTimestamps := make([]int64, rowNum)
	if di.RowNum%di.ColNum != 0 {
		return nil, nil, pgerror.Newf(
			pgcode.Syntax,
			"insert (row %d) has more expressions than target columns, %d expressions for %d targets",
			rowNum, di.RowNum, di.ColNum)
	}
	isFirstCols := false
	for row := 0; row < rowNum; row++ {
		for col := 0; col < di.ColNum; col++ {
			colPos := di.IDMap[col]
			column := &(*cols)[colPos]
			// Determine by column ID that it is the first column, which is the timestamp column.
			if int(column.ID) == 1 {
				isFirstCols = true
			}
			idx := di.PosMap[col] + di.ColNum*row
			if Args[idx] == nil {
				if column.IsNullable() {
					continue
				} else {
					return nil, nil, sqlbase.NewNonNullViolationError(column.Name)
				}
			}

			var err error
			if ArgFormatCodes[idx] == pgwirebase.FormatText {
				switch inferTypes[idx] {
				case oid.T_timestamptz, oid.T_timestamp:
					err = timeFormatText(ptCtx, Args, idx, inferTypes[idx], column, isFirstCols, &rowTimestamps)
				case oid.T_int8, oid.T_int4, oid.T_int2:
					err = intFormatText(Args, idx, inferTypes[idx], column)
				case oid.T_float8, oid.T_float4:
					err = floatFormatText(Args, idx, inferTypes[idx], column)
				case oid.T_varchar, oid.T_bpchar, oid.T_varbytea, types.T_nchar, types.T_nvarchar:
					err = charFormatText(ptCtx, Args, idx, column, isFirstCols, &rowTimestamps)
				case oid.T_bool:
					err = boolFormatText(Args, idx)
				}
			} else {
				switch inferTypes[idx] {
				case oid.T_timestamptz, oid.T_timestamp:
					err = timeFormatBinary(Args, idx, column, isFirstCols, &rowTimestamps)
				case oid.T_int8, oid.T_int4, oid.T_int2:
					err = intFormatBinary(Args, idx, column, inferTypes[idx], isFirstCols, &rowTimestamps)
				case oid.T_float8:
					err = float8FormatBinary(Args, idx, column)
				case oid.T_float4:
					err = float4FormatBinary(Args, idx, column)
				case oid.T_varchar, oid.T_bpchar, oid.T_varbytea, types.T_nchar, types.T_nvarchar:
					err = charFormatBinary(Args, idx, column, isFirstCols, &rowTimestamps)
				case oid.T_bool:
					err = boolFormatBinary(Args, idx)
				}
			}
			if err != nil {
				return nil, nil, err
			}
		}
	}

	return nil, rowTimestamps, nil
}

func bigEndianToLittleEndian(bigEndian []byte) []byte {
	// Reverse the byte slice of the large endings
	// to obtain the byte slice of the small endings
	len := len(bigEndian)
	for i := 0; i < len/2; i++ {
		bigEndian[i], bigEndian[len-1-i] = bigEndian[len-1-i], bigEndian[i]
	}
	return bigEndian
}

// GetColsInfo to obtain relevant column information
func GetColsInfo(
	ctx context.Context,
	EvalContext tree.EvalContext,
	tsColsDesc *[]sqlbase.ColumnDescriptor,
	ins *tree.Insert,
	di *DirectInsert,
	stmt *parser.Statement,
	tsIDGen *sqlbase.TSIDGenerator,
) error {
	colsDesc := *tsColsDesc
	tableColLength := len(colsDesc)
	insertColLength := len(ins.Columns)

	var tagCount, dataCount int
	for i := 0; i < tableColLength; i++ {
		if colsDesc[i].IsDataCol() {
			dataCount++
		} else {
			tagCount++
		}
	}

	tagCols := make([]*sqlbase.ColumnDescriptor, 0, tagCount)
	dataCols := make([]*sqlbase.ColumnDescriptor, 0, dataCount)
	tID, dID := make([]int, 0, tagCount), make([]int, 0, dataCount)
	tPos, dPos := make([]int, 0, tagCount), make([]int, 0, dataCount)

	di.ColIndexs, di.DefIndexs = make(map[int]int, tableColLength), make(map[int]int, tableColLength)

	if insertColLength > 0 {
		columnIndexMap := make(map[string]int, insertColLength)
		for idx, colName := range ins.Columns {
			colNameStr := string(colName)
			if _, exists := columnIndexMap[colNameStr]; exists {
				return pgerror.Newf(pgcode.DuplicateColumn, "multiple assignments to the same column \"%s\"", colNameStr)
			}
			columnIndexMap[colNameStr] = idx
		}

		haveOtherTag, haveDataCol := false, false

		for i := 0; i < tableColLength; i++ {
			col := &colsDesc[i]
			colID := int(col.ID)
			insertPos, exists := columnIndexMap[col.Name]
			columnIndexMap[col.Name] = -1

			if exists {
				di.ColIndexs[colID] = insertPos
			} else {
				di.ColIndexs[colID] = -1
				di.DefIndexs[colID] = i
			}
			if col.IsDataCol() {
				dataCols = append(dataCols, col)
				if exists {
					dID = append(dID, i)
					dPos = append(dPos, insertPos)
					haveDataCol = true
				}
			} else {
				tagCols = append(tagCols, col)
				if exists {
					tID = append(tID, i)
					tPos = append(tPos, insertPos)
					haveOtherTag = true
				}
				if col.IsPrimaryTagCol() {
					di.PrimaryTagCols = append(di.PrimaryTagCols, col)
				}
			}
		}

		valueLength := len(stmt.Insertdirectstmt.InsertValues)
		for idx, name := range ins.Columns {
			if -1 != columnIndexMap[string(name)] {
				if !stmt.Insertdirectstmt.IgnoreBatcherror {
					return sqlbase.NewUndefinedColumnError(string(name))
				}

				cleanInsertValues := make([]string, 0, valueLength)
				// Traverse the unknown columns in each inserted row
				for i := 0; i < valueLength; i += insertColLength {
					if stmt.Insertdirectstmt.InsertValues[i+idx] != "" {
						// BatchInsert: Unknown column has value, record log, delete this row
						(*di).RowNum--
						stmt.Insertdirectstmt.BatchFailedColumn++
						log.Errorf(ctx, "BatchInsert Error: %s", sqlbase.NewUndefinedColumnError(string(name)))
						continue
					}
					// BatchInsert: Unknown column is null, ignore this column and continue inserting this row
					cleanInsertValues = append(cleanInsertValues, stmt.Insertdirectstmt.InsertValues[i:i+insertColLength]...)
				}
				(*stmt).Insertdirectstmt.InsertValues = cleanInsertValues
				valueLength = valueLength - insertColLength
			}
		}

		if !haveDataCol {
			dataCols = dataCols[:0]
		}
		if !haveOtherTag && haveDataCol {
			tagCols = tagCols[:0]
		}
	} else {
		tagFirstIdx := -1
		tagFirstFlag := false
		colIdx := 0
		for i := 0; i < tableColLength; i++ {
			col := &colsDesc[i]
			if col.IsDataCol() {
				dataCols = append(dataCols, col)
				dID = append(dID, i)
				dPos = append(dPos, colIdx)
				di.ColIndexs[int(col.ID)] = colIdx
				colIdx++
			} else if !tagFirstFlag {
				tagFirstIdx = i
				tagFirstFlag = true
			}
		}

		// Process the tag column again.
		for i := tagFirstIdx; i < tableColLength; i++ {
			col := &colsDesc[i]
			if col.IsTagCol() {
				tagCols = append(tagCols, col)
				tID = append(tID, i)
				tPos = append(tPos, colIdx)
				di.ColIndexs[int(col.ID)] = colIdx
				colIdx++
				if col.IsPrimaryTagCol() {
					di.PrimaryTagCols = append(di.PrimaryTagCols, col)
				}
			}
		}
	}

	tsVersion := uint32(1)
	var err error
	di.PArgs, err = execbuilder.BuildPayloadArgs(tsVersion, tsIDGen, di.PrimaryTagCols, tagCols, dataCols)
	if err != nil {
		return err
	}

	di.PrettyCols = di.PArgs.PrettyCols
	di.Dcs = dataCols

	totalLen := len(tID) + len(dID)
	di.IDMap = make([]int, totalLen)
	di.PosMap = make([]int, totalLen)
	copy(di.IDMap, tID)
	copy(di.IDMap[len(tID):], dID)
	copy(di.PosMap, tPos)
	copy(di.PosMap[len(tPos):], dPos)

	inputIdx := -1
	// check for not null columns
	colIndexMap := di.ColIndexs
	for _, col := range di.PrettyCols {
		if colIndexMap[int(col.ID)] < 0 {
			if !col.HasDefault() {
				switch {
				case col.IsPrimaryTagCol():
					priTagNames := make([]string, 0, len(di.PrimaryTagCols))
					for _, ptCol := range di.PrimaryTagCols {
						priTagNames = append(priTagNames, (*ptCol).Name)
					}
					return pgerror.Newf(pgcode.Syntax, "need to specify all primary tag %v", priTagNames)
				case col.IsOrdinaryTagCol() && !col.Nullable:
					return sqlbase.NewNonNullViolationError(col.Name)
				case !col.Nullable && len(dataCols) != 0:
					return sqlbase.NewNonNullViolationError(col.Name)
				}
			} else {
				colsWithDefaultValMap, _ := execbuilder.CheckDefaultVals(&EvalContext, di.PArgs)
				inputIdx = totalLen
				totalLen++
				colIndexMap[int(col.ID)] = inputIdx
				if _, ok := colsWithDefaultValMap[col.ID]; ok {
					if err = setDefaultValues(di, inputIdx, col, stmt); err != nil {
						return err
					}
					di.ColNum++
					di.IDMap, di.PosMap = append(di.IDMap, di.DefIndexs[int(col.ID)]), append(di.PosMap, inputIdx)
				} else {
					return pgerror.Newf(pgcode.CaseNotFound, "column '%s' should have default value '%s' "+
						"but not found in internal struct", col.Name, col.DefaultExprStr())
				}
			}
		}
	}

	// Build column converter cache for optimized type conversion
	di.ColConverters = BuildColConverters(di.PrettyCols)

	return nil
}

func setDefaultValues(
	di *DirectInsert, inputIdx int, col *sqlbase.ColumnDescriptor, stmt *parser.Statement,
) error {
	var def string
	var typ int
	defaultValStr := col.DefaultExprStr()
	for i := 0; i < di.RowNum; i++ {
		idx := i + inputIdx*(i+1)
		switch col.Type.InternalType.Family {
		case types.BoolFamily:
			def, typ = defaultValStr, 1
		case types.IntFamily, types.FloatFamily:
			def, typ = defaultValStr, 0
		case types.StringFamily:
			defaultExpr, err := parser.ParseExpr(col.DefaultExprStr())
			if err != nil {
				return err
			}
			if v, ok := defaultExpr.(*tree.StrVal); ok {
				defaultValStr = v.RawString()
			}
			def, typ = strings.Replace(defaultValStr, "'", "", -1), 1
		case types.TimestampFamily, types.TimestampTZFamily:
			if strings.HasPrefix(col.DefaultExprStr(), "now()") {
				def, typ = "now", 0
			} else if strings.HasPrefix(defaultValStr, "'") {
				def, typ = defaultValStr[1:len(col.DefaultExprStr())-1], 1
			} else {
				def, typ = defaultValStr, 0
			}
		case types.BytesFamily:
			defaultExpr, err := parser.ParseExpr(col.DefaultExprStr())
			if err != nil {
				return err
			}
			if v, ok := defaultExpr.(*tree.StrVal); ok {
				defaultValStr = v.RawString()
			}
			def, typ = defaultValStr, 3
		}
		stmt.Insertdirectstmt.ValuesType = append(stmt.Insertdirectstmt.ValuesType[:idx], append([]parser.TokenType{parser.TokenType(typ)}, stmt.Insertdirectstmt.ValuesType[idx:]...)...)
		stmt.Insertdirectstmt.InsertValues = append(stmt.Insertdirectstmt.InsertValues[:idx], append([]string{def}, stmt.Insertdirectstmt.InsertValues[idx:]...)...)
	}
	return nil
}

// preAllocateDataRowBytes calculates the memory size required by rowBytes based on the data columns
// and preAllocates the memory space.
func preAllocateDataRowBytes(
	rowNum int, dataCols *[]*sqlbase.ColumnDescriptor,
) (rowBytes [][]byte, dataOffset, varDataOffset int, err error) {
	dataRowSize, preSize, err := computeColumnSize(dataCols)
	if err != nil {
		return
	}

	bitmapLen := (len(*dataCols) + 7) >> 3

	singleRowSize := execbuilder.DataLenSize + bitmapLen + dataRowSize + preSize

	buffer := make([]byte, rowNum*singleRowSize)

	rowBytes = make([][]byte, rowNum)

	for i, j := 0, 0; i < rowNum; i++ {
		end := j + singleRowSize
		rowBytes[i] = buffer[j:end:end]
		j += singleRowSize
	}

	dataOffset = execbuilder.DataLenSize + bitmapLen
	varDataOffset = dataOffset + dataRowSize
	return
}

// ComputeColumnSize computes colSize
func computeColumnSize(cols *[]*sqlbase.ColumnDescriptor) (int, int, error) {
	colSize := 0
	preAllocSize := 0

	colsArr := *cols
	colsLen := len(colsArr)

	for i := 0; i < colsLen; i++ {
		col := colsArr[i]
		oidVal := col.Type.Oid()

		switch oidVal {
		case oid.T_int2:
			colSize += 2
		case oid.T_int4, oid.T_float4:
			colSize += 4
		case oid.T_int8, oid.T_float8, oid.T_timestamp, oid.T_timestamptz:
			colSize += 8
		case oid.T_bool:
			colSize++
		case oid.T_char, types.T_nchar, oid.T_text, oid.T_bpchar, oid.T_bytea, types.T_geometry:
			colSize += int(col.TsCol.StorageLen)
		case oid.T_varchar, types.T_nvarchar, types.T_varbytea:
			storageLen := int(col.TsCol.StorageLen)
			if col.TsCol.VariableLengthType == sqlbase.StorageTuple || col.IsPrimaryTagCol() {
				colSize += storageLen
			} else {
				// pre allocate paylaod space for var-length colums
				// here we use some fixed-rule to preallocate more space to improve efficiency
				// StorageLen = userWidth+1
				// varDataLen = StorageLen+2
				switch {
				case storageLen < 68:
					preAllocSize += storageLen // 100%
				case storageLen < 260:
					preAllocSize += (storageLen * 3) / 5 // 60%
				default:
					preAllocSize += (storageLen * 3) / 10 // 30%
				}
				colSize += execbuilder.VarColumnSize
			}
		default:
			return 0, 0, pgerror.Newf(pgcode.DatatypeMismatch, "unsupported input type oid %d (column %s)", oidVal, col.Name)
		}
	}

	return colSize, preAllocSize, nil
}

// columnExecInfo caches per-column metadata used by GetRowBytesForTsInsert to
// avoid repeated map lookups and per-row recalculation.
type columnExecInfo struct {
	column       *sqlbase.ColumnDescriptor
	colIdx       int
	isData       bool
	dataColIdx   int
	isLastData   bool
	curColLength int
	converter    DatumConverter
	isPrimaryTag bool
}

// RowProcessContext encapsulates all context needed for processing rows in getSingleRowBytes.
type RowProcessContext struct {
	// Parse context
	PtCtx tree.ParseTimeContext
	Di    *DirectInsert
	Tp    *execbuilder.TsPayload

	// Row data slices
	RowBytes      [][]byte
	InsertValues  []string
	ValuesType    []parser.TokenType
	InputValues   []tree.Datums
	RowTimestamps []int64

	// Column metadata (computed once, reused for all rows)
	DataInfos []columnExecInfo
	TagInfos  []columnExecInfo

	// Reusable buffer for building tag key
	TagKeyBuf []byte

	// Fixed offsets
	DataOffset        int
	IndependentOffset int
}

// GetRowBytesForTsInsert performs column type conversion and length checking
func GetRowBytesForTsInsert(
	ctx context.Context,
	ptCtx tree.ParseTimeContext,
	di *DirectInsert,
	stmts parser.Statements,
	rowTimestamps []int64,
) ([]tree.Datums, map[string][]int, [][]byte, error) {
	// Pre-allocate all slices with exact sizes to avoid reallocations
	rowNum := di.RowNum
	colNum := di.ColNum
	totalSize := rowNum * colNum
	preSlice := make([]tree.Datum, totalSize)
	inputValues := make([]tree.Datums, rowNum)
	outputValues := make([]tree.Datums, 0, rowNum)

	insertStmt := stmts[0].Insertdirectstmt
	insertValues := insertStmt.InsertValues
	valuesType := insertStmt.ValuesType

	// Initialize input values slice efficiently
	for i, j := 0, 0; i < rowNum; i++ {
		end := j + colNum
		inputValues[i] = preSlice[j:end:end]
		j += colNum
	}
	tp := execbuilder.NewTsPayload()
	rowBytes, dataOffset, independentOffset, err := preAllocateDataRowBytes(rowNum, &di.Dcs)
	if err != nil {
		return nil, nil, nil, err
	}
	// partition input data based on primary tag values
	priTagValMap := make(map[string][]int)
	// Reuse tag key buffer to avoid per-row allocations.
	tagKeyBuf := make([]byte, 0, di.PArgs.PTagNum*8+16)
	// Type check for input values.
	outrowBytes := make([][]byte, 0, rowNum)

	// Track batch errors
	batchFailed := &insertStmt.BatchFailed
	ignoreBatchErr := insertStmt.IgnoreBatcherror

	// Pre-compute column execution metadata to avoid per-row map lookups and
	// repeated length/position calculations.
	dataInfos := make([]columnExecInfo, 0, di.PArgs.DataColNum)
	tagInfos := make([]columnExecInfo, 0, len(di.PrettyCols)-di.PArgs.DataColNum)
	dataIdx := 0
	for i, column := range di.PrettyCols {
		colIdx := di.ColIndexs[int(column.ID)]
		isData := column.IsDataCol()
		info := columnExecInfo{
			column:       column,
			colIdx:       colIdx,
			isData:       isData,
			isPrimaryTag: i < di.PArgs.PTagNum,
		}
		if isData {
			info.dataColIdx = dataIdx
			info.isLastData = dataIdx == di.PArgs.DataColNum-1
			info.curColLength = execbuilder.VarColumnSize
			if int(column.TsCol.VariableLengthType) == sqlbase.StorageTuple {
				info.curColLength = int(column.TsCol.StorageLen)
			}
			dataIdx++
			dataInfos = append(dataInfos, info)
		} else {
			tagInfos = append(tagInfos, info)
		}
		if di.ColConverters != nil && i < len(di.ColConverters) {
			info.converter = di.ColConverters[i]
			if isData {
				dataInfos[len(dataInfos)-1].converter = info.converter
			} else {
				tagInfos[len(tagInfos)-1].converter = info.converter
			}
		}
	}

	// Initialize row processing context once, reuse for all rows
	rowCtx := &RowProcessContext{
		PtCtx:             ptCtx,
		Di:                di,
		Tp:                tp,
		RowBytes:          rowBytes,
		InsertValues:      insertValues,
		ValuesType:        valuesType,
		InputValues:       inputValues,
		RowTimestamps:     rowTimestamps,
		DataInfos:         dataInfos,
		TagInfos:          tagInfos,
		TagKeyBuf:         tagKeyBuf,
		DataOffset:        dataOffset,
		IndependentOffset: independentOffset,
	}

	for row := range inputValues {
		tp.SetPayload(rowBytes[row])

		tagKey, err := getSingleRowBytes(rowCtx, row)
		if err != nil {
			if !ignoreBatchErr {
				return nil, nil, nil, err
			}
			*batchFailed++
			log.Errorf(ctx, "BatchInsert Error: %s", err)
			continue
		}

		adjustedRow := row - *batchFailed

		outputValues = append(outputValues, inputValues[row])
		priTagValMap[tagKey] = append(priTagValMap[tagKey], adjustedRow)
		outrowBytes = append(outrowBytes, rowBytes[row])
	}

	return outputValues, priTagValMap, outrowBytes, nil
}

// getSingleRowBytes processes a single row and returns the tag key.
func getSingleRowBytes(ctx *RowProcessContext, row int) (string, error) {
	// Pre-calculate constants to avoid repeated calculations
	bitmapOffset := execbuilder.DataLenSize
	colNum := ctx.Di.ColNum

	// Pre-fetch frequently accessed values
	rowOffset := row * colNum
	buf := ctx.TagKeyBuf[:0]

	// Extract frequently used fields from context
	tp := ctx.Tp
	ptCtx := ctx.PtCtx
	rowBytes := ctx.RowBytes
	insertValues := ctx.InsertValues
	valuesType := ctx.ValuesType
	inputValues := ctx.InputValues
	rowTimestamps := ctx.RowTimestamps
	dataInfos := ctx.DataInfos
	tagInfos := ctx.TagInfos
	offset := ctx.DataOffset
	varDataOffset := ctx.IndependentOffset

	// Process tag columns first to ensure NOT NULL constraint check order
	// matches column definition order (primary tags -> other tags -> data columns).
	for i := range tagInfos {
		info := &tagInfos[i]
		column := info.column
		colIdx := info.colIdx

		// Early exit for invalid column index
		if colIdx < 0 {
			if !column.IsNullable() {
				return "", sqlbase.NewNonNullViolationError(column.Name)
			}
			continue
		}

		// Handle tag columns
		rawValue := insertValues[colIdx+rowOffset]
		valueType := valuesType[colIdx+rowOffset]

		// Process NULL values
		if valueType != parser.STRINGTYPE && rawValue == "" {
			if !column.IsNullable() {
				return "", sqlbase.NewNonNullViolationError(column.Name)
			}
			inputValues[row][colIdx] = tree.DNull
			continue
		}

		// Get datum for non-NULL values using cached converter
		var datum tree.Datum
		var err error
		if info.converter != nil {
			datum, err = info.converter(ptCtx, column, valueType, rawValue)
		} else {
			datum, err = GetSingleDatum(ptCtx, *column, valueType, rawValue)
		}
		if err != nil {
			return "", err
		}

		inputValues[row][colIdx] = datum

		// Build primary tag key
		if info.isPrimaryTag {
			vString := sqlbase.DatumToString(datum)
			// Distinguish aa + bb = a + abb
			buf = strconv.AppendInt(buf, int64(len(vString)), 10)
			buf = append(buf, ':')
			buf = append(buf, vString...)
		}
	}

	// Process data columns after tag columns.
	for i := range dataInfos {
		info := &dataInfos[i]
		column := info.column
		colIdx := info.colIdx

		dataColIdx := info.dataColIdx
		isLastDataCol := info.isLastData
		curColLength := info.curColLength

		// Handle NULL values for columns not specified in INSERT or with invalid index
		if colIdx < 0 {
			if !column.IsNullable() {
				return "", sqlbase.NewNonNullViolationError(column.Name)
			}
			execbuilder.SetBit(tp, bitmapOffset, dataColIdx)
			offset += curColLength

			if isLastDataCol {
				execbuilder.WriteUint32ToPayload(tp, uint32(varDataOffset-execbuilder.DataLenSize))
				rowBytes[row] = tp.GetPayload(varDataOffset)
			}
			continue
		}

		// Process non-NULL data values
		rawValue := insertValues[colIdx+rowOffset]
		valueType := valuesType[colIdx+rowOffset]

		var datum tree.Datum
		var err error
		if valueType != parser.STRINGTYPE && rawValue == "" {
			if !column.IsNullable() {
				return "", sqlbase.NewNonNullViolationError(column.Name)
			}
			// attempting to insert a NULL value when no value is specified
			datum = tree.DNull
		} else {
			// Use cached converter for optimized type conversion
			if info.converter != nil {
				datum, err = info.converter(ptCtx, column, valueType, rawValue)
			} else {
				datum, err = GetSingleDatum(ptCtx, *column, valueType, rawValue)
			}
			if err != nil {
				return "", err
			}
		}

		inputValues[row][colIdx] = datum

		if inputValues[row][colIdx] == tree.DNull {
			execbuilder.SetBit(tp, bitmapOffset, dataColIdx)
			offset += curColLength
			// Fill the length of rowByte
			if isLastDataCol {
				execbuilder.WriteUint32ToPayload(tp, uint32(varDataOffset-execbuilder.DataLenSize))
				rowBytes[row] = tp.GetPayload(varDataOffset)
			}
			continue
		}

		// Handle first timestamp column
		if dataColIdx == 0 {
			rowTimestamps[row] = int64(*datum.(*tree.DInt))
		}

		// Fill column data
		if varDataOffset, err = tp.FillColData(
			datum, column, false, false,
			offset, varDataOffset, bitmapOffset,
		); err != nil {
			return "", err
		}

		offset += curColLength
		if isLastDataCol {
			tp.SetPayload(tp.GetPayload(varDataOffset))
			execbuilder.WriteUint32ToPayload(tp, uint32(varDataOffset-execbuilder.DataLenSize))
			rowBytes[row] = tp.GetPayload(varDataOffset)
		}
	}

	ctx.TagKeyBuf = buf
	return string(buf), nil
}

// Specialized converters for each type
func convertTimestampTZ(
	ptCtx tree.ParseTimeContext,
	column *sqlbase.ColumnDescriptor,
	valueType parser.TokenType,
	rawValue string,
) (tree.Datum, error) {
	if valueType == parser.NORMALTYPE {
		return convertTimestampNumeric(column, rawValue, valueType)
	}
	if valueType == parser.STRINGTYPE {
		return convertTimestampTZString(ptCtx, column, rawValue)
	}
	return convertTimestampNumeric(column, rawValue, valueType)
}

func convertTimestamp(
	ptCtx tree.ParseTimeContext,
	column *sqlbase.ColumnDescriptor,
	valueType parser.TokenType,
	rawValue string,
) (tree.Datum, error) {
	if valueType == parser.NORMALTYPE {
		return convertTimestampNumeric(column, rawValue, valueType)
	}
	if valueType == parser.STRINGTYPE {
		return convertTimestampString(column, rawValue)
	}
	return convertTimestampNumeric(column, rawValue, valueType)
}

func convertTimestampTZString(
	ptCtx tree.ParseTimeContext, column *sqlbase.ColumnDescriptor, rawValue string,
) (tree.Datum, error) {
	precision := tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision())
	datum, err := tree.ParseDTimestampTZ(ptCtx, rawValue, precision)
	if err != nil {
		return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
	}
	var dVal *tree.DInt
	dVal, err = GetTsTimestampWidth(*column, datum, dVal, rawValue)
	if err != nil {
		return nil, err
	}
	if err = tree.CheckTsTimestampWidth(&column.Type, *dVal, rawValue, column.Name); err != nil {
		return nil, err
	}
	return dVal, nil
}

func convertTimestampString(column *sqlbase.ColumnDescriptor, rawValue string) (tree.Datum, error) {
	precision := tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision())
	datum, err := tree.ParseDTimestamp(nil, rawValue, precision)
	if err != nil {
		return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
	}
	var dVal *tree.DInt
	dVal, err = GetTsTimestampWidth(*column, datum, dVal, rawValue)
	if err != nil {
		return nil, err
	}
	if err = tree.CheckTsTimestampWidth(&column.Type, *dVal, rawValue, column.Name); err != nil {
		return nil, err
	}
	return dVal, nil
}

func convertTimestampNumeric(
	column *sqlbase.ColumnDescriptor, rawValue string, valueType parser.TokenType,
) (tree.Datum, error) {
	if rawValue == "now" {
		dVal, err := tree.LimitTsTimestampWidth(timeutil.Now(), &column.Type, "", column.Name)
		if err != nil {
			return nil, err
		}
		return tree.NewDInt(*dVal), nil
	}
	in, err := strconv.ParseInt(rawValue, 10, 64)
	if err != nil {
		if valueType == parser.NORMALTYPE {
			return nil, pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
		}
		if strings.Contains(err.Error(), "out of range") {
			return nil, err
		}
		return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
	}
	dVal := tree.DInt(in)
	if err = tree.CheckTsTimestampWidth(&column.Type, dVal, rawValue, column.Name); err != nil {
		return nil, err
	}
	return &dVal, nil
}

// fastParseInt attempts to parse an integer string with minimal overhead.
// Returns the parsed value and true if successful, or 0 and false if parsing failed.
// This function handles simple decimal integers (with optional leading minus sign)
// and is designed for the common case of numeric literals.
func fastParseInt(s string) (int64, bool) {
	if len(s) == 0 {
		return 0, false
	}

	var neg bool
	var i int

	// Handle sign
	if s[0] == '-' {
		neg = true
		i = 1
		if len(s) == 1 {
			return 0, false
		}
	} else if s[0] == '+' {
		i = 1
		if len(s) == 1 {
			return 0, false
		}
	}

	// Parse digits
	var n uint64
	for ; i < len(s); i++ {
		ch := s[i]
		if ch < '0' || ch > '9' {
			return 0, false
		}
		// Check for overflow before multiplication
		if n > (math.MaxUint64-9)/10 {
			return 0, false
		}
		n = n*10 + uint64(ch-'0')
	}

	// Check for overflow based on sign
	if neg {
		if n > uint64(-math.MinInt64) {
			return 0, false
		}
		return -int64(n), true
	}
	if n > math.MaxInt64 {
		return 0, false
	}
	return int64(n), true
}

// parseIntCommon handles common int parsing logic, reducing code duplication
// Returns parsed value and whether parsing was successful
func parseIntCommon(
	column *sqlbase.ColumnDescriptor, valueType parser.TokenType, rawValue string,
) (int64, tree.Datum, error) {
	// Fallback to strconv for edge cases and error detection
	in, err := strconv.ParseInt(rawValue, 10, 64)
	if err == nil {
		return in, nil, nil
	}

	if len(rawValue) == 4 && rawValue == "true" {
		return 0, tree.NewDInt(1), nil
	}
	if len(rawValue) == 5 && rawValue == "false" {
		return 0, tree.NewDInt(0), nil
	}

	// Check for numeric overflow - use type assertion directly
	if numErr, ok := err.(*strconv.NumError); ok && numErr.Err == strconv.ErrRange {
		return 0, nil, pgerror.Newf(pgcode.NumericValueOutOfRange, "numeric constant out of int64 range")
	}

	// Return appropriate error based on value type
	if valueType == parser.NORMALTYPE {
		return 0, nil, pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
	}
	return 0, nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
}

func convertInt8(
	ptCtx tree.ParseTimeContext,
	column *sqlbase.ColumnDescriptor,
	valueType parser.TokenType,
	rawValue string,
) (tree.Datum, error) {
	if valueType == parser.STRINGTYPE {
		return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
	}

	// Fast path
	if in, ok := fastParseInt(rawValue); ok {
		d := tree.DInt(in)
		return &d, nil
	}

	in, dat, err := parseIntCommon(column, valueType, rawValue)
	if err != nil {
		return nil, err
	}
	if dat != nil {
		return dat, nil
	}

	d := tree.DInt(in)
	return &d, nil
}

func convertInt4(
	ptCtx tree.ParseTimeContext,
	column *sqlbase.ColumnDescriptor,
	valueType parser.TokenType,
	rawValue string,
) (tree.Datum, error) {
	if valueType == parser.STRINGTYPE {
		return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
	}

	// Fast path
	if in, ok := fastParseInt(rawValue); ok {
		if in < math.MinInt32 || in > math.MaxInt32 {
			return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, errOutOfRange, rawValue, column.Type.SQLString(), column.Name)
		}
		d := tree.DInt(in)
		return &d, nil
	}

	in, dat, err := parseIntCommon(column, valueType, rawValue)
	if err != nil {
		return nil, err
	}
	if dat != nil {
		return dat, nil
	}

	if in < math.MinInt32 || in > math.MaxInt32 {
		return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, errOutOfRange, rawValue, column.Type.SQLString(), column.Name)
	}

	d := tree.DInt(in)
	return &d, nil
}

func convertInt2(
	ptCtx tree.ParseTimeContext,
	column *sqlbase.ColumnDescriptor,
	valueType parser.TokenType,
	rawValue string,
) (tree.Datum, error) {
	if valueType == parser.STRINGTYPE {
		return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
	}

	// Fast path
	if in, ok := fastParseInt(rawValue); ok {
		if in < math.MinInt16 || in > math.MaxInt16 {
			return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, errOutOfRange, rawValue, column.Type.SQLString(), column.Name)
		}
		d := tree.DInt(in)
		return &d, nil
	}

	in, dat, err := parseIntCommon(column, valueType, rawValue)
	if err != nil {
		return nil, err
	}
	if dat != nil {
		return dat, nil
	}

	if in < math.MinInt16 || in > math.MaxInt16 {
		return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, errOutOfRange, rawValue, column.Type.SQLString(), column.Name)
	}

	d := tree.DInt(in)
	return &d, nil
}

func convertString(
	ptCtx tree.ParseTimeContext,
	column *sqlbase.ColumnDescriptor,
	valueType parser.TokenType,
	rawValue string,
) (tree.Datum, error) {
	if valueType == parser.NUMTYPE {
		return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
	}
	if valueType == parser.NORMALTYPE {
		return nil, pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
	}
	if width := column.Type.Width(); width > 0 {
		if len(rawValue) > int(width) {
			return nil, pgerror.Newf(pgcode.StringDataRightTruncation, errTooLong, rawValue, column.Type.SQLString(), column.Name)
		}
	}
	return tree.NewDString(rawValue), nil
}

func convertNString(
	ptCtx tree.ParseTimeContext,
	column *sqlbase.ColumnDescriptor,
	valueType parser.TokenType,
	rawValue string,
) (tree.Datum, error) {
	if valueType == parser.NUMTYPE {
		return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
	}
	if valueType == parser.NORMALTYPE {
		return nil, pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
	}
	if width := column.Type.Width(); width > 0 {
		if utf8.RuneCountInString(rawValue) > int(width) {
			return nil, pgerror.Newf(pgcode.StringDataRightTruncation, errTooLong, rawValue, column.Type.SQLString(), column.Name)
		}
	}
	return tree.NewDString(rawValue), nil
}

func convertBytes(
	ptCtx tree.ParseTimeContext,
	column *sqlbase.ColumnDescriptor,
	valueType parser.TokenType,
	rawValue string,
) (tree.Datum, error) {
	if valueType == parser.NUMTYPE {
		return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
	}
	if valueType == parser.NORMALTYPE {
		return nil, pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
	}
	if width := column.Type.Width(); width > 0 {
		if len(rawValue) > int(width) {
			return nil, pgerror.Newf(pgcode.StringDataRightTruncation, errTooLong, rawValue, column.Type.SQLString(), column.Name)
		}
	}
	if valueType == parser.BYTETYPE {
		rawValue = strings.Trim(tree.NewDBytes(tree.DBytes(rawValue)).String(), "'")
	}
	v, err := tree.ParseDByte(rawValue)
	if err != nil {
		return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
	}
	return v, nil
}

func convertFloat(
	ptCtx tree.ParseTimeContext,
	column *sqlbase.ColumnDescriptor,
	valueType parser.TokenType,
	rawValue string,
) (tree.Datum, error) {
	if valueType == parser.STRINGTYPE {
		return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
	}
	in, err := strconv.ParseFloat(rawValue, 64)
	if err != nil {
		if strings.Contains(err.Error(), "out of range") {
			return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, "float \"%s\" out of range for type %s (column %s)", rawValue, column.Type.SQLString(), column.Name)
		}
		return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
	}
	return tree.NewDFloat(tree.DFloat(in)), nil
}

func convertBool(
	ptCtx tree.ParseTimeContext,
	column *sqlbase.ColumnDescriptor,
	valueType parser.TokenType,
	rawValue string,
) (tree.Datum, error) {
	if valueType == parser.NORMALTYPE {
		return nil, pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
	}
	dBool, err := tree.ParseDBool(rawValue)
	if err != nil {
		return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
	}
	return dBool, nil
}

func convertGeometry(
	ptCtx tree.ParseTimeContext,
	column *sqlbase.ColumnDescriptor,
	valueType parser.TokenType,
	rawValue string,
) (tree.Datum, error) {
	if valueType == parser.NORMALTYPE {
		return nil, pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
	}
	if valueType == parser.NUMTYPE {
		return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
	}
	if _, err := geos.FromWKT(rawValue); err != nil {
		if strings.Contains(err.Error(), "load error") {
			return nil, err
		}
		return nil, pgerror.Newf(pgcode.DataException, errInvalidValue, rawValue, column.Type.SQLString(), column.Name)
	}
	return tree.NewDString(rawValue), nil
}

func convertUnsupported(
	ptCtx tree.ParseTimeContext,
	column *sqlbase.ColumnDescriptor,
	valueType parser.TokenType,
	rawValue string,
) (tree.Datum, error) {
	return nil, pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
}

// datumConverterTable maps OID to corresponding converter function
var datumConverterTable = map[oid.Oid]DatumConverter{
	oid.T_timestamptz: convertTimestampTZ,
	oid.T_timestamp:   convertTimestamp,
	oid.T_int8:        convertInt8,
	oid.T_int4:        convertInt4,
	oid.T_int2:        convertInt2,
	oid.T_cstring:     convertString,
	oid.T_char:        convertString,
	oid.T_text:        convertString,
	oid.T_bpchar:      convertString,
	oid.T_varchar:     convertString,
	oid.T_bytea:       convertBytes,
	oid.T_varbytea:    convertBytes,
	oid.Oid(91004):    convertNString, // nchar
	oid.Oid(91002):    convertNString, // nvarchar
	oid.T_float4:      convertFloat,
	oid.T_float8:      convertFloat,
	oid.T_bool:        convertBool,
	types.T_geometry:  convertGeometry,
}

// GetConverterByOid returns the converter function for given OID
func GetConverterByOid(oidType oid.Oid) DatumConverter {
	if conv, ok := datumConverterTable[oidType]; ok {
		return conv
	}
	return convertUnsupported
}

// BuildColConverters builds converter cache for all columns
func BuildColConverters(cols []*sqlbase.ColumnDescriptor) []DatumConverter {
	converters := make([]DatumConverter, len(cols))
	for i, col := range cols {
		converters[i] = GetConverterByOid(col.Type.Oid())
	}
	return converters
}

// GetSingleDatum gets single datum by columnDesc
func GetSingleDatum(
	ptCtx tree.ParseTimeContext,
	column sqlbase.ColumnDescriptor,
	valueType parser.TokenType,
	rawValue string,
) (tree.Datum, error) {
	oidType := column.Type.Oid()
	if valueType == parser.NORMALTYPE && oidType != oid.T_timestamptz && oidType != oid.T_timestamp {
		return nil, pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
	}
	conv := GetConverterByOid(oidType)
	return conv(ptCtx, &column, valueType, rawValue)
}

// GetTsTimestampWidth checks that the width (for Timestamp/TimestampTZ) of the value fits the
// specified column type.
func GetTsTimestampWidth(
	column sqlbase.ColumnDescriptor, datum tree.Datum, dVal *tree.DInt, rawValue string,
) (*tree.DInt, error) {
	var datumNa int
	var datumunix, datumunixnao int64

	switch t := datum.(type) {
	case *tree.DTimestampTZ:
		datumNa = t.Nanosecond()
		datumunix = t.Unix()
		datumunixnao = t.UnixNano()
	case *tree.DTimestamp:
		datumNa = t.Nanosecond()
		datumunix = t.Unix()
		datumunixnao = t.UnixNano()
	}

	switch column.Type.InternalType.Precision {
	case tree.MilliTimestampWidth, tree.DefaultTimestampWidth:
		if datumunix < tree.TsMinSecondTimestamp || datumunix > tree.TsMaxSecondTimestamp {
			return nil, tree.NewValueOutOfRangeError(&column.Type, rawValue, column.Name)
		}
		roundSec := int64(datumNa / 1e6)
		next := datumNa/1e5 - int(roundSec*10)
		if next > 4 {
			// Round up
			roundSec++
		}
		dVal = tree.NewDInt(tree.DInt(datumunix*1e3 + roundSec))
	case tree.MicroTimestampWidth:
		if datumunix < tree.TsMinSecondTimestamp || datumunix > tree.TsMaxSecondTimestamp {
			return nil, tree.NewValueOutOfRangeError(&column.Type, rawValue, column.Name)
		}
		roundSec := int64(datumNa / 1e3)
		next := datumNa/1e2 - int(roundSec*10)
		if next > 4 {
			roundSec++
		}
		dVal = tree.NewDInt(tree.DInt(datumunix*1e6 + roundSec))
	case tree.NanoTimestampWidth:
		if datumunix < 0 || datumunix > tree.TsMaxNanoTimestamp/1e9 {
			return nil, tree.NewValueOutOfRangeError(&column.Type, rawValue, column.Name)
		}
		dVal = tree.NewDInt(tree.DInt(datumunixnao))
	default:
		return nil, tree.NewUnexpectedWidthError(&column.Type, column.Name)
	}
	return dVal, nil
}

// GetPayloadMapForMuiltNode builds payloads for distributed insertion
func GetPayloadMapForMuiltNode(
	ctx context.Context,
	ptCtx tree.ParseTimeContext,
	dit DirectInsertTable,
	di *DirectInsert,
	stmts parser.Statements,
	evalCtx tree.EvalContext,
	table *sqlbase.ImmutableTableDescriptor,
	cfg *ExecutorConfig,
) error {
	rowTimestamps := make([]int64, di.RowNum)

	inputValues, priTagValMap, rowBytes, err := GetRowBytesForTsInsert(ctx, ptCtx, di, stmts, rowTimestamps)
	if err != nil {
		return err
	}
	di.PArgs.DataColNum, di.PArgs.DataColSize, di.PArgs.PreAllocColSize = 0, 0, 0

	allPayloads := make([]*sqlbase.SinglePayloadInfo, 0, len(priTagValMap))

	cols := di.PArgs.PrettyCols[:di.PArgs.PTagNum+di.PArgs.AllTagNum]
	tabID := sqlbase.ID(dit.TabID)
	hashNum := dit.HashNum

	for _, priTagRowIdx := range priTagValMap {
		// Payload is the encoding of a complete line, which is the first line in a line with the same ptag
		payload, _, err := execbuilder.BuildPayloadForTsInsert(
			&evalCtx,
			evalCtx.Txn,
			inputValues,
			priTagRowIdx[:1],
			cols,
			di.ColIndexs,
			di.PArgs,
			dit.DbID,
			uint32(table.ID),
			hashNum,
		)
		if err != nil {
			return err
		}

		// Make primaryTag key.
		hashPoints := sqlbase.DecodeHashPointFromPayload(payload)
		primaryTagKey := sqlbase.MakeTsPrimaryTagKey(table.ID, hashPoints)
		if !evalCtx.StartSinglenode {
			rowCount := len(priTagRowIdx)
			groupRowBytes := make([][]byte, rowCount)

			var valueSize int32

			for i, idx := range priTagRowIdx {
				groupRowBytes[i] = rowBytes[idx]
				valueSize += int32(len(groupRowBytes[i]))
			}

			hashPoint := uint64(hashPoints[0])
			var startKey, endKey roachpb.Key
			if di.PArgs.RowType == execbuilder.OnlyTag {
				startKey = sqlbase.MakeTsHashPointKey(tabID, hashPoint, hashNum)
				endKey = sqlbase.MakeTsRangeKey(tabID, hashPoint+1, hashNum)
			} else {
				startKey = sqlbase.MakeTsRangeKey(tabID, hashPoint, hashNum)
				endKey = sqlbase.MakeTsRangeKey(tabID, hashPoint+1, hashNum)
			}

			allPayloads = append(allPayloads, &sqlbase.SinglePayloadInfo{
				Payload:       payload,
				RowNum:        uint32(rowCount),
				PrimaryTagKey: primaryTagKey,
				RowBytes:      groupRowBytes,
				StartKey:      startKey,
				EndKey:        endKey,
				ValueSize:     valueSize,
				HashNum:       hashNum,
			})
		} else {
			valueSize := int32(0)
			rowNum := uint32(0)
			for _, idx := range priTagRowIdx {
				valueSize += int32(len(rowBytes[idx]))
				rowNum++
			}

			payloadSize := int32(len(payload)) + valueSize + 4
			payloadBytes := make([]byte, payloadSize)
			copy(payloadBytes, payload)
			offset := len(payload)
			binary.LittleEndian.PutUint32(payloadBytes[offset:], uint32(valueSize))
			offset += 4
			for _, idx := range priTagRowIdx {
				copy(payloadBytes[offset:], rowBytes[idx])
				offset += len(rowBytes[idx])
			}

			binary.LittleEndian.PutUint32(payloadBytes[38:], uint32(rowNum))

			allPayloads = append(allPayloads, &sqlbase.SinglePayloadInfo{
				Payload:       payloadBytes,
				RowNum:        uint32(len(priTagRowIdx)),
				PrimaryTagKey: primaryTagKey,
				HashNum:       hashNum,
			})
		}
	}

	di.PayloadNodeMap = map[int]*sqlbase.PayloadForDistTSInsert{
		int(evalCtx.NodeID): {
			NodeID:          evalCtx.NodeID,
			PerNodePayloads: allPayloads,
		},
	}

	di.PayloadNodeMap[int(evalCtx.NodeID)].CDCData = BuildCDCDataForDirectInsert(
		&evalCtx, uint64(tabID), table.Columns, inputValues, di.ColIndexs, cfg.CDCCoordinator)

	return nil
}

// GetTSColumnByName used for NoSchema
func GetTSColumnByName(
	inputName tree.Name, cols []sqlbase.ColumnDescriptor,
) (*sqlbase.ColumnDescriptor, error) {
	for i := range cols {
		if string(inputName) == cols[i].Name {
			return &cols[i], nil
		}
	}
	return nil, sqlbase.NewUndefinedColumnError(string(inputName))
}

func timeFormatText(
	ptCtx tree.ParseTimeContext,
	Args [][]byte,
	idx int,
	infer oid.Oid,
	column *sqlbase.ColumnDescriptor,
	isFirstCols bool,
	rowTimestamps *[]int64,
) error {
	var tum int64
	var tsStr string
	if infer == oid.T_timestamptz {
		t, err := tree.ParseDTimestampTZ(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
		if err != nil {
			return tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
		}
		tum, tsStr = t.UnixMilli(), t.String()
	} else {
		t, err := tree.ParseDTimestamp(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
		if err != nil {
			return tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
		}
		tum, tsStr = t.UnixMilli(), t.String()
	}
	if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
		return pgerror.Newf(pgcode.StringDataLengthMismatch,
			"value '%s' out of range for type %s (column %s)", tsStr, column.Type.SQLString(), column.Name)
	}
	Args[idx] = make([]byte, 8)
	binary.LittleEndian.PutUint64(Args[idx], uint64(tum))
	if isFirstCols {
		*rowTimestamps = append(*rowTimestamps, tum)
	}
	return nil
}

func intFormatText(Args [][]byte, idx int, infer oid.Oid, column *sqlbase.ColumnDescriptor) error {
	i, err := strconv.ParseInt(string(Args[idx]), 10, 64)
	if err != nil {
		return err
	}
	byteSize, minVal, maxVal := 8, int64(math.MinInt64), int64(math.MaxInt64)
	if infer == oid.T_int4 {
		byteSize, minVal, maxVal = 4, math.MinInt32, math.MaxInt32
	} else if infer == oid.T_int2 {
		byteSize, minVal, maxVal = 2, math.MinInt16, math.MaxInt16
	}
	if i < minVal || i > maxVal {
		return pgerror.Newf(pgcode.NumericValueOutOfRange,
			"integer out of range for type %s (column %q)", column.Type.SQLString(), column.Name)
	}
	Args[idx] = make([]byte, byteSize)
	switch byteSize {
	case 8:
		binary.LittleEndian.PutUint64(Args[idx], uint64(i))
	case 4:
		binary.LittleEndian.PutUint32(Args[idx], uint32(i))
	case 2:
		binary.LittleEndian.PutUint16(Args[idx], uint16(i))
	}
	return nil
}

func floatFormatText(
	Args [][]byte, idx int, infer oid.Oid, column *sqlbase.ColumnDescriptor,
) error {
	f, err := strconv.ParseFloat(string(Args[idx]), 64)
	if infer == oid.T_float8 {
		if err != nil || (f != 0 && (math.Abs(f) < math.SmallestNonzeroFloat64 || math.Abs(f) > math.MaxFloat64)) {
			return pgerror.Newf(pgcode.NumericValueOutOfRange,
				"float \"%g\" out of range for type float (column %s)", f, column.Name)
		}
		Args[idx] = make([]byte, 8)
		binary.LittleEndian.PutUint64(Args[idx], uint64(int64(math.Float64bits(f))))
	} else {
		if err != nil || (f != 0 && (math.Abs(f) < math.SmallestNonzeroFloat32 || math.Abs(f) > math.MaxFloat32)) {
			return pgerror.Newf(pgcode.NumericValueOutOfRange,
				"float \"%s\" out of range for type float4 (column %s)", string(Args[idx]), column.Name)
		}
		Args[idx] = make([]byte, 4)
		binary.LittleEndian.PutUint32(Args[idx], uint32(int32(math.Float32bits(float32(f)))))
	}
	return nil
}

func charFormatText(
	ptCtx tree.ParseTimeContext,
	Args [][]byte,
	idx int,
	column *sqlbase.ColumnDescriptor,
	isFirstCols bool,
	rowTimestamps *[]int64,
) error {
	switch column.Type.Family() {
	case types.StringFamily, types.BytesFamily, types.TimestampFamily, types.TimestampTZFamily, types.BoolFamily:
		switch column.Type.Oid() {
		case oid.T_varchar, oid.T_bpchar, oid.T_text, oid.T_bytea, oid.T_varbytea, types.T_nchar, types.T_nvarchar:
			length := len(string(Args[idx]))
			if column.Type.Oid() == types.T_nchar || column.Type.Oid() == types.T_nvarchar {
				length = utf8.RuneCountInString(string(Args[idx]))
			}
			if column.Type.Width() > 0 && length > int(column.Type.Width()) {
				return pgerror.Newf(pgcode.StringDataRightTruncation,
					"value too long for type %s (column %q)", column.Type.SQLString(), column.Name)
			}
		case oid.T_timestamptz:
			// string type
			t, err := tree.ParseDTimestampTZ(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
			if err != nil {
				return tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
			}
			tum := t.UnixMilli()
			if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
				if column.Type.Oid() == oid.T_timestamptz {
					return pgerror.Newf(pgcode.StringDataLengthMismatch,
						"value '%s' out of range for type %s (column %s)", t.String(), column.Type.SQLString(), column.Name)
				}
				return pgerror.Newf(pgcode.StringDataLengthMismatch,
					"value '%s' out of range for type %s (column %s)", t.String(), column.Type.SQLString(), column.Name)
			}
			Args[idx] = make([]byte, 8)
			binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
			if isFirstCols {
				*rowTimestamps = append(*rowTimestamps, tum)
			}
		case oid.T_timestamp:
			// string type
			t, err := tree.ParseDTimestamp(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
			if err != nil {
				return tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
			}
			tum := t.UnixMilli()
			if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
				if column.Type.Oid() == oid.T_timestamptz {
					return pgerror.Newf(pgcode.StringDataLengthMismatch,
						"value '%s' out of range for type %s (column %s)", t.String(), column.Type.SQLString(), column.Name)
				}
				return pgerror.Newf(pgcode.StringDataLengthMismatch,
					"value '%s' out of range for type %s (column %s)", t.String(), column.Type.SQLString(), column.Name)
			}
			Args[idx] = make([]byte, 8)
			binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
			if isFirstCols {
				*rowTimestamps = append(*rowTimestamps, tum)
			}
		case oid.T_bool:
			davl, err := strconv.ParseBool(string(Args[idx]))
			if err != nil {
				return tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
			}
			Args[idx] = []byte{0}
			if davl {
				Args[idx][0] = 1
			}
		case types.T_geometry:
			_, err := geos.FromWKT(string(Args[idx]))
			if err != nil {
				if strings.Contains(err.Error(), "load error") {
					return err
				}
				return pgerror.Newf(pgcode.DataException, errInvalidValue, string(Args[idx]), column.Type.SQLString(), column.Name)
			}
			if len(string(Args[idx])) > int(column.Type.Width()) {
				return pgerror.Newf(pgcode.StringDataRightTruncation,
					"value '%s' too long for type %s (column %s)", string(Args[idx]), column.Type.SQLString(), column.Name)
			}
		}
	default:
		return tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
	}
	return nil
}

func boolFormatText(Args [][]byte, idx int) error {
	davl, err := strconv.ParseBool(string(Args[idx]))
	if err != nil {
		return pgerror.Wrapf(err, pgcode.ProtocolViolation,
			"error in argument for %s", tree.PlaceholderIdx(idx))
	}
	Args[idx] = []byte{0}
	if davl {
		Args[idx][0] = 1
	}
	return nil
}

func timeFormatBinary(
	Args [][]byte,
	idx int,
	column *sqlbase.ColumnDescriptor,
	isFirstCols bool,
	rowTimestamps *[]int64,
) error {
	if len(Args[idx]) < 8 {
		return tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
	}
	i := int64(binary.BigEndian.Uint64(Args[idx]))
	nanosecond := execbuilder.PgBinaryToTime(i).Nanosecond()
	second := execbuilder.PgBinaryToTime(i).Unix()
	tum := second*1000 + int64(nanosecond/1000000)
	binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
	if isFirstCols {
		*rowTimestamps = append(*rowTimestamps, tum)
	}
	return nil
}

func intFormatBinary(
	Args [][]byte,
	idx int,
	column *sqlbase.ColumnDescriptor,
	infer oid.Oid,
	isFirstCols bool,
	rowTimestamps *[]int64,
) error {
	switch column.Type.Family() {
	case types.IntFamily, types.TimestampTZFamily, types.TimestampFamily, types.BoolFamily:
		switch column.Type.Oid() {
		case oid.T_int2:
			if infer == oid.T_int2 {
				var intValue int16
				for _, b := range Args[idx] {
					intValue = (intValue << 8) | int16(b)
				}
				if intValue < math.MinInt16 || intValue > math.MaxInt16 {
					return pgerror.Newf(pgcode.NumericValueOutOfRange,
						"integer out of range for type %s (column %q)",
						column.Type.SQLString(), column.Name)
				}
				Args[idx] = make([]uint8, len(Args[idx]))
				binary.LittleEndian.PutUint16(Args[idx], uint16(intValue))
			} else {
				var intValue int32
				for _, b := range Args[idx] {
					high, low := int32(b>>4), int32(b&0x0F)
					intValue = (intValue << 8) | (high << 4) | low
				}
				if intValue < math.MinInt16 || intValue > math.MaxInt16 {
					return pgerror.Newf(pgcode.NumericValueOutOfRange,
						"integer out of range for type %s (column %q)",
						column.Type.SQLString(), column.Name)
				}
				if infer == oid.T_int8 {
					binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
				} else {
					binary.LittleEndian.PutUint16(Args[idx], uint16(intValue))
				}
			}
		case oid.T_int4:
			if infer == oid.T_int2 {
				binary.LittleEndian.PutUint16(Args[idx], binary.BigEndian.Uint16(Args[idx]))
			} else {
				if len(Args[idx]) > 4 {
					intValue := int64(binary.BigEndian.Uint64(Args[idx]))
					if intValue < math.MinInt32 || intValue > math.MaxInt32 {
						return pgerror.Newf(pgcode.NumericValueOutOfRange,
							"integer out of range for type %s (column %q)",
							column.Type.SQLString(), column.Name)
					}
				}
				binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
			}
		case oid.T_int8:
			if infer == oid.T_int8 {
				binary.LittleEndian.PutUint64(Args[idx], binary.BigEndian.Uint64(Args[idx]))
			} else if infer == oid.T_int4 {
				binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
			} else {
				binary.LittleEndian.PutUint16(Args[idx], binary.BigEndian.Uint16(Args[idx]))
			}
		case oid.T_timestamptz, oid.T_timestamp:
			tum := binary.BigEndian.Uint64(Args[idx])
			binary.LittleEndian.PutUint64(Args[idx], tum)
			if isFirstCols {
				*rowTimestamps = append(*rowTimestamps, int64(tum))
			}
		case oid.T_bool:
			Args[idx] = []byte{byte(binary.BigEndian.Uint32(Args[idx]) & 1)}
		default:
			return tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
		}
	case types.StringFamily:
		str := strconv.FormatUint(uint64(binary.BigEndian.Uint32(Args[idx])), 10)
		Args[idx] = make([]byte, len(str))
		copy(Args[idx][0:], str)
	default:
		return tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
	}
	return nil
}

func float8FormatBinary(Args [][]byte, idx int, column *sqlbase.ColumnDescriptor) error {
	if column.Type.Family() != types.FloatFamily {
		return tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
	}
	f64 := math.Float64frombits(binary.BigEndian.Uint64(Args[idx]))
	switch column.Type.Oid() {
	case oid.T_float4:
		f32, err := strconv.ParseFloat(strconv.FormatFloat(f64, 'f', -1, 64), 32)
		if err != nil || (f32 != 0 && (math.Abs(f32) < math.SmallestNonzeroFloat32 || math.Abs(f32) > math.MaxFloat32)) {
			return pgerror.Newf(pgcode.NumericValueOutOfRange,
				"float \"%g\" out of range for type float4 (column %s)", f64, column.Name)
		}
		Args[idx] = make([]byte, 4)
		binary.LittleEndian.PutUint32(Args[idx], uint32(int32(math.Float32bits(float32(f32)))))
	case oid.T_float8:
		if f64 != 0 && (math.Abs(f64) < math.SmallestNonzeroFloat64 || math.Abs(f64) > math.MaxFloat64) {
			return pgerror.Newf(pgcode.NumericValueOutOfRange,
				"float \"%g\" out of range for type float (column %s)", f64, column.Name)
		}
		Args[idx] = bigEndianToLittleEndian(Args[idx])
	}
	return nil
}

func float4FormatBinary(Args [][]byte, idx int, column *sqlbase.ColumnDescriptor) error {
	if column.Type.Family() != types.FloatFamily {
		return tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
	}
	f32 := math.Float32frombits(binary.BigEndian.Uint32(Args[idx]))
	switch column.Type.Oid() {
	case oid.T_float4:
		if f32 != 0 && (math.Abs(float64(f32)) < math.SmallestNonzeroFloat32 || math.Abs(float64(f32)) > math.MaxFloat32) {
			return pgerror.Newf(pgcode.NumericValueOutOfRange,
				"float \"%g\" out of range for type float4 (column %s)", f32, column.Name)
		}
		binary.LittleEndian.PutUint32(Args[idx], uint32(int32(math.Float32bits(f32))))
	case oid.T_float8:
		f64, err := strconv.ParseFloat(strconv.FormatFloat(float64(f32), 'f', -1, 32), 64)
		if err != nil || (f64 != 0 && (math.Abs(f64) < math.SmallestNonzeroFloat64 || math.Abs(f64) > math.MaxFloat64)) {
			return pgerror.Newf(pgcode.NumericValueOutOfRange,
				"float \"%g\" out of range for type float (column %s)", f32, column.Name)
		}
		Args[idx] = make([]byte, 8)
		binary.LittleEndian.PutUint64(Args[idx], uint64(int64(math.Float64bits(f64))))
	}
	return nil
}

func charFormatBinary(
	Args [][]byte,
	idx int,
	column *sqlbase.ColumnDescriptor,
	isFirstCols bool,
	rowTimestamps *[]int64,
) error {
	switch column.Type.Family() {
	case types.StringFamily, types.BytesFamily, types.TimestampFamily, types.TimestampTZFamily, types.BoolFamily:
		switch column.Type.Oid() {
		case oid.T_varchar, oid.T_bpchar, oid.T_text, oid.T_bytea, oid.T_varbytea, types.T_nchar, types.T_nvarchar:
			length := len(string(Args[idx]))
			if column.Type.Oid() == types.T_nchar || column.Type.Oid() == types.T_nvarchar {
				length = utf8.RuneCountInString(string(Args[idx]))
			}
			if column.Type.Width() > 0 && length > int(column.Type.Width()) {
				return pgerror.Newf(pgcode.StringDataRightTruncation,
					"value too long for type %s (column %q)", column.Type.SQLString(), column.Name)
			}
		case oid.T_timestamptz, oid.T_timestamp:
			if err := timeFormatBinary(Args, idx, column, isFirstCols, rowTimestamps); err != nil {
				return err
			}
		case oid.T_bool:
			davl, err := tree.ParseDBool(string(Args[idx]))
			if err != nil {
				return tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
			}
			Args[idx] = []byte{0}
			if *davl {
				Args[idx][0] = 1
			}
		case types.T_geometry:
			if _, err := geos.FromWKT(string(Args[idx])); err != nil {
				if strings.Contains(err.Error(), "load error") {
					return err
				}
				return pgerror.Newf(pgcode.DataException, errInvalidValue, string(Args[idx]), column.Type.SQLString(), column.Name)
			}
			if len(string(Args[idx])) > int(column.Type.Width()) {
				return pgerror.Newf(pgcode.StringDataRightTruncation,
					"value '%s' too long for type %s (column %s)", string(Args[idx]), column.Type.SQLString(), column.Name)
			}
		}
	default:
		return tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
	}
	return nil
}

func boolFormatBinary(Args [][]byte, idx int) error {
	if len(Args[idx]) > 0 && (Args[idx][0] == 0 || Args[idx][0] == 1) {
		return nil
	}
	return pgerror.Newf(pgcode.Syntax, "unsupported binary bool: %x", Args[idx])
}

// BuildCDCDataForDirectInsert builds DirectInsert data for cdc.
func BuildCDCDataForDirectInsert(
	evalCtx *tree.EvalContext,
	tableID uint64,
	columns []sqlbase.ColumnDescriptor,
	InputDatums []tree.Datums,
	colIndex map[int]int,
	cdcCoordinator execinfra.CDCCoordinator,
) *sqlbase.CDCData {
	if cdcCoordinator == nil {
		return nil
	}

	if !cdcCoordinator.IsCDCEnabled(tableID) {
		return nil
	}

	columnsPtr := make([]*sqlbase.ColumnDescriptor, len(columns))
	for i := 0; i < len(columns); i++ {
		columnsPtr[i] = &columns[i]
	}

	cdcData, payloadMaxTime := cdcCoordinator.CaptureData(
		evalCtx, tableID, columnsPtr, InputDatums, colIndex)
	return &sqlbase.CDCData{
		TableID:      tableID,
		MinTimestamp: payloadMaxTime,
		PushData:     cdcData,
	}
}
