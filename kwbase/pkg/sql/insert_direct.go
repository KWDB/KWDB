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
	ColsDesc    []sqlbase.ColumnDescriptor
	Tname       *tree.TableName
	Desc        tree.NameList
	TableType   tree.TableType
}

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
}

const (
	errUnsupportedType = "unsupported input type relation \"%s\" (column %s)"
	errOutOfRange      = "integer \"%s\" out of range for type %s (column %s)"
	errInvalidValue    = "value '%s' is invalid for type %s (column %s)"
	errValueOutofRange = "value '%s' out of range for type %s (column %s)"
	errTooLong         = "value '%s' too long for type %s (column %s)"
)

// GetInputValues performs column type conversion and length checking
func GetInputValues(
	ctx context.Context,
	ptCtx tree.ParseTimeContext,
	cols *[]sqlbase.ColumnDescriptor,
	di *DirectInsert,
	stmts parser.Statements,
) ([]tree.Datums, error) {
	rowNum := di.RowNum
	colNum := di.ColNum
	totalSize := rowNum * colNum
	preSlice := make([]tree.Datum, totalSize)
	inputValues := make([]tree.Datums, rowNum)
	outputValues := make([]tree.Datums, 0, rowNum)

	for i, j := 0, 0; i < rowNum; i++ {
		end := j + colNum
		inputValues[i] = preSlice[j:end:end]
		j += colNum
	}

	ignoreError := stmts[0].Insertdirectstmt.IgnoreBatcherror
	for row := 0; row < rowNum; row++ {
		if err := getSingleRecord(ptCtx, cols, *di, stmts, row, inputValues); err != nil {
			if !ignoreError {
				return nil, err
			}
			stmts[0].Insertdirectstmt.BatchFailed++
			log.Errorf(ctx, "BatchInsert Error: %s", err)
			continue
		}
		outputValues = append(outputValues, inputValues[row])
	}

	return outputValues, nil
}

func getSingleRecord(
	ptCtx tree.ParseTimeContext,
	cols *[]sqlbase.ColumnDescriptor,
	di DirectInsert,
	stmts parser.Statements,
	row int,
	inputValues []tree.Datums,
) error {
	rowOffset := row * di.ColNum
	insertStmt := stmts[0].Insertdirectstmt
	idMapLen := len(di.IDMap)

	for i := 0; i < idMapLen; i++ {
		// col position in raw cols slice
		colPos := di.IDMap[i]
		// col position in insert cols slice
		col := di.PosMap[i]

		valueIdx := col + rowOffset
		rawValue := insertStmt.InsertValues[valueIdx]
		valueType := insertStmt.ValuesType[valueIdx]
		column := &(*cols)[colPos]

		if rawValue == "" && valueType != parser.STRINGTYPE {
			if !column.IsNullable() {
				return sqlbase.NewNonNullViolationError(column.Name)
			}
			// attempting to insert a NULL value when no value is specified
			inputValues[row][col] = tree.DNull
			continue
		}

		switch column.Type.Oid() {
		case oid.T_timestamptz:
			var dVal *tree.DInt
			var err error
			if valueType == parser.STRINGTYPE {
				precision := tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision())
				var datum tree.Datum
				var val *tree.DInt
				if datum, err = tree.ParseDTimestampTZ(ptCtx, rawValue, precision); err != nil {
					return tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
				}
				if dVal, err = GetTsTimestampWidth(*column, datum, val, rawValue); err != nil {
					return err
				}
				if err = tree.CheckTsTimestampWidth(&column.Type, *dVal, rawValue, column.Name); err != nil {
					return err
				}
			} else {
				if rawValue == "now" {
					dVal, err = tree.LimitTsTimestampWidth(timeutil.Now(), &column.Type, "", column.Name)
					if err != nil {
						return err
					}
					inputValues[row][col] = tree.NewDInt(*dVal)
					continue
				}
				in, err2 := strconv.ParseInt(rawValue, 10, 64)
				if err2 != nil {
					if valueType == parser.NORMALTYPE {
						return pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
					}
					if strings.Contains(err2.Error(), "out of range") {
						return err2
					}
					return tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
				}
				dVal = (*tree.DInt)(&in)
				if err = tree.CheckTsTimestampWidth(&column.Type, *dVal, "", column.Name); err != nil {
					return err
				}
			}
			inputValues[row][col] = dVal
			continue
		case oid.T_timestamp:
			var dVal *tree.DInt
			var err error
			if valueType == parser.STRINGTYPE {
				precision := tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision())
				var datum tree.Datum
				var val *tree.DInt
				if datum, err = tree.ParseDTimestamp(nil, rawValue, precision); err != nil {
					return tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
				}
				if dVal, err = GetTsTimestampWidth(*column, datum, val, rawValue); err != nil {
					return err
				}
				if err = tree.CheckTsTimestampWidth(&column.Type, *dVal, rawValue, column.Name); err != nil {
					return err
				}
			} else {
				if rawValue == "now" {
					dVal, err = tree.LimitTsTimestampWidth(timeutil.Now(), &column.Type, "", column.Name)
					if err != nil {
						return err
					}
					inputValues[row][col] = tree.NewDInt(*dVal)
					continue
				}
				in, err2 := strconv.ParseInt(rawValue, 10, 64)
				if err2 != nil {
					if valueType == parser.NORMALTYPE {
						return pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
					}
					if strings.Contains(err2.Error(), "out of range") {
						return err2
					}
					return tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
				}
				dVal = (*tree.DInt)(&in)
				if err = tree.CheckTsTimestampWidth(&column.Type, *dVal, "", column.Name); err != nil {
					return err
				}
			}
			inputValues[row][col] = dVal

		case oid.T_int8, oid.T_int4, oid.T_int2:
			if valueType == parser.STRINGTYPE {
				return tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
			}

			in, err := strconv.ParseInt(rawValue, 10, 64)
			if err != nil {
				if dat, err := parserString2Int(rawValue, err, *column); err != nil {
					if valueType == parser.NORMALTYPE {
						return pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
					}
					return err
				} else if dat != nil {
					inputValues[row][col] = dat
					continue
				}
			}

			switch column.Type.Oid() {
			case oid.T_int2:
				if (in>>15) != 0 && (in>>15) != -1 {
					goto rangeError
				}
			case oid.T_int4:
				if (in>>31) != 0 && (in>>31) != -1 {
					goto rangeError
				}
			}

			inputValues[row][col] = tree.NewDInt(tree.DInt(in))
			continue

		rangeError:
			return pgerror.Newf(pgcode.NumericValueOutOfRange,
				"integer \"%d\" out of range for type %s (column %s)", in, column.Type.SQLString(), column.Name)

		case oid.T_cstring, oid.T_char, oid.T_text, oid.T_bpchar, oid.T_varchar, oid.Oid(91004), oid.Oid(91002):
			if valueType == parser.NUMTYPE {
				return tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
			}
			if valueType == parser.NORMALTYPE {
				return pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
			}
			if valueType == parser.BYTETYPE && column.Type.Family() == types.BytesFamily {
				rawValue = strings.Trim(tree.NewDBytes(tree.DBytes(rawValue)).String(), "'")
			}

			// Check string length
			if column.Type.Width() > 0 {
				var strLen int
				if column.Type.Oid() == oid.Oid(91004) || column.Type.Oid() == oid.Oid(91002) {
					strLen = utf8.RuneCountInString(rawValue)
				} else {
					strLen = len(rawValue)
				}
				if strLen > int(column.Type.Width()) {
					return pgerror.Newf(pgcode.StringDataRightTruncation,
						"value '%s' too long for type %s (column %s)", rawValue, column.Type.SQLString(), column.Name)
				}
			}
			inputValues[row][col] = tree.NewDString(rawValue)

		case oid.T_bytea, oid.T_varbytea:
			if valueType == parser.NUMTYPE {
				return tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
			}
			if valueType == parser.NORMALTYPE {
				return pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
			}
			v, err := tree.ParseDByte(rawValue)
			if err != nil {
				return tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
			}
			// bytes(n)/varbytes(n) calculate length by byte
			if column.Type.Width() > 0 && len(rawValue) > int(column.Type.Width()) {
				return pgerror.Newf(pgcode.StringDataRightTruncation,
					"value '%s' too long for type %s (column %s)", rawValue, column.Type.SQLString(), column.Name)
			}
			if valueType == parser.BYTETYPE {
				rawValue = strings.Trim(tree.NewDBytes(tree.DBytes(rawValue)).String(), "'")
				v, err = tree.ParseDByte(rawValue)
				if err != nil {
					return tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
				}
			}
			inputValues[row][col] = v

		case oid.T_float4, oid.T_float8:
			if valueType == parser.NORMALTYPE {
				return pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
			}
			if valueType == parser.STRINGTYPE {
				return tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
			}
			in, err := strconv.ParseFloat(rawValue, 64)
			if err != nil {
				if strings.Contains(err.Error(), "out of range") {
					return pgerror.Newf(pgcode.NumericValueOutOfRange,
						"float \"%s\" out of range for type %s (column %s)", rawValue, column.Type.SQLString(), column.Name)
				}
				return tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
			}
			inputValues[row][col] = tree.NewDFloat(tree.DFloat(in))
		case oid.T_bool:
			var err error
			if valueType == parser.NORMALTYPE {
				return pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
			}
			inputValues[row][col], err = tree.ParseDBool(rawValue)
			if err != nil {
				return tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
			}
		case types.T_geometry:
			if valueType == parser.NORMALTYPE {
				return pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
			}
			if valueType == parser.NUMTYPE {
				return tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
			}
			if _, err := geos.FromWKT(rawValue); err != nil {
				if strings.Contains(err.Error(), "load error") {
					return err
				}
				return pgerror.Newf(pgcode.DataException, errInvalidValue, rawValue, column.Type.SQLString(), column.Name)
			}
			inputValues[row][col] = tree.NewDString(rawValue)
		default:
			return pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)

		}
		continue
	}
	return nil
}

func parserString2Int(
	rawValue string, err error, column sqlbase.ColumnDescriptor,
) (tree.Datum, error) {
	if strings.Contains(err.Error(), "out of range") {
		return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, "numeric constant out of int64 range")
	}

	switch rawValue {
	case "true":
		return tree.NewDInt(1), nil
	case "false":
		return tree.NewDInt(0), nil
	default:
		return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
	}
}

// BuildPayload is used to BuildPayloadForTsInsert
func BuildPayload(
	EvalContext *tree.EvalContext, priTagRowIdx []int, di *DirectInsert, dit DirectInsertTable,
) error {
	payload, _, err := execbuilder.BuildPayloadForTsInsert(
		EvalContext,
		EvalContext.Txn,
		di.InputValues,
		priTagRowIdx,
		di.PrettyCols,
		di.ColIndexs,
		di.PArgs,
		dit.DbID,
		dit.TabID,
	)
	if err != nil {
		return err
	}
	hashPoints := sqlbase.DecodeHashPointFromPayload(payload)
	primaryTagKey := sqlbase.MakeTsPrimaryTagKey(sqlbase.ID(dit.TabID), hashPoints)
	BuildPerNodePayloads(di.PayloadNodeMap, []roachpb.NodeID{1}, payload, priTagRowIdx, primaryTagKey)
	return nil
}

// BuildPreparePayload is used to BuildPayloadForTsInsert
func BuildPreparePayload(
	EvalContext *tree.EvalContext,
	inputValues [][][]byte,
	priTagRowIdx []int,
	di *DirectInsert,
	dit DirectInsertTable,
	qargs [][]byte,
) error {
	payload, _, err := execbuilder.BuildPreparePayloadForTsInsert(
		EvalContext,
		EvalContext.Txn,
		inputValues,
		priTagRowIdx,
		di.PrettyCols,
		di.ColIndexs,
		di.PArgs,
		dit.DbID,
		dit.TabID,
		qargs,
		di.ColNum,
	)
	if err != nil {
		return err
	}
	hashPoints := sqlbase.DecodeHashPointFromPayload(payload)
	primaryTagKey := sqlbase.MakeTsPrimaryTagKey(sqlbase.ID(dit.TabID), hashPoints)
	BuildPerNodePayloads(di.PayloadNodeMap, []roachpb.NodeID{1}, payload, priTagRowIdx, primaryTagKey)
	return nil
}

// BuildPerNodePayloads is used to PerNodePayloads
func BuildPerNodePayloads(
	payloadNodeMap map[int]*sqlbase.PayloadForDistTSInsert,
	nodeID []roachpb.NodeID,
	payload []byte,
	priTagRowIdx []int,
	primaryTagKey roachpb.Key,
) {
	payloadInfo := &sqlbase.SinglePayloadInfo{
		Payload:       payload,
		RowNum:        uint32(len(priTagRowIdx)),
		PrimaryTagKey: primaryTagKey,
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

// BuildpriTagValMap groups input values by primary tag
func BuildpriTagValMap(di DirectInsert) map[string][]int {
	// group the input values by primary tag
	priTagValMap := make(map[string][]int, len(di.InputValues))
	var builder strings.Builder
	builder.Grow(64)
	for i := range di.InputValues {
		builder.Reset()
		var priVal string
		for _, col := range di.PrimaryTagCols {
			builder.WriteString(sqlbase.DatumToString(di.InputValues[i][di.ColIndexs[int(col.ID)]]))
		}
		priVal = builder.String()
		priTagValMap[priVal] = append(priTagValMap[priVal], i)
	}
	return priTagValMap
}

// BuildPreparepriTagValMap groups the values entered in prepare by primary tag
func BuildPreparepriTagValMap(qargs [][]byte, di DirectInsert) map[string][]int {
	// group the input values by primary tag
	rowNum := di.RowNum / di.ColNum
	// Pre-allocate map with estimated size to avoid resizing
	priTagValMap := make(map[string][]int, 10)

	// Pre-calculate column indexes
	colIndexes := make([]int, 0, len(di.PrimaryTagCols))
	for _, col := range di.PrimaryTagCols {
		colIndexes = append(colIndexes, di.ColIndexs[int(col.ID)])
	}

	// Reuse buffer for building keys to reduce allocations
	var keyBuilder strings.Builder
	keyBuilder.Grow(64) // Pre-allocate reasonable buffer size

	for i := 0; i < rowNum; i++ {
		keyBuilder.Reset()
		baseOffset := i * di.ColNum

		// Build composite key from primary tag columns
		for _, idx := range colIndexes {
			keyBuilder.Write(qargs[baseOffset+idx])
		}

		key := keyBuilder.String()
		if existing, exists := priTagValMap[key]; exists {
			priTagValMap[key] = append(existing, i)
		} else {
			// Pre-allocate slice with reasonable capacity
			priTagValMap[key] = make([]int, 1, 4)
			priTagValMap[key][0] = i
		}
	}
	return priTagValMap
}

// BuildRowBytesForPrepareTsInsert builds rows for PrepareTsInsert efficiently
func BuildRowBytesForPrepareTsInsert(
	ptCtx tree.ParseTimeContext,
	Args [][]byte,
	Dit DirectInsertTable,
	di *DirectInsert,
	EvalContext tree.EvalContext,
	table *sqlbase.ImmutableTableDescriptor,
	nodeID roachpb.NodeID,
	rowTimestamps []int64,
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
			case oid.T_varchar, types.T_nvarchar, oid.T_varbytea:
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
			&EvalContext,
			EvalContext.Txn,
			nil,
			priTagRowIdx[:1],
			di.PArgs.PrettyCols[:di.PArgs.PTagNum+di.PArgs.AllTagNum],
			di.ColIndexs,
			di.PArgs,
			Dit.DbID,
			Dit.TabID,
			Args,
			di.ColNum,
		)
		if err != nil {
			return err
		}

		hashPoints := sqlbase.DecodeHashPointFromPayload(payload)
		primaryTagKey := sqlbase.MakeTsPrimaryTagKey(table.ID, hashPoints)

		groupLen := len(priTagRowIdx)
		groupBytes := make([][]byte, groupLen)
		groupTimes := make([]int64, groupLen)
		valueSize := int32(0)
		minTs := rowTimestamps[priTagRowIdx[0]]
		maxTs := minTs

		for i, idx := range priTagRowIdx {
			groupBytes[i] = rowBytes[idx]
			ts := rowTimestamps[idx]
			groupTimes[i] = ts
			valueSize += int32(len(groupBytes[i]))

			if ts < minTs {
				minTs = ts
			}
			if ts > maxTs {
				maxTs = ts
			}
		}

		allPayloads = append(allPayloads, &sqlbase.SinglePayloadInfo{
			Payload:       payload,
			RowNum:        uint32(groupLen),
			PrimaryTagKey: primaryTagKey,
			RowBytes:      groupBytes,
			RowTimestamps: groupTimes,
			StartKey:      sqlbase.MakeTsRangeKey(table.ID, uint64(hashPoints[0]), minTs),
			EndKey:        sqlbase.MakeTsRangeKey(table.ID, uint64(hashPoints[0]), maxTs+1),
			ValueSize:     valueSize,
		})
	}

	di.PayloadNodeMap[int(EvalContext.NodeID)] = &sqlbase.PayloadForDistTSInsert{
		NodeID:          nodeID,
		PerNodePayloads: allPayloads,
	}

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

			if ArgFormatCodes[idx] == pgwirebase.FormatText {
				switch inferTypes[idx] {
				case oid.T_timestamptz:
					// string type
					t, err := tree.ParseDTimestampTZ(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
					if err != nil {
						return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
					}
					tum := t.UnixMilli()
					if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
						return nil, nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
							"value '%s' out of range for type %s (column %s)", t.String(), column.Type.SQLString(), column.Name)
					}
					Args[idx] = make([]byte, 8)
					binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
					if isFirstCols {
						rowTimestamps = append(rowTimestamps, tum)
					}
				case oid.T_timestamp:
					// string type
					t, err := tree.ParseDTimestamp(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
					if err != nil {
						return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
					}
					tum := t.UnixMilli()
					if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
						return nil, nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
							"value %s out of range for type %s (column %s)", t.String(), column.Type.SQLString(), column.Name)
					}
					Args[idx] = make([]byte, 8)
					binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
					if isFirstCols {
						rowTimestamps = append(rowTimestamps, tum)
					}
				case oid.T_int8:
					switch column.Type.Family() {
					case types.IntFamily, types.TimestampTZFamily, types.TimestampFamily, types.BoolFamily:
						switch column.Type.Oid() {
						case oid.T_int2, oid.T_int4:
							i, err := strconv.ParseInt(string(Args[idx]), 10, 64)
							if err != nil {
								return nil, nil, err
							}
							binary.LittleEndian.PutUint32(Args[idx], uint32(i))
						case oid.T_int8:
							i, err := strconv.ParseInt(string(Args[idx]), 10, 64)
							if err != nil {
								return nil, nil, err
							}
							Args[idx] = make([]byte, 8)
							binary.LittleEndian.PutUint64(Args[idx], uint64(i))
						case oid.T_timestamptz, oid.T_timestamp:
							tum := binary.BigEndian.Uint64(Args[idx])
							binary.LittleEndian.PutUint64(Args[idx], tum)
							if isFirstCols {
								rowTimestamps = append(rowTimestamps, int64(tum))
							}
						case oid.T_bool:
							t, err := strconv.ParseBool(string(Args[idx]))
							if err != nil {
								return nil, nil, err
							}
							if !t {
								Args[idx][0] = 1
							} else {
								Args[idx][0] = 0
							}
						default:
							return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
						}
					case types.StringFamily:
						num := binary.BigEndian.Uint32(Args[idx])
						str := strconv.FormatUint(uint64(num), 10)
						Args[idx] = make([]byte, len(str))
						copy(Args[idx][0:], str)
						continue
					default:
						return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
					}
				case oid.T_int4, oid.T_int2:
					switch column.Type.Family() {
					case types.IntFamily, types.TimestampTZFamily, types.TimestampFamily, types.BoolFamily:
						switch column.Type.Oid() {
						case oid.T_int2:
							i, err := strconv.ParseInt(string(Args[idx]), 10, 64)
							if err != nil {
								return nil, nil, err
							}
							Args[idx] = make([]byte, 2)
							binary.LittleEndian.PutUint16(Args[idx], uint16(i))
						case oid.T_int4, oid.T_int8:
							i, err := strconv.ParseInt(string(Args[idx]), 10, 64)
							if err != nil {
								return nil, nil, err
							}
							Args[idx] = make([]byte, 4)
							binary.LittleEndian.PutUint32(Args[idx], uint32(i))
						case oid.T_timestamptz, oid.T_timestamp:
							tum := binary.BigEndian.Uint64(Args[idx])
							binary.LittleEndian.PutUint64(Args[idx], tum)
							if isFirstCols {
								rowTimestamps = append(rowTimestamps, int64(tum))
							}
						case oid.T_bool:
							t, err := strconv.ParseBool(string(Args[idx]))
							if err != nil {
								return nil, nil, err
							}
							if !t {
								Args[idx][0] = 1
							} else {
								Args[idx][0] = 0
							}
						default:
							return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
						}
					case types.StringFamily:
						num := binary.BigEndian.Uint32(Args[idx])
						str := strconv.FormatUint(uint64(num), 10)
						Args[idx] = make([]byte, len(str))
						copy(Args[idx][0:], str)
						continue
					default:
						return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
					}
				case oid.T_float8:
					if column.Type.Family() != types.FloatFamily {
						return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
					}
					f, err := strconv.ParseFloat(string(Args[idx]), 64)
					if err != nil {
						return nil, nil, err
					}
					switch column.Type.Oid() {
					case oid.T_float4:
						Args[idx] = make([]byte, 4)
						binary.LittleEndian.PutUint32(Args[idx], uint32(int32(math.Float32bits(float32((f))))))
					case oid.T_float8:
						Args[idx] = make([]byte, 8)
						binary.LittleEndian.PutUint64(Args[idx], uint64(int64(math.Float64bits(float64(f)))))
					}
				case oid.T_float4:
					if column.Type.Family() != types.FloatFamily {
						return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
					}
					f, err := strconv.ParseFloat(string(Args[idx]), 64)
					if err != nil {
						return nil, nil, err
					}
					switch column.Type.Oid() {
					case oid.T_float4:
						Args[idx] = make([]byte, 4)
						binary.LittleEndian.PutUint32(Args[idx], uint32(int32(math.Float32bits(float32((f))))))
					case oid.T_float8:
						Args[idx] = make([]byte, 8)
						binary.LittleEndian.PutUint64(Args[idx], uint64(int64(math.Float64bits(float64(f)))))
					}
				case oid.T_varchar, oid.T_bpchar, oid.T_varbytea, types.T_nchar, types.T_nvarchar:
					switch column.Type.Family() {
					case types.StringFamily, types.BytesFamily, types.TimestampFamily, types.TimestampTZFamily, types.BoolFamily:
						switch column.Type.Oid() {
						case oid.T_varchar, oid.T_bpchar, oid.T_text, oid.T_bytea, oid.T_varbytea:
							if column.Type.Width() > 0 && len(string(Args[idx])) > int(column.Type.Width()) {
								return nil, nil, pgerror.Newf(pgcode.StringDataRightTruncation,
									"value too long for type %s (column %q)",
									column.Type.SQLString(), column.Name)
							}
						case types.T_nchar, types.T_nvarchar:
							if column.Type.Width() > 0 && utf8.RuneCountInString(string(Args[idx])) > int(column.Type.Width()) {
								return nil, nil, pgerror.Newf(pgcode.StringDataRightTruncation,
									"value too long for type %s (column %q)",
									column.Type.SQLString(), column.Name)
							}
						case oid.T_timestamptz:
							// string type
							t, err := tree.ParseDTimestampTZ(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
							if err != nil {
								return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
							}
							tum := t.UnixMilli()
							if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
								if column.Type.Oid() == oid.T_timestamptz {
									return nil, nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
										"value '%s' out of range for type %s (column %s)", t.String(), column.Type.SQLString(), column.Name)
								}
								return nil, nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
									"value '%s' out of range for type %s (column %s)", t.String(), column.Type.SQLString(), column.Name)
							}
							Args[idx] = make([]byte, 8)
							binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
							if isFirstCols {
								rowTimestamps = append(rowTimestamps, tum)
							}
						case oid.T_timestamp:
							// string type
							t, err := tree.ParseDTimestamp(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
							if err != nil {
								return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
							}
							tum := t.UnixMilli()
							if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
								if column.Type.Oid() == oid.T_timestamptz {
									return nil, nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
										"value '%s' out of range for type %s (column %s)", t.String(), column.Type.SQLString(), column.Name)
								}
								return nil, nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
									"value '%s' out of range for type %s (column %s)", t.String(), column.Type.SQLString(), column.Name)
							}
							Args[idx] = make([]byte, 8)
							binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
							if isFirstCols {
								rowTimestamps = append(rowTimestamps, tum)
							}
						case oid.T_bool:
							t, err := strconv.ParseBool(string(Args[idx]))
							if err != nil {
								return nil, nil, err
							}
							if !t {
								Args[idx][0] = 1
							} else {
								Args[idx][0] = 0
							}
						case types.T_geometry:
							_, err := geos.FromWKT(string(Args[idx]))
							if err != nil {
								if strings.Contains(err.Error(), "load error") {
									return nil, nil, err
								}
								return nil, nil, pgerror.Newf(pgcode.DataException, errInvalidValue, string(Args[idx]), column.Type.SQLString(), column.Name)
							}
							if len(string(Args[idx])) > int(column.Type.Width()) {
								return nil, nil, pgerror.Newf(pgcode.StringDataRightTruncation,
									"value '%s' too long for type %s (column %s)", string(Args[idx]), column.Type.SQLString(), column.Name)
							}
						}
					default:
						return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
					}
				case oid.T_bool:
					davl, err := tree.ParseDBool(string(Args[idx]))
					if err != nil {
						return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
					}
					Args[idx] = make([]byte, 1)
					if *davl {
						Args[idx][0] = 1
					} else {
						Args[idx][0] = 0
					}
				}

			} else {
				switch inferTypes[idx] {
				case oid.T_timestamptz:
					i := int64(binary.BigEndian.Uint64(Args[idx]))
					nanosecond := execbuilder.PgBinaryToTime(i).Nanosecond()
					second := execbuilder.PgBinaryToTime(i).Unix()
					tum := second*1000 + int64(nanosecond/1000000)
					binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
					if isFirstCols {
						rowTimestamps = append(rowTimestamps, tum)
					}
				case oid.T_timestamp:
					i := int64(binary.BigEndian.Uint64(Args[idx]))
					nanosecond := execbuilder.PgBinaryToTime(i).Nanosecond()
					second := execbuilder.PgBinaryToTime(i).Unix()
					tum := second*1000 + int64(nanosecond/1000000)
					binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
					if isFirstCols {
						rowTimestamps = append(rowTimestamps, tum)
					}

				case oid.T_int8:
					switch column.Type.Family() {
					case types.IntFamily, types.TimestampTZFamily, types.TimestampFamily, types.BoolFamily:
						switch column.Type.Oid() {
						case oid.T_int2:
							var intValue int32
							for _, b := range Args[idx] {
								// Split each byte into two 4-bit values and combine them into one integer
								high := int32(b >> 4)
								low := int32(b & 0x0F)
								intValue = (intValue << 8) | (high << 4) | low
							}
							if intValue < math.MinInt16 || intValue > math.MaxInt16 {
								return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
									"integer out of range for type %s (column %q)",
									column.Type.SQLString(), column.Name)
							}
							binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
						case oid.T_int4:
							if len(Args[idx]) > 4 {
								var intValue int64
								for _, b := range Args[idx] {
									intValue = (intValue << 8) | int64(b)
								}
								if intValue < math.MinInt32 || intValue > math.MaxInt32 {
									return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
										"integer out of range for type %s (column %q)",
										column.Type.SQLString(), column.Name)
								}
							}
							binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
						case oid.T_int8:
							binary.LittleEndian.PutUint64(Args[idx], binary.BigEndian.Uint64(Args[idx]))
						case oid.T_timestamptz, oid.T_timestamp:
							tum := binary.BigEndian.Uint64(Args[idx])
							binary.LittleEndian.PutUint64(Args[idx], tum)
							if isFirstCols {
								rowTimestamps = append(rowTimestamps, int64(tum))
							}
						case oid.T_bool:
							num := binary.BigEndian.Uint32(Args[idx])
							// Determine whether the specific positioning is 1 based on bit and operation
							bitIndex := 0 // The bit index to be determined, counting from right to left, starting from 0
							bitValue := num & (1 << bitIndex)
							Args[idx] = make([]byte, 1)
							if bitValue != 0 {
								Args[idx][0] = 1
							} else {
								Args[idx][0] = 0
							}
						default:
							return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
						}
					case types.StringFamily:
						num := binary.BigEndian.Uint32(Args[idx])
						str := strconv.FormatUint(uint64(num), 10)
						Args[idx] = make([]byte, len(str))
						copy(Args[idx][0:], str)
						continue
					default:
						return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
					}
				case oid.T_int4:
					switch column.Type.Family() {
					case types.IntFamily, types.TimestampTZFamily, types.TimestampFamily, types.BoolFamily:
						switch column.Type.Oid() {
						case oid.T_int2:
							var intValue int32
							for _, b := range Args[idx] {
								// Determine whether the specific positioning is 1 based on bit and operation
								high := int32(b >> 4)
								low := int32(b & 0x0F)
								intValue = (intValue << 8) | (high << 4) | low
							}
							if intValue < math.MinInt16 || intValue > math.MaxInt16 {
								return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
									"integer out of range for type %s (column %q)",
									column.Type.SQLString(), column.Name)
							}
							Args[idx] = make([]uint8, len(Args[idx]))
							binary.LittleEndian.PutUint16(Args[idx], uint16(intValue))
						case oid.T_int4:
							if len(Args[idx]) > 4 {
								var intValue int64
								for _, b := range Args[idx] {
									intValue = (intValue << 8) | int64(b)
								}
								if intValue < math.MinInt32 || intValue > math.MaxInt32 {
									return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
										"integer out of range for type %s (column %q)",
										column.Type.SQLString(), column.Name)
								}
							}
							binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
						case oid.T_int8:
							binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
						case oid.T_timestamptz, oid.T_timestamp:
							tum := binary.BigEndian.Uint64(Args[idx])
							binary.LittleEndian.PutUint64(Args[idx], tum)
							if isFirstCols {
								rowTimestamps = append(rowTimestamps, int64(tum))
							}
						case oid.T_bool:
							num := binary.BigEndian.Uint32(Args[idx])
							bitIndex := 0
							bitValue := num & (1 << bitIndex)
							Args[idx] = make([]byte, 1)
							if bitValue != 0 {
								Args[idx][0] = 1
							} else {
								Args[idx][0] = 0
							}
						default:
							return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
						}
					case types.StringFamily:
						num := binary.BigEndian.Uint32(Args[idx])
						str := strconv.FormatUint(uint64(num), 10)
						Args[idx] = make([]byte, len(str))
						copy(Args[idx][0:], str)
						continue
					default:
						return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
					}
				case oid.T_int2:
					switch column.Type.Family() {
					case types.IntFamily, types.TimestampTZFamily, types.TimestampFamily, types.BoolFamily:
						switch column.Type.Oid() {
						case oid.T_int2:
							var intValue int16
							for _, b := range Args[idx] {
								intValue = (intValue << 8) | int16(b)
							}
							if intValue < math.MinInt16 || intValue > math.MaxInt16 {
								return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
									"integer out of range for type %s (column %q)",
									column.Type.SQLString(), column.Name)
							}
							Args[idx] = make([]uint8, len(Args[idx]))
							binary.LittleEndian.PutUint16(Args[idx], uint16(intValue))
						case oid.T_int4:
							if len(Args[idx]) > 4 {
								var intValue int64
								for _, b := range Args[idx] {
									intValue = (intValue << 8) | int64(b)
								}
								if intValue < math.MinInt32 || intValue > math.MaxInt32 {
									return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
										"integer out of range for type %s (column %q)",
										column.Type.SQLString(), column.Name)
								}
							}
							binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
						case oid.T_int8:
							binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
						case oid.T_timestamptz, oid.T_timestamp:
							tum := binary.BigEndian.Uint64(Args[idx])
							binary.LittleEndian.PutUint64(Args[idx], tum)
							if isFirstCols {
								rowTimestamps = append(rowTimestamps, int64(tum))
							}
						case oid.T_bool:
							num := binary.BigEndian.Uint32(Args[idx])
							bitIndex := 0
							bitValue := num & (1 << bitIndex)
							Args[idx] = make([]byte, 1)
							if bitValue != 0 {
								Args[idx][0] = 1
							} else {
								Args[idx][0] = 0
							}
						default:
							return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
						}
					case types.StringFamily:
						num := binary.BigEndian.Uint32(Args[idx])
						str := strconv.FormatUint(uint64(num), 10)
						Args[idx] = make([]byte, len(str))
						copy(Args[idx][0:], str)
						continue
					default:
						return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
					}
				case oid.T_float8:
					if column.Type.Family() != types.FloatFamily {
						return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
					}
					f64 := math.Float64frombits(binary.BigEndian.Uint64(Args[idx]))
					switch column.Type.Oid() {
					case oid.T_float4:
						str := strconv.FormatFloat(float64(f64), 'f', -1, 64)
						f32, err := strconv.ParseFloat(str, 32)
						if err != nil || (f32 != 0 && (math.Abs(f32) < math.SmallestNonzeroFloat32 || math.Abs(f32) > math.MaxFloat32)) {
							return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
								"float \"%g\" out of range for type float4 (column %s)", f64, column.Name)
						}
						Args[idx] = make([]byte, 4)
						binary.LittleEndian.PutUint32(Args[idx], uint32(int32(math.Float32bits(float32((f32))))))
					case oid.T_float8:
						if f64 != 0 && (math.Abs(f64) < math.SmallestNonzeroFloat64 || math.Abs(f64) > math.MaxFloat64) {
							return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
								"float \"%g\" out of range for type float (column %s)", f64, column.Name)
						}
						Args[idx] = bigEndianToLittleEndian(Args[idx])
					}
				case oid.T_float4:
					if column.Type.Family() != types.FloatFamily {
						return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
					}
					f32 := math.Float32frombits(binary.BigEndian.Uint32(Args[idx]))
					switch column.Type.Oid() {
					case oid.T_float4:
						if f32 != 0 && (math.Abs(float64(f32)) < math.SmallestNonzeroFloat32 || math.Abs(float64(f32)) > math.MaxFloat32) {
							return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
								"float \"%g\" out of range for type float4 (column %s)", f32, column.Name)
						}
						binary.LittleEndian.PutUint32(Args[idx], uint32(int32(math.Float32bits(float32((f32))))))
					case oid.T_float8:
						str := strconv.FormatFloat(float64(f32), 'f', -1, 32)
						f64, err := strconv.ParseFloat(str, 64)
						if err != nil || (f64 != 0 && (math.Abs(f64) < math.SmallestNonzeroFloat64 || math.Abs(f64) > math.MaxFloat64)) {
							return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
								"float \"%g\" out of range for type float (column %s)", f32, column.Name)
						}
						Args[idx] = make([]byte, 8)
						binary.LittleEndian.PutUint64(Args[idx], uint64(int64(math.Float64bits(float64(f64)))))
					}
				case oid.T_varchar, oid.T_bpchar, oid.T_varbytea, types.T_nchar, types.T_nvarchar:
					switch column.Type.Family() {
					case types.StringFamily, types.BytesFamily, types.TimestampFamily, types.TimestampTZFamily, types.BoolFamily:
						switch column.Type.Oid() {
						case oid.T_varchar, oid.T_bpchar, oid.T_text, oid.T_bytea, oid.T_varbytea:
							if column.Type.Width() > 0 && len(string(Args[idx])) > int(column.Type.Width()) {
								return nil, nil, pgerror.Newf(pgcode.StringDataRightTruncation,
									"value too long for type %s (column %q)",
									column.Type.SQLString(), column.Name)
							}
						case types.T_nchar, types.T_nvarchar:
							if column.Type.Width() > 0 && utf8.RuneCountInString(string(Args[idx])) > int(column.Type.Width()) {
								return nil, nil, pgerror.Newf(pgcode.StringDataRightTruncation,
									"value too long for type %s (column %q)",
									column.Type.SQLString(), column.Name)
							}
						case oid.T_timestamptz:
							if ArgFormatCodes[idx] == pgwirebase.FormatText {
								// string type
								t, err := tree.ParseDTimestampTZ(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
								if err != nil {
									return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
								}
								tum := t.UnixMilli()
								if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
									if column.Type.Oid() == oid.T_timestamptz {
										return nil, nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
											"value '%s' out of range for type %s (column %s)", t.String(), column.Type.SQLString(), column.Name)
									}
									return nil, nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
										"value '%s' out of range for type %s (column %s)", t.String(), column.Type.SQLString(), column.Name)
								}
								Args[idx] = make([]byte, 8)
								binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
								if isFirstCols {
									rowTimestamps = append(rowTimestamps, tum)
								}
							} else {
								i := int64(binary.BigEndian.Uint64(Args[idx]))
								nanosecond := execbuilder.PgBinaryToTime(i).Nanosecond()
								second := execbuilder.PgBinaryToTime(i).Unix()
								tum := second*1000 + int64(nanosecond/1000000) - 8*60*60*1000
								binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
								if isFirstCols {
									rowTimestamps = append(rowTimestamps, tum)
								}
							}
						case oid.T_timestamp:
							if ArgFormatCodes[idx] == pgwirebase.FormatText {
								// string type
								t, err := tree.ParseDTimestamp(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
								if err != nil {
									return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
								}
								tum := t.UnixMilli()
								if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
									if column.Type.Oid() == oid.T_timestamptz {
										return nil, nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
											"value '%s' out of range for type %s (column %s)", t.String(), column.Type.SQLString(), column.Name)
									}
									return nil, nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
										"value '%s' out of range for type %s (column %s)", t.String(), column.Type.SQLString(), column.Name)
								}
								Args[idx] = make([]byte, 8)
								binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
								if isFirstCols {
									rowTimestamps = append(rowTimestamps, tum)
								}
							} else {
								i := int64(binary.BigEndian.Uint64(Args[idx]))
								nanosecond := execbuilder.PgBinaryToTime(i).Nanosecond()
								second := execbuilder.PgBinaryToTime(i).Unix()
								tum := second*1000 + int64(nanosecond/1000000) - 8*60*60*1000
								binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
								if isFirstCols {
									rowTimestamps = append(rowTimestamps, tum)
								}
							}
						case oid.T_bool:
							davl, err := tree.ParseDBool(string(Args[idx]))
							if err != nil {
								return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
							}
							Args[idx] = make([]byte, 1)
							if *davl {
								Args[idx][0] = 1
							} else {
								Args[idx][0] = 0
							}
						case types.T_geometry:
							_, err := geos.FromWKT(string(Args[idx]))
							if err != nil {
								if strings.Contains(err.Error(), "load error") {
									return nil, nil, err
								}
								return nil, nil, pgerror.Newf(pgcode.DataException, errInvalidValue, string(Args[idx]), column.Type.SQLString(), column.Name)
							}
							if len(string(Args[idx])) > int(column.Type.Width()) {
								return nil, nil, pgerror.Newf(pgcode.StringDataRightTruncation,
									"value '%s' too long for type %s (column %s)", string(Args[idx]), column.Type.SQLString(), column.Name)
							}
						}
					default:
						return nil, nil, tree.NewDatatypeMismatchError(column.Name, string(Args[idx]), column.Type.SQLString())
					}
				case oid.T_bool:
					if len(Args[idx]) > 0 && (Args[idx][0] == 0 || Args[idx][0] == 1) {
						continue
					}
					return nil, nil, pgerror.Newf(pgcode.Syntax, "unsupported binary bool: %x", Args[idx])
				}
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
	di.PArgs, err = execbuilder.BuildPayloadArgs(tsVersion, di.PrimaryTagCols, tagCols, dataCols)
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
			if !col.IsTagCol() && i == 0 {
				// The first timestamp column in the data column needs to reserve 16 bytes for LSN
				colSize += sqlbase.FirstTsDataColSize
			} else {
				colSize += 8
			}
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
	// Type check for input values.
	var buf strings.Builder
	outrowBytes := make([][]byte, 0, rowNum)

	// Track batch errors
	batchFailed := &stmts[0].Insertdirectstmt.BatchFailed
	ignoreBatchErr := stmts[0].Insertdirectstmt.IgnoreBatcherror

	for row := range inputValues {
		tp.SetPayload(rowBytes[row])

		if err := getSingleRowBytes(ptCtx, di, tp, dataOffset, independentOffset,
			row, rowBytes, stmts, inputValues, &buf, rowTimestamps); err != nil {
			if !ignoreBatchErr {
				return nil, nil, nil, err
			}
			*batchFailed++
			log.Errorf(ctx, "BatchInsert Error: %s", err)
			continue
		}

		adjustedRow := row - *batchFailed
		tagKey := buf.String()

		outputValues = append(outputValues, inputValues[row])
		priTagValMap[tagKey] = append(priTagValMap[tagKey], adjustedRow)
		outrowBytes = append(outrowBytes, rowBytes[row])

		buf.Reset()
	}

	return outputValues, priTagValMap, outrowBytes, nil
}

func getSingleRowBytes(
	ptCtx tree.ParseTimeContext,
	di *DirectInsert,
	tp *execbuilder.TsPayload,
	offset, varDataOffset, row int,
	rowBytes [][]byte,
	stmts parser.Statements,
	inputValues []tree.Datums,
	buf *strings.Builder,
	rowTimestamps []int64,
) error {
	// Pre-calculate constants to avoid repeated calculations
	bitmapOffset := execbuilder.DataLenSize
	pTagNum := di.PArgs.PTagNum
	allTagNum := di.PArgs.AllTagNum
	dataColNum := di.PArgs.DataColNum
	colNum := di.ColNum

	// Pre-fetch frequently accessed values
	insertValues := stmts[0].Insertdirectstmt.InsertValues
	valuesType := stmts[0].Insertdirectstmt.ValuesType
	rowOffset := row * colNum

	// Process columns in a single pass
	for i, column := range di.PrettyCols {
		colIdx := di.ColIndexs[int(column.ID)]
		isDataCol := column.IsDataCol()

		// Early exit for invalid column index
		if colIdx < 0 {
			if !column.IsNullable() {
				return sqlbase.NewNonNullViolationError(column.Name)
			}
			if !isDataCol {
				continue
			}
		}

		// Handle data columns
		if isDataCol {
			dataColIdx := i - pTagNum - allTagNum
			isLastDataCol := dataColIdx == dataColNum-1

			// Calculate column length once
			curColLength := execbuilder.VarColumnSize
			if int(column.TsCol.VariableLengthType) == sqlbase.StorageTuple {
				curColLength = int(column.TsCol.StorageLen)
			}

			// Handle NULL values efficiently
			if colIdx < 0 {
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
					return sqlbase.NewNonNullViolationError(column.Name)
				}
				// attempting to insert a NULL value when no value is specified
				datum = tree.DNull
			} else {
				datum, err = GetSingleDatum(ptCtx, *column, valueType, rawValue)
				if err != nil {
					return err
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
				return err
			}

			offset += curColLength
			if isLastDataCol {
				tp.SetPayload(tp.GetPayload(varDataOffset))
				execbuilder.WriteUint32ToPayload(tp, uint32(varDataOffset-execbuilder.DataLenSize))
				rowBytes[row] = tp.GetPayload(varDataOffset)
			}
			continue
		}

		// Handle tag columns
		rawValue := insertValues[colIdx+rowOffset]
		valueType := valuesType[colIdx+rowOffset]

		// Process NULL values
		if valueType != parser.STRINGTYPE && rawValue == "" {
			if !column.IsNullable() {
				return sqlbase.NewNonNullViolationError(column.Name)
			}
			inputValues[row][colIdx] = tree.DNull
			continue
		}

		// Get datum for non-NULL values
		datum, err := GetSingleDatum(ptCtx, *column, valueType, rawValue)
		if err != nil {
			return err
		}

		inputValues[row][colIdx] = datum

		// Build primary tag key
		if i < pTagNum {
			buf.WriteString(sqlbase.DatumToString(datum))
		}
	}

	return nil
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

	switch oidType {
	case oid.T_timestamptz, oid.T_timestamp:
		if valueType == parser.STRINGTYPE {
			precision := tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision())
			var datum tree.Datum
			var err error

			if oidType == oid.T_timestamptz {
				if datum, err = tree.ParseDTimestampTZ(ptCtx, rawValue, precision); err != nil {
					return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
				}
				var dVal *tree.DInt
				dVal, err = GetTsTimestampWidth(column, datum, dVal, rawValue)
				if err != nil {
					return nil, err
				}

				err = tree.CheckTsTimestampWidth(&column.Type, *dVal, rawValue, column.Name)
				if err != nil {
					return nil, err
				}
				return dVal, nil
			}

			if datum, err = tree.ParseDTimestamp(nil, rawValue, precision); err != nil {
				return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
			}
			var dVal *tree.DInt
			dVal, err = GetTsTimestampWidth(column, datum, dVal, rawValue)
			if err != nil {
				return nil, err
			}
			err = tree.CheckTsTimestampWidth(&column.Type, *dVal, rawValue, column.Name)
			if err != nil {
				return nil, err
			}
			return dVal, nil
		}

		if rawValue == "now" {
			return tree.NewDInt(tree.DInt(timeutil.Now().UnixNano() / int64(time.Millisecond))), nil
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
		err = tree.CheckTsTimestampWidth(&column.Type, dVal, rawValue, column.Name)
		if err != nil {
			return nil, err
		}
		return &dVal, nil

	case oid.T_int8, oid.T_int4, oid.T_int2:
		if valueType == parser.STRINGTYPE {
			return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
		}

		in, err := strconv.ParseInt(rawValue, 10, 64)
		if err != nil {
			if dat, err := parserString2Int(rawValue, err, column); err != nil {
				if valueType == parser.NORMALTYPE {
					return nil, pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
				}
				return nil, err
			} else if dat != nil {
				return dat, nil
			}
		}

		var minVal, maxVal int64
		switch oidType {
		case oid.T_int2:
			minVal, maxVal = math.MinInt16, math.MaxInt16
		case oid.T_int4:
			minVal, maxVal = math.MinInt32, math.MaxInt32
		case oid.T_int8:
			minVal, maxVal = math.MinInt64, math.MaxInt64
		}

		if in < minVal || in > maxVal {
			return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, errOutOfRange, rawValue, column.Type.SQLString(), column.Name)
		}
		d := tree.DInt(in)
		return &d, nil

	case oid.T_cstring, oid.T_char, oid.T_text, oid.T_bpchar, oid.T_varchar,
		oid.T_bytea, oid.T_varbytea, oid.Oid(91004), oid.Oid(91002):
		if valueType == parser.NUMTYPE {
			return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
		}

		if width := column.Type.Width(); width > 0 {
			var length int
			if oidType == oid.Oid(91004) || oidType == oid.Oid(91002) {
				length = utf8.RuneCountInString(rawValue)
			} else {
				length = len(rawValue)
			}
			if length > int(width) {
				return nil, pgerror.Newf(pgcode.StringDataRightTruncation, errTooLong, rawValue, column.Type.SQLString(), column.Name)
			}
		}

		if oidType == oid.T_bytea || oidType == oid.T_varbytea {
			if valueType == parser.BYTETYPE {
				rawValue = strings.Trim(tree.NewDBytes(tree.DBytes(rawValue)).String(), "'")
			}
			v, err := tree.ParseDByte(rawValue)
			if err != nil {
				return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
			}
			return v, nil
		}

		return tree.NewDString(rawValue), nil

	case oid.T_float4, oid.T_float8:
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

	case oid.T_bool:
		dBool, err := tree.ParseDBool(rawValue)
		if err != nil {
			return nil, tree.NewDatatypeMismatchError(column.Name, rawValue, column.Type.SQLString())
		}
		return dBool, nil

	case types.T_geometry:
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

	default:
		return nil, pgerror.Newf(pgcode.Syntax, errUnsupportedType, rawValue, column.Name)
	}
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
	EvalContext tree.EvalContext,
	table *sqlbase.ImmutableTableDescriptor,
	nodeid roachpb.NodeID,
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

	for _, priTagRowIdx := range priTagValMap {
		// Payload is the encoding of a complete line, which is the first line in a line with the same ptag
		payload, _, err := execbuilder.BuildPayloadForTsInsert(
			&EvalContext,
			EvalContext.Txn,
			inputValues,
			priTagRowIdx[:1],
			cols,
			di.ColIndexs,
			di.PArgs,
			dit.DbID,
			uint32(table.ID),
		)
		if err != nil {
			return err
		}

		// Make primaryTag key.
		hashPoints := sqlbase.DecodeHashPointFromPayload(payload)
		primaryTagKey := sqlbase.MakeTsPrimaryTagKey(table.ID, hashPoints)

		rowCount := len(priTagRowIdx)
		groupRowBytes := make([][]byte, rowCount)
		groupRowTime := make([]int64, rowCount)

		minTs, maxTs := int64(math.MaxInt64), int64(math.MinInt64)
		var valueSize int32

		for i, idx := range priTagRowIdx {
			groupRowBytes[i] = rowBytes[idx]
			ts := rowTimestamps[idx]
			groupRowTime[i] = ts

			if ts > maxTs {
				maxTs = ts
			}
			if ts < minTs {
				minTs = ts
			}
			valueSize += int32(len(groupRowBytes[i]))
		}

		hashPoint := uint64(hashPoints[0])
		var startKey, endKey roachpb.Key
		if di.PArgs.RowType == execbuilder.OnlyTag {
			startKey = sqlbase.MakeTsHashPointKey(tabID, hashPoint)
			endKey = sqlbase.MakeTsRangeKey(tabID, hashPoint, math.MaxInt64)
		} else {
			startKey = sqlbase.MakeTsRangeKey(tabID, hashPoint, minTs)
			endKey = sqlbase.MakeTsRangeKey(tabID, hashPoint, maxTs+1)
		}

		allPayloads = append(allPayloads, &sqlbase.SinglePayloadInfo{
			Payload:       payload,
			RowNum:        uint32(rowCount),
			PrimaryTagKey: primaryTagKey,
			RowBytes:      groupRowBytes,
			RowTimestamps: groupRowTime,
			StartKey:      startKey,
			EndKey:        endKey,
			ValueSize:     valueSize,
		})
	}

	di.PayloadNodeMap = map[int]*sqlbase.PayloadForDistTSInsert{
		int(EvalContext.NodeID): {
			NodeID:          nodeid,
			PerNodePayloads: allPayloads,
		},
	}

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
