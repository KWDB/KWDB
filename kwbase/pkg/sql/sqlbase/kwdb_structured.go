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

package sqlbase

import (
	"context"
	"encoding/binary"
	"strings"
	"unicode/utf8"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const (
	// TSInsertPayloadVersion Indicates the version of TSInsertPayload,
	// which is used to verify compatibility during the version upgrade.
	TSInsertPayloadVersion = 1
	// ColumnValIsNull is a flag that the value of the insert column is NULL.
	ColumnValIsNull = -1
	// ColumnValNotExist is a flag that the value of the insert column is NULL.
	ColumnValNotExist = -2
	// BothDataAndTagColumn is the flag that the insert column contains data column and tag column.
	BothDataAndTagColumn = 0
	// OnlyDataColumn is the flag that the insert column only contains data column.
	OnlyDataColumn = 1
	// OnlyTagColumn is the flag that the insert column only contains tag column.
	OnlyTagColumn = 2
	// RangeGroupIDOffset offset of range_group_id in the payload header
	RangeGroupIDOffset int = 16
	// RangeGroupIDSize length of range_group_id in the payload header
	RangeGroupIDSize int = 2
	// OsnIDOffset offset of osn_id in the payload header
	OsnIDOffset = 0
	// OsnIDSize length of osn_id in the payload header (Only 8 bytes have been used).
	OsnIDSize = 16
)

// InstNameSpace Stores the relationship between instance table names and ids
type InstNameSpace struct {
	// instance table name
	InstName string
	// instance table id
	InstTableID ID
	// template table id
	TmplTableID ID
	// db name.
	DBName string
	ChildDesc
}

// DecodeHashPointFromPayload decodes hash points from a payload of ts insert/import.
// RangeGroupIDOffset and RangeGroupIDSize are copies of execbuilder.RangeGroupIDOffset and execbuilder.RangeGroupIDSize.
func DecodeHashPointFromPayload(payload []byte) []api.HashPoint {
	hashPoint := binary.LittleEndian.Uint16(payload[RangeGroupIDOffset : RangeGroupIDOffset+RangeGroupIDSize])
	return []api.HashPoint{api.HashPoint(hashPoint)}
}

// DecodeOsnIDFromPayload decodes osnID from a payload.
func DecodeOsnIDFromPayload(payload []byte) uint64 {
	osnID := binary.LittleEndian.Uint64(payload[OsnIDOffset : OsnIDOffset+OsnIDSize])
	return osnID
}

// GetKWDBMetadataRow obtains the keyValue pair by the primary key, decodes
// the pair according to the corresponding column type, and returns the value
// of the row containing the primary key column.
func GetKWDBMetadataRow(
	ctx context.Context, txn *kv.Txn, indexKey roachpb.Key, systemTable TableDescriptor,
) (tree.Datums, error) {
	if txn == nil {
		return nil, pgerror.New(pgcode.NoActiveSQLTransaction, "must provide a non-nil transaction")
	}
	keyVal, err := txn.Get(ctx, indexKey)
	if err != nil {
		return nil, err
	}
	// TODO: The encoded index needs to be taken as an input.
	index, err := systemTable.FindIndexByID(IndexID(indexKey[1] - IntZero))
	if err != nil {
		return nil, err
	}
	datumRow, err := decodeKeyValue(keyVal, index, systemTable)
	if err != nil {
		return nil, pgerror.New(pgcode.DataException, err.Error())
	}
	return datumRow, nil
}

// GetKWDBMetadataRows scans the entire table to obtain keyValue pairs,
// decodes them according to the corresponding column types, and returns
// the values of the entire table.
func GetKWDBMetadataRows(
	ctx context.Context, txn *kv.Txn, startKey roachpb.Key, systemTable TableDescriptor,
) ([]tree.Datums, error) {
	if txn == nil {
		return nil, pgerror.New(pgcode.NoActiveSQLTransaction, "must provide a non-nil transaction")
	}
	KVs, err := txn.Scan(ctx, startKey, startKey.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}
	// TODO: The encoded index needs to be taken as an input.
	index, err := systemTable.FindIndexByID(IndexID(startKey[1] - IntZero))
	if err != nil {
		return nil, err
	}
	var res []tree.Datums
	var singleRow tree.Datums
	const headSize = 5
	for _, keyVal := range KVs {
		if len(keyVal.Value.RawBytes) <= headSize {
			// TODO(zxy): need to optimize
		}
		datumRow, err := decodeKeyValue(keyVal, index, systemTable)
		if err != nil {
			return nil, err
		}
		if datumRow == nil {
			continue
		}
		if len(singleRow) < len(systemTable.Columns) {
			if len(singleRow) == 0 {
				singleRow = append(singleRow, datumRow...)
			} else {
				primaryCol := len(index.ColumnIDs)
				singleRow = append(singleRow, datumRow[primaryCol:]...)
			}
		}
		if len(singleRow) < len(index.ColumnIDs)+len(index.ExtraColumnIDs) {
			continue
		}
		res = append(res, singleRow)
		singleRow = nil
	}
	return res, nil
}

// GetKWDBMetadataRowWithUnConsistency retrieves the value for a key with un consistency,
// returning the retrieved key/value or an error.
func GetKWDBMetadataRowWithUnConsistency(
	ctx context.Context, txn *kv.Txn, indexKey roachpb.Key, systemTable TableDescriptor,
) (tree.Datums, error) {
	if txn == nil {
		return nil, pgerror.New(pgcode.NoActiveSQLTransaction, "must provide a non-nil transaction")
	}
	keyVal, err := txn.GetWithUnConsistency(ctx, indexKey)
	if err != nil {
		return nil, err
	}
	// TODO: The encoded index needs to be taken as an input.
	return GetMetadataRowsFromKeyValue(ctx, &keyVal, &systemTable)
}

// IntZero is the initial offset corresponding to 0 when encoding key.
// See also encoding.intZero.
const IntZero = 136

// GetKWDBMetadataRowsWithUnConsistency retrieves the value for keys with un consistency,
// returning the retrieved key/values or an error.
func GetKWDBMetadataRowsWithUnConsistency(
	ctx context.Context, txn *kv.Txn, startKey roachpb.Key, systemTable TableDescriptor,
) ([]tree.Datums, error) {
	if txn == nil {
		return nil, pgerror.New(pgcode.NoActiveSQLTransaction, "must provide a non-nil transaction")
	}
	KVs, err := txn.ScanWithUnReadConsistency(ctx, startKey, startKey.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}
	// TODO: The encoded index needs to be taken as an input.
	index, err := systemTable.FindIndexByID(IndexID(startKey[1] - IntZero))
	if err != nil {
		return nil, err
	}
	var res []tree.Datums
	var singleRow tree.Datums
	const headSize = 5
	for _, keyVal := range KVs {
		if len(keyVal.Value.RawBytes) <= headSize {
			// TODO(zxy): need to optimize
			//continue
		}
		datumRow, err := decodeKeyValue(keyVal, index, systemTable)
		if err != nil {
			return nil, err
		}
		if datumRow == nil {
			continue
		}
		if len(singleRow) < len(systemTable.Columns) {
			if len(singleRow) == 0 {
				singleRow = append(singleRow, datumRow...)
			} else {
				primaryCol := len(index.ColumnIDs)
				singleRow = append(singleRow, datumRow[primaryCol:]...)
			}
		}
		if len(singleRow) < len(index.ColumnIDs)+len(index.ExtraColumnIDs) {
			continue
		}
		res = append(res, singleRow)
		singleRow = nil
	}
	return res, nil
}

// GetMetadataRowsFromKeyValue gets the metadata rows from KwyValue
func GetMetadataRowsFromKeyValue(
	ctx context.Context, keyValue *kv.KeyValue, systemTable *TableDescriptor,
) (tree.Datums, error) {
	index, err := systemTable.FindIndexByID(IndexID(keyValue.Key[1] - IntZero))
	if err != nil {
		return nil, err
	}
	datumRows, err := decodeKeyValue(*keyValue, index, *systemTable)
	if err != nil {
		return nil, pgerror.New(pgcode.DataException, err.Error())
	}
	return datumRows, err
}

// decodeKeyValue decodes keyVal according to the column type.
func decodeKeyValue(
	keyVal kv.KeyValue, index *IndexDescriptor, systemTable TableDescriptor,
) (tree.Datums, error) {
	if !keyVal.Exists() {
		return nil, errors.Newf("%s: object cannot found", systemTable.Name)
	}

	var res tree.Datums
	indexVal, err := DecodeKWDBTableKey(keyVal.Key, index, systemTable)
	if err != nil {
		return nil, err
	}
	res = append(res, indexVal...)

	if len(index.ExtraColumnIDs) != 0 {
		extraVals, err := decodeExtraValue(systemTable, index, keyVal)
		if err != nil {
			return nil, err
		}
		res = append(res, extraVals...)
		return res, nil
	}

	// Decode according to different value encoding types.
	t := keyVal.Value.GetTag()
	switch t {
	case roachpb.ValueType_INT:
		v, err := keyVal.Value.GetInt()
		if err != nil {
			return nil, err
		}
		res = append(res, tree.NewDInt(tree.DInt(v)))
	case roachpb.ValueType_TUPLE:
		datums, err := decodeValueFromTuple(keyVal, index, systemTable)
		if err != nil {
			return nil, err
		}
		if datums == nil {
			return nil, nil
		}
		res = append(res, datums...)
	case roachpb.ValueType_BYTES:
		v, err := keyVal.Value.GetBytes()
		if err != nil {
			return nil, err
		}
		res = append(res, tree.NewDBytes(tree.DBytes(v)))
	default:
		return nil, errors.Errorf("unsupported type %v to decode", roachpb.ValueType_name[int32(t)])
	}
	return res, nil
}

// decodeValueFromTuple decodes tuple bytes according to the column type.
func decodeValueFromTuple(
	keyVal kv.KeyValue, index *IndexDescriptor, tbl TableDescriptor,
) (tree.Datums, error) {
	valueBytes, err := keyVal.Value.GetTuple()
	if err != nil {
		return nil, err
	}
	if len(valueBytes) == 0 {
		return nil, nil
	}

	var lastColID ColumnID
	indexColNum := len(index.ColumnIDs)
	valueColsFound, neededValueCols := 0, len(tbl.Columns)-indexColNum
	encDatumRow := make(EncDatumRow, neededValueCols)

	for len(valueBytes) > 0 && valueColsFound < neededValueCols {
		typeOffset, dataOffset, colIDDiff, typ, err := encoding.DecodeValueTag(valueBytes)
		if err != nil {
			return nil, err
		}
		colID := lastColID + ColumnID(colIDDiff)
		lastColID = colID

		var encValue EncDatum
		encValue, valueBytes, err = EncDatumValueFromBufferWithOffsetsAndType(valueBytes, typeOffset,
			dataOffset, typ)
		if err != nil {
			return nil, err
		}
		encDatumRow[valueColsFound] = encValue
		valueColsFound++
	}
	var res tree.Datums
	var alloc DatumAlloc
	for i := 0; i < len(encDatumRow); i++ {
		err := encDatumRow[i].EnsureDecoded(&tbl.Columns[i+indexColNum].Type, &alloc)
		if err != nil {
			return nil, err
		}
		res = append(res, encDatumRow[i].Datum)
	}
	return res, nil
}

func decodeExtraValue(
	systemTable TableDescriptor, index *IndexDescriptor, keyVal kv.KeyValue,
) (tree.Datums, error) {
	extraTypes, err := GetColumnTypes(&systemTable, index.ExtraColumnIDs)
	if err != nil {
		return nil, err
	}
	extraValues := make([]EncDatum, len(index.ExtraColumnIDs))
	dirs := make([]IndexDescriptor_Direction, len(index.ExtraColumnIDs))
	for i := range index.ExtraColumnIDs {
		// Implicit columns are always encoded Ascending.
		dirs[i] = IndexDescriptor_ASC
	}
	extraKey, err := keyVal.Value.GetBytes()
	if err != nil {
		return nil, err
	}
	_, _, err = DecodeKeyVals(extraTypes, extraValues, dirs, extraKey)
	if err != nil {
		return nil, err
	}
	var extraVal tree.Datums
	for i := range extraValues {
		col, _ := systemTable.FindColumnByID(index.ExtraColumnIDs[i])
		a := &DatumAlloc{}
		if err := extraValues[i].EnsureDecoded(&col.Type, a); err != nil {
			return nil, err
		}
		extraVal = append(extraVal, extraValues[i].Datum)
	}
	return extraVal, nil
}

func makeInstNamespaceByRow(row tree.Datums) InstNameSpace {
	// system.kwdb_ts_table
	var instanceDesc ChildDesc
	err := protoutil.Unmarshal([]byte(tree.MustBeDBytes(row[4])), &instanceDesc)
	if err != nil {
		log.Error(context.Background(), err)
	}
	desc := InstNameSpace{
		InstTableID: ID(tree.MustBeDInt(row[0])),
		DBName:      string(tree.MustBeDString(row[1])),
		InstName:    string(tree.MustBeDString(row[2])),
		TmplTableID: ID(tree.MustBeDInt(row[3])),
		ChildDesc:   instanceDesc,
	}
	return desc
}

// EmptyTagValue Represents the default return value when the query property value is null.
const EmptyTagValue = "NULL"

// AllInstTableInfo Indicates IDs and names about all instance tables in the template table.
type AllInstTableInfo struct {
	// InstTableIDs is instance table ID.
	InstTableIDs []ID
	// InstTableNames is instance table name.
	InstTableNames []string
}

// GetAllInstanceByTmplTableID Returns the Name/ID of all instance tables in the template table.
func GetAllInstanceByTmplTableID(
	ctx context.Context, txn *kv.Txn, sTbID ID, mustFound bool, ie tree.InternalExecutor,
) (*AllInstTableInfo, error) {
	// Initialize AllInstTableInfo to avoid returning a null pointer
	res := &AllInstTableInfo{InstTableIDs: make([]ID, 0), InstTableNames: make([]string, 0)}
	//
	stmt := `select instance_id,instance_name from system.kwdb_ts_table where template_id=$1`
	rows, err := ie.Query(
		ctx,
		"query obj_attribute",
		txn,
		stmt,
		sTbID,
	)
	if err != nil {
		return res, err
	}
	for _, row := range rows {
		id := ID(tree.MustBeDInt(row[0]))
		name := string(tree.MustBeDString(row[1]))
		res.InstTableIDs = append(res.InstTableIDs, id)
		res.InstTableNames = append(res.InstTableNames, name)
	}
	return res, nil
}

// LookUpNameSpaceBySTbID return all InstNameSpace by sTbID
func LookUpNameSpaceBySTbID(
	ctx context.Context, txn *kv.Txn, tmplTableID ID, ie tree.InternalExecutor,
) ([]InstNameSpace, error) {
	// Gets all instance tables under this template table from the system table.
	stmt := `select * from system.kwdb_ts_table where template_id=$1`
	rows, err := ie.Query(
		ctx,
		"query obj_attribute",
		txn,
		stmt,
		tmplTableID,
	)
	if err != nil {
		return nil, err
	}
	res := make([]InstNameSpace, len(rows))
	for _, row := range rows {
		instanceID := ID(tree.MustBeDInt(row[0]))
		dbName := string(tree.MustBeDString(row[1]))
		instanceName := string(tree.MustBeDString(row[2]))
		templateID := ID(tree.MustBeDInt(row[3]))
		var desc ChildDesc
		if err := protoutil.Unmarshal([]byte(tree.MustBeDBytes(row[4])), &desc); err != nil {
			return nil, err
		}
		instNameSpace := InstNameSpace{
			InstName:    instanceName,
			InstTableID: instanceID,
			TmplTableID: templateID,
			DBName:      dbName,
			ChildDesc:   desc,
		}
		res = append(res, instNameSpace)
	}
	return res, nil
}

// GetInstNamespaceByInstID input: ctx, txn, instance table id
// This function is a generic function that gets the instance table namespace by reading the
// system table by making the key to the system table kwdb_ts_table.
func GetInstNamespaceByInstID(
	ctx context.Context, txn *kv.Txn, instID uint32,
) (*InstNameSpace, error) {
	tsTableKey, err := MakeKWDBMetadataKeyInt(KWDBTsTableTable, []uint64{uint64(instID)})
	if err != nil {
		return nil, err
	}
	row, err := GetKWDBMetadataRow(ctx, txn, tsTableKey, KWDBTsTableTable)
	if err != nil {
		return nil, err
	}
	instNamespace := makeInstNamespaceByRow(row)
	return &instNamespace, nil
}

// GetTmplTableIDByInstID Returns ID of the template table.
// output: template table id, error
func GetTmplTableIDByInstID(ctx context.Context, txn *kv.Txn, instID uint32) (ID, error) {
	instNamespace, err := GetInstNamespaceByInstID(ctx, txn, instID)
	if err != nil {
		return InvalidID, err
	}
	return instNamespace.TmplTableID, nil
}

// ResolveInstanceName resolve instance table through db name and instance table name.
func ResolveInstanceName(
	ctx context.Context, txn *kv.Txn, dbName, ctbName string,
) (InstNameSpace, bool, error) {
	instNameSpace, err := GetInstNamespaceByName(ctx, txn, dbName, ctbName)
	if err != nil {
		if strings.Contains(err.Error(), "object cannot found") {
			// found = false ->instance table does not exist.
			return InstNameSpace{}, false, nil
		}
		// found = true && err !=nil ->The query failed because the instance table may not exist.
		return InstNameSpace{}, true, err
	}
	return *instNameSpace, true, nil
}

// GetInstNamespaceByName Gets the primary key value according to the unique index
// encoding in the system table kwdb_ts_table, and then gets the value of all columns
// by the primary key.
func GetInstNamespaceByName(
	ctx context.Context, txn *kv.Txn, dbName, instName string,
) (*InstNameSpace, error) {
	k := keys.MakeTablePrefix(uint32(KWDBTsTableTable.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(KWDBTsTableTable.Indexes[0].ID))
	k = encoding.EncodeStringAscending(k, dbName)
	k = encoding.EncodeStringAscending(k, instName)
	k = encoding.EncodeUvarintAscending(k, uint64(KWDBTsTableTable.Families[0].ID))

	row, err := GetKWDBMetadataRow(ctx, txn, k, KWDBTsTableTable)
	if err != nil {
		return nil, err
	}
	cTableID := uint32(tree.MustBeDInt(row[2]))
	instNamespace, err := GetInstNamespaceByInstID(ctx, txn, cTableID)
	if err != nil {
		return nil, err
	}
	return instNamespace, nil
}

// GetTsTableDescByTableID Returns template table descriptor or normal time series table descriptor.
// input: ctx, txn, instance table id
// output: template table descriptor/normal time series table descriptor, error
// This function is a generic function that gets the table descriptor by calling GetSTableIDByCID and GetTableDescFromID.
func GetTsTableDescByTableID(
	ctx context.Context, txn *kv.Txn, cTbID uint32,
) (*TableDescriptor, error) {

	// get normal time series table descriptor by table id
	normalTsTableDesc, err := GetTableDescFromID(ctx, txn, ID(cTbID))
	if err != nil {
		// get template table id by instance table id
		tmplTableID, err := GetTmplTableIDByInstID(ctx, txn, cTbID)
		if err != nil {
			return nil, err
		}
		// get template table descriptor by template table id
		desc, err := GetTableDescFromID(ctx, txn, tmplTableID)
		if err != nil {
			return nil, err
		}

		return desc, nil
	}

	return normalTsTableDesc, nil
}

// NeedConvert checks whether an attribute value of bytes needs to be converted to a string.
func NeedConvert(str string) bool {
	str = strings.Trim(str, "'")
	// Normal strings are true. The value is false in bytes format.
	isValid := utf8.ValidString(str)
	if !isValid {
		return true
	}
	byt := []byte(str)
	// ascii code 0 to 31 special characters and 127(delete) need to be converted
	return len(byt) == 1 && (byt[0] >= 0 && byt[0] <= 31 || byt[0] == 127)
}

// DatumToString Converts datum to string. Determines whether a value of the
// DBytes type needs to be converted to a string.
func DatumToString(d tree.Datum) string {
	var res string
	switch val := d.(type) {
	case *tree.DString:
		res = string(*val)
	case *tree.DBytes:
		if NeedConvert(string(*val)) {
			res = strings.Trim(tree.NewDBytes(*val).String(), "'")
		} else {
			res = string(*val)
		}
	case *tree.DTimestamp:
		res = val.String()
		if len(res) > 0 && res[0] == '\'' {
			res = res[1:]
		}
		if len(res) > 0 && res[len(res)-1] == '\'' {
			res = res[:len(res)-1]
		}
	case *tree.DNullExtern:
		res = EmptyTagValue

	default:
		res = val.String()
	}
	return res
}
