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
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

const (
	// RangeGroupIDOffset offset of range_group_id in the payload header
	RangeGroupIDOffset int = 16
	// RangeGroupIDSize length of range_group_id in the payload header
	RangeGroupIDSize int = 2
	// OsnIDOffset offset of osn_id in the payload header
	OsnIDOffset = 0
	// OsnIDSize length of osn_id in the payload header (Only 8 bytes have been used).
	OsnIDSize = 16
	// VarColumnSize is the fixed length memory taken by var-length data type
	VarColumnSize = 8
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

// ComputeTSColumnSize computes colSize and allocSize of a column according to its type.
func ComputeTSColumnSize(
	oidType oid.Oid, fixedLengthStr bool, storageLen int, allSize, allocSize *int,
) bool {
	colSize := 0
	AllocSize := 0
	switch oidType {
	case oid.T_int2:
		colSize += 2
	case oid.T_int4, oid.T_float4:
		colSize += 4
	case oid.T_int8, oid.T_float8, oid.T_timestamp, oid.T_timestamptz:
		colSize += 8
	case oid.T_bool:
		colSize++
	case oid.T_char, types.T_nchar, oid.T_text, oid.T_bpchar, oid.T_bytea, types.T_geometry:
		colSize += storageLen
	case oid.T_varchar, types.T_nvarchar, types.T_varbytea:
		if fixedLengthStr {
			colSize += storageLen
		} else {
			// pre allocate paylaod space for var-length colums
			// here we use some fixed-rule to preallocate more space to improve efficiency
			// StorageLen = userWidth+1
			// varDataLen = StorageLen+2
			if storageLen < 68 {
				// 100%
				AllocSize += storageLen
			} else if storageLen < 260 {
				// 60%
				AllocSize += int(storageLen/5) * 3
			} else {
				// 30%
				AllocSize += int(storageLen/10) * 3
			}
			colSize += VarColumnSize
		}
	default:
		return false
	}
	if allSize != nil {
		*allSize += colSize
	}

	if allocSize != nil {
		*allocSize += AllocSize
	}

	return true
}

// parsePtagValue parse the value of a string into the corresponding type of value and add it to the payload.
func parsePtagValue(payload *[]byte, value string, offset int, typ *types.T) error {
	parseInt := func(val string) (*tree.DInt, error) {
		v, err := tree.ParseDInt(val)
		if err != nil {
			return nil, err
		}
		// Width is defined in bits.
		width := uint(typ.Width() - 1)
		// We're performing bounds checks inline with Go's implementation of min and max ints in Math.go.
		shifted := *v >> width
		if (*v >= 0 && shifted > 0) || (*v < 0 && shifted < -1) {
			return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
				"integer \"%d\" out of range for type %s", *v, typ.SQLString())
		}
		return v, nil
	}
	switch typ.InternalType.Oid {
	case oid.T_int2:
		v, err := parseInt(value)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint16((*payload)[offset:], uint16(*v))
	case oid.T_int4:
		v, err := parseInt(value)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint32((*payload)[offset:], uint32(*v))
	case oid.T_int8:
		v, err := parseInt(value)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint64((*payload)[offset:], uint64(*v))
	case oid.T_bool:
		v, err := tree.ParseDBool(value)
		if err != nil {
			return err
		}
		if *v {
			(*payload)[offset] = 1
		} else {
			(*payload)[offset] = 0
		}
	case types.T_nchar, oid.T_varchar, oid.T_bpchar:
		copy((*payload)[offset:], value)

	default:
		return errors.Errorf("unsupported int oid %v", typ.InternalType.Oid)
	}
	return nil
}

// GetPtagPayloads construct payloads of primary tag values
// ex: PrimaryTagValues: [1,2,3], [3,4,5] ->
// [13,14,15,23,24,25,33,34,35]
func GetPtagPayloads(
	payloads [][]byte, source []string, offset int, typ *types.T, pTagSize int,
) ([][]byte, error) {
	if len(payloads) == 0 {
		ret := make([][]byte, len(source))
		for i := range ret {
			ret[i] = make([]byte, pTagSize)
		}
		for k := range source {
			err := parsePtagValue(&ret[k], source[k], offset, typ)
			if err != nil {
				return nil, err
			}
		}
		return ret, nil
	}
	ret := make([][]byte, len(payloads)*len(source))
	for i := range ret {
		ret[i] = make([]byte, pTagSize)
	}
	i := 0
	for k := range payloads {
		for idx := range source {
			newPayload := make([]byte, len(payloads[k]))
			copy(newPayload, payloads[k])
			err := parsePtagValue(&newPayload, source[idx], offset, typ)
			if err != nil {
				return nil, err
			}
			ret[i] = newPayload
			i++
		}
	}
	return ret, nil
}

// GetValidColumnsByPrimaryTagValue is used to get IDs of valid columns.
func GetValidColumnsByPrimaryTagValue(
	ctx context.Context,
	txn *kv.Txn,
	tableID uint64,
	version uint32,
	hashNum uint64,
	primaryTagValues ...[]byte,
) (map[uint32]struct{}, error) {
	points, err := api.GetHashPointByPrimaryTag(hashNum, primaryTagValues...)
	if err != nil {
		return nil, err
	}

	type rangeTags struct {
		startKey roachpb.Key
		pTags    [][]byte
	}
	var rangeTagsMap = make(map[roachpb.RangeID]rangeTags)
	for i, hashPoint := range points {
		var descs []roachpb.RangeDescriptor
		key := MakeTsRangeKey(ID(tableID), uint64(hashPoint), hashNum)

		descs, err := getRangeDescs(ctx, txn, roachpb.Span{
			Key:    key,
			EndKey: key.PrefixEnd(),
		})
		if err != nil {
			return nil, err
		}
		//it := kvcoord.NewRangeIterator(ds)
		//for it.Seek(ctx, roachpb.RKey(key), kvcoord.Ascending); it.Valid(); it.Next(ctx) {
		//	descs = append(descs, it.Desc())
		//	newTableID, newHashPoint, err := DecodeTsRangeKey(it.Desc().EndKey, true, hashNum)
		//	if err != nil {
		//		return nil, err
		//	}
		//	if newHashPoint > uint64(hashPoint) || newTableID > tableID {
		//		break
		//	}
		//}
		//if len(descs) == 0 {
		//	return nil, errors.Errorf("failed seek any range descriptor, seekKey: %v", key)
		//}
		desc := descs[0]

		if rTags, ok := rangeTagsMap[desc.RangeID]; ok {
			rTags.pTags = append(rTags.pTags, primaryTagValues[i])
			rangeTagsMap[desc.RangeID] = rTags
		} else {
			rangeTagsMap[desc.RangeID] = rangeTags{
				desc.StartKey.AsRawKey(),
				[][]byte{primaryTagValues[i]},
			}
		}
	}
	var ba = txn.NewBatch()
	for _, val := range rangeTagsMap {
		ba.AddRawRequest(&roachpb.TsGetValidColumnsRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: val.startKey,
			},
			TableId:     tableID,
			Version:     version,
			PrimaryTags: val.pTags,
		})
	}
	if err = txn.DB().Run(ctx, ba); err != nil {
		return nil, err
	}

	validColumnsIDs := make(map[uint32]struct{})
	addIDs := func(ids []uint32) {
		for _, id := range ids {
			validColumnsIDs[id] = struct{}{}
		}
	}
	for respsID := range ba.RawResponse().Responses {
		resp := ba.RawResponse().Responses[respsID].GetInner().(*roachpb.TsGetValidColumnsResponse)
		addIDs(resp.ValidIDs)
	}
	return validColumnsIDs, nil
}

// getRangeDescs returns the descs for the ranges that touch the given span.
func getRangeDescs(
	ctx context.Context, txn *kv.Txn, span roachpb.Span,
) ([]roachpb.RangeDescriptor, error) {
	metaStart := keys.RangeMetaKey(keys.MustAddr(span.Key).Next())
	metaEnd := keys.RangeMetaKey(keys.MustAddr(span.EndKey))

	var descs []roachpb.RangeDescriptor
	kvs, err := txn.Scan(ctx, metaStart, metaEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 || !kvs[len(kvs)-1].Key.Equal(metaEnd.AsRawKey()) {
		// Normally we need to scan one more KV because the ranges are addressed by
		// the end key.
		extraKV, err := txn.Scan(ctx, metaEnd, keys.Meta2Prefix.PrefixEnd(), 1 /* one result */)
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, extraKV[0])
	}
	for _, r := range kvs {
		var desc roachpb.RangeDescriptor
		if err := r.ValueProto(&desc); err != nil {
			return nil, err
		}
		descs = append(descs, desc)
	}
	return descs, nil
}
