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

// #cgo CPPFLAGS: -I../../../kwdbts2/include
// #cgo LDFLAGS: -lkwdbts2 -lcommon  -lstdc++
// #cgo LDFLAGS: -lprotobuf
// #cgo linux LDFLAGS: -lrt -lpthread
//
// #include <stdlib.h>
// #include <libkwdbts2.h>
import "C"
import (
	"context"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

var handler = getTableHandler{}

// getTableHandler is used to get desc from storage.
type getTableHandler struct {
	db  *kv.DB
	tse *tse.TsEngine
}

// Init initialize the handler and init the db.
func Init(db *kv.DB, tse *tse.TsEngine) {
	handler.db = db
	handler.tse = tse
}

// GetNameSpaceByParentID gets NameSpace by dbID and schemaID.
func GetNameSpaceByParentID(
	ctx context.Context, txn *kv.Txn, dbID, schemaID sqlbase.ID,
) ([]sqlbase.Namespace, error) {
	NameKey, _ := sqlbase.MakeKWDBMetadataKeyInt(sqlbase.NamespaceTable, []uint64{uint64(dbID), uint64(schemaID)})
	rows, err := sqlbase.GetKWDBMetadataRows(ctx, txn, NameKey, sqlbase.NamespaceTable)
	if err != nil {
		if IsObjectCannotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	var res []sqlbase.Namespace
	for i := range rows {
		res = append(res, makeNameSpaceByRow(rows[i]))
	}
	return res, nil
}

func makeNameSpaceByRow(row tree.Datums) sqlbase.Namespace {
	// from system.namespace
	return sqlbase.Namespace{
		ParentID:       uint64(tree.MustBeDInt(row[0])),
		ParentSchemaID: uint64(tree.MustBeDInt(row[1])),
		Name:           string(tree.MustBeDString(row[2])),
		ID:             uint64(tree.MustBeDInt(row[3])),
	}
}

// IsObjectCannotFoundError checks if error is object cannot found.
func IsObjectCannotFoundError(err error) bool {
	if strings.Contains(err.Error(), "object cannot found") {
		return true
	}
	return false
}

//export checkTableMetaExist
func checkTableMetaExist(id C.TSTableID) C.bool {
	ctx := context.Background()
	tableID := uint64(id)
	var tb *sqlbase.TableDescriptor
	err := handler.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// get tableDesc
		descKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(tableID))
		endKey := descKey.PrefixEnd()
		var kvs []kv.KeyValue
		var err error
		kvs, err = txn.Scan(ctx, descKey, endKey, 0)
		if err != nil {
			return err
		}
		for _, v := range kvs {
			desc := &sqlbase.Descriptor{}
			if err := v.ValueProto(desc); err != nil {
				return err
			}
			switch t := desc.Union.(type) {
			case *sqlbase.Descriptor_Table:
				tb = desc.GetTable()
				break
			default:
				return errors.AssertionFailedf("Descriptor.Union has unexpected type %T", t)
			}
		}
		return nil
	})
	if err != nil || tb == nil {
		log.Error(ctx, err)
		return C.bool(false)
	}
	return C.bool(!tb.Dropped())
}

//export getTableMetaByVersion
func getTableMetaByVersion(
	id C.TSTableID, tsVer C.uint64_t, outputLen *C.size_t, errMsg **C.char,
) *C.char {
	ctx := context.Background()
	var res []byte
	tableID := uint64(id)
	tsVersion := uint32(tsVer)
	err := handler.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// get tableDesc with specific tsVersion
		descKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(tableID))
		endKey := descKey.PrefixEnd()
		var kvs []kv.KeyValue
		var err error
		isGetLatestVersion := tsVersion == 0
		if isGetLatestVersion {
			kvs, err = txn.Scan(ctx, descKey, endKey, 0)
		} else {
			kvs, err = txn.ScanAllMvccVerForOneTable(ctx, descKey, endKey, 0)
		}
		if err != nil {
			return err
		}
		var targetDesc *sqlbase.TableDescriptor
		var biggerDescVersion uint32
		for _, kv := range kvs {
			desc := &sqlbase.Descriptor{}
			if err := kv.ValueProto(desc); err != nil {
				return err
			}
			switch t := desc.Union.(type) {
			case *sqlbase.Descriptor_Table:
				table := desc.GetTable()
				if isGetLatestVersion {
					targetDesc = table
					break
				}
				// when we get multiple tableDesc with the same tsVersion, use the one with bigger tableDesc.Version
				if uint32(table.TsTable.TsVersion) == tsVersion && uint32(table.Version) > biggerDescVersion {
					targetDesc = table
				}
			default:
				return errors.AssertionFailedf("Descriptor.Union has unexpected type %T", t)
			}
		}
		if targetDesc == nil {
			return pgerror.Newf(pgcode.UndefinedTable, "can not find table %d with tsVersion %d", tableID, tsVersion)
		}

		d := jobspb.SyncMetaCacheDetails{SNTable: *targetDesc}
		createKObjectTable := makeKObjectTableForTs(d)
		meta, err := protoutil.Marshal(&createKObjectTable)
		if err != nil {
			panic(err.Error())
		}
		res = meta
		return nil
	})
	if err != nil {
		log.Error(ctx, err)
		*errMsg = C.CString(err.Error())
		*outputLen = 0
		return nil
	}
	cResult := C.CBytes(res)
	*outputLen = C.size_t(len(res))
	*errMsg = nil
	return (*C.char)(cResult)
}
