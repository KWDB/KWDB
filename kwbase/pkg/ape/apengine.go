// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
//
//	http://license.coscl.org.cn/MulanPSL2
//
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.
package ape

// #cgo CPPFLAGS: -I../../../kwdbts2/include
// #cgo LDFLAGS: -lkwdbts2 -lcommon  -lstdc++
// #cgo LDFLAGS: -lprotobuf
// #cgo linux LDFLAGS: -lrt -lpthread
//
// #include <stdlib.h>
// #include <string.h>
// #include <libkwdbts2.h>
import "C"
import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	duck "github.com/duckdb-go-bindings"
)

type ApEngine struct {
	stopper    *stop.Stopper
	db         *duck.Database
	dbStruct   *C.APEngine
	Connection *duck.Connection
	DbPath     string
	mu         syncutil.Mutex
	dbMap      map[string]*duck.Database
}

// QueryInfo the parameter and return value passed by the query
type QueryInfo struct {
	Buf      []byte
	RowNum   int
	ID       int
	UniqueID int
	TimeZone int
	Code     int
	Handle   unsafe.Pointer
	RowCount int
	SQL      string
}

func NewApEngine(stopper *stop.Stopper, dbPath string) (*ApEngine, error) {
	db := duck.Database{}
	var state duck.State
	state = duck.Open(dbPath+"/tpch", &db)
	if state != duck.StateSuccess {
		return nil, errors.New("failed to open the ap database")
	}

	connection := duck.Connection{}
	state = duck.Connect(db, &connection)
	if state != duck.StateSuccess {
		return nil, errors.New("failed to connect to ap database")
	}
	var dbStruct *C.APEngine
	C.APOpen(&dbStruct)
	return &ApEngine{
		stopper:    stopper,
		db:         &db,
		Connection: &connection,
		DbPath:     dbPath,
		dbStruct:   dbStruct,
		dbMap:      make(map[string]*duck.Database),
	}, nil
}

// Close close TsEngine
func (r *ApEngine) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	duck.Close(r.db)
	duck.Disconnect(r.Connection)
	if r.dbMap != nil {
		for key, db := range r.dbMap {
			if db != nil {
				duck.Close(db)
			}
			delete(r.dbMap, key)
		}
		r.dbMap = nil
	}
}

// InitHandle corresponding to init ts handle
func (r *ApEngine) InitHandle(
	ctx *context.Context, queryInfo QueryInfo,
) (respInfo QueryInfo, err error) {
	return r.Execute(ctx, C.MQ_TYPE_DML_INIT, queryInfo)
}

// SetupFlow send timing execution plan and receive execution results
func (r *ApEngine) SetupFlow(
	ctx *context.Context, queryInfo QueryInfo,
) (respInfo QueryInfo, err error) {
	return r.Execute(ctx, C.MQ_TYPE_DML_SETUP, queryInfo)
}

// NextFlow drive timing execution plan, receive execution results
func (r *ApEngine) NextFlow(
	ctx *context.Context, queryInfo QueryInfo,
) (respInfo QueryInfo, err error) {
	return r.Execute(ctx, C.MQ_TYPE_DML_NEXT, queryInfo)
}

// NextFlowPgWire drive timing execution plan, receive execution results
func (r *ApEngine) NextFlowPgWire(
	ctx *context.Context, queryInfo QueryInfo,
) (respInfo QueryInfo, err error) {
	return r.Execute(ctx, C.MQ_TYPE_DML_PG_RESULT, queryInfo)
}

// Execute call the engine dml interface to issue a request and return the result
func (r *ApEngine) Execute(
	ctx *context.Context, tp C.EnMqType, queryInfo QueryInfo,
) (respInfo QueryInfo, err error) {
	if len(queryInfo.Buf) == 0 {
		return respInfo, errors.New("query buf is nul")
	}
	var cQueryInfo C.APQueryInfo
	bufC := C.CBytes(queryInfo.Buf)
	defer C.free(unsafe.Pointer(bufC))
	cQueryInfo.value = bufC
	cQueryInfo.len = C.uint(len(queryInfo.Buf))
	cQueryInfo.tp = tp
	cQueryInfo.id = C.int(queryInfo.ID)
	cQueryInfo.handle = queryInfo.Handle
	cQueryInfo.unique_id = C.int(queryInfo.UniqueID)
	cQueryInfo.time_zone = C.int(queryInfo.TimeZone)
	cQueryInfo.relation_ctx = C.uint64_t(uintptr(unsafe.Pointer(ctx)))
	cQueryInfo.db = r.db.Ptr
	cQueryInfo.connection = r.Connection.Ptr
	cTsSlice := C.TSSlice{
		data: (*C.char)(C.CBytes([]byte(queryInfo.SQL))),
		len:  C.size_t(len(queryInfo.SQL)),
	}
	defer C.free(unsafe.Pointer(cTsSlice.data))
	cQueryInfo.sql = cTsSlice

	// init fetcher of analyse
	var vecFetcher C.VecTsFetcher
	vecFetcher.collected = C.bool(false)
	var retInfo C.APRespInfo
	retInfo.value = nil
	C.APExecQuery(r.dbStruct, &cQueryInfo, &retInfo)
	respInfo.ID = int(retInfo.id)
	respInfo.UniqueID = int(retInfo.unique_id)
	respInfo.Handle = unsafe.Pointer(retInfo.handle)
	respInfo.Code = int(retInfo.code)
	respInfo.RowNum = int(retInfo.row_num)
	if unsafe.Pointer(retInfo.value) != nil {
		respInfo.Buf = C.GoBytes(unsafe.Pointer(retInfo.value), C.int(retInfo.len))
		C.TSFree(unsafe.Pointer(retInfo.value))
	}
	if respInfo.Code > 1 {
		if unsafe.Pointer(retInfo.value) != nil {
			strCode := make([]byte, 5)
			code := respInfo.Code
			for i := 0; i < 5; i++ {
				strCode[i] = byte(((code) & 0x3F) + '0')
				code = code >> 6
			}
			err = pgerror.Newf(string(strCode), string(respInfo.Buf))
		} else {
			err = fmt.Errorf("error Code: %s", strconv.Itoa(respInfo.Code))
		}
	} else if retInfo.ret < 1 {
		err = fmt.Errorf("unknown error")
		fmt.Printf("AE return  ret = %v", retInfo)
	}

	return respInfo, err
}

// CreateConnection create new coneection with ap engine.
func (r *ApEngine) CreateConnection(dbName string) (*duck.Connection, error) {
	dbPath := r.DbPath + "/" + dbName
	_, err := os.Stat(dbPath)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("database %s not exist", dbName)
	}
	var db *duck.Database
	r.mu.Lock()
	defer r.mu.Unlock()
	db, ok := r.dbMap[dbName]
	if !ok {
		db = &duck.Database{}
		state := duck.Open(dbPath, db)
		if state != duck.StateSuccess {
			return nil, errors.New("failed to open the ap database")
		}
		r.dbMap[dbName] = db
	}
	conn := duck.Connection{}
	state := duck.Connect(*db, &conn)
	if state != duck.StateSuccess {
		return nil, errors.New("failed to connect to ap database")
	}
	return &conn, nil
}

func (r *ApEngine) DestroyConnection(conn *duck.Connection) {
	duck.Disconnect(conn)
}

// Exec execute sql in input connection.
func (r *ApEngine) Exec(conn *duck.Connection, stmt string) error {
	var res duck.Result
	state1 := duck.Query(*conn, stmt, &res)
	if state1 != duck.StateSuccess {
		errMsg := duck.ResultError(&res)
		return pgerror.New(pgcode.Warning, errMsg)
	}
	duck.DestroyResult(&res)
	return nil
}
