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
	"strconv"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	duck "github.com/duckdb-go-bindings"
)

type ApEngine struct {
	stopper    *stop.Stopper
	db         *duck.Database
	dbStruct   *C.APEngine
	Connection *duck.Connection
	DbPath     string
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
	state = duck.Open(dbPath+"/ap_database.db", &db)
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
	}, nil
}

// Close close TsEngine
func (r *ApEngine) Close() {
	duck.Close(r.db)
	duck.Disconnect(r.Connection)
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
