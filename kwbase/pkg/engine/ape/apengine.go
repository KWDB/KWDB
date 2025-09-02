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

// #cgo CPPFLAGS: -I../../../../kwdbap/src/include -I../../../../common/src/include
// #cgo LDFLAGS: -lduckdb -lcommon  -lstdc++
// #cgo LDFLAGS: -lprotobuf
// #cgo linux LDFLAGS: -lrt -lpthread
//
// #include <stdlib.h>
// #include <string.h>
// #include <libkwdbap.h>
// #include <duckdb.h>
import "C"
import (
	"context"
	"fmt"
	"os"
	"strconv"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"github.com/pkg/errors"
)

// defaultApDatabase will be attach when ap engine init
const defaultApDatabase = "defaultdb"

type Engine struct {
	stopper  *stop.Stopper
	opened   bool
	dbStruct *C.APEngine
	conn     unsafe.Pointer
	cfg      EngineConfig
	attachDB map[string]struct{}
}

// EngineConfig configuration of Engine
type EngineConfig struct {
	Dir string
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

// A Error wraps an error returned from a TsEngine operation.
type Error struct {
	msg string
}

func (e Error) Error() string {
	return e.msg
}

const (
	// MaxArrayLen is a safe maximum length for slices on this architecture.
	MaxArrayLen = 1<<50 - 1
)

func cSliceToUnsafeGoBytes(s C.TSSlice) []byte {
	if s.data == nil {
		return nil
	}
	// Interpret the C pointer as a pointer to a Go array, then slice.
	return (*[MaxArrayLen]byte)(unsafe.Pointer(s.data))[:s.len:s.len]
}

func cStringToGoString(s C.TSString) string {
	if s.data == nil {
		return ""
	}
	// Reinterpret the string as a slice, then cast to string which does a copy.
	result := string(cSliceToUnsafeGoBytes(C.TSSlice{s.data, s.len}))
	C.free(unsafe.Pointer(s.data))
	return result
}

func statusToError(s C.TSStatus) error {
	if s.data == nil {
		return nil
	}
	return &Error{msg: cStringToGoString(s)}
}

func convertToChar(val string) *C.char {
	return (*C.char)(C.CBytes([]byte(val)))
}

func NewApEngine(stopper *stop.Stopper, cfg EngineConfig) (*Engine, error) {
	var eg Engine
	eg.stopper = stopper
	eg.cfg = cfg
	eg.attachDB = make(map[string]struct{})
	return &eg, nil
}

func (r *Engine) TableExist(tableID uint64) (bool, error) {
	return false, nil
}

func (r *Engine) CreateTable(tableID uint64, hashNum uint64, meta []byte, rangeGroups []api.RangeGroup) error {
	return nil
}

func (r *Engine) SetRaftLogCombinedWAL(combined bool) {
}

func (r *Engine) DropLeftTsTableGarbage() error {
	return nil
}

func (r *Engine) CheckpointForTable(tableID uint32) error {
	return nil
}

// GetDBPath returns db path
func (r *Engine) GetDBPath() string {
	return r.cfg.Dir
}

// IsOpen returns when the ts engine has been open.
func (r *Engine) IsOpen() bool {
	return r.opened
}

// Open opens the ts engine.
func (r *Engine) Open(rangeIndex []roachpb.RangeIndex) error {
	if _, err := os.Stat(r.cfg.Dir); err != nil {
		os.Mkdir(r.cfg.Dir, 0755)
	}
	var CString C.APString
	CString.value = (*C.char)(C.CBytes([]byte(r.cfg.Dir)))
	CString.len = (C.uint32_t)(len(r.cfg.Dir))
	var conn C.APConnectionPtr
	status := C.APOpen(&r.dbStruct, &conn, &CString)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not open engine instance")
	}

	r.conn = unsafe.Pointer(conn)

	r.opened = true
	return nil
}

// Close closes Engine
func (r *Engine) Close() {
	status := C.APClose(r.dbStruct)
	if err := statusToError(status); err != nil {
		log.Errorf(context.TODO(), "could not close ts engine instance")
	}
}

// InitHandle corresponding to init ts handle
func (r *Engine) InitHandle(
	ctx *context.Context, queryInfo QueryInfo,
) (respInfo QueryInfo, err error) {
	return r.Execute(ctx, C.MQ_TYPE_DML_INIT, queryInfo)
}

// SetupFlow send timing execution plan and receive execution results
func (r *Engine) SetupFlow(
	ctx *context.Context, queryInfo QueryInfo,
) (respInfo QueryInfo, err error) {
	return r.Execute(ctx, C.MQ_TYPE_DML_SETUP, queryInfo)
}

// NextFlow drive timing execution plan, receive execution results
func (r *Engine) NextFlow(
	ctx *context.Context, queryInfo QueryInfo,
) (respInfo QueryInfo, err error) {
	return r.Execute(ctx, C.MQ_TYPE_DML_NEXT, queryInfo)
}

// NextFlowPgWire drive timing execution plan, receive execution results
func (r *Engine) NextFlowPgWire(
	ctx *context.Context, queryInfo QueryInfo,
) (respInfo QueryInfo, err error) {
	return r.Execute(ctx, C.MQ_TYPE_DML_PG_RESULT, queryInfo)
}

// CloseFlow close the actuator corresponding to the current flow
func (r *Engine) CloseFlow(ctx *context.Context, queryInfo QueryInfo) (err error) {
	_, err = r.Execute(ctx, C.MQ_TYPE_DML_CLOSE, queryInfo)
	return err
}

// Execute call the engine dml interface to issue a request and return the result
func (r *Engine) Execute(
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
	cDBPathSlice := C.TSSlice{
		data: (*C.char)(C.CBytes([]byte(r.cfg.Dir))),
		len:  C.size_t(len(r.cfg.Dir)),
	}
	cQueryInfo.db_path = cDBPathSlice
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

// CreateConnection create new connection with ap engine.
func (r *Engine) CreateConnection(dbName string) error {
	if dbName != "" {
		if err := r.ExecSql(fmt.Sprintf("USE '%s'", dbName)); err != nil {
			return err
		}
	}
	return nil
}

func (r *Engine) DestroyConnection() {
}

func getErrorString(rep *C.APQueryInfo) error {
	if rep.code > 1 {
		if unsafe.Pointer(rep.value) != nil {
			tmp := C.GoBytes(unsafe.Pointer(rep.value), C.int(rep.len))
			C.TSFree(unsafe.Pointer(rep.value))

			strCode := make([]byte, 5)
			code := rep.code
			for i := 0; i < 5; i++ {
				strCode[i] = byte(((code) & 0x3F) + '0')
				code = code >> 6
			}
			return pgerror.Newf(string(strCode), string(tmp))
		} else {
			return fmt.Errorf("error Code: %s", strconv.Itoa(int(rep.code)))
		}
	} else if rep.ret < 1 {
		return fmt.Errorf("unknown error")
	}

	return nil
}

func (r *Engine) ExecSql(stmt string) error {
	var retInfo C.APRespInfo
	var CString C.APString
	CString.value = (*C.char)(C.CBytes([]byte(stmt)))
	CString.len = (C.uint32_t)(len(stmt))
	status := C.APExecSQL(r.dbStruct, &CString, &retInfo)
	if err := statusToError(status); err != nil {
		if innErr := getErrorString(&retInfo); innErr != nil {
			return innErr
		}
		return err
	}
	return nil
}

// ExecSqlForResult gets exec sql result
func (r *Engine) ExecSqlForResult(stmt string, count *int) error {
	var retInfo C.APRespInfo
	var CString C.APString
	CString.value = (*C.char)(C.CBytes([]byte(stmt)))
	CString.len = (C.uint32_t)(len(stmt))
	status := C.APExecSQL(r.dbStruct, &CString, &retInfo)
	if err := statusToError(status); err != nil {
		if innErr := getErrorString(&retInfo); innErr != nil {
			return innErr
		}
		return err
	}

	*count = int(retInfo.row_num)
	return nil
}

func (r *Engine) dbOperate(dbName string, op C.EnDBOperateType) error {
	var CString C.APString
	CString.value = (*C.char)(C.CBytes([]byte(dbName)))
	CString.len = (C.uint32_t)(len(dbName))
	status := C.APDatabaseOperate(r.dbStruct, &CString, op)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not operate db")
	}
	return nil
}

func (r *Engine) CreateDB(dbName string) error {
	return r.dbOperate(dbName, C.DB_CREATE)
}

func (r *Engine) Attach(dbName string) error {
	return r.dbOperate(dbName, C.DB_ATTACH)
}

func (r *Engine) Detach(dbName string) error {
	defer delete(r.attachDB, dbName)
	return r.dbOperate(dbName, C.DB_DETACH)
}

func (r *Engine) ExecSqlInDB(dbName string, sql string) error {
	if err := r.Attach(dbName); err != nil {
		return err
	}

	if err := r.ExecSql(sql); err != nil {
		return errors.Wrap(err, "could not exec sql "+sql)
	}

	return nil
}

func (r *Engine) DropTable(dbName string, sql string) error {
	if err := r.Attach(dbName); err != nil {
		return err
	}

	if err := r.ExecSql(sql); err != nil {
		return errors.Wrap(err, "could not exec sql "+sql)
	}

	return nil
}

// AttachDatabase attachs ap database
func (r *Engine) AttachDatabase(dbs []string) error {
	// todo implement in c++ interface
	for _, v := range dbs {
		if err := r.dbOperate(v, C.DB_ATTACH); err != nil {
			return err
		}
		r.attachDB[v] = struct{}{}
	}
	return nil
}

func (r *Engine) DropDatabase(currentDB string, dropDB string, rm bool) error {
	var use string
	if currentDB == dropDB {
		use = `use defaultdb`
	} else {
		use = fmt.Sprintf(`use %s`, currentDB)
	}
	if err := r.ExecSql(use); err != nil {
		return errors.Wrap(err, "could not exec sql "+use)
	}

	r.Detach(dropDB)

	if rm {
		filePath := r.cfg.Dir + "/" + dropDB
		if _, err := os.Stat(filePath); err != nil {
			_ = os.Remove(filePath)
		}
	}

	return nil
}
