// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package tse

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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/apd"
	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
)

const (
	// MaxArrayLen is a safe maximum length for slices on this architecture.
	MaxArrayLen = 1<<50 - 1
)

const (
	compressInterval = "ts.compress_interval"
	vacuumInterval   = "ts.vacuum_interval"
)

const (
	// Nanosecond is one nano second
	Nanosecond = 1
	// Microsecond is one micro second to nano seconds
	Microsecond = 1000 * Nanosecond
	// Millisecond is one milli second to microseconds
	Millisecond = 1000 * Microsecond
	// Second is one second to milli seconds
	Second = 1000 * Millisecond
	// Minute is one minute to seconds
	Minute = 60 * Second
	// Hour is one hour to minutes
	Hour = 60 * Minute
	// Day is one day to hours
	Day = 24 * Hour
)

// TsPayloadSizeLimit is the max size of per payload.
var TsPayloadSizeLimit = settings.RegisterNonNegativeIntSetting(
	"ts.payload_size_limit",
	"max size of payload(bytes)",
	1<<20, // (1MiB)
)

// name of processor in time series
const (
	TsUnknownName int8 = iota
	TsTableReaderName
	TsAggregatorName
	TsNoopName
	TsSorterName
	TsStatisticReaderName
	TsSynchronizerName
	TsSamplerName
	TsTagReaderName
	TsDistinctName
)

// TsGetNameValue get name of tsProcessor.
func TsGetNameValue(this *execinfrapb.TSProcessorCoreUnion) int8 {
	if this.TableReader != nil {
		return TsTableReaderName
	}
	if this.Aggregator != nil {
		return TsAggregatorName
	}
	if this.Noop != nil {
		return TsNoopName
	}
	if this.Sorter != nil {
		return TsSorterName
	}
	if this.StatisticReader != nil {
		return TsStatisticReaderName
	}
	if this.Synchronizer != nil {
		return TsSynchronizerName
	}
	if this.Sampler != nil {
		return TsSamplerName
	}
	if this.TagReader != nil {
		return TsTagReaderName
	}
	if this.Distinct != nil {
		return TsDistinctName
	}
	return TsUnknownName
}

// A Error wraps an error returned from a TsEngine operation.
type Error struct {
	msg string
}

func (e Error) Error() string {
	return e.msg
}

// goToCTSSlice converts a go byte slice to a TSSlice. Note that this is
// potentially dangerous as the DBSlice holds a reference to the go
// byte slice memory that the Go GC does not know about. This method
// is only intended for use in converting arguments to C
// functions. The C function must copy any data that it wishes to
// retain once the function returns.
func goToCTSSlice(b []byte) C.TSSlice {
	if len(b) == 0 {
		return C.TSSlice{data: nil, len: 0}
	}
	return C.TSSlice{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.size_t(len(b)),
	}
}

// TsEngineConfig configuration of TsEngine
type TsEngineConfig struct {
	Attrs          roachpb.Attributes
	Dir            string
	ThreadPoolSize int
	TaskQueueSize  int
	BufferPoolSize int
	Settings       *cluster.Settings
	LogCfg         log.Config
	ExtraOptions   []byte
	IsSingleNode   bool
}

// TsQueryInfo the parameter and return value passed by the query
type TsQueryInfo struct {
	Buf      []byte
	RowNum   int
	ID       int
	UniqueID int
	TimeZone int
	Code     int
	Handle   unsafe.Pointer
	Fetcher  TsFetcher
	// only pass the data chunk to tse for multiple model processing
	// when the switch is on and the server starts with single node mode.
	PushData *DataChunkGo
	RowCount int
	// PullData TsDataChunkToGo
}

// TsColumnInfoToGo ts col info To Go
type TsColumnInfoToGo struct {
	FixedLen    uint32
	ReturnType  types.Family
	StorageLen  uint32
	StorageType sqlbase.DataType
}

// TsDataChunkToGo ts struct
type TsDataChunkToGo struct {
	ColumnNum   uint32
	Column      []TsColumnInfoToGo
	BitmapSize  uint32
	RowSize     uint32
	DataCount   uint32
	Capacity    uint32
	Data        []coldata.Vec
	IsDataOwner bool
	Begin       int
	NeedType    []coltypes.T
}

// IsReadComplete read complete
func (r *TsDataChunkToGo) IsReadComplete() bool {
	return uint32(r.Begin) == r.DataCount
}

// ColumnInfo defines ColumInfo structure used for pushing batchlookup data down
// only create the columnInfo to build data chunk in blj for multiple model processing
// when the switch is on and the server starts with single node mode.
type ColumnInfo struct {
	StorageLen      uint32
	FixedStorageLen uint32
	StorageType     oid.Oid
}

// DataChunkGo defines DataChunk structure used for pushing batchlookup data down
// only use for creating push data chunk for multiple model processing
// when the switch is on and the server starts with single node mode.
type DataChunkGo struct {
	Data         []byte
	ColInfo      []ColumnInfo
	ColOffset    []uint32
	BitmapOffset []uint32
	RowCount     uint32
}

// DedupResult is PutData dedup result
type DedupResult struct {
	DedupRule     int    // Deduplication mode
	DedupRows     int    // The number of inserted data rows affected
	DiscardBitmap []byte // The bitmap of discard data
}

// EntitiesAffect is affect of entities and unordered row count
type EntitiesAffect struct {
	EntityCount    uint16
	UnorderedCount uint32
}

// TsEngine is ts database instance.
type TsEngine struct {
	stopper *stop.Stopper
	cfg     TsEngineConfig
	tdb     *C.TSEngine
	opened  bool
	openCh  chan struct{}
}

// IsSingleNode Returns whether TsEngine is started in singleNode mode
func (r *TsEngine) IsSingleNode() bool {
	return r.cfg.IsSingleNode
}

// DeDuplicateRule indicates the deduplicate rule of the ts engine
var DeDuplicateRule = settings.RegisterPublicStringSetting(
	"ts.dedup.rule",
	"remove time series data duplicate rule",
	"override",
)

// TsRaftLogCombineWAL indicates whether combine raft log and wal
var TsRaftLogCombineWAL = settings.RegisterPublicBoolSetting(
	"ts.raftlog_combine_wal.enabled",
	"combine raft log and wal to reduce write amplification, but still ensure data consistency",
	false,
)

// TsWALFlushInterval indicates the WAL flush interval of TsEngine
var TsWALFlushInterval = settings.RegisterPublicDurationSetting(
	"ts.wal.flush_interval",
	"ts WAL flush interval in TsEngine. when 0, wal will be flushed on wrote. when -1, WAL is disable.",
	0,
)

// TsWALBufferSize indicates the WAL buffer size of TsEngine
var TsWALBufferSize = settings.RegisterValidatedByteSizeSetting(
	"ts.wal.buffer_size",
	"ts WAL buffer size, default 4Mib",
	4<<20,
	func(v int64) error {
		if v < 4<<20 {
			return errors.Errorf("WAL buffer size can not less than 4(Mib)")
		}
		return nil
	},
)

// TsWALFileSize indicates the WAL file size of TsEngine
var TsWALFileSize = settings.RegisterPublicValidatedByteSizeSetting(
	"ts.wal.file_size",
	"ts WAL file size, default 64Mib",
	64<<20,
	func(v int64) error {
		if v < 64<<20 {
			return errors.Errorf("WAL file size must more than 64(Mib)")
		}
		return nil
	},
)

// TsWALFilesInGroup indicates the WAL file num of in one entity group
var TsWALFilesInGroup = settings.RegisterPublicValidatedIntSetting(
	"ts.wal.files_in_group",
	"ts WAL files num of a entity group in TsEngine, default 3",
	3,
	func(v int64) error {
		if v < 3 {
			return errors.Errorf("WAL files num in group must more than 3")
		}
		return nil
	},
)

// TsWALCheckpointInterval indicates the wal checkpoint interval of TsEngine
var TsWALCheckpointInterval = settings.RegisterPublicDurationSetting(
	"ts.wal.checkpoint_interval",
	"ts WAL checkpoint interval in TsEngine",
	time.Minute,
)

// SQLTimeseriesTrace set trace for timeseries.
var SQLTimeseriesTrace = settings.RegisterStringSetting(
	"ts.trace.on_off_list",
	"collection/push switch",
	"",
)

// TsFreeSpaceAlertThreshold indicates alarm threshold of the available disk space of TsEngine
var TsFreeSpaceAlertThreshold = settings.RegisterPublicValidatedByteSizeSetting(
	"ts.disk_free_space.alert_threshold",
	"ts disk free space alert threshold for insert, default 0Byte, indicates no alert",
	0,
	func(v int64) error {
		if v < 0 {
			return errors.Errorf("%d is not nonnegative", v)
		}
		return nil
	},
)

//export isCanceledCtx
func isCanceledCtx(goCtxPtr C.uint64_t) C.bool {
	ctx := *(*context.Context)(unsafe.Pointer(uintptr(goCtxPtr)))
	select {
	case <-ctx.Done():
		return C.bool(true)
	default:
		return C.bool(false)
	}
}

// NewTsEngine new ts engine
func NewTsEngine(
	ctx context.Context, cfg TsEngineConfig, stopper *stop.Stopper,
) (*TsEngine, error) {
	if cfg.Dir == "" {
		return nil, errors.New("dir must be non-empty")
	}

	r := &TsEngine{
		stopper: stopper,
		cfg:     cfg,
		openCh:  make(chan struct{}),
	}

	return r, nil
}

// IsOpen returns when the ts engine has been open.
func (r *TsEngine) IsOpen() bool {
	return r.opened
}

// CheckOrWaitForOpen check whether the ts engine has been open and
// waits for open if it is not.
func (r *TsEngine) checkOrWaitForOpen() {
	if r.opened {
		return
	}
	_ = <-r.openCh
}

// Open opens the ts engine.
func (r *TsEngine) Open(rangeIndex []roachpb.RangeIndex) error {
	var walLevel uint8
	if TsRaftLogCombineWAL.Get(&r.cfg.Settings.SV) {
		walLevel = 3
		if v, ok := envutil.EnvString("KW_WAL_LEVEL", 0); ok && v != "3" {
			if err := os.Setenv("KW_WAL_LEVEL", "3"); err != nil {
				return errors.Wrap(err, "failed adjust env KW_WAL_LEVEL to 3")
			}
		}
	} else {
		interval := TsWALFlushInterval.Get(&r.cfg.Settings.SV)
		if interval < 0 {
			walLevel = 0
		} else if interval >= 0 && interval <= 200*time.Millisecond {
			walLevel = 2
		} else {
			walLevel = 1
		}
		if v, ok := envutil.EnvString("KW_WAL_LEVEL", 0); ok && v == "3" {
			if err := os.Setenv("KW_WAL_LEVEL", fmt.Sprintf("%d", walLevel)); err != nil {
				return errors.Wrapf(err, "failed adjust env KW_WAL_LEVEL to %d", walLevel)
			}
		}
	}

	walBufferSize := TsWALBufferSize.Get(&r.cfg.Settings.SV) >> 20
	walFileSize := TsWALFileSize.Get(&r.cfg.Settings.SV) >> 20
	walFilesInGroup := TsWALFilesInGroup.Get(&r.cfg.Settings.SV)

	traceLevel := SQLTimeseriesTrace.Get(&r.cfg.Settings.SV)
	optLog := C.TsLogOptions{
		Dir:                       goToTSSlice([]byte(r.cfg.LogCfg.Dir)),
		LogFileMaxSize:            C.long(r.cfg.LogCfg.LogFileMaxSize),
		LogFilesCombinedMaxSize:   C.long(r.cfg.LogCfg.LogFilesCombinedMaxSize),
		LogFileVerbosityThreshold: C.LgSeverity(r.cfg.LogCfg.LogFileVerbosityThreshold),
		Trace_on_off_list:         goToTSSlice([]byte(traceLevel)),
	}

	if len(rangeIndex) == 0 {
		status := C.TSOpen(&r.tdb, goToTSSlice([]byte(r.cfg.Dir)),
			C.TSOptions{
				wal_level:         C.uint8_t(walLevel),
				wal_buffer_size:   C.uint16_t(uint16(walBufferSize)),
				wal_file_size:     C.uint16_t(uint16(walFileSize)),
				wal_file_in_group: C.uint16_t(uint16(walFilesInGroup)),
				extra_options:     goToTSSlice(r.cfg.ExtraOptions),
				thread_pool_size:  C.uint16_t(uint16(r.cfg.ThreadPoolSize)),
				task_queue_size:   C.uint16_t(uint16(r.cfg.TaskQueueSize)),
				buffer_pool_size:  C.uint32_t(uint32(r.cfg.BufferPoolSize)),
				lg_opts:           optLog,
				is_single_node:    C.bool(r.cfg.IsSingleNode),
			},
			nil,
			C.uint64_t(0))
		if err := statusToError(status); err != nil {
			return errors.Wrap(err, "could not open tsengine instance")
		}
	} else {
		appliedRangeIndex := make([]C.AppliedRangeIndex, len(rangeIndex))
		for idx, rangeIdx := range rangeIndex {
			appliedRangeIndex[idx] = C.AppliedRangeIndex{
				range_id:      C.uint64_t(rangeIdx.RangeId),
				applied_index: C.uint64_t(rangeIdx.ApplyIndex),
			}
		}

		status := C.TSOpen(&r.tdb, goToTSSlice([]byte(r.cfg.Dir)),
			C.TSOptions{
				wal_level:         C.uint8_t(walLevel),
				wal_buffer_size:   C.uint16_t(uint16(walBufferSize)),
				wal_file_size:     C.uint16_t(uint16(walFileSize)),
				wal_file_in_group: C.uint16_t(uint16(walFilesInGroup)),
				extra_options:     goToTSSlice(r.cfg.ExtraOptions),
				thread_pool_size:  C.uint16_t(uint16(r.cfg.ThreadPoolSize)),
				task_queue_size:   C.uint16_t(uint16(r.cfg.TaskQueueSize)),
				buffer_pool_size:  C.uint32_t(uint32(r.cfg.BufferPoolSize)),
				lg_opts:           optLog,
				is_single_node:    C.bool(r.cfg.IsSingleNode),
			},
			&appliedRangeIndex[0],
			C.uint64_t(len(appliedRangeIndex)))
		if err := statusToError(status); err != nil {
			return errors.Wrap(err, "could not open tsengine instance")
		}
	}

	r.manageWAL()
	r.opened = true
	close(r.openCh)
	return nil
}

// CreateTsTable create ts table
func (r *TsEngine) CreateTsTable(tableID uint64, meta []byte, rangeGroups []api.RangeGroup) error {
	r.checkOrWaitForOpen()
	nRange := len(rangeGroups)
	cRanges := make([]C.RangeGroup, nRange)
	for i := 0; i < nRange; i++ {
		cRanges[i].range_group_id = C.uint64_t(rangeGroups[i].RangeGroupID)
		cRanges[i].typ = C.int8_t(rangeGroups[i].Type)
	}
	cRangeGroups := C.RangeGroups{
		ranges: (*C.RangeGroup)(unsafe.Pointer(&cRanges[0])),
		len:    C.int32_t(len(cRanges)),
	}
	status := C.TSCreateTsTable(r.tdb, C.TSTableID(tableID), goToTSSlice(meta), cRangeGroups)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not CreateTsTable")
	}
	return nil
}

// GetMetaData get meta from source of the snapshot
func (r *TsEngine) GetMetaData(tableID uint64, rangeGroup api.RangeGroup) ([]byte, error) {
	r.checkOrWaitForOpen()
	cRangeGroup := C.RangeGroup{
		range_group_id: C.uint64_t(rangeGroup.RangeGroupID),
	}
	var tableMeta C.TSSlice
	status := C.TSGetMetaData(r.tdb, C.TSTableID(tableID), cRangeGroup, &tableMeta)
	if err := statusToError(status); err != nil {
		return nil, errors.Wrap(err, "could not CreateTsTable")
	}
	defer C.free(unsafe.Pointer(tableMeta.data))
	meta := cSliceToGoBytes(tableMeta)
	return meta, nil
}

// TSIsTsTableExist checks if ts table exists.
func (r *TsEngine) TSIsTsTableExist(tableID uint64) (bool, error) {
	r.checkOrWaitForOpen()
	var isExist C.bool
	status := C.TSIsTsTableExist(r.tdb, C.TSTableID(tableID), &isExist)
	if err := statusToError(status); err != nil {
		return false, errors.Wrap(err, "get error")
	}
	return bool(isExist), nil
}

// DropTsTable drop ts table.
func (r *TsEngine) DropTsTable(tableID uint64) error {
	r.checkOrWaitForOpen()
	status := C.TSDropTsTable(r.tdb, C.TSTableID(tableID))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not DropTsTable")
	}
	return nil
}

// DropLeftTsTableGarbage drop left ts table metadata garbage.
func (r *TsEngine) DropLeftTsTableGarbage() error {
	r.checkOrWaitForOpen()
	status := C.TSDropResidualTsTable(r.tdb)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not drop residual ts table")
	}
	return nil
}

// AddTSColumn adds column for ts table.
func (r *TsEngine) AddTSColumn(
	tableID uint64, currentTSVersion, newTSVersion uint32, transactionID []byte, colMeta []byte,
) error {
	r.checkOrWaitForOpen()
	status := C.TSAddColumn(
		r.tdb, C.TSTableID(tableID), (*C.char)(unsafe.Pointer(&transactionID[0])), goToTSSlice(colMeta), C.uint32_t(currentTSVersion), C.uint32_t(newTSVersion))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not AddTsColumn")
	}
	return nil
}

// DropTSColumn drop column for ts table.
func (r *TsEngine) DropTSColumn(
	tableID uint64, currentTSVersion, newTSVersion uint32, transactionID []byte, colMeta []byte,
) error {
	r.checkOrWaitForOpen()
	status := C.TSDropColumn(
		r.tdb, C.TSTableID(tableID), (*C.char)(unsafe.Pointer(&transactionID[0])), goToTSSlice(colMeta), C.uint32_t(currentTSVersion), C.uint32_t(newTSVersion))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not DropTsColumn")
	}
	return nil
}

// CreateNormalTagIndex create index on normal tag of ts table
func (r *TsEngine) CreateNormalTagIndex(
	tableID uint64,
	indexID uint64,
	curVersion, newVersion uint32,
	transactionID []byte,
	indexColumns []uint32,
) error {
	numColumn := len(indexColumns)
	cIndexs := make([]C.uint, numColumn)
	for i := 0; i < numColumn; i++ {
		cIndexs[i] = C.uint(indexColumns[i])
	}
	cIndexColumns := C.IndexColumns{
		index_column: (*C.uint32_t)(unsafe.Pointer(&cIndexs[0])),
		len:          C.int32_t(len(cIndexs)),
	}
	status := C.TSCreateNormalTagIndex(r.tdb, C.TSTableID(tableID), C.uint64_t(indexID), (*C.char)(unsafe.Pointer(&transactionID[0])), C.uint32_t(curVersion), C.uint32_t(newVersion), cIndexColumns)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not CreateNormalTagIndex")
	}
	return nil
}

// DropNormalTagIndex drop index on normal tag of ts table
func (r *TsEngine) DropNormalTagIndex(
	tableID uint64, indexID uint64, curVersion, newVersion uint32, transactionID []byte,
) error {
	status := C.TSDropNormalTagIndex(r.tdb, C.TSTableID(tableID), C.uint64_t(indexID), (*C.char)(unsafe.Pointer(&transactionID[0])), C.uint32_t(curVersion), C.uint32_t(newVersion))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not DropNormalTagIndex")
	}
	return nil
}

// AlterPartitionInterval alter partition interval for ts table.
func (r *TsEngine) AlterPartitionInterval(tableID uint64, partitionInterval uint64) error {
	r.checkOrWaitForOpen()
	status := C.TSAlterPartitionInterval(r.tdb, C.TSTableID(tableID), C.uint64_t(partitionInterval))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not AlterPartitionInterval")
	}
	return nil
}

// AlterTSColumnType alter column/tag type of ts table.
func (r *TsEngine) AlterTSColumnType(
	tableID uint64,
	currentTSVersion, newTSVersion uint32,
	transactionID []byte,
	colMeta []byte,
	originColMeta []byte,
) error {
	r.checkOrWaitForOpen()
	status := C.TSAlterColumnType(
		r.tdb,
		C.TSTableID(tableID),
		(*C.char)(unsafe.Pointer(&transactionID[0])),
		goToTSSlice(colMeta),
		goToTSSlice(originColMeta),
		C.uint32_t(currentTSVersion),
		C.uint32_t(newTSVersion),
	)
	if err := statusToError(status); err != nil {
		return err
	}
	return nil
}

// PutEntity write in, update tag data and write in ts data
func (r *TsEngine) PutEntity(
	rangeGroupID uint64, tableID uint64, payload [][]byte, tsTxnID uint64,
) error {
	if len(payload) == 0 {
		return errors.New("payload is nul")
	}

	r.checkOrWaitForOpen()
	cTsSlice := make([]C.TSSlice, len(payload))
	for i, p := range payload {
		if len(p) == 0 {
			cTsSlice[i].data = nil
			cTsSlice[i].len = 0
		} else {
			dataPtr := C.CBytes(p)
			defer C.free(dataPtr)

			cTsSlice[i].data = (*C.char)(dataPtr)
			cTsSlice[i].len = C.size_t(len(p))
		}
	}
	// mock
	cRangeGroup := C.RangeGroup{
		range_group_id: C.uint64_t(rangeGroupID),
		typ:            C.int8_t(0),
	}
	status := C.TSPutEntity(r.tdb, C.TSTableID(tableID), &cTsSlice[0], (C.size_t)(len(cTsSlice)), cRangeGroup, C.uint64_t(tsTxnID))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not PutEntity")
	}
	return nil
}

// PutData write in tag data and write in ts data
func (r *TsEngine) PutData(
	tableID uint64, payload [][]byte, tsTxnID uint64, writeWAL bool,
) (DedupResult, EntitiesAffect, error) {
	if len(payload) == 0 {
		return DedupResult{}, EntitiesAffect{}, errors.New("payload is nul")
	}

	r.checkOrWaitForOpen()
	cTsSlice := make([]C.TSSlice, len(payload))
	for i, p := range payload {
		if len(p) == 0 {
			cTsSlice[i].data = nil
			cTsSlice[i].len = 0
		} else {
			dataPtr := C.CBytes(p)
			defer C.free(dataPtr)

			cTsSlice[i].data = (*C.char)(dataPtr)
			cTsSlice[i].len = C.size_t(len(p))
		}
	}
	// mock
	cRangeGroup := C.RangeGroup{
		range_group_id: C.uint64_t(101),
		typ:            C.int8_t(0),
	}

	var dedupResult C.DedupResult
	var affect EntitiesAffect
	var entitiesAffected C.uint16_t
	var unorderedAffected C.uint32_t
	status := C.TSPutData(r.tdb, C.TSTableID(tableID), &cTsSlice[0], (C.size_t)(len(cTsSlice)), cRangeGroup, C.uint64_t(tsTxnID),
		&entitiesAffected, &unorderedAffected, &dedupResult, C.bool(writeWAL))
	if err := statusToError(status); err != nil {
		return DedupResult{}, EntitiesAffect{}, errors.Wrap(err, "could not PutData")
	}

	res := DedupResult{
		DedupRule:     int(dedupResult.dedup_rule),
		DedupRows:     int(dedupResult.dedup_rows),
		DiscardBitmap: cSliceToGoBytes(dedupResult.discard_bitmap),
	}
	defer C.free(unsafe.Pointer(dedupResult.discard_bitmap.data))
	affect.EntityCount = uint16(entitiesAffected)
	affect.UnorderedCount = uint32(unorderedAffected)
	return res, affect, nil
}

// PutRowData 行存tags值和时序数据写入
func (r *TsEngine) PutRowData(
	tableID uint64, headerPrefix []byte, payload [][]byte, size int32, tsTxnID uint64, writeWAL bool,
) (DedupResult, EntitiesAffect, error) {
	if len(payload) == 0 {
		return DedupResult{}, EntitiesAffect{}, errors.New("payload is nul")
	}

	r.checkOrWaitForOpen()
	sizeLimit := int32(TsPayloadSizeLimit.Get(&r.cfg.Settings.SV))
	var cTsSlice C.TSSlice
	// The structure of HeaderPrefix: | Header | primary_tag_len | primary_tag | tag_ten | tags | data_len |
	// Header: | txn(16) | group_id(2) | payload_version(4) | database_id(4) | table_id(8) | ts_version(4) | row_num(4) | flags(1) |
	const rowNumOffset = 38 // offset of row_num, pay attention to any change of the structure of HeaderPrefix
	const dataLen = 4       // length of data_len in HeaderPrefix. The location is at the end of HeaderPrefix

	headerLen := len(headerPrefix)
	cTsSlice.len = C.size_t(int(size) + headerLen + dataLen)
	cTsSlice.data = (*C.char)(C.malloc(cTsSlice.len))
	if cTsSlice.data == nil {
		return DedupResult{}, EntitiesAffect{}, errors.New("failed malloc")
	}
	defer C.free(unsafe.Pointer(cTsSlice.data))

	C.memcpy(unsafe.Pointer(cTsSlice.data), unsafe.Pointer(&headerPrefix[0]), C.size_t(headerLen))
	dataPtr := uintptr(unsafe.Pointer(cTsSlice.data)) + uintptr(headerLen) // pointer to the data_len

	// mock
	cRangeGroup := C.RangeGroup{
		range_group_id: C.uint64_t(1),
		typ:            C.int8_t(0),
	}
	payloadPtr := dataPtr + uintptr(dataLen)
	payloadSize := 0
	partRowCnt := 0
	totalRowCnt := len(payload)
	var res DedupResult
	var affect EntitiesAffect
	for i := 0; i < totalRowCnt; i++ {
		p := payload[i]
		if len(p) == 0 {
			continue
		}
		partLen := len(p)
		// need to check whether the payload size exceeds limit, so calculate it before add the row to payload.
		payloadSize += partLen
		if payloadSize > int(sizeLimit) {
			// fill data_len
			*(*int32)(unsafe.Pointer(dataPtr)) = int32(payloadSize - partLen)
			// fill row_num
			*(*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(cTsSlice.data)) + rowNumOffset)) = int32(partRowCnt)
			var dedupResult C.DedupResult
			var entitiesAffected C.uint16_t
			var unorderedAffected C.uint32_t
			status := C.TSPutDataByRowType(r.tdb, C.TSTableID(tableID), &cTsSlice, (C.size_t)(1), cRangeGroup, C.uint64_t(tsTxnID),
				&entitiesAffected, &unorderedAffected, &dedupResult, C.bool(writeWAL))
			if err := statusToError(status); err != nil {
				return DedupResult{}, EntitiesAffect{}, errors.Wrap(err, "could not PutData")
			}
			res.DedupRows += int(dedupResult.dedup_rows)
			affect.EntityCount += uint16(entitiesAffected)
			affect.UnorderedCount += uint32(unorderedAffected)
			C.free(unsafe.Pointer(dedupResult.discard_bitmap.data))

			payloadSize = partLen
			payloadPtr = dataPtr + uintptr(dataLen)
			partRowCnt = 0
		}
		partRowCnt++
		C.memcpy(unsafe.Pointer(payloadPtr), unsafe.Pointer(&p[0]), C.size_t(partLen))
		payloadPtr += uintptr(partLen)
	}

	// fill data_len
	*(*int32)(unsafe.Pointer(dataPtr)) = int32(payloadSize)
	// fill row_num
	*(*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(cTsSlice.data)) + rowNumOffset)) = int32(partRowCnt)
	var dedupResult C.DedupResult
	var entitiesAffected C.uint16_t
	var unorderedAffected C.uint32_t
	status := C.TSPutDataByRowType(r.tdb, C.TSTableID(tableID), &cTsSlice, (C.size_t)(1), cRangeGroup, C.uint64_t(tsTxnID),
		&entitiesAffected, &unorderedAffected, &dedupResult, C.bool(writeWAL))
	if err := statusToError(status); err != nil {
		return DedupResult{}, EntitiesAffect{}, errors.Wrap(err, "could not PutData")
	}

	res.DedupRows += int(dedupResult.dedup_rows)
	res.DedupRule = int(dedupResult.dedup_rule)
	affect.EntityCount += uint16(entitiesAffected)
	affect.UnorderedCount += uint32(unorderedAffected)
	// the DiscardBitmap is not complete if the payload is truncated due to the size limit.
	res.DiscardBitmap = cSliceToGoBytes(dedupResult.discard_bitmap)
	C.free(unsafe.Pointer(dedupResult.discard_bitmap.data))

	return res, affect, nil
}

// GetDataVolume gets DataVolume for ts range.
// should not call this for relational ranges.
func (r *TsEngine) GetDataVolume(
	tableID uint64, startHashPoint, endHashPoint uint64, startTimestamp, endTimestamp int64,
) (uint64, error) {
	r.checkOrWaitForOpen()
	var volume C.uint64_t
	status := C.TSGetDataVolume(
		r.tdb,
		C.TSTableID(tableID),
		C.uint64_t(startHashPoint),
		C.uint64_t(endHashPoint),
		C.KwTsSpan{
			begin: C.int64_t(startTimestamp),
			end:   C.int64_t(endTimestamp),
		},
		&volume,
	)
	if err := statusToError(status); err != nil {
		log.Errorf(context.TODO(), "GetDataVolume failed. err is :%+v. tableID: %d startHashPoint: %d, endHashPoint:%d startTimeStamp: %d, endTimeStamp: %d",
			err, tableID, startHashPoint, endHashPoint, startTimestamp, endTimestamp)
		return 0, errors.Wrap(err, "get data Volume failed")
	}
	return uint64(volume), nil
}

// GetDataVolumeHalfTS returns haslTS
func (r *TsEngine) GetDataVolumeHalfTS(
	tableID uint64, startHashPoint, endHashPoint uint64, startTimestamp, endTimestamp int64,
) (int64, error) {
	r.checkOrWaitForOpen()
	var halfTimestamp C.int64_t
	status := C.TSGetDataVolumeHalfTS(
		r.tdb,
		C.TSTableID(tableID),
		C.uint64_t(startHashPoint),
		C.uint64_t(endHashPoint),
		C.KwTsSpan{
			begin: C.int64_t(startTimestamp),
			end:   C.int64_t(endTimestamp),
		},
		&halfTimestamp,
	)
	if err := statusToError(status); err != nil {
		return 0, errors.Wrap(err, "get half timestamp data Volume failed")
	}

	return int64(halfTimestamp), nil
}

// GetAvgTableRowSize gets AvgTableRowSize
func (r *TsEngine) GetAvgTableRowSize(tableID uint64) (uint64, error) {
	r.checkOrWaitForOpen()
	var avgRowSize C.uint64_t
	status := C.TSGetAvgTableRowSize(
		r.tdb,
		C.TSTableID(tableID),
		&avgRowSize,
	)
	if err := statusToError(status); err != nil {
		return 0, errors.Wrap(err, "get avg table row size failed")
	}
	return uint64(avgRowSize), nil
}

// TsFetcher collect information in explain analyse
type TsFetcher struct {
	Collected bool
	CFetchers []C.TsFetcher
	Size      int
	Mu        *syncutil.Mutex
}

// TsFetcherStats collect information in explain analyse
type TsFetcherStats struct {
	ProcessorID      int32
	ProcessorName    int8
	RowNum           int64
	StallTime        int64 // time of execute
	BytesRead        int64 // byte of rows
	MaxAllocatedMem  int64 // maximum number of memory
	MaxAllocatedDisk int64 // Maximum number of disk
	OutputRowNum     int64 // row of aggregation
	// BuildTime only be used for hash tag scan op for multiple model processing
	// when the switch is on and the server starts with single node mode.
	BuildTime int64 // hash tag build time
}

// sendDmlMsgToTs call the tsengine dml interface to issue a request and return the result
func (r *TsEngine) tsExecute(
	ctx *context.Context, tp C.EnMqType, tsQueryInfo TsQueryInfo,
) (tsRespInfo TsQueryInfo, err error) {
	if len(tsQueryInfo.Buf) == 0 {
		return tsRespInfo, errors.New("query buf is nul")
	}
	var queryInfo C.QueryInfo
	bufC := C.CBytes(tsQueryInfo.Buf)
	defer C.free(unsafe.Pointer(bufC))
	queryInfo.value = bufC
	queryInfo.len = C.uint(len(tsQueryInfo.Buf))
	queryInfo.tp = tp
	queryInfo.id = C.int(tsQueryInfo.ID)
	queryInfo.handle = tsQueryInfo.Handle
	queryInfo.unique_id = C.int(tsQueryInfo.UniqueID)
	queryInfo.time_zone = C.int(tsQueryInfo.TimeZone)
	queryInfo.relation_ctx = C.uint64_t(uintptr(unsafe.Pointer(ctx)))
	// process push data for batch lookup join for multiple model processing
	// only store the data chunk pointer into tsQueryInfo and push it down to tse
	// when the switch is on and the server starts with single node mode.
	dataChunk := tsQueryInfo.PushData
	if dataChunk != nil {
		pushDatabufC := C.CBytes(dataChunk.Data)
		queryInfo.relRowCount = C.int(dataChunk.RowCount)
		defer C.free(unsafe.Pointer(pushDatabufC))
		queryInfo.relBatchData = pushDatabufC
		dataChunk = nil // clean datachunk
	}

	// init fetcher of analyse
	var vecFetcher C.VecTsFetcher
	vecFetcher.collected = C.bool(false)
	if tsQueryInfo.Fetcher.Collected {
		vecFetcher.collected = C.bool(true)
		vecFetcher.size = C.int8_t(tsQueryInfo.Fetcher.Size)
		// vecFetcher.TsFetchers = &tsQueryInfo.Fetcher.CFetchers[0]
		vecFetcher.goMutux = C.uint64_t(uintptr(unsafe.Pointer(&tsQueryInfo.Fetcher)))
	} else {
		tsFetchers := make([]C.TsFetcher, 1)
		tsFetchers[0].processor_id = C.int32_t(-1)
		tsQueryInfo.Fetcher.CFetchers = tsFetchers
	}
	var retInfo C.QueryInfo
	retInfo.value = nil
	C.TSExecQuery(r.tdb, &queryInfo, &retInfo, &tsQueryInfo.Fetcher.CFetchers[0], unsafe.Pointer(&vecFetcher))
	fet := tsQueryInfo.Fetcher
	tsRespInfo.Fetcher = fet
	tsRespInfo.ID = int(retInfo.id)
	tsRespInfo.UniqueID = int(retInfo.unique_id)
	tsRespInfo.Handle = unsafe.Pointer(retInfo.handle)
	tsRespInfo.Code = int(retInfo.code)
	tsRespInfo.RowNum = int(retInfo.row_num)
	if unsafe.Pointer(retInfo.value) != nil {
		tsRespInfo.Buf = C.GoBytes(unsafe.Pointer(retInfo.value), C.int(retInfo.len))
		C.TSFree(unsafe.Pointer(retInfo.value))
	}
	if tsRespInfo.Code > 1 {
		if unsafe.Pointer(retInfo.value) != nil {
			strCode := make([]byte, 5)
			code := tsRespInfo.Code
			for i := 0; i < 5; i++ {
				strCode[i] = byte(((code) & 0x3F) + '0')
				code = code >> 6
			}
			err = pgerror.Newf(string(strCode), string(tsRespInfo.Buf))
		} else {
			err = fmt.Errorf("Error Code: %s", strconv.Itoa(tsRespInfo.Code))
		}
	} else if retInfo.ret < 1 {
		err = fmt.Errorf("Unknown error")
	}

	return tsRespInfo, err
}

// tsVectorizedExecute Vectorized tsengine.go:137Execute
func (r *TsEngine) tsVectorizedExecute(
	ctx *context.Context, tp C.EnMqType, tsQueryInfo TsQueryInfo, rev *TsDataChunkToGo,
) (tsRespInfo TsQueryInfo, err error) {
	if len(tsQueryInfo.Buf) == 0 {
		return tsRespInfo, errors.New("query buf is nul")
	}
	var queryInfo C.QueryInfo
	bufC := C.CBytes(tsQueryInfo.Buf)
	defer C.free(unsafe.Pointer(bufC))
	queryInfo.value = bufC
	queryInfo.len = C.uint(len(tsQueryInfo.Buf))
	queryInfo.tp = tp
	queryInfo.id = C.int(tsQueryInfo.ID)
	queryInfo.handle = tsQueryInfo.Handle
	queryInfo.unique_id = C.int(tsQueryInfo.UniqueID)
	queryInfo.time_zone = C.int(tsQueryInfo.TimeZone)
	queryInfo.relation_ctx = C.uint64_t(uintptr(unsafe.Pointer(ctx)))
	// process push data for batch lookup join for multiple model processing
	// only store the data chunk pointer into tsQueryInfo and push it down to tse
	// when the switch is on and the server starts with single node mode.
	dataChunk := tsQueryInfo.PushData
	if dataChunk != nil {
		pushDatabufC := C.CBytes(dataChunk.Data)
		queryInfo.relRowCount = C.int(dataChunk.RowCount)
		defer C.free(unsafe.Pointer(pushDatabufC))
		queryInfo.relBatchData = pushDatabufC
		dataChunk = nil // clean datachunk
	}

	// init fetcher of analyse
	var vecFetcher C.VecTsFetcher
	vecFetcher.collected = C.bool(false)
	if tsQueryInfo.Fetcher.Collected {
		vecFetcher.collected = C.bool(true)
		vecFetcher.size = C.int8_t(tsQueryInfo.Fetcher.Size)
		// vecFetcher.TsFetchers = &tsQueryInfo.Fetcher.CFetchers[0]
		vecFetcher.goMutux = C.uint64_t(uintptr(unsafe.Pointer(&tsQueryInfo.Fetcher)))
	} else {
		tsFetchers := make([]C.TsFetcher, 1)
		tsFetchers[0].processor_id = C.int32_t(-1)
		tsQueryInfo.Fetcher.CFetchers = tsFetchers
	}
	var retInfo C.QueryInfo
	retInfo.value = nil
	C.TSExecQuery(r.tdb, &queryInfo, &retInfo, &tsQueryInfo.Fetcher.CFetchers[0], unsafe.Pointer(&vecFetcher))
	fet := tsQueryInfo.Fetcher
	tsRespInfo.Fetcher = fet
	tsRespInfo.ID = int(retInfo.id)
	tsRespInfo.UniqueID = int(retInfo.unique_id)
	tsRespInfo.Handle = unsafe.Pointer(retInfo.handle)
	tsRespInfo.Code = int(retInfo.code)
	tsRespInfo.RowNum = int(retInfo.row_num)

	if tsRespInfo.RowNum > 0 {
		capacity := rev.Capacity
		rev.Begin = 0
		rev.ColumnNum = uint32(retInfo.vectorize_data.column_num_)
		rev.BitmapSize = uint32(retInfo.vectorize_data.bitmap_size_)
		rev.RowSize = uint32(retInfo.vectorize_data.row_size_)
		rev.DataCount = uint32(retInfo.vectorize_data.data_count_)
		rev.Capacity = uint32(retInfo.vectorize_data.capacity_)
		rev.IsDataOwner = bool(retInfo.vectorize_data.is_data_owner_)
		if nil == rev.Column {
			rev.Column = make([]TsColumnInfoToGo, rev.ColumnNum)
			for i := 0; i < int(rev.ColumnNum); i++ {
				cData := (*C.TsColumnInfo)(unsafe.Pointer(uintptr(unsafe.Pointer(retInfo.vectorize_data.column_)) + uintptr(i)*unsafe.Sizeof(*(retInfo.vectorize_data.column_))))

				rev.Column[i].FixedLen = uint32(cData.fixed_len_)
				rev.Column[i].ReturnType = types.Family(cData.return_type_)
				rev.Column[i].StorageLen = uint32(cData.storage_len_)
				rev.Column[i].StorageType = sqlbase.DataType(cData.storage_type_)
			}
		}

		if nil == rev.Data || rev.Capacity > capacity {
			if nil == rev.Data {
				rev.Data = make([]coldata.Vec, rev.ColumnNum)
			}
			for i := range rev.NeedType {
				rev.Data[i] = coldata.NewMemColumn(rev.NeedType[i], int(rev.Capacity))
			}
		}

		for i := range rev.NeedType {
			if err != nil {
				break
			}
			coli := rev.Column[i]
			colData := (*C.TsColumnData)(unsafe.Pointer(uintptr(unsafe.Pointer(retInfo.vectorize_data.column_data_)) + uintptr(i)*unsafe.Sizeof(*(retInfo.vectorize_data.column_data_))))
			length := C.ulong(rev.DataCount) * C.ulong(coli.FixedLen)
			nullBit := C.GoBytes(unsafe.Pointer(colData.bitmap_ptr_), C.int(rev.BitmapSize))
			for k := range nullBit {
				nullBit[k] ^= 0xff
			}
			rev.Data[i].Nulls().CopyNullsBitmap(nullBit, int(rev.BitmapSize))
			if coli.StorageType == sqlbase.DataType_NULLVAL {
				continue
			}

			switch coli.ReturnType {
			case types.BoolFamily:
				C.memcpy(unsafe.Pointer(&(rev.Data[i].Bool()[0])), colData.data_ptr_, length)
			case types.IntFamily:
				switch coli.FixedLen {
				case 1, 2:
					if rev.Data[i].Type() == coltypes.Int32 || rev.Data[i].Type() == coltypes.Int64 {
						ioBuf := bytes.NewReader(C.GoBytes(unsafe.Pointer(colData.data_ptr_), C.int(length)))
						for j := 0; uint32(j) < rev.DataCount; j++ {
							var value int16
							err = binary.Read(ioBuf, binary.LittleEndian, &value)
							if err != nil {
								fmt.Println("Error reading bytes:", err)
								break
							}
							if rev.Data[i].Type() == coltypes.Int32 {
								rev.Data[i].Int32()[j] = int32(value)
							} else {
								rev.Data[i].Int64()[j] = int64(value)
							}
						}
					} else {
						C.memcpy(unsafe.Pointer(&(rev.Data[i].Int16()[0])), colData.data_ptr_, length)
					}
				case 4:
					if rev.Data[i].Type() == coltypes.Int16 || rev.Data[i].Type() == coltypes.Int64 {
						ioBuf := bytes.NewReader(C.GoBytes(unsafe.Pointer(colData.data_ptr_), C.int(length)))
						for j := 0; uint32(j) < rev.DataCount; j++ {
							var value int32
							err = binary.Read(ioBuf, binary.LittleEndian, &value)
							if err != nil {
								fmt.Println("Error reading bytes:", err)
								break
							}
							if rev.Data[i].Type() == coltypes.Int16 {
								rev.Data[i].Int16()[j] = int16(value)
							} else {
								rev.Data[i].Int64()[j] = int64(value)
							}
						}
					} else {
						C.memcpy(unsafe.Pointer(&(rev.Data[i].Int32()[0])), colData.data_ptr_, length)
					}
				default:
					if rev.Data[i].Type() == coltypes.Int16 || rev.Data[i].Type() == coltypes.Int32 {
						ioBuf := bytes.NewReader(C.GoBytes(unsafe.Pointer(colData.data_ptr_), C.int(length)))
						for j := 0; uint32(j) < rev.DataCount; j++ {
							var value int64
							err = binary.Read(ioBuf, binary.LittleEndian, &value)
							if err != nil {
								fmt.Println("Error reading bytes:", err)
								break
							}
							if rev.Data[i].Type() == coltypes.Int16 {
								rev.Data[i].Int16()[j] = int16(value)
							} else {
								rev.Data[i].Int32()[j] = int32(value)
							}
						}
					} else {
						C.memcpy(unsafe.Pointer(&(rev.Data[i].Int64()[0])), colData.data_ptr_, length)
					}
				}
			case types.FloatFamily:
				switch coli.FixedLen {
				case 4:
					ioBuf := bytes.NewReader(C.GoBytes(unsafe.Pointer(colData.data_ptr_), C.int(length)))
					for j := 0; uint32(j) < rev.DataCount; j++ {
						var value float32
						err = binary.Read(ioBuf, binary.LittleEndian, &value)
						if err != nil {
							fmt.Println("Error reading bytes:", err)
							break
						}
						rev.Data[i].Float64()[j] = float64(value)
					}
				case 8:
					C.memcpy(unsafe.Pointer(&(rev.Data[i].Float64()[0])), colData.data_ptr_, length)
				}
			case types.DecimalFamily:
				switch coli.StorageType {
				case sqlbase.DataType_BIGINT:
					ioBuf := bytes.NewReader(C.GoBytes(unsafe.Pointer(colData.data_ptr_), C.int(length)))
					var value int64
					for j := 0; uint32(j) < rev.DataCount; j++ {
						err = binary.Read(ioBuf, binary.LittleEndian, &value)
						if err != nil {
							fmt.Println("Error reading bytes:", err)
							break
						}
						rev.Data[i].Decimal()[j].SetInt64(value)
					}
				case sqlbase.DataType_DOUBLE:
					ioBuf := bytes.NewReader(C.GoBytes(unsafe.Pointer(colData.data_ptr_), C.int(length)))
					var value float64
					for j := 0; uint32(j) < rev.DataCount; j++ {
						err = binary.Read(ioBuf, binary.LittleEndian, &value)
						if err != nil {
							fmt.Println("Error reading bytes:", err)
							break
						}
						newDecimal, _, err := apd.NewFromString(strconv.FormatFloat(value, 'f', -1, 64))
						if err != nil {
							fmt.Println("Error reading bytes:", err)
							break
						}
						rev.Data[i].Decimal()[j].Set(newDecimal)
					}
				case sqlbase.DataType_DECIMAL:
					ioBuf := bytes.NewReader(C.GoBytes(unsafe.Pointer(colData.data_ptr_), C.int(length)))
					val := make([]byte, uint32(coli.FixedLen))
					for j := 0; uint32(j) < rev.DataCount; j++ {
						err = binary.Read(ioBuf, binary.LittleEndian, &val)
						if err != nil {
							fmt.Println("Error reading bytes:", err)
							break
						}
						if val[0] != 1 {
							var data int64
							buf := bytes.NewReader(val[1:])
							err := binary.Read(buf, binary.LittleEndian, &data)
							if err != nil {
								fmt.Println("Error reading int64:", err)
								break
							}
							rev.Data[i].Decimal()[j].SetInt64(data)
						} else {
							bits := binary.LittleEndian.Uint64(val[1:])
							data := math.Float64frombits(bits)
							newDecimal, _, err := apd.NewFromString(strconv.FormatFloat(data, 'f', -1, 64))
							if err != nil {
								fmt.Println("Error reading bytes:", err)
								break
							}
							rev.Data[i].Decimal()[j].Set(newDecimal)
						}
					}
				}
			case types.TimestampFamily, types.TimestampTZFamily:
				var interval int64 = 1000
				var sec int64
				var nsec int64
				switch coli.StorageType {
				case sqlbase.DataType_TIMESTAMP_MICRO, sqlbase.DataType_TIMESTAMPTZ_MICRO:
					interval = 1000000
				case sqlbase.DataType_TIMESTAMP_NANO, sqlbase.DataType_TIMESTAMPTZ_NANO:
					interval = 1000000000
				}
				src := coldata.NewMemColumn(coltypes.Int64, int(rev.DataCount))
				C.memcpy(unsafe.Pointer(&(src.Int64()[0])), colData.data_ptr_, length)
				for j := 0; uint32(j) < rev.DataCount; j++ {
					val := src.Int64()[j]
					if val < 0 && val%interval != 0 {
						sec = val/interval - 1
						nsec = ((val % interval) + interval) * (1000000000 / interval)
					} else {
						sec = val / interval
						nsec = val % interval * (1000000000 / interval)
					}
					t := timeutil.Unix(sec, nsec)
					rev.Data[i].Timestamp()[j] = t
				}
			case types.IntervalFamily:
				ioBuf := bytes.NewReader(C.GoBytes(unsafe.Pointer(colData.data_ptr_), C.int(length)))
				switch coli.StorageType {
				case sqlbase.DataType_TIMESTAMP_MICRO, sqlbase.DataType_TIMESTAMPTZ_MICRO:
					for j := 0; uint32(j) < rev.DataCount; j++ {
						var value int64
						err := binary.Read(ioBuf, binary.LittleEndian, &value)
						if err != nil {
							fmt.Println("Error reading int64:", err)
							break
						}
						extraDays := value / (Day / 1000)
						days := extraDays
						value -= extraDays * (Day / 1000)
						var nanos = value * 1000
						var d duration.Duration = duration.DecodeDuration(0, days, nanos)
						rev.Data[i].Interval()[j] = d
					}
				case sqlbase.DataType_TIMESTAMP_NANO, sqlbase.DataType_TIMESTAMPTZ_NANO:
					for j := 0; uint32(j) < rev.DataCount; j++ {
						var value int64
						err := binary.Read(ioBuf, binary.LittleEndian, &value)
						if err != nil {
							fmt.Println("Error reading int64:", err)
							break
						}
						extraDays := value / (Day / 1)
						days := extraDays
						value -= extraDays * (Day / 1)
						var nanos = value * 1
						var d duration.Duration = duration.DecodeDuration(0, days, nanos)
						rev.Data[i].Interval()[j] = d
					}
				default:
					for j := 0; uint32(j) < rev.DataCount; j++ {
						var value int64
						err := binary.Read(ioBuf, binary.LittleEndian, &value)
						if err != nil {
							fmt.Println("Error reading int64:", err)
							break
						}
						extraDays := value / (Day / 1000000)
						days := extraDays
						value -= extraDays * (Day / 1000000)
						var nanos = value * 1000000
						var d duration.Duration = duration.DecodeDuration(0, days, nanos)
						rev.Data[i].Interval()[j] = d
					}
				}
			case types.BytesFamily, types.StringFamily:
				rev.Data[i].Bytes().Reset()
				colOffset := (*[1 << 30]int32)(unsafe.Pointer(colData.offset_))[: rev.DataCount+1 : rev.DataCount+1]
				offset := make([]int32, rev.DataCount+1)
				copy(offset[:], colOffset[:])
				buf := C.GoBytes(unsafe.Pointer(colData.data_ptr_), C.int(offset[rev.DataCount]))
				coldata.BytesFromArrowSerializationFormat(rev.Data[i].Bytes(), buf, offset)
				C.TSFree(unsafe.Pointer(colData.data_ptr_))
				C.TSFree(unsafe.Pointer(colData.offset_))
			default:
				err = fmt.Errorf("Unknown column return type")
				break
			}
		}

		if rev.IsDataOwner {
			C.TsMemPoolFree(unsafe.Pointer(retInfo.vectorize_data.data_))
		}
		C.TSFree(unsafe.Pointer(retInfo.vectorize_data.column_))
		C.TSFree(unsafe.Pointer(retInfo.vectorize_data.column_data_))
	}
	if tsRespInfo.Code > 1 {
		if unsafe.Pointer(retInfo.value) != nil {
			strCode := make([]byte, 5)
			code := tsRespInfo.Code
			for i := 0; i < 5; i++ {
				strCode[i] = byte(((code) & 0x3F) + '0')
				code = code >> 6
			}
			tsRespInfo.Buf = C.GoBytes(unsafe.Pointer(retInfo.value), C.int(retInfo.len))
			err = pgerror.Newf(string(strCode), string(tsRespInfo.Buf))
		} else {
			err = fmt.Errorf("Error Code: %s", strconv.Itoa(tsRespInfo.Code))
		}
	} else if retInfo.ret < 1 {
		err = fmt.Errorf("Unknown error")
	}

	return tsRespInfo, err
}

func freeTSSlice(cTsSlice []C.TSSlice) {
	for _, slice := range cTsSlice {
		if slice.data != nil {
			C.free(unsafe.Pointer(slice.data))
		}
	}
}

// DeleteEntities delete entity, containing tag data and ts data
func (r *TsEngine) DeleteEntities(
	tableID uint64, rangeGroupID uint64, primaryTags [][]byte, isDrop bool, tsTxnID uint64,
) (uint64, error) {
	if len(primaryTags) == 0 {
		return 0, errors.New("primaryTags is null")
	}

	r.checkOrWaitForOpen()
	cTsSlice := make([]C.TSSlice, len(primaryTags))
	defer freeTSSlice(cTsSlice)
	for i, p := range primaryTags {
		if len(p) == 0 {
			cTsSlice[i].data = nil
			cTsSlice[i].len = 0
		} else {
			dataPtr := C.CBytes(p)
			cTsSlice[i].data = (*C.char)(dataPtr)
			cTsSlice[i].len = C.size_t(len(p))
		}
	}

	var delCnt C.uint64_t
	status := C.TsDeleteEntities(r.tdb, C.TSTableID(tableID), &cTsSlice[0], (C.size_t)(len(cTsSlice)),
		C.uint64_t(rangeGroupID), &delCnt, C.uint64_t(tsTxnID))
	if err := statusToError(status); err != nil {
		if isDrop {
			return 0, err
		}
		log.Errorf(context.TODO(), "failed to delete ts entities")
	}
	return uint64(delCnt), nil
}

// DeleteRangeData delete entities data in the range
func (r *TsEngine) DeleteRangeData(
	tableID uint64,
	rangeGroupID uint64,
	beginHash uint64,
	endHash uint64,
	tsSpans []*roachpb.TsSpan,
	tsTxnID uint64,
) (uint64, error) {
	r.checkOrWaitForOpen()
	cKwHashIDSpans := C.HashIdSpan{
		begin: C.uint64_t(beginHash),
		end:   C.uint64_t(endHash),
	}

	cTsSpans := make([]C.KwTsSpan, len(tsSpans))
	for i := 0; i < len(tsSpans); i++ {
		cTsSpans[i].begin = C.int64_t(tsSpans[i].TsStart)
		cTsSpans[i].end = C.int64_t(tsSpans[i].TsEnd)
	}
	cKwTsSpans := C.KwTsSpans{
		spans: (*C.KwTsSpan)(unsafe.Pointer(&cTsSpans[0])),
		len:   C.int32_t(len(tsSpans)),
	}

	var delCnt C.uint64_t
	status := C.TsDeleteRangeData(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID), cKwHashIDSpans, cKwTsSpans, &delCnt, C.uint64_t(tsTxnID))
	if err := statusToError(status); err != nil {
		return uint64(delCnt), errors.New("Data deletion failed or partially failed")
	}
	return uint64(delCnt), nil
}

// DeleteTsRangeData delete entities data in the range
func (r *TsEngine) DeleteTsRangeData(
	tableID, beginHash, endHash uint64, startTs, endTs int64, tsTxnID uint64,
) error {
	r.checkOrWaitForOpen()
	tsSpan := C.KwTsSpan{
		begin: C.int64_t(startTs),
		end:   C.int64_t(endTs),
	}
	status := C.TsDeleteTotalRange(r.tdb, C.TSTableID(tableID), C.uint64_t(beginHash),
		C.uint64_t(endHash), tsSpan, C.uint64_t(tsTxnID))
	if err := statusToError(status); err != nil {
		return errors.New("range data deletion failed")
	}
	return nil
}

// DeleteData delete some one entity data
func (r *TsEngine) DeleteData(
	tableID uint64, rangeGroupID uint64, primaryTag []byte, tsSpans []*roachpb.TsSpan, tsTxnID uint64,
) (uint64, error) {
	if len(primaryTag) == 0 {
		return 0, errors.New("primaryTag is null")
	}

	r.checkOrWaitForOpen()
	cTsSlice := C.TSSlice{
		data: (*C.char)(C.CBytes(primaryTag)),
		len:  C.size_t(len(primaryTag)),
	}
	defer C.free(unsafe.Pointer(cTsSlice.data))

	cTsSpans := make([]C.KwTsSpan, len(tsSpans))
	for i := 0; i < len(tsSpans); i++ {
		cTsSpans[i].begin = C.int64_t(tsSpans[i].TsStart)
		cTsSpans[i].end = C.int64_t(tsSpans[i].TsEnd)
	}
	cKwTsSpans := C.KwTsSpans{
		spans: (*C.KwTsSpan)(unsafe.Pointer(&cTsSpans[0])),
		len:   C.int32_t(len(tsSpans)),
	}

	var delCnt C.uint64_t
	status := C.TsDeleteData(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID), cTsSlice, cKwTsSpans, &delCnt, C.uint64_t(tsTxnID))
	if err := statusToError(status); err != nil {
		return uint64(delCnt), errors.Wrap(err, "failed to delete ts data")
	}
	return uint64(delCnt), nil
}

// CompressTsTable compress partitions with maximum time<=ts
func (r *TsEngine) CompressTsTable(tableID uint64, ts int64) error {
	r.checkOrWaitForOpen()
	status := C.TSCompressTsTable(r.tdb, C.TSTableID(tableID), C.int64_t(ts))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to compress ts table")
	}
	return nil
}

// CompressImmediately compress partitions immediately
func (r *TsEngine) CompressImmediately(ctx context.Context, tableID uint64) error {
	r.checkOrWaitForOpen()
	goCtxPtr := C.uint64_t(uintptr(unsafe.Pointer(&ctx)))
	status := C.TSCompressImmediately(r.tdb, goCtxPtr, C.TSTableID(tableID))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to compress ts table")
	}
	return nil
}

// VacuumTsTable vacuum partitions after compress
func (r *TsEngine) VacuumTsTable(tableID uint64, tsVersion uint32) error {
	r.checkOrWaitForOpen()
	status := C.TSVacuumTsTable(r.tdb, C.TSTableID(tableID), C.uint32_t(tsVersion))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to vacuum ts table")
	}
	return nil
}

// DeleteExpiredData delete expired data from time partitions that fall completely within the [min_int64, end) interval
func (r *TsEngine) DeleteExpiredData(tableID uint64, _ int64, end int64) error {
	r.checkOrWaitForOpen()
	if end == math.MinInt64 {
		return nil
	}
	status := C.TSDeleteExpiredData(r.tdb, C.TSTableID(tableID), C.int64_t(end))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to delete expired data")
	}
	return nil
}

// TsTableAutonomy Autonomous Evaluation
func (r *TsEngine) TsTableAutonomy(tableID uint64) error {
	r.checkOrWaitForOpen()
	status := C.TSTableAutonomy(r.tdb, C.TSTableID(tableID))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to ts table's autonomy")
	}
	return nil
}

// SetupTsFlow send timing execution plan and receive execution results
func (r *TsEngine) SetupTsFlow(
	ctx *context.Context, tsQueryInfo TsQueryInfo,
) (tsRespInfo TsQueryInfo, err error) {
	r.checkOrWaitForOpen()
	return r.tsExecute(ctx, C.MQ_TYPE_DML_SETUP, tsQueryInfo)
}

// NextTsFlow drive timing execution plan, receive execution results
func (r *TsEngine) NextTsFlow(
	ctx *context.Context, tsQueryInfo TsQueryInfo,
) (tsRespInfo TsQueryInfo, err error) {
	r.checkOrWaitForOpen()
	return r.tsExecute(ctx, C.MQ_TYPE_DML_NEXT, tsQueryInfo)
}

// NextVectorizedTsFlow drive timing execution plan, receive execution results
func (r *TsEngine) NextVectorizedTsFlow(
	ctx *context.Context, tsQueryInfo TsQueryInfo, rev *TsDataChunkToGo,
) (tsRespInfo TsQueryInfo, err error) {
	return r.tsVectorizedExecute(ctx, C.MQ_TYPE_DML_VECTORIZE_NEXT, tsQueryInfo, rev)
}

// NextTsFlowPgWire drive timing execution plan, receive execution results
func (r *TsEngine) NextTsFlowPgWire(
	ctx *context.Context, tsQueryInfo TsQueryInfo,
) (tsRespInfo TsQueryInfo, err error) {
	r.checkOrWaitForOpen()
	return r.tsExecute(ctx, C.MQ_TYPE_DML_PG_RESULT, tsQueryInfo)
}

// PushTsFlow drive pushing relational data to TSEngine for join
// only push PUSH type req and pass data chunk pointer to tse for multiple model processing
// when the switch is on and the server starts with single node mode.
func (r *TsEngine) PushTsFlow(ctx *context.Context, tsQueryInfo TsQueryInfo) (err error) {
	r.checkOrWaitForOpen()
	_, err = r.tsExecute(ctx, C.MQ_TYPE_DML_PUSH, tsQueryInfo)
	return err
}

// CloseTsFlow close the TS actuator corresponding to the current flow
func (r *TsEngine) CloseTsFlow(ctx *context.Context, tsQueryInfo TsQueryInfo) (err error) {
	r.checkOrWaitForOpen()
	_, err = r.tsExecute(ctx, C.MQ_TYPE_DML_CLOSE, tsQueryInfo)
	return err
}

// InitTsHandle corresponding to init ts handle
func (r *TsEngine) InitTsHandle(
	ctx *context.Context, tsQueryInfo TsQueryInfo,
) (tsRespInfo TsQueryInfo, err error) {
	r.checkOrWaitForOpen()
	return r.tsExecute(ctx, C.MQ_TYPE_DML_INIT, tsQueryInfo)
}

// FlushBuffer flush WALs of all ts tables to files in the node
func (r *TsEngine) FlushBuffer() error {
	r.checkOrWaitForOpen()
	status := C.TSFlushBuffer(r.tdb)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to flush WAL buffer")
	}

	return nil
}

// Checkpoint create checkpoint
func (r *TsEngine) Checkpoint() error {
	r.checkOrWaitForOpen()
	status := C.TSCreateCheckpoint(r.tdb)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to create WAL checkpoint")
	}

	return nil
}

// CheckpointForTable create checkpoint for a table
func (r *TsEngine) CheckpointForTable(tableID uint32) error {
	r.checkOrWaitForOpen()
	status := C.TSCreateCheckpointForTable(r.tdb, C.uint64_t(tableID))
	if err := statusToError(status); err != nil {
		return err
	}
	return nil
}

// DeleteRangeGroup Delete RangeGroup
func (r *TsEngine) DeleteRangeGroup(tableID uint64, rangeGroup api.RangeGroup) error {
	r.checkOrWaitForOpen()
	cRangeGroup := C.RangeGroup{
		range_group_id: C.uint64_t(rangeGroup.RangeGroupID),
	}
	status := C.TSDeleteRangeGroup(r.tdb, C.TSTableID(tableID), cRangeGroup)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to delete range group")
	}
	return nil
}

// CreateSnapshotForRead create snapshot
func (r *TsEngine) CreateSnapshotForRead(
	tableID uint64, beginHash uint64, endHash uint64, beginTs int64, endTs int64,
) (uint64, error) {
	r.checkOrWaitForOpen()
	var snapshotID C.uint64_t
	tsSpan := C.KwTsSpan{
		begin: C.int64_t(beginTs),
		end:   C.int64_t(endTs),
	}
	status := C.TSCreateSnapshotForRead(r.tdb, C.TSTableID(tableID),
		C.uint64_t(beginHash), C.uint64_t(endHash), tsSpan, &snapshotID)
	if err := statusToError(status); err != nil {
		return 0, errors.Wrap(err, "failed to create snapshot")
	}
	return uint64(snapshotID), nil
}

// CreateSnapshotForWrite preparing for writing snapshots
func (r *TsEngine) CreateSnapshotForWrite(
	tableID uint64, beginHash uint64, endHash uint64, beginTs int64, endTs int64,
) (uint64, error) {
	r.checkOrWaitForOpen()
	var snapshotID C.uint64_t
	tsSpan := C.KwTsSpan{
		begin: C.int64_t(beginTs),
		end:   C.int64_t(endTs),
	}
	status := C.TSCreateSnapshotForWrite(r.tdb, C.TSTableID(tableID),
		C.uint64_t(beginHash), C.uint64_t(endHash), tsSpan, &snapshotID)
	if err := statusToError(status); err != nil {
		return 0, errors.Wrap(err, "failed to create snapshot")
	}
	return uint64(snapshotID), nil
}

// GetSnapshotNextBatchData get data of the snapshot
func (r *TsEngine) GetSnapshotNextBatchData(tableID uint64, snapshotID uint64) ([]byte, error) {
	r.checkOrWaitForOpen()
	var data C.TSSlice
	status := C.TSGetSnapshotNextBatchData(r.tdb, C.TSTableID(tableID), C.uint64_t(snapshotID), &data)
	if err := statusToError(status); err != nil {
		return nil, errors.Wrap(err, "failed to get snapshot data")
	}
	defer C.free(unsafe.Pointer(data.data))
	return cSliceToGoBytes(data), nil
}

// WriteSnapshotBatchData write snapshot data
func (r *TsEngine) WriteSnapshotBatchData(tableID uint64, snapshotID uint64, data []byte) error {
	if len(data) == 0 {
		return errors.New("snapshot data is null")
	}

	r.checkOrWaitForOpen()
	cTsSlice := C.TSSlice{
		data: (*C.char)(C.CBytes(data)),
		len:  C.size_t(len(data)),
	}
	defer C.free(unsafe.Pointer(cTsSlice.data))

	status := C.TSWriteSnapshotBatchData(r.tdb, C.TSTableID(tableID), C.uint64_t(snapshotID), cTsSlice)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to write snapshot data")
	}
	return nil
}

// WriteSnapshotSuccess apply snapshot
func (r *TsEngine) WriteSnapshotSuccess(tableID uint64, snapshotID uint64) error {
	r.checkOrWaitForOpen()
	status := C.TSWriteSnapshotSuccess(r.tdb, C.TSTableID(tableID), C.uint64_t(snapshotID))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to apply snapshot")
	}
	return nil
}

// WriteSnapshotRollback rollback snapshot
func (r *TsEngine) WriteSnapshotRollback(tableID uint64, snapshotID uint64) error {
	r.checkOrWaitForOpen()
	status := C.TSWriteSnapshotRollback(r.tdb, C.TSTableID(tableID), C.uint64_t(snapshotID))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to rollback snapshot")
	}
	return nil
}

// DeleteSnapshot drops Snapshot.
func (r *TsEngine) DeleteSnapshot(tableID uint64, snapshotID uint64) error {
	r.checkOrWaitForOpen()
	status := C.TSDeleteSnapshot(r.tdb, C.TSTableID(tableID), C.uint64_t(snapshotID))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to drop snapshot")
	}
	return nil
}

// MtrBegin BEGIN a TS mini-transaction
func (r *TsEngine) MtrBegin(
	tableID uint64, rangeGroupID uint64, rangeID uint64, index uint64,
) (uint64, error) {
	r.checkOrWaitForOpen()
	var miniTransID C.uint64_t
	status := C.TSMtrBegin(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID), C.uint64_t(rangeID),
		C.uint64_t(index), &miniTransID)
	if err := statusToError(status); err != nil {
		return 0, errors.Wrap(err, "failed to BEGIN a TS mini-transaction")
	}
	return uint64(miniTransID), nil
}

// MtrCommit COMMIT a TS mini-transaction
func (r *TsEngine) MtrCommit(tableID uint64, rangeGroupID uint64, miniTransID uint64) error {
	r.checkOrWaitForOpen()
	status := C.TSMtrCommit(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID), C.uint64_t(miniTransID))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to COMMIT a TS mini-transaction")
	}
	return nil
}

// MtrRollback ROLLBACK a TS mini-transaction
func (r *TsEngine) MtrRollback(tableID uint64, rangeGroupID uint64, miniTransID uint64) error {
	r.checkOrWaitForOpen()
	status := C.TSMtrRollback(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID), C.uint64_t(miniTransID))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to ROLLBACK a TS mini-transaction")
	}
	return nil
}

// TransBegin BEGIN a TS transaction
func (r *TsEngine) TransBegin(tableID uint64, transactionID []byte) error {
	r.checkOrWaitForOpen()
	status := C.TSxBegin(r.tdb, C.TSTableID(tableID), (*C.char)(unsafe.Pointer(&transactionID[0])))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to BEGIN a TS mini-transaction")
	}
	return nil
}

// TransCommit COMMIT a TS transaction
func (r *TsEngine) TransCommit(tableID uint64, transactionID []byte) error {
	r.checkOrWaitForOpen()
	status := C.TSxCommit(r.tdb, C.TSTableID(tableID), (*C.char)(unsafe.Pointer(&transactionID[0])))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to COMMIT a TS mini-transaction")
	}
	return nil
}

// TransRollback ROLLBACK a TS transaction
func (r *TsEngine) TransRollback(tableID uint64, transactionID []byte) error {
	r.checkOrWaitForOpen()
	status := C.TSxRollback(r.tdb, C.TSTableID(tableID), (*C.char)(unsafe.Pointer(&transactionID[0])))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to ROLLBACK a TS mini-transaction")
	}
	return nil
}

// TSGetWaitThreadNum is used to get wait thread num from time series engine
func (r *TsEngine) TSGetWaitThreadNum() (uint32, error) {
	r.checkOrWaitForOpen()
	var info C.ThreadInfo
	status := C.TSGetWaitThreadNum(r.tdb, unsafe.Pointer(&info))
	if err := statusToError(status); err != nil {
		return 0, errors.Wrap(err, "failed to get wait threads number")
	}

	return uint32(info.wait_threads), nil
}

// SetCompressInterval send compress interval to AE
func SetCompressInterval(interval []byte) {
	C.TSSetClusterSetting(goToTSSlice([]byte(compressInterval)), goToTSSlice(interval))
}

// SetVacuumInterval send vacuum interval to AE
func SetVacuumInterval(interval []byte) {
	C.TSSetClusterSetting(goToTSSlice([]byte(vacuumInterval)), goToTSSlice(interval))
}

// Close close TsEngine
func (r *TsEngine) Close() {
	status := C.TSClose(r.tdb)
	if err := statusToError(status); err != nil {
		log.Errorf(context.TODO(), "could not close ts engine instance")
	}
}

func (r *TsEngine) manageWAL() {
	ctx := context.Background()
	r.stopper.RunWorker(ctx, func(ctx context.Context) {
		flushTimer := timeutil.NewTimer()
		checkpointTimer := timeutil.NewTimer()

		defer flushTimer.Stop()
		defer checkpointTimer.Stop()

		flushInterval := TsWALFlushInterval.Get(&r.cfg.Settings.SV)
		checkpointInterval := TsWALCheckpointInterval.Get(&r.cfg.Settings.SV)
		flushTimer.Reset(flushInterval)
		checkpointTimer.Reset(checkpointInterval)

		for {
			select {
			case <-r.stopper.ShouldStop():
				return
			case <-flushTimer.C:
				if flushInterval <= 200*time.Millisecond {
					continue
				}
				flushTimer.Read = true
				_ = r.FlushBuffer()
				flushTimer.Reset(flushInterval)
			case <-checkpointTimer.C:
				checkpointInterval = TsWALCheckpointInterval.Get(&r.cfg.Settings.SV)
				checkpointTimer.Read = true
				_ = r.Checkpoint()
				checkpointTimer.Reset(checkpointInterval)

				newFlushInterval := TsWALFlushInterval.Get(&r.cfg.Settings.SV)
				if flushInterval != newFlushInterval {
					flushInterval = newFlushInterval
					flushTimer.Read = true
					flushTimer.Reset(flushInterval)
				}
			}
		}
	})
}

// DeleteReplicaTSData delete replica ts data
func (r *TsEngine) DeleteReplicaTSData(
	tableID uint64, beginHash uint64, endHash uint64, startTs int64, endTs int64,
) error {
	r.checkOrWaitForOpen()
	tsSpan := C.KwTsSpan{
		begin: C.int64_t(startTs),
		end:   C.int64_t(endTs),
	}
	status := C.TsDeleteTotalRange(r.tdb, C.TSTableID(tableID),
		C.uint64_t(beginHash), C.uint64_t(endHash), tsSpan, C.uint64_t(0))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to delete replica ts data")
	}
	return nil
}

func goToTSSlice(b []byte) C.TSSlice {
	if len(b) == 0 {
		return C.TSSlice{data: nil, len: 0}
	}
	return C.TSSlice{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.size_t(len(b)),
	}
}

func goToTSAppliedRangeIndexe(b []byte) C.TSSlice {
	if len(b) == 0 {
		return C.TSSlice{data: nil, len: 0}
	}
	return C.TSSlice{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.size_t(len(b)),
	}
}

func statusToError(s C.TSStatus) error {
	if s.data == nil {
		return nil
	}
	return &Error{msg: cStringToGoString(s)}
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

func cSliceToGoBytes(s C.TSSlice) []byte {
	if s.data == nil {
		return nil
	}
	return gobytes(unsafe.Pointer(s.data), int(s.len))
}

func cSliceToUnsafeGoBytes(s C.TSSlice) []byte {
	if s.data == nil {
		return nil
	}
	// Interpret the C pointer as a pointer to a Go array, then slice.
	return (*[MaxArrayLen]byte)(unsafe.Pointer(s.data))[:s.len:s.len]
}

// NewTsFetcher init tsFetcher
func NewTsFetcher(specs []execinfrapb.TSProcessorSpec) []C.TsFetcher {
	i := 0
	tsFetchers := make([]C.TsFetcher, len(specs))
	for j := len(specs) - 1; j >= 0; j-- {
		tsFetchers[i].processor_id = C.int32_t(specs[j].ProcessorID)
		i++
	}
	return tsFetchers
}

// AddStatsList add data to statsList
func AddStatsList(tsFetcher TsFetcher, statss []TsFetcherStats) []TsFetcherStats {
	for i := 0; i < tsFetcher.Size; i++ {
		fetcher := tsFetcher.CFetchers[i]
		if fetcher.row_num > 0 {
			statss[i].RowNum = int64(fetcher.row_num)
		}
		if fetcher.stall_time > 0 {
			statss[i].StallTime = int64(fetcher.stall_time)
		}
		if fetcher.bytes_read > 0 {
			statss[i].BytesRead = int64(fetcher.bytes_read)
		}
		if fetcher.max_allocated_mem > 0 {
			statss[i].MaxAllocatedMem = int64(fetcher.max_allocated_mem)
		}
		if fetcher.max_allocated_disk > 0 {
			statss[i].MaxAllocatedDisk = int64(fetcher.max_allocated_disk)
		}
		if fetcher.max_allocated_disk > 0 {
			statss[i].MaxAllocatedDisk = int64(fetcher.max_allocated_disk)
		}
		if fetcher.output_row_num > 0 {
			statss[i].OutputRowNum = int64(fetcher.output_row_num)
		}
		// build_time only be used for hash tag scan op for multiple model processing
		// when the switch is on and the server starts with single node mode.
		if fetcher.build_time > 0 {
			statss[i].BuildTime = int64(fetcher.build_time)
		}
	}
	return statss
}

//export goLock
func goLock(goMutux C.uint64_t) {
	fet := *(*TsFetcher)(unsafe.Pointer(uintptr(goMutux)))
	if fet.Mu != nil {
		fet.Mu.Lock()
	}
}

//export goUnLock
func goUnLock(goMutux C.uint64_t) {
	fet := *(*TsFetcher)(unsafe.Pointer(uintptr(goMutux)))
	if fet.Mu != nil {
		fet.Mu.Unlock()
	}
}

// GetTsVersion get current version of ts table
func (r *TsEngine) GetTsVersion(tableID uint64) (uint32, error) {
	r.checkOrWaitForOpen()
	var tsVersion C.uint32_t
	status := C.TsGetTableVersion(r.tdb, C.TSTableID(tableID), &tsVersion)
	if err := statusToError(status); err != nil {
		return uint32(tsVersion), errors.Wrap(err, "failed to get ts version")
	}
	return uint32(tsVersion), nil
}

// GetWalLevel get current wal level of ts engine
func (r *TsEngine) GetWalLevel() (int, error) {
	r.checkOrWaitForOpen()
	var walLevel C.uint8_t
	status := C.TsGetWalLevel(r.tdb, &walLevel)
	if err := statusToError(status); err != nil {
		return int(walLevel), errors.Wrap(err, "failed to get ts version")
	}
	return int(walLevel), nil
}

// GetTableMetaByVersion is used for unit test, try to get the tableMeta with specific tsVersion
func (r *TsEngine) GetTableMetaByVersion(tableID uint64, tsVer uint64) error {
	var tsVersion C.uint64_t
	tsVersion = C.uint64_t(tsVer)
	status := C.TsTestGetAndAddSchemaVersion(r.tdb, C.TSTableID(tableID), tsVersion)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to create ts table by specific tsVersion")
	}
	return nil
}
