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

//go:build windows
// +build windows

package tse

import "C"
import (
	"context"
	"time"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
)

const (
	// MaxArrayLen is a safe maximum length for slices on this architecture.
	MaxArrayLen = 1<<50 - 1
	// WALLEVELOFF  wal level = 0
	WALLEVELOFF = 0
	// WALLEVELON wal level = 1
	WALLEVELON = 1
	// WALLEVELSYNC wal level = 2
	WALLEVELSYNC = 2
	// WALREPLICATION wal level = 3
	WALREPLICATION = 3
	// WALLEVELBYRL wal level = 4
	WALLEVELBYRL = 4
)

const (
	compressInterval = "ts.compress_interval"
	vacuumInterval   = "ts.vacuum_interval"
	// FlushInterval ts WAL flush interval in TsEngine. when 0, wal will be flushed on wrote. when -1, WAL is disable.
	FlushInterval = "ts.wal.flush_interval"
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
) //.WithSystemAdmin() // douzt: TOOD

var ErrNotSupportTsEngine = errors.New("ts engine does not support Windows environment.")

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
func TsGetNameValue(this *execinfrapb.ProcessorCoreUnion) int8 {
	if this.TsTableReader != nil {
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
	if this.TsStatisticReader != nil {
		return TsStatisticReaderName
	}
	if this.TsSynchronizer != nil {
		return TsSynchronizerName
	}
	if this.Sampler != nil {
		return TsSamplerName
	}
	if this.TsTagReader != nil {
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

// TsMetrics is to define the metrics used in TsEngine.
var (
	MetaTsBlockCacheHitCount = metric.Metadata{
		Name:        "ts.blockcache.hit.count",
		Help:        "Counter of ts engine block cache hit count",
		Measurement: "Ts Engine",
		Unit:        metric.Unit_COUNT,
	}
	MetaTsBlockCacheMissCount = metric.Metadata{
		Name:        "ts.blockcache.miss.count",
		Help:        "Counter of ts engine block cache miss count",
		Measurement: "Ts Engine",
		Unit:        metric.Unit_COUNT,
	}
	MetaTsBlockCacheHitRatio = metric.Metadata{
		Name:        "ts.blockcache.hit.ratio",
		Help:        "Hit ratio of ts engine block cache",
		Measurement: "Ts Engine",
		Unit:        metric.Unit_PERCENT,
	}
	MetaTsBlockCacheMemorySize = metric.Metadata{
		Name:        "ts.blockcache.memory.size",
		Help:        "Bytes of ts engine block cache memory size",
		Measurement: "Ts Engine",
		Unit:        metric.Unit_BYTES,
	}
	MetaTsDeleteExpiredCount = metric.Metadata{
		Name:        "ts.delete.expired.count",
		Help:        "Counter of schedulers currently running to delete the expired data",
		Measurement: "Ts Engine",
		Unit:        metric.Unit_COUNT,
	}
	MetaTsCompressCount = metric.Metadata{
		Name:        "ts.compress.count",
		Help:        "Counter of schedulers currently running to compress the ts table",
		Measurement: "Ts Engine",
		Unit:        metric.Unit_COUNT,
	}
	MetaTsVacuumCount = metric.Metadata{
		Name:        "ts.vacuum.count",
		Help:        "Counter of schedulers currently running to vacuum the ts table",
		Measurement: "Ts Engine",
		Unit:        metric.Unit_COUNT,
	}
	MetaTsDeleteExpiredNanos = metric.Metadata{
		Name:        "ts.delete.expired.processingnanos",
		Help:        "Nanoseconds spent deleting the expired data",
		Measurement: "Ts Engine",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaTsCompressNanos = metric.Metadata{
		Name:        "ts.compress.processingnanos",
		Help:        "Nanoseconds spent compressing the ts table",
		Measurement: "Ts Engine",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaTsVacuumNanos = metric.Metadata{
		Name:        "ts.vacuum.processingnanos",
		Help:        "Nanoseconds spent vacuuming the ts table",
		Measurement: "Ts Engine",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// TsEngineMetrics is the set of metrics for the tsEngine server.
type TsEngineMetrics struct {
	TsBlockCacheHitCount   *metric.Gauge
	TsBlockCacheMissCount  *metric.Gauge
	TsBlockCacheHitRatio   *metric.GaugeFloat64
	TsBlockCacheMemorySize *metric.Gauge
	TsDeleteExpiredCount   *metric.Gauge
	TsCompressCount        *metric.Gauge
	TsVacuumCount          *metric.Gauge
	TsDeleteExpiredNanos   *metric.Counter
	TsCompressNanos        *metric.Counter
	TsVacuumNanos          *metric.Counter
}

func makeTsEngineMetrics() *TsEngineMetrics {
	return &TsEngineMetrics{
		TsBlockCacheHitCount:   metric.NewGauge(MetaTsBlockCacheHitCount),
		TsBlockCacheMissCount:  metric.NewGauge(MetaTsBlockCacheMissCount),
		TsBlockCacheHitRatio:   metric.NewGaugeFloat64(MetaTsBlockCacheHitRatio),
		TsBlockCacheMemorySize: metric.NewGauge(MetaTsBlockCacheMemorySize),
		TsDeleteExpiredCount:   metric.NewGauge(MetaTsDeleteExpiredCount),
		TsCompressCount:        metric.NewGauge(MetaTsCompressCount),
		TsVacuumCount:          metric.NewGauge(MetaTsVacuumCount),
		TsDeleteExpiredNanos:   metric.NewCounter(MetaTsDeleteExpiredNanos),
		TsCompressNanos:        metric.NewCounter(MetaTsCompressNanos),
		TsVacuumNanos:          metric.NewCounter(MetaTsVacuumNanos),
	}
}

// goToCTSSlice converts a go byte slice to a TSSlice. Note that this is
// potentially dangerous as the DBSlice holds a reference to the go
// byte slice memory that the Go GC does not know about. This method
// is only intended for use in converting arguments to C
// functions. The C function must copy any data that it wishes to
// retain once the function returns.
// func goToCTSSlice(b []byte) C.TSSlice {
// 	if len(b) == 0 {
// 		return C.TSSlice{data: nil, len: 0}
// 	}
// 	return C.TSSlice{
// 		data: (*C.char)(unsafe.Pointer(&b[0])),
// 		len:  C.size_t(len(b)),
// 	}
// }

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
	BRPCAddr       string
	ClusterID      string
	TsIDGen        *sqlbase.TSIDGenerator
}

// TsQueryInfo the parameter and return value passed by the query
type TsQueryInfo struct {
	Buf          []byte
	RowNum       int
	ID           int
	UniqueID     int
	TimeZone     int
	TimeZoneName string // IANA timezone name (e.g. "Europe/Berlin") for DST support
	UseDST       bool   // DST flag calculated by Go layer
	Code         int
	Handle       unsafe.Pointer
	Fetcher      TsFetcher
	// only pass the data chunk to tse for multiple model processing
	// when the switch is on and the server starts with single node mode.
	PushData *DataChunkGo
	RowCount int
	SQL      string
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
	//tdb      *C.TSEngine
	opened   bool
	openCh   chan struct{}
	writeWAL bool
	metrics  *TsEngineMetrics
	syncutil.RWMutex
	Version string
	TsIDGen *sqlbase.TSIDGenerator
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

// TsRaftLogSyncPeriod determine the sync interval of ts raft logs.
var TsRaftLogSyncPeriod = settings.RegisterPublicDurationSetting(
	"ts.raft_log.sync_period",
	"the min duration between every 2 syncs to disk of ts raft logs",
	10*time.Second,
)

// TsWALLevelClusterSettingName is the name of the ts wal level cluster setting.
const TsWALLevelClusterSettingName = "ts.wal.wal_level"

// TsWALLevel indicates the WAL level
var TsWALLevel = settings.RegisterPublicIntSetting(
	TsWALLevelClusterSettingName,
	"ts WAL level, default 2(flush)",
	2,
)

// TsWALCheckpointInterval indicates the wal checkpoint interval of TsEngine
var TsWALCheckpointInterval = settings.RegisterPublicDurationSetting(
	"ts.wal.checkpoint_interval",
	"ts WAL checkpoint interval in TsEngine",
	time.Minute,
) //.WithSystemAdmin() // douzt: TOOD

// SQLTimeseriesTrace set trace for timeseries.
var SQLTimeseriesTrace = settings.RegisterStringSetting(
	"ts.trace.on_off_list",
	"collection/push switch",
	"",
) //.WithSystemAdmin()

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
// func isCanceledCtx(goCtxPtr C.uint64_t) C.bool {
// ctx := *(*context.Context)(unsafe.Pointer(uintptr(goCtxPtr)))
// select {
// case <-ctx.Done():
// 	return C.bool(true)
// default:
// 	return C.bool(false)
// }
// }

// NewTsEngine new ts engine
func NewTsEngine(
	ctx context.Context, cfg TsEngineConfig, stopper *stop.Stopper,
) (*TsEngine, error) {
	if cfg.Dir == "" {
		return nil, errors.New("dir must be non-empty")
	}

	metrics := makeTsEngineMetrics()

	r := &TsEngine{
		stopper: stopper,
		cfg:     cfg,
		openCh:  make(chan struct{}),
		metrics: metrics,
	}
	return r, nil
}

// Metrics returns the metrics for the ts engine.
func (r *TsEngine) Metrics() *TsEngineMetrics {
	return r.metrics
}

// IsOpen returns when the ts engine has been open.
func (r *TsEngine) IsOpen() bool {
	return r.opened
}

// CheckOrWaitForOpen check whether the ts engine has been open and
// waits for open if it is not.
func (r *TsEngine) CheckOrWaitForOpen() {
	if r.opened {
		return
	}
	_ = <-r.openCh
}

// Open opens the ts engine.
func (r *TsEngine) Open(rangeIndex []roachpb.RangeIndex) error {
	// douzt TODO
	return nil
}

// SetRaftLogCombinedWAL set the state of TsRaftLogCombineWAL to AE
func (r *TsEngine) SetRaftLogCombinedWAL(combined bool) {
}

// GetTableBlocksDistribution get table block distribution info
func (r *TsEngine) GetTableBlocksDistribution(tableID uint64) ([]byte, error) {
	return nil, nil
}

// GetDBBlocksDistribution get database block distribution info
func (r *TsEngine) GetDBBlocksDistribution(dbID uint64) ([]byte, error) {
	return nil, nil
}

// CreateTsTable create ts table
func (r *TsEngine) CreateTsTable(
	tableID uint64, hashNum uint64, meta []byte, rangeGroups []api.RangeGroup,
) error {
	return nil
}

// GetMetaData get meta from source of the snapshot
func (r *TsEngine) GetMetaData(tableID uint64, rangeGroup api.RangeGroup) ([]byte, error) {
	return nil, nil
}

// TSIsTsTableExist checks if ts table exists.
func (r *TsEngine) TSIsTsTableExist(tableID uint64) (bool, error) {
	return true, nil
}

// TSConfirmTableVersionExist checks if ts table exists.
func (r *TsEngine) TSConfirmTableVersionExist(tableID uint64, tsVersion uint32) error {
	return nil
}

// DropTsTable drop ts table.
func (r *TsEngine) DropTsTable(tableID uint64) error {
	return nil
}

// DropLeftTsTableGarbage drop left ts table metadata garbage.
func (r *TsEngine) DropLeftTsTableGarbage() error {
	return nil
}

// TSFlushVGroups flush v-groups after import
func (r *TsEngine) TSFlushVGroups() error {
	return nil
}

// TsGetRecentBlockCacheInfo get storage block cache hit, miss counts and current used memory size(M) for showing hit ratio in web console
func (r *TsEngine) TsGetRecentBlockCacheInfo() (int, int, int64, error) {
	return 0, 0, 0, nil
}

// SamplePeriodicMetrics sample tsengine metrics periodically (default 10s).
// If you need to collect other ts engine metrics periodically, please add them in this function.
func (r *TsEngine) SamplePeriodicMetrics() {
	// Sample BlockCache Info
	hitCount, missCount, memorySize, _ := r.TsGetRecentBlockCacheInfo()
	hitRatio := float64(hitCount) / (float64(hitCount) + float64(missCount) + 1e-8) // add 1e-8 to avoid division by zero
	r.metrics.TsBlockCacheHitCount.Update(int64(hitCount))
	r.metrics.TsBlockCacheMissCount.Update(int64(missCount))
	r.metrics.TsBlockCacheHitRatio.Update(hitRatio)
	r.metrics.TsBlockCacheMemorySize.Update(int64(memorySize))
}

// AddTSColumn adds column for ts table.
func (r *TsEngine) AddTSColumn(
	tableID uint64, currentTSVersion, newTSVersion uint32, transactionID []byte, colMeta []byte,
) error {
	return nil
}

// DropTSColumn drop column for ts table.
func (r *TsEngine) DropTSColumn(
	tableID uint64, currentTSVersion, newTSVersion uint32, transactionID []byte, colMeta []byte,
) error {
	// r.checkOrWaitForOpen()
	// status := C.TSDropColumn(
	// 	r.tdb, C.TSTableID(tableID), (*C.char)(unsafe.Pointer(&transactionID[0])), goToTSSlice(colMeta), C.uint32_t(currentTSVersion), C.uint32_t(newTSVersion))
	// if err := statusToError(status); err != nil {
	// 	return errors.Wrap(err, "could not DropTsColumn")
	// }
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
	// numColumn := len(indexColumns)
	// cIndexs := make([]C.uint, numColumn)
	// for i := 0; i < numColumn; i++ {
	// 	cIndexs[i] = C.uint(indexColumns[i])
	// }
	// cIndexColumns := C.IndexColumns{
	// 	index_column: (*C.uint32_t)(unsafe.Pointer(&cIndexs[0])),
	// 	len:          C.int32_t(len(cIndexs)),
	// }
	// status := C.TSCreateNormalTagIndex(r.tdb, C.TSTableID(tableID), C.uint64_t(indexID), (*C.char)(unsafe.Pointer(&transactionID[0])), C.uint32_t(curVersion), C.uint32_t(newVersion), cIndexColumns)
	// if err := statusToError(status); err != nil {
	// 	return errors.Wrap(err, "could not CreateNormalTagIndex")
	// }
	return nil
}

// DropNormalTagIndex drop index on normal tag of ts table
func (r *TsEngine) DropNormalTagIndex(
	tableID uint64, indexID uint64, curVersion, newVersion uint32, transactionID []byte,
) error {
	// status := C.TSDropNormalTagIndex(r.tdb, C.TSTableID(tableID), C.uint64_t(indexID), (*C.char)(unsafe.Pointer(&transactionID[0])), C.uint32_t(curVersion), C.uint32_t(newVersion))
	// if err := statusToError(status); err != nil {
	// 	return errors.Wrap(err, "could not DropNormalTagIndex")
	// }
	return nil
}

// AlterLifetime alter lifetime interval for this table.
func (r *TsEngine) AlterLifetime(tableID uint64, lifeTime uint64) error {
	// r.checkOrWaitForOpen()
	// status := C.TSAlterLifetime(r.tdb, C.TSTableID(tableID), C.uint64_t(lifeTime))
	// if err := statusToError(status); err != nil {
	// 	return errors.Wrap(err, "failed to set table lifetime")
	// }
	return nil
}

// AlterPartitionInterval alter partition interval for ts table.
func (r *TsEngine) AlterPartitionInterval(tableID uint64, partitionInterval uint64) error {
	// r.checkOrWaitForOpen()
	// status := C.TSAlterPartitionInterval(r.tdb, C.TSTableID(tableID), C.uint64_t(partitionInterval))
	// if err := statusToError(status); err != nil {
	// 	return errors.Wrap(err, "could not AlterPartitionInterval")
	// }
	return nil
}

// AlterTSColumnType alter column/tag type of ts table.
func (r *TsEngine) AlterTSColumnType(
	tableID uint64,
	currentTSVersion, newTSVersion uint32,
	transactionID []byte,
	colMeta []byte,
	originColMeta []byte,
	isAlterType bool,
	isAlterCompress bool,
) error {
	// r.checkOrWaitForOpen()
	// status := C.TSAlterColumnType(
	// 	r.tdb,
	// 	C.TSTableID(tableID),
	// 	(*C.char)(unsafe.Pointer(&transactionID[0])),
	// 	goToTSSlice(colMeta),
	// 	goToTSSlice(originColMeta),
	// 	C.uint32_t(currentTSVersion),
	// 	C.uint32_t(newTSVersion),
	// )
	// if err := statusToError(status); err != nil {
	// 	return err
	// }
	return nil
}

// PutEntity write in, update tag data and write in ts data
func (r *TsEngine) PutEntity(
	rangeGroupID uint64, tableID uint64, payload [][]byte, tsTxnID uint64, osnID uint64,
) error {
	if len(payload) == 0 {
		return errors.New("payload is nul")
	}

	return nil
}

// PutData write in tag data and write in ts data
func (r *TsEngine) PutData(
	tableID uint64, payload [][]byte, tsTxnID uint64, writeWAL bool, transactionID []byte,
) (DedupResult, EntitiesAffect, error) {
	// if len(payload) == 0 {
	// 	return DedupResult{}, EntitiesAffect{}, errors.New("payload is nul")
	// }

	// r.checkOrWaitForOpen()
	// cTsSlice := make([]C.TSSlice, len(payload))
	// for i, p := range payload {
	// 	if len(p) == 0 {
	// 		cTsSlice[i].data = nil
	// 		cTsSlice[i].len = 0
	// 	} else {
	// 		dataPtr := C.CBytes(p)
	// 		defer C.free(dataPtr)

	// 		cTsSlice[i].data = (*C.char)(dataPtr)
	// 		cTsSlice[i].len = C.size_t(len(p))
	// 	}
	// }
	// // mock
	// cRangeGroup := C.RangeGroup{
	// 	range_group_id: C.uint64_t(101),
	// 	typ:            C.int8_t(0),
	// }

	// var dedupResult C.DedupResult
	// var affect EntitiesAffect
	// var entitiesAffected C.uint16_t
	// var unorderedAffected C.uint32_t
	// var status C.TSStatus
	// if transactionID != nil {
	// 	cstr := C.CString(string(transactionID))
	// 	defer C.free(unsafe.Pointer(cstr))
	// 	status = C.TSPutDataByRowTypeExplicit(r.tdb, C.TSTableID(tableID), &cTsSlice[0], (C.size_t)(len(cTsSlice)), cRangeGroup, C.uint64_t(tsTxnID),
	// 		&entitiesAffected, &unorderedAffected, &dedupResult, C.bool(writeWAL), cstr)
	// } else {
	// 	status = C.TSPutDataByRowType(r.tdb, C.TSTableID(tableID), &cTsSlice[0], (C.size_t)(len(cTsSlice)), cRangeGroup, C.uint64_t(tsTxnID),
	// 		&entitiesAffected, &unorderedAffected, &dedupResult, C.bool(writeWAL))
	// }
	// if err := statusToError(status); err != nil {
	// 	return DedupResult{}, EntitiesAffect{}, errors.Wrap(err, "could not PutData")
	// }

	// res := DedupResult{
	// 	DedupRule:     int(dedupResult.dedup_rule),
	// 	DedupRows:     int(dedupResult.dedup_rows),
	// 	DiscardBitmap: cSliceToGoBytes(dedupResult.discard_bitmap),
	// }
	// defer C.free(unsafe.Pointer(dedupResult.discard_bitmap.data))
	// affect.EntityCount = uint16(entitiesAffected)
	// affect.UnorderedCount = uint32(unorderedAffected)
	return DedupResult{}, EntitiesAffect{}, nil
}

// PutRowData 行存tags值和时序数据写入
func (r *TsEngine) PutRowData(
	tableID uint64,
	headerPrefix []byte,
	payload [][]byte,
	size int32,
	tsTxnID uint64,
	writeWAL bool,
	transactionID []byte,
) (DedupResult, EntitiesAffect, error) {
	// if len(payload) == 0 {
	// 	return DedupResult{}, EntitiesAffect{}, errors.New("payload is nul")
	// }

	// r.checkOrWaitForOpen()
	// sizeLimit := int32(TsPayloadSizeLimit.Get(&r.cfg.Settings.SV))
	// var cTsSlice C.TSSlice
	// // The structure of HeaderPrefix: | Header | primary_tag_len | primary_tag | tag_ten | tags | data_len |
	// // Header: | txn(16) | group_id(2) | payload_version(4) | database_id(4) | table_id(8) | ts_version(4) | row_num(4) | flags(1) |
	// const rowNumOffset = 38 // offset of row_num, pay attention to any change of the structure of HeaderPrefix
	// const dataLen = 4       // length of data_len in HeaderPrefix. The location is at the end of HeaderPrefix

	// headerLen := len(headerPrefix)
	// cTsSlice.data = (*C.char)(C.malloc(C.size_t(int(size) + headerLen + dataLen)))
	// if cTsSlice.data == nil {
	// 	return DedupResult{}, EntitiesAffect{}, errors.New("failed malloc")
	// }
	// defer C.free(unsafe.Pointer(cTsSlice.data))

	// C.memcpy(unsafe.Pointer(cTsSlice.data), unsafe.Pointer(&headerPrefix[0]), C.size_t(headerLen))
	// dataPtr := uintptr(unsafe.Pointer(cTsSlice.data)) + uintptr(headerLen) // pointer to the data_len

	// // mock
	// cRangeGroup := C.RangeGroup{
	// 	range_group_id: C.uint64_t(1),
	// 	typ:            C.int8_t(0),
	// }
	// payloadPtr := dataPtr + uintptr(dataLen)
	// payloadSize := 0
	// partRowCnt := 0
	// totalRowCnt := len(payload)
	// var res DedupResult
	// var affect EntitiesAffect
	// for i := 0; i < totalRowCnt; i++ {
	// 	p := payload[i]
	// 	if len(p) == 0 {
	// 		continue
	// 	}
	// 	partLen := len(p)
	// 	// need to check whether the payload size exceeds limit, so calculate it before add the row to payload.
	// 	payloadSize += partLen
	// 	if payloadSize > int(sizeLimit) {
	// 		payloadSize -= partLen
	// 		// fill data_len
	// 		*(*int32)(unsafe.Pointer(dataPtr)) = int32(payloadSize)
	// 		// fill row_num
	// 		*(*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(cTsSlice.data)) + rowNumOffset)) = int32(partRowCnt)
	// 		// set tsSlice len
	// 		cTsSlice.len = C.size_t(payloadSize + headerLen + dataLen)
	// 		var dedupResult C.DedupResult
	// 		var entitiesAffected C.uint16_t
	// 		var unorderedAffected C.uint32_t
	// 		status := C.TSPutDataByRowType(r.tdb, C.TSTableID(tableID), &cTsSlice, (C.size_t)(1), cRangeGroup, C.uint64_t(tsTxnID),
	// 			&entitiesAffected, &unorderedAffected, &dedupResult, C.bool(writeWAL))
	// 		if err := statusToError(status); err != nil {
	// 			return DedupResult{}, EntitiesAffect{}, errors.Wrap(err, "could not PutData")
	// 		}
	// 		res.DedupRows += int(dedupResult.dedup_rows)
	// 		affect.EntityCount += uint16(entitiesAffected)
	// 		affect.UnorderedCount += uint32(unorderedAffected)
	// 		C.free(unsafe.Pointer(dedupResult.discard_bitmap.data))

	// 		payloadSize = partLen
	// 		payloadPtr = dataPtr + uintptr(dataLen)
	// 		partRowCnt = 0
	// 	}
	// 	partRowCnt++
	// 	C.memcpy(unsafe.Pointer(payloadPtr), unsafe.Pointer(&p[0]), C.size_t(partLen))
	// 	payloadPtr += uintptr(partLen)
	// }

	// // fill data_len
	// *(*int32)(unsafe.Pointer(dataPtr)) = int32(payloadSize)
	// // fill row_num
	// *(*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(cTsSlice.data)) + rowNumOffset)) = int32(partRowCnt)
	// // set tsSlice len
	// cTsSlice.len = C.size_t(payloadSize + headerLen + dataLen)
	// var dedupResult C.DedupResult
	// var entitiesAffected C.uint16_t
	// var unorderedAffected C.uint32_t
	// var status C.TSStatus
	// if transactionID != nil {
	// 	cstr := C.CString(string(transactionID))
	// 	defer C.free(unsafe.Pointer(cstr))
	// 	status = C.TSPutDataByRowTypeExplicit(r.tdb, C.TSTableID(tableID), &cTsSlice, (C.size_t)(1), cRangeGroup, C.uint64_t(tsTxnID),
	// 		&entitiesAffected, &unorderedAffected, &dedupResult, C.bool(writeWAL), cstr)
	// } else {
	// 	status = C.TSPutDataByRowType(r.tdb, C.TSTableID(tableID), &cTsSlice, (C.size_t)(1), cRangeGroup, C.uint64_t(tsTxnID),
	// 		&entitiesAffected, &unorderedAffected, &dedupResult, C.bool(writeWAL))
	// }
	// if err := statusToError(status); err != nil {
	// 	return DedupResult{}, EntitiesAffect{}, errors.Wrap(err, "could not PutData")
	// }

	// res.DedupRows += int(dedupResult.dedup_rows)
	// res.DedupRule = int(dedupResult.dedup_rule)
	// affect.EntityCount += uint16(entitiesAffected)
	// affect.UnorderedCount += uint32(unorderedAffected)
	// // the DiscardBitmap is not complete if the payload is truncated due to the size limit.
	// res.DiscardBitmap = cSliceToGoBytes(dedupResult.discard_bitmap)
	// C.free(unsafe.Pointer(dedupResult.discard_bitmap.data))

	// return res, affect, nil
	return DedupResult{}, EntitiesAffect{}, nil
}

// GetDataVolume gets DataVolume for ts range.
// should not call this for relational ranges.
func (r *TsEngine) GetDataVolume(
	tableID uint64, startHashPoint, endHashPoint uint64, startTimestamp, endTimestamp int64,
) (uint64, error) {
	// r.checkOrWaitForOpen()
	// var volume C.uint64_t
	// status := C.TSGetDataVolume(
	// 	r.tdb,
	// 	C.TSTableID(tableID),
	// 	C.uint64_t(startHashPoint),
	// 	C.uint64_t(endHashPoint),
	// 	C.KwTsSpan{
	// 		begin: C.int64_t(startTimestamp),
	// 		end:   C.int64_t(endTimestamp),
	// 	},
	// 	&volume,
	// )
	// if err := statusToError(status); err != nil {
	// 	log.Errorf(context.TODO(), "GetDataVolume failed. err is :%+v. tableID: %d startHashPoint: %d, endHashPoint:%d startTimeStamp: %d, endTimeStamp: %d",
	// 		err, tableID, startHashPoint, endHashPoint, startTimestamp, endTimestamp)
	// 	return 0, errors.Wrap(err, "get data Volume failed")
	// }
	// return uint64(volume), nil
	return 0, nil
}

// GetDataVolumeHalfTS returns haslTS
func (r *TsEngine) GetDataVolumeHalfTS(
	tableID uint64, startHashPoint, endHashPoint uint64, startTimestamp, endTimestamp int64,
) (int64, error) {
	return 0, nil
}

// GetAvgTableRowSize gets AvgTableRowSize
func (r *TsEngine) GetAvgTableRowSize(tableID uint64) (uint64, error) {
	return 0, nil
}

// TsFetcher collect information in explain analyse
type TsFetcher struct {
	Collected bool
	CFetchers []byte
	Size      int
}

// TsFetcherStats collect information in explain analyse
type TsFetcherStats struct {
	ProcessorID        int32
	ProcessorName      int8
	RowNum             int64
	StallTime          int64   // time of execute
	BytesRead          int64   // byte of rows
	MaxAllocatedMem    int64   // maximum number of memory
	MaxAllocatedDisk   int64   // Maximum number of disk
	OutputRowNum       int64   // row of aggregation
	MemoryBlockCount   int32   // scanned memory block count
	LastBlockCount     int32   // scanned last block count
	EntityBlockCount   int64   // scanned entity block count
	PartitionAggCount  int64   // partition pre-aggregation usage count
	BlockCacheHitRatio float32 //entity block cache hit ratio
	BlockBytes         int64   // scanned block bytes
	AggBytes           int64   // scanned agg bytes
	HeaderBytes        int64   // scanned header bytes
	// BuildTime only be used for hash tag scan op for multiple model processing
	// when the switch is on and the server starts with single node mode.
	BuildTime int64 // hash tag build time
}

// sendDmlMsgToTs call the tsengine dml interface to issue a request and return the result
func (r *TsEngine) tsExecute(
	ctx *context.Context, tsQueryInfo TsQueryInfo,
) (tsRespInfo TsQueryInfo, err error) {
	return tsQueryInfo, err
}

// tsVectorizedExecute Vectorized tsengine.go:137Execute
func (r *TsEngine) tsVectorizedExecute(
	ctx *context.Context, tsQueryInfo TsQueryInfo, rev *TsDataChunkToGo,
) (tsRespInfo TsQueryInfo, err error) {

	return tsQueryInfo, err
}

// DeleteEntities delete entity, containing tag data and ts data
func (r *TsEngine) DeleteEntities(
	tableID uint64,
	rangeGroupID uint64,
	primaryTags [][]byte,
	isDrop bool,
	tsTxnID uint64,
	osnID uint64,
) (uint64, error) {
	return 0, nil
}

// DeleteRangeData delete entities data in the range
func (r *TsEngine) DeleteRangeData(
	tableID uint64,
	rangeGroupID uint64,
	beginHash uint64,
	endHash uint64,
	tsSpans []*roachpb.TsSpan,
	tsTxnID uint64,
	osnID uint64,
) (uint64, error) {
	return 0, nil
}

// DeleteTsRangeData delete entities data in the range
func (r *TsEngine) DeleteTsRangeData(
	tableID, beginHash, endHash uint64, startTs, endTs int64, tsTxnID uint64,
) error {
	return nil
}

// DeleteData delete some one entity data
func (r *TsEngine) DeleteData(
	tableID uint64,
	rangeGroupID uint64,
	primaryTag []byte,
	tsSpans []*roachpb.TsSpan,
	tsTxnID uint64,
	osnID uint64,
) (uint64, error) {
	return 0, nil
}

// CountRangeData delete entities data in the range
func (r *TsEngine) CountRangeData(
	tableID uint64,
	rangeGroupID uint64,
	beginHash uint64,
	endHash uint64,
	tsSpans []*roachpb.TsSpan,
	tsTxnID uint64,
	osnID uint64,
) (uint64, error) {
	return 0, nil
}

// Vacuum vacuum partitions
func (r *TsEngine) Vacuum(ctx context.Context, manual bool, onlyAgg bool) error {
	// if r == nil {
	// 	return nil
	// }
	// r.checkOrWaitForOpen()
	// status := C.TSVacuum(r.tdb)
	// if err := statusToError(status); err != nil {
	// 	return errors.Wrap(err, "failed to vacuum ts storage")
	// }
	return nil
}

// MigrateTsTable migrate partitions for hot and cold data tiering
func (r *TsEngine) MigrateTsTable(tableID uint64) error {
	return nil
}

// CancelBatchJob roll back data when backup or restore failed
func (r *TsEngine) CancelBatchJob(jobID uint64) error {
	return nil
}

// BatchJobFinish clear cache data when backup or restore succeed
func (r *TsEngine) BatchJobFinish(jobID uint64) error {
	return nil
}

// TsTableAutonomy Autonomous Evaluation
func (r *TsEngine) TsTableAutonomy(tableID uint64) error {
	// if r == nil {
	// 	return nil
	// }
	// r.checkOrWaitForOpen()
	// status := C.TSTableAutonomy(r.tdb, C.TSTableID(tableID))
	// if err := statusToError(status); err != nil {
	// 	return errors.Wrap(err, "failed to ts table's autonomy")
	// }
	return nil
}

// CollectTsStorageData collect TS storage statistics data
func (r *TsEngine) CollectTsStorageData(tableID uint64) (uint64, uint64, uint64, uint64, error) {
	//var entityNum C.uint64_t
	//var insertRowsPerDay C.uint64_t
	//var maxEntitiesOfLastSubgroup C.uint64_t
	//var partitionInterval C.uint64_t
	//
	//if r == nil {
	//	log.Errorf(context.TODO(), "null pointer of TsEngine, r.tbd: %d", tableID)
	//} else {
	//	log.Infof(context.TODO(), "CollectTsStorageData: %d from tsengine", tableID)
	//	status := C.CollectTsStorageData(r.tdb, C.TSTableID(tableID), &entityNum, &insertRowsPerDay, &maxEntitiesOfLastSubgroup)
	//	if err := statusToError(status); err != nil {
	//		log.Errorf(context.TODO(), "error when collect ts table statistics of table %d from tsengine, error: %s", tableID, err)
	//		return 0, 0, 0, 0, errors.Wrap(err, "failed to collect storage data")
	//	}
	//	status = C.TSGetPartitionInterval(r.tdb, C.TSTableID(tableID), &partitionInterval)
	//	if err := statusToError(status); err != nil {
	//		log.Errorf(context.TODO(), "error when collect partition interval of ts table %d from tsengine, error: %s", tableID, err)
	//		return 0, 0, 0, 0, errors.Wrap(err, "failed to collect storage data")
	//	}
	//}

	return 0, 0, 0, 0, nil
}

// SetupTsFlow send timing execution plan and receive execution results
func (r *TsEngine) SetupTsFlow(
	ctx *context.Context, tsQueryInfo TsQueryInfo,
) (tsRespInfo TsQueryInfo, err error) {
	// r.checkOrWaitForOpen()
	// return r.tsExecute(ctx, C.MQ_TYPE_DML_SETUP, tsQueryInfo)
	return tsQueryInfo, nil
}

// NextTsFlow drive timing execution plan, receive execution results
func (r *TsEngine) NextTsFlow(
	ctx *context.Context, tsQueryInfo TsQueryInfo,
) (tsRespInfo TsQueryInfo, err error) {
	// r.checkOrWaitForOpen()
	// return r.tsExecute(ctx, C.MQ_TYPE_DML_NEXT, tsQueryInfo)
	return tsQueryInfo, nil
}

// NextVectorizedTsFlow drive timing execution plan, receive execution results
func (r *TsEngine) NextVectorizedTsFlow(
	ctx *context.Context, tsQueryInfo TsQueryInfo, rev *TsDataChunkToGo,
) (tsRespInfo TsQueryInfo, err error) {
	// return r.tsVectorizedExecute(ctx, C.MQ_TYPE_DML_VECTORIZE_NEXT, tsQueryInfo, rev)
	return tsQueryInfo, nil
}

// NextTsFlowPgWire drive timing execution plan, receive execution results
func (r *TsEngine) NextTsFlowPgWire(
	ctx *context.Context, tsQueryInfo TsQueryInfo,
) (tsRespInfo TsQueryInfo, err error) {
	// r.checkOrWaitForOpen()
	// return r.tsExecute(ctx, C.MQ_TYPE_DML_PG_RESULT, tsQueryInfo)
	return tsQueryInfo, nil
}

// PushTsFlow drive pushing relational data to TSEngine for join
// only push PUSH type req and pass data chunk pointer to tse for multiple model processing
// when the switch is on and the server starts with single node mode.
func (r *TsEngine) PushTsFlow(ctx *context.Context, tsQueryInfo TsQueryInfo) (err error) {
	// r.checkOrWaitForOpen()
	// _, err = r.tsExecute(ctx, C.MQ_TYPE_DML_PUSH, tsQueryInfo)
	// return err
	return nil
}

// CloseTsFlow close the TS actuator corresponding to the current flow
func (r *TsEngine) CloseTsFlow(ctx *context.Context, tsQueryInfo TsQueryInfo) (err error) {
	// r.checkOrWaitForOpen()
	// _, err = r.tsExecute(ctx, C.MQ_TYPE_DML_CLOSE, tsQueryInfo)
	// return err
	return nil
}

// InitTsHandle corresponding to init ts handle
func (r *TsEngine) InitTsHandle(
	ctx *context.Context, tsQueryInfo TsQueryInfo,
) (tsRespInfo TsQueryInfo, err error) {
	// r.checkOrWaitForOpen()
	// return r.tsExecute(ctx, C.MQ_TYPE_DML_INIT, tsQueryInfo)
	return tsQueryInfo, nil
}

// FlushBuffer flush WALs of all ts tables to files in the node
func (r *TsEngine) FlushBuffer() error {
	// r.checkOrWaitForOpen()
	// status := C.TSFlushBuffer(r.tdb)
	// if err := statusToError(status); err != nil {
	// 	return errors.Wrap(err, "failed to flush WAL buffer")
	// }

	return nil
}

// Checkpoint create checkpoint
func (r *TsEngine) Checkpoint() error {
	// r.checkOrWaitForOpen()
	// status := C.TSCreateCheckpoint(r.tdb)
	// if err := statusToError(status); err != nil {
	// 	return errors.Wrap(err, "failed to create WAL checkpoint")
	// }

	return nil
}

// CheckpointForTable create checkpoint for a table
func (r *TsEngine) CheckpointForTable(tableID uint32) error {
	//r.CheckOrWaitForOpen()
	//status := C.TSCreateCheckpointForTable(r.tdb, C.uint64_t(tableID))
	//if err := statusToError(status); err != nil {
	//	return err
	//}
	return nil
}

// CheckpointInReplication a replication checkpoint
func (r *TsEngine) CheckpointInReplication(vgroupIDs, lsns []uint64) error {
	//log.Infof(context.TODO(), "CheckpointInReplication from lsn ,vgroups are %+v, lsns are %+v", vgroupIDs, lsns)
	//r.CheckOrWaitForOpen()
	//var tableNum C.uint64_t
	//var cVgroupIDs *C.uint64_t
	//var cLSNs *C.uint64_t
	//tableNum = C.uint64_t(len(vgroupIDs))
	//cVgroupIDs = (*C.uint64_t)(C.malloc(C.size_t(tableNum) * C.size_t(unsafe.Sizeof(C.uint64_t(0)))))
	//defer C.free(unsafe.Pointer(cVgroupIDs))
	//cLSNs = (*C.uint64_t)(C.malloc(C.size_t(tableNum) * C.size_t(unsafe.Sizeof(C.uint64_t(0)))))
	//defer C.free(unsafe.Pointer(cLSNs))
	//
	//for i := 0; i < len(vgroupIDs); i++ {
	//	tableID := vgroupIDs[i]
	//	lsn := lsns[i]
	//	ptr := (*C.uint64_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cVgroupIDs)) + uintptr(i)*unsafe.Sizeof(C.uint64_t(0))))
	//	*ptr = C.uint64_t(tableID)
	//	ptr = (*C.uint64_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cLSNs)) + uintptr(i)*unsafe.Sizeof(C.uint64_t(0))))
	//	*ptr = C.uint64_t(lsn)
	//}
	//status := C.TSCreateCheckpointInReplication(r.tdb, tableNum, cVgroupIDs, cLSNs)
	//for i := 0; i < len(vgroupIDs); i++ {
	//	ptr := (*C.uint64_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cLSNs)) + uintptr(i)*unsafe.Sizeof(C.uint64_t(0))))
	//	lsns[i] = uint64(*ptr)
	//}
	//if err := statusToError(status); err != nil {
	//	return errors.Wrap(err, "failed to InitWALReaders")
	//}
	return nil
}

// LockVgroupWithTimeout is lock vgroup with timeout, used for replication
func (r *TsEngine) LockVgroupWithTimeout(vgroupID uint64, timeout uint64) error {
	//r.CheckOrWaitForOpen()
	//status := C.TSLockVgroupWithTimeoutForReplication(r.tdb, C.uint64_t(vgroupID), C.uint64_t(timeout))
	//if err := statusToError(status); err != nil {
	//	return err
	//}
	return nil
}

// ResetAllReaders is reset all readers in tsEngine
func (r *TsEngine) ResetAllReaders() error {
	//r.CheckOrWaitForOpen()
	//status := C.ResetAllReaders(r.tdb)
	//if err := statusToError(status); err != nil {
	//	return err
	//}
	return nil
}

// DeleteRangeGroup Delete RangeGroup
func (r *TsEngine) DeleteRangeGroup(tableID uint64, rangeGroup api.RangeGroup) error {
	// r.checkOrWaitForOpen()
	// cRangeGroup := C.RangeGroup{
	// 	range_group_id: C.uint64_t(rangeGroup.RangeGroupID),
	// }
	// status := C.TSDeleteRangeGroup(r.tdb, C.TSTableID(tableID), cRangeGroup)
	// if err := statusToError(status); err != nil {
	// 	return errors.Wrap(err, "failed to delete range group")
	// }
	return nil
}

// WriteBatchData write batch data
func (r *TsEngine) WriteBatchData(
	tableID uint64, tableVersion uint64, jobID uint64, data []byte,
) (uint32, error) {
	//if len(data) == 0 {
	//	return 0, errors.New("batch data is null")
	//}
	//
	//r.CheckOrWaitForOpen()
	//cTsSlice := C.TSSlice{
	//	data: (*C.char)(C.CBytes(data)),
	//	len:  C.size_t(len(data)),
	//}
	//defer C.free(unsafe.Pointer(cTsSlice.data))
	//
	//var writeCnt C.uint32_t
	//status := C.TSWriteBatchData(r.tdb, C.TSTableID(tableID), C.uint64_t(tableVersion),
	//	C.uint64_t(jobID), &cTsSlice, &writeCnt)
	//if err := statusToError(status); err != nil {
	//	return 0, errors.Wrap(err, "failed to write batch data")
	//}
	return 0, nil
}

// ReadBatchData get batch data
func (r *TsEngine) ReadBatchData(
	tableID uint64,
	tableVersion uint64,
	beginHash uint64,
	endHash uint64,
	beginTs int64,
	endTs int64,
	jobID uint64,
) ([]byte, uint32, error) {

	return nil, 0, nil
}

// CreateSnapshotForRead create snapshot
func (r *TsEngine) CreateSnapshotForRead(
	tableID uint64, beginHash uint64, endHash uint64, beginTs int64, endTs int64,
) (uint64, error) {

	return 0, nil
}

// CreateSnapshotForWrite preparing for writing snapshots
func (r *TsEngine) CreateSnapshotForWrite(
	tableID uint64, beginHash uint64, endHash uint64, beginTs int64, endTs int64,
) (uint64, error) {

	return 0, nil
}

// GetSnapshotNextBatchData get data of the snapshot
func (r *TsEngine) GetSnapshotNextBatchData(tableID uint64, snapshotID uint64) ([]byte, error) {

	return nil, nil
}

// WriteSnapshotBatchData write snapshot data
func (r *TsEngine) WriteSnapshotBatchData(tableID uint64, snapshotID uint64, data []byte) error {

	return nil
}

// WriteSnapshotSuccess apply snapshot
func (r *TsEngine) WriteSnapshotSuccess(tableID uint64, snapshotID uint64) error {

	return nil
}

// WriteSnapshotRollback rollback snapshot
func (r *TsEngine) WriteSnapshotRollback(tableID uint64, snapshotID uint64) error {

	return nil
}

// DeleteSnapshot drops Snapshot.
func (r *TsEngine) DeleteSnapshot(tableID uint64, snapshotID uint64) error {

	return nil
}

// MtrBegin BEGIN a TS mini-transaction
func (r *TsEngine) MtrBegin(
	tableID uint64, rangeGroupID uint64, rangeID uint64, index uint64, transactionID []byte,
) (uint64, error) {

	return 0, nil
}

// MtrCommit COMMIT a TS mini-transaction
func (r *TsEngine) MtrCommit(
	tableID uint64, rangeGroupID uint64, miniTransID uint64, transactionID []byte,
) error {

	return nil
}

// MtrRollback ROLLBACK a TS mini-transaction
func (r *TsEngine) MtrRollback(
	tableID uint64, rangeGroupID uint64, miniTransID uint64, transactionID []byte,
) error {

	return nil
}

// TransBegin BEGIN a TS transaction
func (r *TsEngine) TransBegin(tableID uint64, transactionID []byte) error {

	return nil
}

// TransCommit COMMIT a TS transaction
func (r *TsEngine) TransCommit(tableID uint64, transactionID []byte) error {

	return nil
}

// TransRollback ROLLBACK a TS transaction
func (r *TsEngine) TransRollback(tableID uint64, transactionID []byte) error {

	return nil
}

// TSGetWaitThreadNum is used to get wait thread num from time series engine
func (r *TsEngine) TSGetWaitThreadNum() (uint32, error) {

	return 0, nil
}

// SetCompressInterval send compress interval to AE
func SetCompressInterval(interval []byte) {

}

// SetVacuumInterval send vacuum interval to AE
func SetVacuumInterval(interval []byte) {

}

// Close close TsEngine
func (r *TsEngine) Close() {

}

func (r *TsEngine) manageWAL() {

}

// DeleteReplicaTSData delete replica ts data
func (r *TsEngine) DeleteReplicaTSData(
	tableID uint64, beginHash uint64, endHash uint64, startTs int64, endTs int64,
) error {

	return nil
}

// NewTsFetcher init tsFetcher
func NewTsFetcher(specs []execinfrapb.ProcessorSpec) []byte {

	return nil
}

// AddStatsList add data to statsList
func AddStatsList(tsFetcher TsFetcher, stats []TsFetcherStats) []TsFetcherStats {

	return nil
}

// GetTsVersion get current version of ts table
func (r *TsEngine) GetTsVersion(tableID uint64) (uint32, error) {

	return 0, nil
}

// GetWalLevel get current wal level of ts engine
func (r *TsEngine) GetWalLevel() (int, error) {

	return 0, nil
}

func (r *TsEngine) GetTSWalLevel() (int, error) {
	return 0, nil
}

func (r *TsEngine) WriteDDLWALForRepl(
	walLog []byte, length uint64, tableID uint64,
) (uint64, error) {
	return 0, nil
}

// GetTableMetaByVersion is used for unit test, try to get the tableMeta with specific tsVersion
func (r *TsEngine) GetTableMetaByVersion(tableID uint64, tsVer uint64) error {

	return nil
}

func (r *TsEngine) CheckReplReady() bool {
	return true
}

// SDDLIfExist is check  wal if exist in wal0
func (r *TsEngine) SDDLIfExist(txnID []byte) bool {

	return false
}

// GetReplicationWalLogs is get wal logs from file
func (r *TsEngine) GetReplicationWalLogs() ([][]byte, func(), error) {
	return nil, nil, nil
}

// InitWALReaders init wal reader
func (r *TsEngine) InitWALReaders(concurrencyMap map[uint64]uint64) error {
	return nil
}

// TSEngineNotInitErr is  err of tsengine not init
var TSEngineNotInitErr = errors.Errorf("no tsengine")

// ApplyWAL by RecoverByLog
func (r *TsEngine) ApplyWAL(tableID uint64, data []byte, lsn uint64) error {
	return nil
}

// GetCurrentTablesAndLSNs get current tables and lsns
func (r *TsEngine) GetCurrentTablesAndLSNs() (tables []uint64, currentLSNs []uint64, err error) {
	return nil, nil, nil
}

// GetWALChecksumAndCurrentLSN check sum
func (r *TsEngine) GetWALChecksumAndCurrentLSN(
	tableIDs, startLSNs, endLSNs []uint64,
) (checksums []uint64, currentLSNs []uint64, err error) {
	return nil, nil, nil
}

// GetCurrentLSNs get current lsns
func (r *TsEngine) GetCurrentLSNs(tableIDs []uint64) (currentLSNs []uint64, err error) {
	return nil, nil
}

// RollbackFromLSN roll back from lsn
func (r *TsEngine) RollbackFromLSN(tableIDs, lsns []uint64) error {
	return nil
}

// WriteWALForReceiver  write  wal to receiver engine
func (r *TsEngine) WriteWALForReceiver(
	tableID uint64, walLog []byte, length uint64,
) (uint64, error) {
	return 0, nil
}

// TsDeleteMetricByTag deletes metric data by specific tag values and ts span
// Parameters:
//
//	beginHash - Start value of the replica hash range
//	endHash - End value of the replica hash range
//	primaryTags - List of primary tag values
//	indexColumns - List of tag IDs (uint32) corresponding to the primaryTags
//	tsSpans - Time range spans to filter data (only delete data within these time ranges)
//
// Returns:
//
//	uint64 - Number of rows successfully deleted
func (r *TsEngine) TsDeleteMetricByTag(
	tableID uint64,
	beginHash uint64,
	endHash uint64,
	primaryTags [][]byte,
	indexColumns []uint32,
	tsSpans []*roachpb.TsSpan,
	tsTxnID uint64,
	osnID uint64,
) (uint64, error) {
	return 0, nil
}

// TsDeleteEntitiesByTag delete entity, including tag data and ts data
// Parameters:
//
//	beginHash - Start value of the replica hash range
//	endHash - End value of the replica hash range
//	primaryTags - List of part primary tag values
//	indexColumns - List of tag IDs corresponding to the primaryTags
//
// Returns:
//
//	uint64 - Number of rows successfully deleted
func (r *TsEngine) TsDeleteEntitiesByTag(
	tableID uint64,
	beginHash uint64,
	endHash uint64,
	primaryTags [][]byte,
	indexColumns []uint32,
	isDrop bool,
	tsTxnID uint64,
	osnID uint64,
) (uint64, error) {
	return 0, nil
}

// ResetWALLogCheckpoints is reset wal checkpoint
func (r *TsEngine) ResetWALLogCheckpoints(tableIDs, lsns []uint64) error {
	return nil
}

// CancelTsFlow cancel the TS actuator corresponding to the current flow
func (r *TsEngine) CancelTsFlow(ctx *context.Context, tsQueryInfo TsQueryInfo) (err error) {
	return err
}

// ShowTsWal show the wal
func ShowTsWal(dbPath string, tableID uint64, ergID uint64) error {
	return nil
}

// ShowTsWalV2 is show ts wal withe store v3.0
func ShowTsWalV2(dbPath string, vgroup string, showAll bool, showPtag bool) error {
	return nil
}
