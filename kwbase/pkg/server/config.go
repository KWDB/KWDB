// Copyright 2015 The Cockroach Authors.
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

package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/gossip/resolver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/status"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/ts"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"github.com/cockroachdb/pebble"
	"github.com/elastic/gosigar"
	"github.com/pkg/errors"
)

// Context defaults.
const (
	// DefaultCacheSize is the default size of the RocksDB and Pebble caches. We
	// default the cache size and SQL memory pool size to 128 MiB. Larger values
	// might provide significantly better performance, but we're not sure what
	// type of system we're running on (development or production or some shared
	// environment). Production users should almost certainly override these
	// settings and we'll warn in the logs about doing so.
	DefaultCacheSize         = 128 << 20 // 128 MB
	defaultSQLMemoryPoolSize = 128 << 20 // 128 MB
	defaultScanInterval      = 10 * time.Minute
	defaultScanMinIdleTime   = 10 * time.Millisecond
	defaultScanMaxIdleTime   = 1 * time.Second
	// NB: this can't easily become a variable as the UI hard-codes it to 10s.
	// See https://gitee.com/kwbasedb/kwbase/issues/20310.
	DefaultMetricsSampleInterval   = 10 * time.Second
	DefaultHistogramWindowInterval = 6 * DefaultMetricsSampleInterval
	defaultStorePath               = "kwbase-data"
	// TempDirPrefix is the filename prefix of any temporary subdirectory
	// created.
	TempDirPrefix = "kwbase-temp"
	// TempDirsRecordFilename is the filename for the record file
	// that keeps track of the paths of the temporary directories created.
	TempDirsRecordFilename = "temp-dirs-record.txt"
	defaultEventLogEnabled = true

	maximumMaxClockOffset = 5 * time.Second

	minimumNetworkFileDescriptors     = 256
	recommendedNetworkFileDescriptors = 5000

	defaultSQLTableStatCacheSize = 256

	// This comes out to 1024 cache entries.
	defaultSQLQueryCacheSize = 8 * 1024 * 1024

	defaultSQLPseudoCatalogCacheSize = 128

	configName = "kaiwudb.cfg"

	// Procedure cache occupies 64M.
	defaultProcedureCacheSize = 64 * 1024 * 1024
)

var productionSettingsWebpage = fmt.Sprintf(
	"please see %s for more details",
	base.DocsURL("recommended-production-settings.html"),
)

// MaxOffsetType stores the configured MaxOffset.
type MaxOffsetType time.Duration

// Type implements the pflag.Value interface.
func (mo *MaxOffsetType) Type() string {
	return "MaxOffset"
}

// Set implements the pflag.Value interface.
func (mo *MaxOffsetType) Set(v string) error {
	nanos, err := time.ParseDuration(v)
	if err != nil {
		return err
	}
	if nanos > maximumMaxClockOffset {
		return errors.Errorf("%s is not a valid max offset, must be less than %v.", v, maximumMaxClockOffset)
	}
	*mo = MaxOffsetType(nanos)
	return nil
}

// String implements the pflag.Value interface.
func (mo *MaxOffsetType) String() string {
	return time.Duration(*mo).String()
}

// Config holds parameters needed to setup a server.
type Config struct {
	// Embed the base context.
	*base.Config

	Settings *cluster.Settings

	base.RaftConfig

	// LeaseManagerConfig holds configuration values specific to the LeaseManager.
	LeaseManagerConfig *base.LeaseManagerConfig

	// SocketFile, if non-empty, sets up a TLS-free local listener using
	// a unix datagram socket at the specified path.
	SocketFile string

	// Stores is specified to enable durable key-value storage.
	Stores base.StoreSpecList

	// TsStores is specified to enable durable key-value storage.
	TsStores base.StoreSpecList

	// StorageEngine specifies the engine type (eg. rocksdb, pebble) to use to
	// instantiate stores.
	StorageEngine enginepb.EngineType

	// TempStorageConfig is used to configure temp storage, which stores
	// ephemeral data when processing large queries.
	TempStorageConfig base.TempStorageConfig

	// ExternalIOConfig is used to configure external storage
	// access (http://, nodelocal://, etc)
	ExternalIOConfig base.ExternalIOConfig

	// Attrs specifies a colon-separated list of node topography or machine
	// capabilities, used to match capabilities or location preferences specified
	// in zone configs.
	Attrs string

	// JoinList is a list of node addresses that act as bootstrap hosts for
	// connecting to the gossip network.
	JoinList base.JoinListType

	// JoinPreferSRVRecords, if set, causes the lookup logic for the
	// names in JoinList to prefer SRV records from DNS, if available,
	// to A/AAAA records.
	JoinPreferSRVRecords bool

	// RetryOptions controls the retry behavior of the server.
	RetryOptions retry.Options

	// CacheSize is the amount of memory in bytes to use for caching data.
	// The value is split evenly between the stores if there are more than one.
	CacheSize int64

	// TimeSeriesServerConfig contains configuration specific to the time series
	// server.
	TimeSeriesServerConfig ts.ServerConfig

	// SQLMemoryPoolSize is the amount of memory in bytes that can be
	// used by SQL clients to store row data in server RAM.
	SQLMemoryPoolSize int64

	// SQLAuditLogDirName is the target directory name for SQL audit logs.
	SQLAuditLogDirName *log.DirName

	// SQLTableStatCacheSize is the size (number of tables) of the table
	// statistics cache.
	SQLTableStatCacheSize int

	// CTableTagValuesCacheSize is the size (number of tables) of the child table
	// tagValues cache.
	CTableTagValuesCacheSize int

	// SQLQueryCacheSize is the memory size (in bytes) of the query plan cache.
	SQLQueryCacheSize int64

	//PseudoCatalogCacheSize is the  size (number of hint) of the pseudo catalog cache.
	SQLPseudoCatalogCacheSize int

	// GoroutineDumpDirName is the directory name for goroutine dumps using
	// goroutinedumper.
	GoroutineDumpDirName string

	// HeapProfileDirName is the directory name for heap profiles using
	// heapprofiler. If empty, no heap profiles will be collected.
	HeapProfileDirName string

	// Parsed values.

	// NodeAttributes is the parsed representation of Attrs.
	NodeAttributes roachpb.Attributes

	// GossipBootstrapResolvers is a list of gossip resolvers used
	// to find bootstrap nodes for connecting to the gossip network.
	GossipBootstrapResolvers []resolver.Resolver

	// The following values can only be set via environment variables and are
	// for testing only. They are not meant to be set by the end user.

	// Enables linearizable behavior of operations on this node by making sure
	// that no commit timestamp is reported back to the client until all other
	// node clocks have necessarily passed it.
	// Environment Variable: KWBASE_EXPERIMENTAL_LINEARIZABLE
	Linearizable bool

	// Maximum allowed clock offset for the cluster. If observed clock
	// offsets exceed this limit, inconsistency may result, and servers
	// will panic to minimize the likelihood of inconsistent data.
	// Increasing this value will increase time to recovery after
	// failures, and increase the frequency and impact of
	// ReadWithinUncertaintyIntervalError.
	MaxOffset MaxOffsetType

	// TimestampCachePageSize is the size in bytes of the pages in the
	// timestamp cache held by each store.
	TimestampCachePageSize uint32

	// ScanInterval determines a duration during which each range should be
	// visited approximately once by the range scanner. Set to 0 to disable.
	// Environment Variable: KWBASE_SCAN_INTERVAL
	ScanInterval time.Duration

	// ScanMinIdleTime is the minimum time the scanner will be idle between ranges.
	// If enabled (> 0), the scanner may complete in more than ScanInterval for large
	// stores.
	// Environment Variable: KWBASE_SCAN_MIN_IDLE_TIME
	ScanMinIdleTime time.Duration

	// ScanMaxIdleTime is the maximum time the scanner will be idle between ranges.
	// If enabled (> 0), the scanner may complete in less than ScanInterval for small
	// stores.
	// Environment Variable: KWBASE_SCAN_MAX_IDLE_TIME
	ScanMaxIdleTime time.Duration

	// TestingKnobs is used for internal test controls only.
	TestingKnobs base.TestingKnobs

	// AmbientCtx is used to annotate contexts used inside the server.
	AmbientCtx log.AmbientContext

	// DefaultZoneConfig is used to set the default zone config inside the server.
	// It can be overridden during tests by setting the DefaultZoneConfigOverride
	// server testing knob.
	DefaultZoneConfig zonepb.ZoneConfig
	// DefaultSystemZoneConfig is used to set the default system zone config
	// inside the server. It can be overridden during tests by setting the
	// DefaultSystemZoneConfigOverride server testing knob.
	DefaultSystemZoneConfig zonepb.ZoneConfig

	// Locality is a description of the topography of the server.
	Locality roachpb.Locality

	// LocalityAddresses contains private IP addresses the can only be accessed
	// in the corresponding locality.
	LocalityAddresses []roachpb.LocalityAddress

	// EventLogEnabled is a switch which enables recording into kwbase's SQL
	// event log tables. These tables record transactional events about changes
	// to cluster metadata, such as DDL statements and range rebalancing
	// actions.
	EventLogEnabled bool

	// ReadyFn is called when the server has started listening on its
	// sockets.
	// The argument waitForInit indicates (iff true) that the
	// server is not bootstrapped yet, will not bootstrap itself and
	// will be waiting for an `init` command or accept bootstrapping
	// from a joined node.
	ReadyFn func(waitForInit bool)

	// DelayedBootstrapFn is called if the boostrap process does not complete
	// in a timely fashion, typically 30s after the server starts listening.
	DelayedBootstrapFn func()

	// EnableWebSessionAuthentication enables session-based authentication for
	// the Admin API's HTTP endpoints.
	EnableWebSessionAuthentication bool

	enginesCreated bool

	// ModeFlag means whether current node is start mode.
	ModeFlag int

	// StartMode means kwbase start mode
	StartMode string

	// ImportPath is accept a path of import file
	ImportPath string
	// ImportPassword is accept a password of decompress file
	ImportPassword string
	// ImportFileName is accept a filename of decompress file
	ImportFileName string

	// LogConfig is a descriptor of the LogConfig of the server
	LogConfig log.Config

	// Forbid cache coredump for go test, for example TestFuncNull
	ForbidCatchCoreDump bool

	// ProcedureCacheSize is the memory size (in bytes) of the procedure.
	ProcedureCacheSize int64
}

const (
	// StartModeMPP mpp mode
	StartModeMPP = 1 << 0
	// StartModeSingleNode signle node mode
	StartModeSingleNode = 1 << 1
)

// SetMppModeFlag set mpp mode flag
func SetMppModeFlag(flags int) int {
	flags |= StartModeMPP
	return flags
}

// GetMppModeFlag get mpp mode flag
func GetMppModeFlag(flags int) bool {
	return flags&StartModeMPP > 0
}

// SetSingleNodeModeFlag set single node mode flag
func SetSingleNodeModeFlag(flags int) int {
	flags |= StartModeSingleNode
	return flags
}

// GetSingleNodeModeFlag get single node mode flag
func GetSingleNodeModeFlag(flags int) bool {
	return flags&StartModeSingleNode > 0
}

// HistogramWindowInterval is used to determine the approximate length of time
// that individual samples are retained in in-memory histograms. Currently,
// it is set to the arbitrary length of six times the Metrics sample interval.
//
// The length of the window must be longer than the sampling interval due to
// issue #12998, which was causing histograms to return zero values when sampled
// because all samples had been evicted.
//
// Note that this is only intended to be a temporary fix for the above issue,
// as our current handling of metric histograms have numerous additional
// problems. These are tracked in github issue #7896, which has been given
// a relatively high priority in light of recent confusion around histogram
// metrics. For more information on the issues underlying our histogram system
// and the proposed fixes, please see issue #7896.
func (cfg Config) HistogramWindowInterval() time.Duration {
	hwi := DefaultHistogramWindowInterval

	// Rudimentary overflow detection; this can result if
	// DefaultMetricsSampleInterval is set to an extremely large number, likely
	// in the context of a test or an intentional attempt to disable metrics
	// collection. Just return the default in this case.
	if hwi < DefaultMetricsSampleInterval {
		return DefaultMetricsSampleInterval
	}
	return hwi
}

// setOpenFileLimit sets the soft limit for open file descriptors to the hard
// limit if needed. Returns an error if the hard limit is too low. Returns the
// value to set maxOpenFiles to for each store.
//
// # Minimum - 1700 per store, 256 saved for networking
//
// # Constrained - 256 saved for networking, rest divided evenly per store
//
// # Constrained (network only) - 10000 per store, rest saved for networking
//
// # Recommended - 10000 per store, 5000 for network
//
// Please note that current and max limits are commonly referred to as the soft
// and hard limits respectively.
//
// On Windows there is no need to change the file descriptor, known as handles,
// limit. This limit cannot be changed and is approximately 16,711,680. See
// https://blogs.technet.microsoft.com/markrussinovich/2009/09/29/pushing-the-limits-of-windows-handles/
func setOpenFileLimit(physicalStoreCount int) (uint64, error) {
	return setOpenFileLimitInner(physicalStoreCount)
}

// SetOpenFileLimitForOneStore sets the soft limit for open file descriptors
// when there is only one store.
func SetOpenFileLimitForOneStore() (uint64, error) {
	return setOpenFileLimit(1)
}

// MakeConfig returns a Context with default values.
func MakeConfig(ctx context.Context, st *cluster.Settings) Config {
	storeSpec, err := base.NewStoreSpec(defaultStorePath)
	if err != nil {
		panic(err)
	}

	disableWebLogin := envutil.EnvOrDefaultBool("KWBASE_DISABLE_WEB_LOGIN", false)

	cfg := Config{
		Config:                         new(base.Config),
		DefaultZoneConfig:              zonepb.DefaultZoneConfig(),
		DefaultSystemZoneConfig:        zonepb.DefaultSystemZoneConfig(),
		MaxOffset:                      MaxOffsetType(base.DefaultMaxClockOffset),
		Settings:                       st,
		CacheSize:                      DefaultCacheSize,
		SQLMemoryPoolSize:              defaultSQLMemoryPoolSize,
		SQLTableStatCacheSize:          defaultSQLTableStatCacheSize,
		SQLQueryCacheSize:              defaultSQLQueryCacheSize,
		SQLPseudoCatalogCacheSize:      defaultSQLPseudoCatalogCacheSize,
		ScanInterval:                   defaultScanInterval,
		ScanMinIdleTime:                defaultScanMinIdleTime,
		ScanMaxIdleTime:                defaultScanMaxIdleTime,
		EventLogEnabled:                defaultEventLogEnabled,
		EnableWebSessionAuthentication: !disableWebLogin,
		Stores: base.StoreSpecList{
			Specs: []base.StoreSpec{storeSpec},
		},
		StorageEngine: storage.DefaultStorageEngine,
		TempStorageConfig: base.TempStorageConfigFromEnv(
			ctx, st, storeSpec, "" /* parentDir */, base.DefaultTempStorageMaxSizeBytes, 0),
		ProcedureCacheSize: defaultProcedureCacheSize,
	}
	cfg.AmbientCtx.Tracer = st.Tracer

	cfg.Config.InitDefaults()
	cfg.RaftConfig.SetDefaults()
	cfg.LeaseManagerConfig = base.NewLeaseManagerConfig()

	return cfg
}

// String implements the fmt.Stringer interface.
func (cfg *Config) String() string {
	var buf bytes.Buffer

	w := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
	fmt.Fprintln(w, "max offset\t", cfg.MaxOffset)
	fmt.Fprintln(w, "cache size\t", humanizeutil.IBytes(cfg.CacheSize))
	fmt.Fprintln(w, "SQL memory pool size\t", humanizeutil.IBytes(cfg.SQLMemoryPoolSize))
	fmt.Fprintln(w, "scan interval\t", cfg.ScanInterval)
	fmt.Fprintln(w, "scan min idle time\t", cfg.ScanMinIdleTime)
	fmt.Fprintln(w, "scan max idle time\t", cfg.ScanMaxIdleTime)
	fmt.Fprintln(w, "event log enabled\t", cfg.EventLogEnabled)
	if cfg.Linearizable {
		fmt.Fprintln(w, "linearizable\t", cfg.Linearizable)
	}
	_ = w.Flush()

	return buf.String()
}

// Report logs an overview of the server configuration parameters via
// the given context.
func (cfg *Config) Report(ctx context.Context) {
	if memSize, err := status.GetTotalMemory(ctx); err != nil {
		log.Infof(ctx, "unable to retrieve system total memory: %v", err)
	} else {
		log.Infof(ctx, "system total memory: %s", humanizeutil.IBytes(memSize))
	}
	log.Info(ctx, "server configuration:\n", cfg)
}

// Engines is a container of engines, allowing convenient closing.
type Engines []storage.Engine

// Close closes all the Engines.
// This method has a pointer receiver so that the following pattern works:
//
//	func f() {
//		engines := Engines(engineSlice)
//		defer engines.Close()  // make sure the engines are Closed if this
//		                       // function returns early.
//		... do something with engines, pass ownership away...
//		engines = nil  // neutralize the preceding defer
//	}
func (e *Engines) Close() {
	for _, eng := range *e {
		eng.Close()
	}
	*e = nil
}

// CreateEngines creates Engines based on the specs in cfg.Stores.
func (cfg *Config) CreateEngines(ctx context.Context) (Engines, error) {
	engines := Engines(nil)
	defer engines.Close()

	if cfg.enginesCreated {
		return Engines{}, errors.Errorf("engines already created")
	}
	if cfg.TsStores.Specs == nil {
		cfg.TsStores = cfg.Stores
	}
	cfg.enginesCreated = true

	var details []string

	var cache storage.RocksDBCache
	var pebbleCache *pebble.Cache
	if cfg.StorageEngine == enginepb.EngineTypePebble || cfg.StorageEngine == enginepb.EngineTypeTeePebbleRocksDB {
		details = append(details, fmt.Sprintf("Pebble cache size: %s", humanizeutil.IBytes(cfg.CacheSize)))
		pebbleCache = pebble.NewCache(cfg.CacheSize)
		defer pebbleCache.Unref()
	}
	if cfg.StorageEngine == enginepb.EngineTypeDefault ||
		cfg.StorageEngine == enginepb.EngineTypeRocksDB || cfg.StorageEngine == enginepb.EngineTypeTeePebbleRocksDB {
		details = append(details, fmt.Sprintf("RocksDB cache size: %s", humanizeutil.IBytes(cfg.CacheSize)))
		cache = storage.NewRocksDBCache(cfg.CacheSize)
		defer cache.Release()
	}

	var physicalStores int
	for _, spec := range cfg.Stores.Specs {
		if !spec.InMemory {
			physicalStores++
		}
	}
	openFileLimitPerStore, err := setOpenFileLimit(physicalStores)
	if err != nil {
		return Engines{}, err
	}

	log.Event(ctx, "initializing engines")

	skipSizeCheck := cfg.TestingKnobs.Store != nil &&
		cfg.TestingKnobs.Store.(*kvserver.StoreTestingKnobs).SkipMinSizeCheck
	for i, spec := range cfg.Stores.Specs {
		log.Eventf(ctx, "initializing %+v", spec)
		var sizeInBytes = spec.Size.InBytes
		if spec.InMemory {
			if spec.Size.Percent > 0 {
				sysMem, err := status.GetTotalMemory(ctx)
				if err != nil {
					return Engines{}, errors.Errorf("could not retrieve system memory")
				}
				sizeInBytes = int64(float64(sysMem) * spec.Size.Percent / 100)
			}
			if sizeInBytes != 0 && !skipSizeCheck && sizeInBytes < base.MinimumStoreSize {
				return Engines{}, errors.Errorf("%f%% of memory is only %s bytes, which is below the minimum requirement of %s",
					spec.Size.Percent, humanizeutil.IBytes(sizeInBytes), humanizeutil.IBytes(base.MinimumStoreSize))
			}
			details = append(details, fmt.Sprintf("store %d: in-memory, size %s",
				i, humanizeutil.IBytes(sizeInBytes)))
			if spec.StickyInMemoryEngineID != "" {
				e, err := getOrCreateStickyInMemEngine(
					ctx, spec.StickyInMemoryEngineID, cfg.StorageEngine, spec.Attributes, sizeInBytes,
				)
				if err != nil {
					return Engines{}, err
				}
				engines = append(engines, e)
			} else {
				engines = append(engines, storage.NewInMem(ctx, cfg.StorageEngine, spec.Attributes, sizeInBytes))
			}
		} else {
			if spec.Size.Percent > 0 {
				fileSystemUsage := gosigar.FileSystemUsage{}
				if err := fileSystemUsage.Get(spec.Path); err != nil {
					return Engines{}, err
				}
				sizeInBytes = int64(float64(fileSystemUsage.Total) * spec.Size.Percent / 100)
			}
			if sizeInBytes != 0 && !skipSizeCheck && sizeInBytes < base.MinimumStoreSize {
				return Engines{}, errors.Errorf("%f%% of %s's total free space is only %s bytes, which is below the minimum requirement of %s",
					spec.Size.Percent, spec.Path, humanizeutil.IBytes(sizeInBytes), humanizeutil.IBytes(base.MinimumStoreSize))
			}

			details = append(details, fmt.Sprintf("store %d: RocksDB, max size %s, max open file limit %d",
				i, humanizeutil.IBytes(sizeInBytes), openFileLimitPerStore))

			var eng storage.Engine
			var err error
			storageConfig := base.StorageConfig{
				Attrs:           spec.Attributes,
				Dir:             spec.Path,
				TsDir:           cfg.TsStores.Specs[i].Path,
				MaxSize:         sizeInBytes,
				Settings:        cfg.Settings,
				UseFileRegistry: spec.UseFileRegistry,
				ExtraOptions:    spec.ExtraOptions,
			}
			if cfg.StorageEngine == enginepb.EngineTypePebble {
				// TODO(itsbilal): Tune these options, and allow them to be overridden
				// in the spec (similar to the existing spec.RocksDBOptions and others).
				pebbleConfig := storage.PebbleConfig{
					StorageConfig: storageConfig,
					Opts:          storage.DefaultPebbleOptions(),
				}
				pebbleConfig.Opts.Cache = pebbleCache
				pebbleConfig.Opts.MaxOpenFiles = int(openFileLimitPerStore)
				eng, err = storage.NewPebble(ctx, pebbleConfig)
			} else if cfg.StorageEngine == enginepb.EngineTypeRocksDB || cfg.StorageEngine == enginepb.EngineTypeDefault {
				rocksDBConfig := storage.RocksDBConfig{
					StorageConfig:           storageConfig,
					MaxOpenFiles:            openFileLimitPerStore,
					WarnLargeBatchThreshold: 500 * time.Millisecond,
					RocksDBOptions:          spec.RocksDBOptions,
				}

				eng, err = storage.NewRocksDB(rocksDBConfig, cache)
			} else {
				// cfg.StorageEngine == enginepb.EngineTypeTeePebbleRocksDB
				pebbleConfig := storage.PebbleConfig{
					StorageConfig: storageConfig,
					Opts:          storage.DefaultPebbleOptions(),
				}
				pebbleConfig.Dir = filepath.Join(pebbleConfig.Dir, "pebble")
				pebbleConfig.Opts.Cache = pebbleCache
				pebbleConfig.Opts.MaxOpenFiles = int(openFileLimitPerStore)
				pebbleEng, err := storage.NewPebble(ctx, pebbleConfig)
				if err != nil {
					return nil, err
				}

				rocksDBConfig := storage.RocksDBConfig{
					StorageConfig:           storageConfig,
					MaxOpenFiles:            openFileLimitPerStore,
					WarnLargeBatchThreshold: 500 * time.Millisecond,
					RocksDBOptions:          spec.RocksDBOptions,
				}
				rocksDBConfig.Dir = filepath.Join(rocksDBConfig.Dir, "rocksdb")

				rocksdbEng, err := storage.NewRocksDB(rocksDBConfig, cache)
				if err != nil {
					return nil, err
				}

				eng = storage.NewTee(ctx, rocksdbEng, pebbleEng)
			}
			if err != nil {
				return Engines{}, err
			}
			engines = append(engines, eng)
		}
	}

	log.Infof(ctx, "%d storage engine%s initialized",
		len(engines), util.Pluralize(int64(len(engines))))
	for _, s := range details {
		log.Info(ctx, s)
	}
	enginesCopy := engines
	engines = nil
	return enginesCopy, nil
}

// CreateTsEngine create ts engine
func (cfg *Config) CreateTsEngine(
	ctx context.Context, stopper *stop.Stopper,
) (*tse.TsEngine, error) {

	threadPoolSize, err := strconv.Atoi(cfg.ThreadPoolSize)
	if err != nil {
		return nil, err
	}

	taskQueueSize, err := strconv.Atoi(cfg.TaskQueueSize)
	if err != nil {
		return nil, err
	}

	bufferPoolSize, err := strconv.Atoi(cfg.BufferPoolSize)
	if err != nil {
		return nil, err
	}

	//TODO Use the rocksdb store directory +tsdb suffix
	tsConfig := tse.TsEngineConfig{
		Dir:            cfg.TsStores.Specs[0].Path,
		Settings:       cfg.Settings,
		ThreadPoolSize: threadPoolSize,
		TaskQueueSize:  taskQueueSize,
		BufferPoolSize: bufferPoolSize,
		LogCfg:         cfg.LogConfig,
		IsSingleNode:   GetSingleNodeModeFlag(cfg.ModeFlag),
	}
	tsDB, err := tse.NewTsEngine(ctx, tsConfig, stopper)
	if err != nil {
		return nil, err
	}
	return tsDB, nil
}

// InitNode parses node attributes and initializes the gossip bootstrap
// resolvers.
func (cfg *Config) InitNode(ctx context.Context) error {
	cfg.readEnvironmentVariables()

	// Initialize attributes.
	cfg.NodeAttributes = parseAttributes(cfg.Attrs)

	// Expose HistogramWindowInterval to parts of the code that can't import the
	// server package. This code should be cleaned up within a month or two.
	cfg.Config.HistogramWindowInterval = cfg.HistogramWindowInterval()

	// Get the gossip bootstrap resolvers.
	resolvers, err := cfg.parseGossipBootstrapResolvers(ctx)
	if err != nil {
		return err
	}
	if len(resolvers) > 0 {
		cfg.GossipBootstrapResolvers = resolvers
	}

	return nil
}

// FilterGossipBootstrapResolvers removes any gossip bootstrap resolvers which
// match either this node's listen address or its advertised host address.
func (cfg *Config) FilterGossipBootstrapResolvers(
	ctx context.Context, listen, advert net.Addr,
) []resolver.Resolver {
	filtered := make([]resolver.Resolver, 0, len(cfg.GossipBootstrapResolvers))
	addrs := make([]string, 0, len(cfg.GossipBootstrapResolvers))
	for _, r := range cfg.GossipBootstrapResolvers {
		if r.Addr() == advert.String() || r.Addr() == listen.String() {
			if log.V(1) {
				log.Infof(ctx, "skipping -join address %q, because a node cannot join itself", r.Addr())
			}
		} else {
			filtered = append(filtered, r)
			addrs = append(addrs, r.Addr())
		}
	}
	if log.V(1) {
		log.Infof(ctx, "initial resolvers: %v", addrs)
	}
	return filtered
}

// RequireWebSession indicates whether the server should require authentication
// sessions when serving admin API requests.
func (cfg *Config) RequireWebSession() bool {
	return !cfg.Insecure && cfg.EnableWebSessionAuthentication
}

// readEnvironmentVariables populates all context values that are environment
// variable based. Note that this only happens when initializing a node and not
// when NewContext is called.
func (cfg *Config) readEnvironmentVariables() {
	cfg.Linearizable = envutil.EnvOrDefaultBool("KWBASE_EXPERIMENTAL_LINEARIZABLE", cfg.Linearizable)
	cfg.ScanInterval = envutil.EnvOrDefaultDuration("KWBASE_SCAN_INTERVAL", cfg.ScanInterval)
	cfg.ScanMinIdleTime = envutil.EnvOrDefaultDuration("KWBASE_SCAN_MIN_IDLE_TIME", cfg.ScanMinIdleTime)
	cfg.ScanMaxIdleTime = envutil.EnvOrDefaultDuration("KWBASE_SCAN_MAX_IDLE_TIME", cfg.ScanMaxIdleTime)
}

// parseGossipBootstrapResolvers parses list of gossip bootstrap resolvers.
func (cfg *Config) parseGossipBootstrapResolvers(ctx context.Context) ([]resolver.Resolver, error) {
	var bootstrapResolvers []resolver.Resolver
	for _, address := range cfg.JoinList {
		if cfg.JoinPreferSRVRecords {
			// The following code substitutes the entry in --join by the
			// result of SRV resolution, if suitable SRV records are found
			// for that name.
			//
			// TODO(knz): Delay this lookup. The logic for "regular" resolvers
			// is delayed until the point the connection is attempted, so that
			// fresh DNS records are used for a new connection. This makes
			// it possible to update DNS records without restarting the node.
			// The SRV logic here does not have this property (yet).
			srvAddrs, err := resolver.SRV(ctx, address)
			if err != nil {
				return nil, err
			}

			if len(srvAddrs) > 0 {
				for _, sa := range srvAddrs {
					resolver, err := resolver.NewResolver(sa)
					if err != nil {
						return nil, err
					}
					bootstrapResolvers = append(bootstrapResolvers, resolver)
				}

				continue
			}
		}

		// Otherwise, use the address.
		resolver, err := resolver.NewResolver(address)
		if err != nil {
			return nil, err
		}
		bootstrapResolvers = append(bootstrapResolvers, resolver)
	}

	return bootstrapResolvers, nil
}

// parseAttributes parses a colon-separated list of strings,
// filtering empty strings (i.e. "::" will yield no attributes.
// Returns the list of strings as Attributes.
func parseAttributes(attrsStr string) roachpb.Attributes {
	var filtered []string
	for _, attr := range strings.Split(attrsStr, ":") {
		if len(attr) != 0 {
			filtered = append(filtered, attr)
		}
	}
	return roachpb.Attributes{Attrs: filtered}
}
