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

package status

/*
#cgo CPPFLAGS: -I../../../../common/src/h
#cgo LDFLAGS: -lcommon
#include "../../../../common/src/h/mm_malloc_cgo.h"
*/
import "C"

import (
	"bufio"
	"context"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/build"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"
	"github.com/shirou/gopsutil/net"
)

var (
	metaCgoCalls = metric.Metadata{
		Name:        "sys.cgocalls",
		Help:        "Total number of cgo calls",
		Measurement: "cgo Calls",
		Unit:        metric.Unit_COUNT,
	}
	metaGoroutines = metric.Metadata{
		Name:        "sys.goroutines",
		Help:        "Current number of goroutines",
		Measurement: "goroutines",
		Unit:        metric.Unit_COUNT,
	}
	metaGoAllocBytes = metric.Metadata{
		Name:        "sys.go.allocbytes",
		Help:        "Current bytes of memory allocated by go",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaGoTotalBytes = metric.Metadata{
		Name:        "sys.go.totalbytes",
		Help:        "Total bytes of memory allocated by go, but not released",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaCgoAllocBytes = metric.Metadata{
		Name:        "sys.cgo.allocbytes",
		Help:        "Current bytes of memory allocated by cgo",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaCgoTotalBytes = metric.Metadata{
		Name:        "sys.cgo.totalbytes",
		Help:        "Total bytes of memory allocated by cgo, but not released",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaGCCount = metric.Metadata{
		Name:        "sys.gc.count",
		Help:        "Total number of GC runs",
		Measurement: "GC Runs",
		Unit:        metric.Unit_COUNT,
	}
	metaGCPauseNS = metric.Metadata{
		Name:        "sys.gc.pause.ns",
		Help:        "Total GC pause",
		Measurement: "GC Pause",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaGCPausePercent = metric.Metadata{
		Name:        "sys.gc.pause.percent",
		Help:        "Current GC pause percentage",
		Measurement: "GC Pause",
		Unit:        metric.Unit_PERCENT,
	}
	metaCPUUserNS = metric.Metadata{
		Name:        "sys.cpu.user.ns",
		Help:        "Total user cpu time",
		Measurement: "CPU Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaCPUUserPercent = metric.Metadata{
		Name:        "sys.cpu.user.percent",
		Help:        "Current user cpu percentage",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
	}
	metaCPUSysNS = metric.Metadata{
		Name:        "sys.cpu.sys.ns",
		Help:        "Total system cpu time",
		Measurement: "CPU Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaCPUSysPercent = metric.Metadata{
		Name:        "sys.cpu.sys.percent",
		Help:        "Current system cpu percentage",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
	}
	metaCPUCombinedPercentNorm = metric.Metadata{
		Name:        "sys.cpu.combined.percent-normalized",
		Help:        "Current user+system cpu percentage, normalized 0-1 by number of cores",
		Measurement: "CPU Time",
		Unit:        metric.Unit_PERCENT,
	}
	metaRSSBytes = metric.Metadata{
		Name:        "sys.rss",
		Help:        "Current process RSS",
		Measurement: "RSS",
		Unit:        metric.Unit_BYTES,
	}
	metaFDOpen = metric.Metadata{
		Name:        "sys.fd.open",
		Help:        "Process open file descriptors",
		Measurement: "File Descriptors",
		Unit:        metric.Unit_COUNT,
	}
	metaFDSoftLimit = metric.Metadata{
		Name:        "sys.fd.softlimit",
		Help:        "Process open FD soft limit",
		Measurement: "File Descriptors",
		Unit:        metric.Unit_COUNT,
	}
	metaUptime = metric.Metadata{
		Name:        "sys.uptime",
		Help:        "Process uptime",
		Measurement: "Uptime",
		Unit:        metric.Unit_SECONDS,
	}
	metaKmallocBytes = metric.Metadata{
		Name:        "sys.kmalloc.totalbytes",
		Help:        "Current bytes of memory allocated by Kmalloc",
		Measurement: "Kamlloc",
		Unit:        metric.Unit_BYTES,
	}
	metaMemoryMapVirtualBytes = metric.Metadata{
		Name:        "sys.memory.map.virtualbytes",
		Help:        "Total virtual bytes of memory map",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaMemoryMapPhysicalBytes = metric.Metadata{
		Name:        "sys.memory.map.physicalbytes",
		Help:        "Total physical bytes of memory map allocated",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaMemoryMapCount = metric.Metadata{
		Name:        "sys.memory.map.totalcount",
		Help:        "Total region count of memory map allocated",
		Measurement: "Memory",
		Unit:        metric.Unit_COUNT,
	}

	// These disk and network stats are counters of the number of operations, packets, bytes, and
	// cumulative time of the disk and net IO that has been done across the whole host *since this
	// Cockroach process started up*. By taking the derivatives of these metrics, we can see the
	// IO throughput.
	metaHostDiskReadCount = metric.Metadata{
		Name:        "sys.host.disk.read.count",
		Unit:        metric.Unit_COUNT,
		Measurement: "Operations",
		Help:        "Disk read operations across all disks since this process started",
	}
	metaHostDiskReadBytes = metric.Metadata{
		Name:        "sys.host.disk.read.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes read from all disks since this process started",
	}
	metaHostDiskReadTime = metric.Metadata{
		Name:        "sys.host.disk.read.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Time spent reading from all disks since this process started",
	}
	metaHostDiskWriteCount = metric.Metadata{
		Name:        "sys.host.disk.write.count",
		Unit:        metric.Unit_COUNT,
		Measurement: "Operations",
		Help:        "Disk write operations across all disks since this process started",
	}
	metaHostDiskWriteBytes = metric.Metadata{
		Name:        "sys.host.disk.write.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes written to all disks since this process started",
	}
	metaHostDiskWriteTime = metric.Metadata{
		Name:        "sys.host.disk.write.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Time spent writing to all disks since this process started",
	}
	metaHostDiskIOTime = metric.Metadata{
		Name:        "sys.host.disk.io.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Time spent reading from or writing to all disks since this process started",
	}
	metaHostDiskWeightedIOTime = metric.Metadata{
		Name:        "sys.host.disk.weightedio.time",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Time",
		Help:        "Weighted time spent reading from or writing to to all disks since this process started",
	}
	metaHostIopsInProgress = metric.Metadata{
		Name:        "sys.host.disk.iopsinprogress",
		Unit:        metric.Unit_COUNT,
		Measurement: "Operations",
		Help:        "IO operations currently in progress on this host",
	}
	metaHostNetRecvBytes = metric.Metadata{
		Name:        "sys.host.net.recv.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes received on all network interfaces since this process started",
	}
	metaHostNetRecvPackets = metric.Metadata{
		Name:        "sys.host.net.recv.packets",
		Unit:        metric.Unit_COUNT,
		Measurement: "Packets",
		Help:        "Packets received on all network interfaces since this process started",
	}
	metaHostNetSendBytes = metric.Metadata{
		Name:        "sys.host.net.send.bytes",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
		Help:        "Bytes sent on all network interfaces since this process started",
	}
	metaHostNetSendPackets = metric.Metadata{
		Name:        "sys.host.net.send.packets",
		Unit:        metric.Unit_COUNT,
		Measurement: "Packets",
		Help:        "Packets sent on all network interfaces since this process started",
	}
)

// getCgoMemStats is a function that fetches stats for the C++ portion of the code.
// We will not necessarily have implementations for all builds, so check for nil first.
// Returns the following:
// allocated uint: bytes allocated by application
// total     uint: total bytes requested from system
// error           : any issues fetching stats. This should be a warning only.
var getCgoMemStats func(context.Context) (uint, uint, error)

// RuntimeStatSampler is used to periodically sample the runtime environment
// for useful statistics, performing some rudimentary calculations and storing
// the resulting information in a format that can be easily consumed by status
// logging systems.
type RuntimeStatSampler struct {
	clock *hlc.Clock

	startTimeNanos int64
	// The last sampled values of some statistics are kept only to compute
	// derivative statistics.
	last struct {
		now         int64
		utime       int64
		stime       int64
		cgoCall     int64
		gcCount     int64
		gcPauseTime uint64
		disk        diskStats
		net         net.IOCountersStat
	}

	initialDiskCounters diskStats
	initialNetCounters  net.IOCountersStat

	// Only show "not implemented" errors once, we don't need the log spam.
	fdUsageNotImplemented bool

	// Metric gauges maintained by the sampler.
	// Go runtime stats.
	CgoCalls          *metric.Gauge
	Goroutines        *metric.Gauge
	GoAllocBytes      *metric.Gauge
	GoTotalBytes      *metric.Gauge
	CgoAllocBytes     *metric.Gauge
	CgoTotalBytes     *metric.Gauge
	KmallocTotalBytes *metric.Gauge
	MmapVirtualBytes  *metric.Gauge
	MmapPhysicalBytes *metric.Gauge
	MmapTotalCount    *metric.Gauge
	GcCount           *metric.Gauge
	GcPauseNS         *metric.Gauge
	GcPausePercent    *metric.GaugeFloat64
	// CPU stats.
	CPUUserNS              *metric.Gauge
	CPUUserPercent         *metric.GaugeFloat64
	CPUSysNS               *metric.Gauge
	CPUSysPercent          *metric.GaugeFloat64
	CPUCombinedPercentNorm *metric.GaugeFloat64
	// Memory stats.
	RSSBytes *metric.Gauge
	// File descriptor stats.
	FDOpen      *metric.Gauge
	FDSoftLimit *metric.Gauge
	// Disk and network stats.
	HostDiskReadBytes      *metric.Gauge
	HostDiskReadCount      *metric.Gauge
	HostDiskReadTime       *metric.Gauge
	HostDiskWriteBytes     *metric.Gauge
	HostDiskWriteCount     *metric.Gauge
	HostDiskWriteTime      *metric.Gauge
	HostDiskIOTime         *metric.Gauge
	HostDiskWeightedIOTime *metric.Gauge
	IopsInProgress         *metric.Gauge
	HostNetRecvBytes       *metric.Gauge
	HostNetRecvPackets     *metric.Gauge
	HostNetSendBytes       *metric.Gauge
	HostNetSendPackets     *metric.Gauge
	// Uptime and build.
	Uptime         *metric.Gauge // We use a gauge to be able to call Update.
	BuildTimestamp *metric.Gauge
}

// NewRuntimeStatSampler constructs a new RuntimeStatSampler object.
func NewRuntimeStatSampler(ctx context.Context, clock *hlc.Clock) *RuntimeStatSampler {
	// Construct the build info metric. It is constant.
	// We first build set the labels on the metadata.
	info := build.GetInfo()
	timestamp, err := info.Timestamp()
	if err != nil {
		// We can't panic here, tests don't have a build timestamp.
		log.Warningf(ctx, "Could not parse build timestamp: %v", err)
	}

	// Build information.
	metaBuildTimestamp := metric.Metadata{
		Name:        "build.timestamp",
		Help:        "Build information",
		Measurement: "Build Time",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaBuildTimestamp.AddLabel("tag", info.Tag)
	metaBuildTimestamp.AddLabel("go_version", info.GoVersion)

	buildTimestamp := metric.NewGauge(metaBuildTimestamp)
	buildTimestamp.Update(timestamp)

	diskCounters, err := getSummedDiskCounters(ctx)
	if err != nil {
		log.Errorf(ctx, "could not get initial disk IO counters: %v", err)
	}
	netCounters, err := getSummedNetStats(ctx)
	if err != nil {
		log.Errorf(ctx, "could not get initial disk IO counters: %v", err)
	}

	rsr := &RuntimeStatSampler{
		clock:                  clock,
		startTimeNanos:         clock.PhysicalNow(),
		initialNetCounters:     netCounters,
		initialDiskCounters:    diskCounters,
		CgoCalls:               metric.NewGauge(metaCgoCalls),
		Goroutines:             metric.NewGauge(metaGoroutines),
		GoAllocBytes:           metric.NewGauge(metaGoAllocBytes),
		GoTotalBytes:           metric.NewGauge(metaGoTotalBytes),
		CgoAllocBytes:          metric.NewGauge(metaCgoAllocBytes),
		CgoTotalBytes:          metric.NewGauge(metaCgoTotalBytes),
		KmallocTotalBytes:      metric.NewGauge(metaKmallocBytes),
		MmapVirtualBytes:       metric.NewGauge(metaMemoryMapVirtualBytes),
		MmapPhysicalBytes:      metric.NewGauge(metaMemoryMapPhysicalBytes),
		MmapTotalCount:         metric.NewGauge(metaMemoryMapCount),
		GcCount:                metric.NewGauge(metaGCCount),
		GcPauseNS:              metric.NewGauge(metaGCPauseNS),
		GcPausePercent:         metric.NewGaugeFloat64(metaGCPausePercent),
		CPUUserNS:              metric.NewGauge(metaCPUUserNS),
		CPUUserPercent:         metric.NewGaugeFloat64(metaCPUUserPercent),
		CPUSysNS:               metric.NewGauge(metaCPUSysNS),
		CPUSysPercent:          metric.NewGaugeFloat64(metaCPUSysPercent),
		CPUCombinedPercentNorm: metric.NewGaugeFloat64(metaCPUCombinedPercentNorm),
		RSSBytes:               metric.NewGauge(metaRSSBytes),
		HostDiskReadBytes:      metric.NewGauge(metaHostDiskReadBytes),
		HostDiskReadCount:      metric.NewGauge(metaHostDiskReadCount),
		HostDiskReadTime:       metric.NewGauge(metaHostDiskReadTime),
		HostDiskWriteBytes:     metric.NewGauge(metaHostDiskWriteBytes),
		HostDiskWriteCount:     metric.NewGauge(metaHostDiskWriteCount),
		HostDiskWriteTime:      metric.NewGauge(metaHostDiskWriteTime),
		HostDiskIOTime:         metric.NewGauge(metaHostDiskIOTime),
		HostDiskWeightedIOTime: metric.NewGauge(metaHostDiskWeightedIOTime),
		IopsInProgress:         metric.NewGauge(metaHostIopsInProgress),
		HostNetRecvBytes:       metric.NewGauge(metaHostNetRecvBytes),
		HostNetRecvPackets:     metric.NewGauge(metaHostNetRecvPackets),
		HostNetSendBytes:       metric.NewGauge(metaHostNetSendBytes),
		HostNetSendPackets:     metric.NewGauge(metaHostNetSendPackets),
		FDOpen:                 metric.NewGauge(metaFDOpen),
		FDSoftLimit:            metric.NewGauge(metaFDSoftLimit),
		Uptime:                 metric.NewGauge(metaUptime),
		BuildTimestamp:         buildTimestamp,
	}
	rsr.last.disk = rsr.initialDiskCounters
	rsr.last.net = rsr.initialNetCounters
	return rsr
}

// GoMemStats groups a runtime.MemStats structure with the timestamp when it
// was collected.
type GoMemStats struct {
	runtime.MemStats
	// Collected is the timestamp at which these values were collected.
	Collected time.Time
}

// CGoMemStats reports what has been allocated outside of Go.
type CGoMemStats struct {
	// CGoAllocated represents allocated bytes.
	CGoAllocatedBytes int64
	// CGoTotal represents total bytes (allocated + metadata etc).
	CGoTotalBytes int64
}

// KmallocMemStats reports what has been allocated by Kmalloc
type KmallocMemStats struct {
	// KmallocTotalBytes represents Kmalloc allocated bytes.
	KmallocTotalBytes int64
}

// MmapStats reports what has been allocated by memory map
type MmapStats struct {
	// MmapVirtualBytes is the virtual size of memory map, the unit is bytes.
	MmapVirtualBytes int64
	// MmapPhysicalBytes is the physical size of memory map, the unit is bytes.
	MmapPhysicalBytes int64
	// MmapTotalCount is the number of memory map regions, the unit is count.
	MmapTotalCount int64
}

// GetCGoMemStats collects non-Go memory statistics.
func GetCGoMemStats(ctx context.Context) *CGoMemStats {
	var cgoAllocated, cgoTotal uint
	if getCgoMemStats != nil {
		var err error
		cgoAllocated, cgoTotal, err = getCgoMemStats(ctx)
		if err != nil {
			log.Warningf(ctx, "problem fetching CGO memory stats: %s; CGO stats will be empty.", err)
		}
	}
	return &CGoMemStats{
		CGoAllocatedBytes: int64(cgoAllocated),
		CGoTotalBytes:     int64(cgoTotal),
	}
}

// GetKmallocStats collects Kmalloc statistics.
func GetKmallocStats(ctx context.Context) *KmallocMemStats {
	var kmallocTotal uint
	kmallocTotal = uint(C.malloc_used_bytes())
	return &KmallocMemStats{
		KmallocTotalBytes: int64(kmallocTotal),
	}
}

// GetMmapStatsBySysFile collects memory map size by system file.
func GetMmapStatsBySysFile(ctx context.Context) *MmapStats {
	var mmapVirtualBytes uint
	var mmapPhysicalBytes uint
	var mmapCount uint

	pid := os.Getpid()
	statusPath := "/proc/" + strconv.Itoa(pid) + "/status"
	mapsPath := "/proc/" + strconv.Itoa(pid) + "/maps"
	statmPath := "/proc/" + strconv.Itoa(pid) + "/statm"

	// Get the virtual memory map size.
	statusFile, err := os.Open(statusPath)
	if err != nil {
		log.Errorf(ctx, "Error opening file %s: %s", statusPath, err)
	} else {
		defer statusFile.Close()
		scanner := bufio.NewScanner(statusFile)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "VmSize:") {
				fields := strings.Fields(line)
				if len(fields) < 2 {
					continue
				}
				sizeStr := strings.TrimSpace(fields[1])
				size, err := strconv.ParseInt(sizeStr, 10, 64)
				if err != nil {
					log.Errorf(ctx, "Error parsing VmSize from %s: %s", statusPath, err)
				}
				// VmSize is in kB, Convert from kB to bytes
				mmapVirtualBytes = uint(size * 1024)
				break
			}
		}
	}

	// Get the physical memory map size.
	statmFile, err := os.Open(statmPath)
	if err != nil {
		log.Errorf(ctx, "Error opening file %s: %s", statmPath, err)
	} else {
		defer statmFile.Close()
		scanner := bufio.NewScanner(statmFile)
		if scanner.Scan() {
			line := scanner.Text()
			pageSize := int64(os.Getpagesize())
			fields := strings.Fields(line)
			if len(fields) >= 3 {
				sizeStr := fields[2]
				size, err := strconv.ParseInt(sizeStr, 10, 64)
				if err != nil {
					log.Errorf(ctx, "Error parsing shared pages in %s: %s", statmPath, err)
					size = 0
				}
				mmapPhysicalBytes = uint(size * pageSize)
			} else {
				log.Errorf(ctx, "Not enough fields in %s", statmPath)
			}
		}
	}

	// Get the total memory map range count.
	mapsFile, err := os.Open(mapsPath)
	if err != nil {
		log.Errorf(ctx, "Error opening file %s: %s", mapsPath, err)
	} else {
		defer mapsFile.Close()
		scanner := bufio.NewScanner(mapsFile)
		for scanner.Scan() {
			mmapCount++
		}
	}

	return &MmapStats{
		MmapVirtualBytes:  int64(mmapVirtualBytes),
		MmapPhysicalBytes: int64(mmapPhysicalBytes),
		MmapTotalCount:    int64(mmapCount),
	}
}

// SampleEnvironment queries the runtime system for various interesting metrics,
// storing the resulting values in the set of metric gauges maintained by
// RuntimeStatSampler. This makes runtime statistics more convenient for
// consumption by the time series and status systems.
//
// This method should be called periodically by a higher level system in order
// to keep runtime statistics current.
//
// SampleEnvironment takes GoMemStats as input because that is collected
// separately, on a different schedule.
// The CGoMemStats should be provided via GetCGoMemStats().
func (rsr *RuntimeStatSampler) SampleEnvironment(
	ctx context.Context, ms *GoMemStats, cs *CGoMemStats,
) {
	// Note that debug.ReadGCStats() does not suffer the same problem as
	// runtime.ReadMemStats(). The only way you can know that is by reading the
	// source.
	gc := &debug.GCStats{}
	debug.ReadGCStats(gc)

	numCgoCall := runtime.NumCgoCall()
	numGoroutine := runtime.NumGoroutine()

	// Retrieve Mem and CPU statistics.
	pid := os.Getpid()
	mem := gosigar.ProcMem{}
	if err := mem.Get(pid); err != nil {
		log.Errorf(ctx, "unable to get mem usage: %v", err)
	}
	cpuTime := gosigar.ProcTime{}
	if err := cpuTime.Get(pid); err != nil {
		log.Errorf(ctx, "unable to get cpu usage: %v", err)
	}

	fds := gosigar.ProcFDUsage{}
	if err := fds.Get(pid); err != nil {
		if _, ok := err.(gosigar.ErrNotImplemented); ok {
			if !rsr.fdUsageNotImplemented {
				rsr.fdUsageNotImplemented = true
				log.Warningf(ctx, "unable to get file descriptor usage (will not try again): %s", err)
			}
		} else {
			log.Errorf(ctx, "unable to get file descriptor usage: %s", err)
		}
	}

	var deltaDisk diskStats
	diskCounters, err := getSummedDiskCounters(ctx)
	if err != nil {
		log.Warningf(ctx, "problem fetching disk stats: %s; disk stats will be empty.", err)
	} else {
		deltaDisk = diskCounters
		subtractDiskCounters(&deltaDisk, rsr.last.disk)
		rsr.last.disk = diskCounters
		subtractDiskCounters(&diskCounters, rsr.initialDiskCounters)

		rsr.HostDiskReadBytes.Update(diskCounters.readBytes)
		rsr.HostDiskReadCount.Update(diskCounters.readCount)
		rsr.HostDiskReadTime.Update(int64(diskCounters.readTime))
		rsr.HostDiskWriteBytes.Update(diskCounters.writeBytes)
		rsr.HostDiskWriteCount.Update(diskCounters.writeCount)
		rsr.HostDiskWriteTime.Update(int64(diskCounters.writeTime))
		rsr.HostDiskIOTime.Update(int64(diskCounters.ioTime))
		rsr.HostDiskWeightedIOTime.Update(int64(diskCounters.weightedIOTime))
		rsr.IopsInProgress.Update(diskCounters.iopsInProgress)
	}

	var deltaNet net.IOCountersStat
	netCounters, err := getSummedNetStats(ctx)
	if err != nil {
		log.Warningf(ctx, "problem fetching net stats: %s; net stats will be empty.", err)
	} else {
		deltaNet = netCounters
		subtractNetworkCounters(&deltaNet, rsr.last.net)
		rsr.last.net = netCounters
		subtractNetworkCounters(&netCounters, rsr.initialNetCounters)

		rsr.HostNetSendBytes.Update(int64(netCounters.BytesSent))
		rsr.HostNetSendPackets.Update(int64(netCounters.PacketsSent))
		rsr.HostNetRecvBytes.Update(int64(netCounters.BytesRecv))
		rsr.HostNetRecvPackets.Update(int64(netCounters.PacketsRecv))
	}

	// Time statistics can be compared to the total elapsed time to create a
	// useful percentage of total CPU usage, which would be somewhat less accurate
	// if calculated later using downsampled time series data.
	now := rsr.clock.PhysicalNow()
	dur := float64(now - rsr.last.now)
	// cpuTime.{User,Sys} are in milliseconds, convert to nanoseconds.
	utime := int64(cpuTime.User) * 1e6
	stime := int64(cpuTime.Sys) * 1e6
	uPerc := float64(utime-rsr.last.utime) / dur
	sPerc := float64(stime-rsr.last.stime) / dur
	combinedNormalizedPerc := (sPerc + uPerc) / float64(runtime.NumCPU())
	gcPausePercent := float64(uint64(gc.PauseTotal)-rsr.last.gcPauseTime) / dur
	rsr.last.now = now
	rsr.last.utime = utime
	rsr.last.stime = stime
	rsr.last.gcPauseTime = uint64(gc.PauseTotal)

	var kmallocTotal = GetKmallocStats(ctx).KmallocTotalBytes
	mmapStats := GetMmapStatsBySysFile(ctx)
	var mmapVirtualBytes = mmapStats.MmapVirtualBytes
	var mmapPhysicalBytes = mmapStats.MmapPhysicalBytes
	var mmapTotalCount = mmapStats.MmapTotalCount

	// Log summary of statistics to console.
	cgoRate := float64((numCgoCall-rsr.last.cgoCall)*int64(time.Second)) / dur
	goMemStatsStale := timeutil.Now().Sub(ms.Collected) > time.Second
	var staleMsg = ""
	if goMemStatsStale {
		staleMsg = "(stale)"
	}
	goTotal := ms.Sys - ms.HeapReleased
	log.Infof(ctx, "runtime stats: %s RSS, %d goroutines, %s/%s/%s GO alloc/idle/total%s, "+
		"%s/%s CGO alloc/total, %s Kmalloc total, %.1f CGO/sec, %.1f/%.1f %%(u/s)time, %.1f %%gc (%dx), "+
		"%s/%s (r/w)net",
		humanize.IBytes(mem.Resident), numGoroutine,
		humanize.IBytes(ms.HeapAlloc), humanize.IBytes(ms.HeapIdle), humanize.IBytes(goTotal),
		staleMsg,
		humanize.IBytes(uint64(cs.CGoAllocatedBytes)), humanize.IBytes(uint64(cs.CGoTotalBytes)),
		humanize.IBytes(uint64(kmallocTotal)),
		cgoRate, 100*uPerc, 100*sPerc, 100*gcPausePercent, gc.NumGC-rsr.last.gcCount,
		humanize.IBytes(deltaNet.BytesRecv), humanize.IBytes(deltaNet.BytesSent),
	)
	rsr.last.cgoCall = numCgoCall
	rsr.last.gcCount = gc.NumGC

	rsr.GoAllocBytes.Update(int64(ms.HeapAlloc))
	rsr.GoTotalBytes.Update(int64(goTotal))
	rsr.CgoCalls.Update(numCgoCall)
	rsr.Goroutines.Update(int64(numGoroutine))
	rsr.CgoAllocBytes.Update(cs.CGoAllocatedBytes)
	rsr.CgoTotalBytes.Update(cs.CGoTotalBytes)
	rsr.KmallocTotalBytes.Update(kmallocTotal)
	rsr.MmapVirtualBytes.Update(mmapVirtualBytes)
	rsr.MmapPhysicalBytes.Update(mmapPhysicalBytes)
	rsr.MmapTotalCount.Update(mmapTotalCount)
	rsr.GcCount.Update(gc.NumGC)
	rsr.GcPauseNS.Update(int64(gc.PauseTotal))
	rsr.GcPausePercent.Update(gcPausePercent)
	rsr.CPUUserNS.Update(utime)
	rsr.CPUUserPercent.Update(uPerc)
	rsr.CPUSysNS.Update(stime)
	rsr.CPUSysPercent.Update(sPerc)
	rsr.CPUCombinedPercentNorm.Update(combinedNormalizedPerc)
	rsr.FDOpen.Update(int64(fds.Open))
	rsr.FDSoftLimit.Update(int64(fds.SoftLimit))
	rsr.RSSBytes.Update(int64(mem.Resident))
	rsr.Uptime.Update((now - rsr.startTimeNanos) / 1e9)
}

// GetCPUCombinedPercentNorm is part of the rowexec.RuntimeStats interface.
func (rsr *RuntimeStatSampler) GetCPUCombinedPercentNorm() float64 {
	return rsr.CPUCombinedPercentNorm.Value()
}

// diskStats contains the disk statistics returned by the operating
// system. Interpretation of some of these stats varies by platform,
// although as much as possible they are normalized to the semantics
// used by linux's diskstats interface.
//
// Except for iopsInProgress, these metrics act like counters (always
// increasing, and best interpreted as a rate).
type diskStats struct {
	readBytes int64
	readCount int64

	// readTime (and writeTime) may increase more than 1s per second if
	// access to storage is parallelized.
	readTime time.Duration

	writeBytes int64
	writeCount int64
	writeTime  time.Duration

	// ioTime is the amount of time that iopsInProgress is non-zero (so
	// its increase is capped at 1s/s). Only available on linux.
	ioTime time.Duration

	// weightedIOTime is a linux-specific metric that attempts to
	// represent "an easy measure of both I/O completion time and the
	// backlog that may be accumulating."
	weightedIOTime time.Duration

	// iopsInProgress is a gauge of the number of pending IO operations.
	// Not available on macOS.
	iopsInProgress int64
}

func getSummedDiskCounters(ctx context.Context) (diskStats, error) {
	diskCounters, err := getDiskCounters(ctx)
	if err != nil {
		return diskStats{}, err
	}

	return sumDiskCounters(diskCounters), nil
}

func getSummedNetStats(ctx context.Context) (net.IOCountersStat, error) {
	netCounters, err := net.IOCountersWithContext(ctx, true /* per NIC */)
	if err != nil {
		return net.IOCountersStat{}, err
	}

	return sumNetworkCounters(netCounters), nil
}

// sumDiskCounters returns a new disk.IOCountersStat whose values are the sum of the
// values in the slice of disk.IOCountersStats passed in.
func sumDiskCounters(disksStats []diskStats) diskStats {
	output := diskStats{}
	for _, stats := range disksStats {
		output.readBytes += stats.readBytes
		output.readCount += stats.readCount
		output.readTime += stats.readTime

		output.writeBytes += stats.writeBytes
		output.writeCount += stats.writeCount
		output.writeTime += stats.writeTime

		output.ioTime += stats.ioTime
		output.weightedIOTime += stats.weightedIOTime

		output.iopsInProgress += stats.iopsInProgress
	}
	return output
}

// subtractDiskCounters subtracts the counters in `sub` from the counters in `from`,
// saving the results in `from`.
func subtractDiskCounters(from *diskStats, sub diskStats) {
	from.writeCount -= sub.writeCount
	from.writeBytes -= sub.writeBytes
	from.writeTime -= sub.writeTime

	from.readCount -= sub.readCount
	from.readBytes -= sub.readBytes
	from.readTime -= sub.readTime

	from.ioTime -= sub.ioTime
	from.weightedIOTime -= sub.weightedIOTime
}

// sumNetworkCounters returns a new net.IOCountersStat whose values are the sum of the
// values in the slice of net.IOCountersStats passed in.
func sumNetworkCounters(netCounters []net.IOCountersStat) net.IOCountersStat {
	output := net.IOCountersStat{}
	for _, counter := range netCounters {
		output.BytesRecv += counter.BytesRecv
		output.BytesSent += counter.BytesSent
		output.PacketsRecv += counter.PacketsRecv
		output.PacketsSent += counter.PacketsSent
	}
	return output
}

// subtractNetworkCounters subtracts the counters in `sub` from the counters in `from`,
// saving the results in `from`.
func subtractNetworkCounters(from *net.IOCountersStat, sub net.IOCountersStat) {
	from.BytesRecv -= sub.BytesRecv
	from.BytesSent -= sub.BytesSent
	from.PacketsRecv -= sub.PacketsRecv
	from.PacketsSent -= sub.PacketsSent
}
