// Copyright 2019 The Cockroach Authors.
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

package catalog

// chart_catalog.go represents a catalog of pre-defined Admin UI charts
// to aid users in debugging CockroachDB clusters. This file represents
// a simplified structure of the catalog, meant to make it easier for
// developers to add charts to the catalog. You can find more detail at
// pkg/ts/catalog/catalog_generator.go.

// The structure of the catalog is not rigorously defined and should be
// iterated on as it's used.

// A few notes:
// - When adding sectionDescriptions...
//		- The first element of the organization field's inner array values
//       must be one of the consts defined in catalog_generator.go.
//		- All of the inner arrays must have either 2 or 3 levels.
// - When adding chartDescriptions...
//		Undefined values in chartDescriptions will be completed using default
//		values based on the type of metrics used in the chart
//		(chartDefaultsPerMetricType).

var charts = []sectionDescription{
	{
		Organization: [][]string{{SQLLayer, "SQL attribute"}},
		Charts: []chartDescription{
			{
				Title:   "Relational Data Replication Latency",
				Metrics: []string{"replica.rengine.data.latency"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL attribute"}},
		Charts: []chartDescription{
			{
				Title:   "TS Data Replication Latency",
				Metrics: []string{"replica.tsengine.data.latency"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL attribute"}},
		Charts: []chartDescription{
			{
				Title:   "TS Data Replication Total Row Count",
				Metrics: []string{"replica.tsengine.data.total_count"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL attribute"}},
		Charts: []chartDescription{
			{
				Title:   "TS Data Replication Completed Row Count",
				Metrics: []string{"replica.tsengine.data.completed_count"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL attribute"}},
		Charts: []chartDescription{
			{
				Title:   "Latency between active and standby nodes",
				Metrics: []string{"replica.clock-offset.meannanos"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit attribute"}},
		Charts: []chartDescription{
			{
				Title:   "audit attribute",
				Metrics: []string{"audit.ATTRIBUTE.CREATE"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit audits"}},
		Charts: []chartDescription{
			{
				Title:   "audit audits",
				Metrics: []string{"audit.AUDIT.ALTER", "audit.AUDIT.CREATE", "audit.AUDIT.DROP"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit changefeed"}},
		Charts: []chartDescription{
			{
				Title:   "audit changefeed",
				Metrics: []string{"audit.CHANGEFEED.CREATE"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit cluster"}},
		Charts: []chartDescription{
			{
				Title:   "audit cluster",
				Metrics: []string{"audit.CLUSTER.INIT"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit connect"}},
		Charts: []chartDescription{
			{
				Title:   "audit connect",
				Metrics: []string{"audit.CONN.LOGIN", "audit.CONN.LOGOUT"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit database"}},
		Charts: []chartDescription{
			{
				Title:   "audit database",
				Metrics: []string{"audit.DATABASE.ALTER", "audit.DATABASE.CREATE", "audit.DATABASE.DROP", "audit.DATABASE.FLASHBACK", "audit.DATABASE.EXPORT", "audit.DATABASE.IMPORT"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit cluster settings"}},
		Charts: []chartDescription{
			{
				Title:   "audit cluster settings",
				Metrics: []string{"audit.CLUSTERSETTINGS.RESET", "audit.CLUSTERSETTINGS.SET"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit device"}},
		Charts: []chartDescription{
			{
				Title:   "audit device",
				Metrics: []string{"audit.DEVICE.ALTER", "audit.DEVICE.CREATE", "audit.DEVICE.DROP", "audit.DEVICE.SELECT"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit index"}},
		Charts: []chartDescription{
			{
				Title:   "audit index",
				Metrics: []string{"audit.INDEX.ALTER", "audit.INDEX.CREATE", "audit.INDEX.DROP"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit instance"}},
		Charts: []chartDescription{
			{
				Title:   "audit instance",
				Metrics: []string{"audit.INSTANCE.ALTER"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit job"}},
		Charts: []chartDescription{
			{
				Title:   "audit job",
				Metrics: []string{"audit.JOB.CANCEL", "audit.JOB.PAUSE", "audit.JOB.RESUME"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit node"}},
		Charts: []chartDescription{
			{
				Title:   "audit node",
				Metrics: []string{"audit.NODE.DECOMMISSION", "audit.NODE.JOIN", "audit.NODE.QUIT", "audit.NODE.RECOMMISSION", "audit.NODE.RESTART"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit portal"}},
		Charts: []chartDescription{
			{
				Title:   "audit potral",
				Metrics: []string{"audit.PORTAL.ALTER", "audit.PORTAL.CREATE", "audit.PORTAL.DROP"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit privilege"}},
		Charts: []chartDescription{
			{
				Title:   "audit privilege",
				Metrics: []string{"audit.PRIVILEGE.GRANT", "audit.PRIVILEGE.REVOKE"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit query"}},
		Charts: []chartDescription{
			{
				Title:   "audit query",
				Metrics: []string{"audit.QUERY.CANCEL", "audit.QUERY.EXPLAIN"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit range"}},
		Charts: []chartDescription{
			{
				Title:   "audit range",
				Metrics: []string{"audit.RANGE.ALTER"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit role"}},
		Charts: []chartDescription{
			{
				Title:   "audit role",
				Metrics: []string{"audit.ROLE.ALTER", "audit.ROLE.CREATE", "audit.ROLE.DROP", "audit.ROLE.GRANT", "audit.ROLE.REVOKE"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit schedule"}},
		Charts: []chartDescription{
			{
				Title:   "audit schedule",
				Metrics: []string{"audit.SCHEDULE.ALTER", "audit.SCHEDULE.PAUSE", "audit.SCHEDULE.RESUME"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit schema"}},
		Charts: []chartDescription{
			{
				Title:   "audit schema",
				Metrics: []string{"audit.SCHEMA.ALTER", "audit.SCHEMA.CHANGE", "audit.SCHEMA.CREATE", "audit.SCHEMA.DROP", "audit.SCHEMA.ROLLBACK"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit schema"}},
		Charts: []chartDescription{
			{
				Title:   "audit schema",
				Metrics: []string{"audit.SCHEMA.ALTER", "audit.SCHEMA.CHANGE", "audit.SCHEMA.CREATE", "audit.SCHEMA.DROP", "audit.SCHEMA.ROLLBACK"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit sequence"}},
		Charts: []chartDescription{
			{
				Title:   "audit sequence",
				Metrics: []string{"audit.SEQUENCE.ALTER", "audit.SEQUENCE.CREATE", "audit.SEQUENCE.DROP"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit session"}},
		Charts: []chartDescription{
			{
				Title:   "audit session",
				Metrics: []string{"audit.SESSION.CANCEL", "audit.SESSION.RESET", "audit.SESSION.SET"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit sequence"}},
		Charts: []chartDescription{
			{
				Title:   "audit sequence",
				Metrics: []string{"audit.SEQUENCE.ALTER", "audit.SEQUENCE.CREATE", "audit.SEQUENCE.DROP"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit store"}},
		Charts: []chartDescription{
			{
				Title:   "audit store",
				Metrics: []string{"audit.STORE.DISABLE", "audit.STORE.ENABLE"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit store"}},
		Charts: []chartDescription{
			{
				Title:   "audit store",
				Metrics: []string{"audit.TABLE.ALTER", "audit.TABLE.CREATE", "audit.TABLE.DELETE", "audit.TABLE.DROP", "audit.TABLE.FLASHBACK", "audit.TABLE.INSERT", "audit.TABLE.SELECT", "audit.TABLE.TRUNCATE", "audit.TABLE.UPDATE", "audit.TABLE.EXPORT", "audit.TABLE.IMPORT"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit tenant"}},
		Charts: []chartDescription{
			{
				Title:   "audit tenant",
				Metrics: []string{"audit.TENANT.ALTER", "audit.TENANT.CREATE", "audit.TENANT.DROP"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit topic"}},
		Charts: []chartDescription{
			{
				Title:   "audit topic",
				Metrics: []string{"audit.TOPIC.CREATE", "audit.TOPIC.DROP"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit trigger"}},
		Charts: []chartDescription{
			{
				Title:   "audit trigger",
				Metrics: []string{"audit.TRIGGER.ALTER", "audit.TRIGGER.CREATE", "audit.TRIGGER.DROP"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit user"}},
		Charts: []chartDescription{
			{
				Title:   "audit user",
				Metrics: []string{"audit.USER.ALTER", "audit.USER.CREATE", "audit.USER.DROP"},
			},
		},
	},
	{
		Organization: [][]string{{Audits, "audit view"}},
		Charts: []chartDescription{
			{
				Title:   "audit view",
				Metrics: []string{"audit.VIEW.ALTER", "audit.VIEW.CREATE", "audit.VIEW.DELETE", "audit.VIEW.DROP", "audit.VIEW.INSERT", "audit.VIEW.REFRESH", "audit.VIEW.SELECT", "audit.VIEW.UPDATE"},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Build Info"}},
		Charts: []chartDescription{
			{
				Title:       "Timestamp",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"build.timestamp"},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Certificates"}},
		Charts: []chartDescription{
			{
				Title:       "CA Expiration",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"security.certificate.expiration.ca"},
			},
			{
				Title:       "Client CA Expiration",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"security.certificate.expiration.client-ca"},
			},
			{
				Title:       "Node Cert Expiration",
				Downsampler: DescribeAggregator_MAX,
				Aggregator:  DescribeAggregator_MAX,
				Metrics:     []string{"security.certificate.expiration.node"},
			},
			{
				Title:       "Node Client Cert Expiration",
				Downsampler: DescribeAggregator_MAX,
				Aggregator:  DescribeAggregator_MAX,
				Metrics:     []string{"security.certificate.expiration.node-client"},
			},
			{
				Title:       "UI Cert Expiration",
				Downsampler: DescribeAggregator_MAX,
				Aggregator:  DescribeAggregator_MAX,
				Metrics:     []string{"security.certificate.expiration.ui"},
			},
			{
				Title:       "UI CA Cert Expiration",
				Downsampler: DescribeAggregator_MAX,
				Aggregator:  DescribeAggregator_MAX,
				Metrics:     []string{"security.certificate.expiration.ui-ca"},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Clocks"}},
		Charts: []chartDescription{
			{
				Title:   "Roundtrip Latency",
				Metrics: []string{"round-trip-latency"},
			},
		},
	},
	{
		Organization: [][]string{{Process, "CPU"}},
		Charts: []chartDescription{
			{
				Title: "Percentage",
				Metrics: []string{
					"sys.cpu.sys.percent",
					"sys.cpu.user.percent",
					"sys.cpu.combined.percent-normalized",
				},
			},
			{
				Title: "Time",
				Metrics: []string{
					"sys.cpu.sys.ns",
					"sys.cpu.user.ns",
				},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Network"}},
		Charts: []chartDescription{
			{
				Title: "Packets",
				Metrics: []string{
					"sys.host.net.recv.packets",
					"sys.host.net.send.packets",
				},
			},
			{
				Title: "Size",
				Metrics: []string{
					"sys.host.net.recv.bytes",
					"sys.host.net.send.bytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Node"}},
		Charts: []chartDescription{
			{
				Title:       "ID",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"node-id"},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Server", "cgo"}},
		Charts: []chartDescription{
			{
				Title:   "Calls",
				Metrics: []string{"sys.cgocalls"},
			},
			{
				Title: "Memory",
				Metrics: []string{
					"sys.cgo.allocbytes",
					"sys.cgo.totalbytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Server", "Kmalloc"}},
		Charts: []chartDescription{
			{
				Title: "Memory",
				Metrics: []string{
					"sys.kmalloc.totalbytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Server", "Mmap"}},
		Charts: []chartDescription{
			{
				Title: "Memory Map Virtual Size",
				Metrics: []string{
					"sys.memory.map.virtualbytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Server", "Mmap"}},
		Charts: []chartDescription{
			{
				Title: "Memory Map Physical Size",
				Metrics: []string{
					"sys.memory.map.physicalbytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Server", "Mmap"}},
		Charts: []chartDescription{
			{
				Title: "Memory Map Count",
				Metrics: []string{
					"sys.memory.map.totalcount",
				},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Server", "go"}},
		Charts: []chartDescription{
			{
				Title:   "goroutines",
				Metrics: []string{"sys.goroutines"},
			},
			{
				Title: "Memory",
				Metrics: []string{
					"sys.go.allocbytes",
					"sys.go.totalbytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{Process, "Server", "Overview"}},
		Charts: []chartDescription{
			{
				Title: "File Descriptors (FD)",
				Metrics: []string{
					"sys.fd.open",
					"sys.fd.softlimit",
				},
			},
			{
				Title:   "RSS",
				Metrics: []string{"sys.rss"},
			},
			{
				Title:   "Uptime",
				Metrics: []string{"sys.uptime"},
			},
		},
	},
	{
		Organization: [][]string{
			{Process, "Server", "Disk"},
			{StorageLayer, "Host Disk"},
		},
		Charts: []chartDescription{
			{
				Title: "IOPS in Progress",
				Metrics: []string{
					"sys.host.disk.iopsinprogress",
				},
			},
			{
				Title: "Operations Count",
				Metrics: []string{
					"sys.host.disk.read.count",
					"sys.host.disk.write.count",
				},
			},
			{
				Title: "Operations Size",
				Metrics: []string{
					"sys.host.disk.read.bytes",
					"sys.host.disk.write.bytes",
				},
			},
			{
				Title: "Time",
				Metrics: []string{
					"sys.host.disk.io.time",
					"sys.host.disk.weightedio.time",
					"sys.host.disk.read.time",
					"sys.host.disk.write.time",
				},
			},
		},
	},
	{
		Organization: [][]string{{DistributionLayer, "DistSender"}},
		Charts: []chartDescription{
			{
				Title: "Batches",
				Metrics: []string{
					"distsender.batches",
					"distsender.batches.partial",
					"distsender.batches.async.sent",
					"distsender.batches.async.throttled",
				},
				AxisLabel: "Batches",
			},
			{
				Title: "Errors",
				Metrics: []string{
					"distsender.rpc.sent.nextreplicaerror",
					"distsender.errors.notleaseholder",
					"distsender.errors.inleasetransferbackoffs",
				},
				AxisLabel: "Error Count",
			},
			{
				Title: "Range Lookups",
				Metrics: []string{
					"distsender.rangelookups",
				},
			},
			{
				Title: "RPCs",
				Metrics: []string{
					"distsender.rpc.sent.local",
					"distsender.rpc.sent",
				},
			},
		},
	},
	{
		Organization: [][]string{{DistributionLayer, "RPC", "Heartbeats"}},
		Charts: []chartDescription{
			{
				Title: "Overview",
				Metrics: []string{
					"rpc.heartbeats.initializing",
					"rpc.heartbeats.nominal",
					"rpc.heartbeats.failed",
				},
				AxisLabel: "Heartbeats",
			},
			{
				Title: "Loops",
				Metrics: []string{
					"rpc.heartbeats.loops.exited",
					"rpc.heartbeats.loops.started",
				},
				AxisLabel: "Heartbeat Loops",
			},
		},
	},
	{
		Organization: [][]string{{DistributionLayer, "Gossip"}},
		Charts: []chartDescription{
			{
				Title:       "Active Connections",
				Downsampler: DescribeAggregator_MAX,
				Aggregator:  DescribeAggregator_MAX,
				Metrics: []string{
					"gossip.connections.incoming",
					"gossip.connections.outgoing",
				},
			},
			{
				Title: "Bytes",
				Metrics: []string{
					"gossip.bytes.received",
					"gossip.bytes.sent",
				},
			},
			{
				Title: "Infos",
				Metrics: []string{
					"gossip.infos.received",
					"gossip.infos.sent",
				},
			},
			{
				Title:       "Refused Connections",
				Downsampler: DescribeAggregator_MAX,
				Aggregator:  DescribeAggregator_MAX,
				Metrics:     []string{"gossip.connections.refused"},
			},
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Merge Queue"},
			{ReplicationLayer, "Merge Queue"},
		},
		Charts: []chartDescription{
			{
				Title:       "Pending",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics: []string{
					"queue.merge.pending",
					"queue.merge.purgatory",
				},
			},
			{
				Title:       "Successes",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics: []string{
					"queue.merge.process.failure",
					"queue.merge.process.success",
				},
			},
			{
				Title:   "Time Spent",
				Metrics: []string{"queue.merge.processingnanos"},
			},
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Ranges"},
			{ReplicationLayer, "Ranges"},
		},
		Charts: []chartDescription{
			{
				Title: "Overview",
				Metrics: []string{
					"ranges",
					"ranges.unavailable",
					"ranges.underreplicated",
					"ranges.overreplicated",
				},
			},
			{
				Title: "Operations",
				Metrics: []string{
					"range.adds",
					"range.splits",
					"range.merges",
					"range.removes",
				},
			},
			{
				Title: "Rangefeed",
				Metrics: []string{
					"kv.rangefeed.catchup_scan_nanos",
				},
			},
			{
				Title: "Snapshots",
				Metrics: []string{
					"range.snapshots.generated",
					"range.snapshots.normal-applied",
					"range.snapshots.preemptive-applied",
					"range.snapshots.learner-applied",
				},
			},
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Rebalancing"},
			{ReplicationLayer, "Leases"},
		},
		Charts: []chartDescription{
			{
				Title:   "Rebalancing Lease Transfers",
				Metrics: []string{"rebalancing.lease.transfers"},
			},
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Rebalancing"},
			{ReplicationLayer, "Ranges"},
		},
		Charts: []chartDescription{
			{
				Title:   "Range Rebalances",
				Metrics: []string{"rebalancing.range.rebalances"},
			},
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Rebalancing"},
		},
		Charts: []chartDescription{
			{
				Title:   "QPS",
				Metrics: []string{"rebalancing.queriespersecond"},
			},
		},
	},
	{
		Organization: [][]string{
			{DistributionLayer, "Split Queue"},
			{ReplicationLayer, "Split Queue"},
		},
		Charts: []chartDescription{
			{
				Title:       "Pending",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics: []string{
					"queue.split.pending",
					"queue.split.purgatory",
				},
			},
			{
				Title:       "Successes",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics: []string{
					"queue.split.process.failure",
					"queue.split.process.success",
				},
			},
			{
				Title:   "Time Spent",
				Metrics: []string{"queue.split.processingnanos"},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Clocks"}}, Charts: []chartDescription{
			{
				Title:   "Roundtrip Latency",
				Metrics: []string{"round-trip-latency"},
			},
		},
	},
	{
		Organization: [][]string{
			{KVTransactionLayer, "Clocks"},
			{Process, "Clocks"},
		},
		Charts: []chartDescription{
			{
				Title: "Offsets",
				Metrics: []string{
					"clock-offset.meannanos",
					"clock-offset.stddevnanos",
				},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Follower Reads"}},
		Charts: []chartDescription{
			{
				Title:   "Closed Timestamp",
				Metrics: []string{"kv.closed_timestamp.max_behind_nanos"},
			},
			{
				Title:   "Count",
				Metrics: []string{"follower_reads.success_count"},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Protected Timestamps", "Reconciliation"}},
		Charts: []chartDescription{
			{
				Title: "Records Processed",
				Metrics: []string{
					"kv.protectedts.reconciliation.errors",
					"kv.protectedts.reconciliation.records_processed",
					"kv.protectedts.reconciliation.records_removed",
				},
			},
			{
				Title: "Reconciliation Runs",
				Metrics: []string{
					"kv.protectedts.reconciliation.num_runs",
				},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Keys"}},
		Charts: []chartDescription{
			{
				Title: "AbortSpan",
				Metrics: []string{
					"queue.gc.info.abortspanconsidered",
					"queue.gc.info.abortspangcnum",
					"queue.gc.info.abortspanscanned",
				},
			},
			{
				Title:   "Distinct Txns",
				Metrics: []string{"queue.gc.info.intenttxns"},
			},
			{
				Title:   "Enteries in Txn Spans",
				Metrics: []string{"queue.gc.info.transactionspanscanned"},
			},
			{
				Title: "Intents",
				Metrics: []string{
					"queue.gc.info.resolvesuccess",
					"queue.gc.info.resolvetotal",
				},
			},
			{
				Title:   "Keys with GC'able Data",
				Metrics: []string{"queue.gc.info.numkeysaffected"},
			},
			{
				Title:   "Old Intents",
				Metrics: []string{"queue.gc.info.intentsconsidered"},
			},
			{
				Title:   "Pushes",
				Metrics: []string{"queue.gc.info.pushtxn"},
			},
			{
				Title: "Txn Relationship",
				Metrics: []string{
					"queue.gc.info.transactionspangcaborted",
					"queue.gc.info.transactionspangccommitted",
					"queue.gc.info.transactionspangcpending",
					"queue.gc.info.transactionspangcstaging",
				},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Garbage Collection (GC)", "Overview"}},
		Charts: []chartDescription{
			{
				Title:   "Current GC Pause Percent",
				Metrics: []string{"sys.gc.pause.percent"},
			},
			{
				Title:   "Total GC Pause (NS)",
				Metrics: []string{"sys.gc.pause.ns"},
			},
			{
				Title:   "Total GC Runs",
				Metrics: []string{"sys.gc.count"},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Requests", "Overview"}},
		Charts: []chartDescription{
			{
				Title:   "Latency",
				Metrics: []string{"exec.latency"},
			},
			{
				Title:       "Success",
				Downsampler: DescribeAggregator_MAX,
				Rate:        DescribeDerivative_DERIVATIVE,
				Percentiles: false,
				Metrics: []string{
					"exec.error",
					"exec.success",
				},
			},
			{
				Title:       "Storage Engine Stalls",
				Downsampler: DescribeAggregator_MAX,
				Rate:        DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
				Percentiles: false,
				Metrics:     []string{"engine.stalls"},
			},
		},
	},
	{
		Organization: [][]string{
			{KVTransactionLayer, "Requests", "Backpressure"},
			{ReplicationLayer, "Requests", "Backpressure"},
		},
		Charts: []chartDescription{
			{
				Title:       "Writes Waiting on Range Split",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"requests.backpressure.split"},
			},
		},
	},
	{
		Organization: [][]string{
			{KVTransactionLayer, "Requests", "Slow"},
			{ReplicationLayer, "Requests", "Slow"},
			{DistributionLayer, "Requests", "Slow"},
		},
		Charts: []chartDescription{
			{
				Title:       "Latch",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"requests.slow.latch"},
			},
			{
				Title:       "Stuck Acquiring Lease",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"requests.slow.lease"},
			},
			{
				Title:       "Stuck in Raft",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"requests.slow.raft"},
			},
			{
				Title:       "Stuck sending RPCs to range",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"requests.slow.distsender"},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Storage"}},
		Charts: []chartDescription{
			{
				Rate:        DescribeDerivative_DERIVATIVE,
				Percentiles: false,
				Title:       "Metric Update Frequency",
				Metrics:     []string{"lastupdatenanos"},
			},
			{
				Title:     "Counts",
				AxisLabel: "MVCC Keys & Values",
				Metrics: []string{
					"intentcount",
					"keycount",
					"livecount",
					"syscount",
					"valcount",
				},
			},
			{
				Title:   "Cumultative Age of Non-Live Data",
				Metrics: []string{"gcbytesage"},
			},
			{
				Title:   "Cumultative Intent Age",
				Metrics: []string{"intentage"},
			},
			{
				Title: "Size",
				Metrics: []string{
					"intentbytes",
					"keybytes",
					"livebytes",
					"sysbytes",
					"totalbytes",
					"valbytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Timestamp Cache"}},
		Charts: []chartDescription{
			{
				Title: "Page Counts",
				Metrics: []string{
					"tscache.skl.pages",
				},
			},
			{
				Title: "Page Rotations",
				Metrics: []string{
					"tscache.skl.rotations",
				},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "Intents"}},
		Charts: []chartDescription{
			{
				Title: "Intent Resolver",
				Metrics: []string{
					"intentresolver.async.throttled",
				},
			},
			{
				Title: "Overview",
				Metrics: []string{
					"intents.abort-attempts",
					"intents.poison-attempts",
					"intents.resolve-attempts",
				},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "TxnWaitQueue"}},
		Charts: []chartDescription{
			{
				Title: "Deadlocks",
				Metrics: []string{
					"txnwaitqueue.deadlocks_total",
				},
			},
			{
				Title: "Slow Pusher",
				Metrics: []string{
					"txnwaitqueue.pusher.slow",
				},
			},
			{
				Title: "Wait Time",
				Metrics: []string{
					"txnwaitqueue.pusher.wait_time",
					"txnwaitqueue.query.wait_time",
				},
				AxisLabel: "Wait Time",
			},
			{
				Title: "Waiting",
				Metrics: []string{
					"txnwaitqueue.pushee.waiting",
					"txnwaitqueue.pusher.waiting",
					"txnwaitqueue.query.waiting",
				},
				AxisLabel: "Actors",
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Charts: []chartDescription{
			{
				Title:   "Aborts",
				Metrics: []string{"txn.aborts"},
			},
			{
				Title:   "Successful refreshes",
				Metrics: []string{"txn.refresh.success"},
			},
			{
				Title:   "Failed refreshes",
				Metrics: []string{"txn.refresh.fail"},
			},
			{
				Title:   "Failed refreshes with condensed spans",
				Metrics: []string{"txn.refresh.fail_with_condensed_spans"},
			},
			{
				Title:   "Transactions exceeding refresh spans memory limit",
				Metrics: []string{"txn.refresh.memory_limit_exceeded"},
			},
			{
				Title: "Commits",
				Metrics: []string{
					"txn.commits",
					"txn.commits1PC",
					"txn.parallelcommits",
				},
			},
			{
				Title:   "Durations",
				Metrics: []string{"txn.durations"},
			},
			{
				Title: "Restart Cause Mix",
				Metrics: []string{
					"txn.restarts.serializable",
					"txn.restarts.writetooold",
					"txn.restarts.asyncwritefailure",
					"txn.restarts.readwithinuncertainty",
					"txn.restarts.txnaborted",
					"txn.restarts.txnpush",
					"txn.restarts.unknown",
					"txn.restarts.writetoooldmulti",
				},
			},
			{
				Title:       "Restarts",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: true,
				Metrics:     []string{"txn.restarts"},
			},
			{
				Title:       "Intents condensing - historical",
				Downsampler: DescribeAggregator_MAX,
				Metrics: []string{
					"txn.condensed_intent_spans",
				},
			},
			{
				Title:       "Intents condensing - current",
				Downsampler: DescribeAggregator_MAX,
				Metrics: []string{
					"txn.condensed_intent_spans_gauge",
				},
			},
		},
	},
	{
		Organization: [][]string{{KVTransactionLayer, "Transactions", "Recovery"}},
		Charts: []chartDescription{
			{
				Title: "Successes",
				Metrics: []string{
					"txnrecovery.successes.committed",
					"txnrecovery.successes.pending",
					"txnrecovery.successes.aborted",
				},
			},
			{
				Title: "Total Attempts",
				Metrics: []string{
					"txnrecovery.attempts.total",
				},
			},
			{
				Title: "Pending Attempts",
				Metrics: []string{
					"txnrecovery.attempts.pending",
				},
			},
			{
				Title: "Failures",
				Metrics: []string{
					"txnrecovery.failures",
				},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Changefeed"}},
		Charts: []chartDescription{
			{
				Title: "Emitted Bytes",
				Metrics: []string{
					"changefeed.emitted_bytes",
				},
			},
			{
				Title: "Emitted Messages",
				Metrics: []string{
					"changefeed.emitted_messages",
				},
			},
			{
				Title: "Entries",
				Metrics: []string{
					"changefeed.buffer_entries.in",
					"changefeed.buffer_entries.out",
				},
			},
			{
				Title: "Errors",
				Metrics: []string{
					"changefeed.error_retries",
				},
			},
			{
				Title: "Flushes",
				Metrics: []string{
					"changefeed.flushes",
				},
			},
			{
				Title: "Max Behind Nanos",
				Metrics: []string{
					"changefeed.max_behind_nanos",
				},
			},
			{
				Title: "Min High Water",
				Metrics: []string{
					"changefeed.min_high_water",
				},
			},
			{
				Title: "Poll Request Time",
				Metrics: []string{
					"changefeed.poll_request_nanos",
				},
			},
			{
				Title: "Total Time Spent",
				Metrics: []string{
					"changefeed.emit_nanos",
					"changefeed.flush_nanos",
					"changefeed.processing_nanos",
					"changefeed.table_metadata_nanos",
				},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Consistency Checker Queue"}},
		Charts: []chartDescription{
			{
				Title:   "Pending",
				Metrics: []string{"queue.consistency.pending"},
			},
			{
				Title: "Successes",
				Metrics: []string{
					"queue.consistency.process.failure",
					"queue.consistency.process.success",
				},
			},
			{
				Title:   "Time Spent",
				Metrics: []string{"queue.consistency.processingnanos"},
			},
		},
	},
	{
		Organization: [][]string{
			{ReplicationLayer, "Garbage Collection"},
			{StorageLayer, "Garbage Collection"},
		},
		Charts: []chartDescription{
			{
				Title:   "Queue Pending",
				Metrics: []string{"queue.gc.pending"},
			},
			{
				Title: "Queue Success",
				Metrics: []string{
					"queue.gc.process.failure",
					"queue.gc.process.success",
				},
			},
			{
				Title:   "Queue Time",
				Metrics: []string{"queue.gc.processingnanos"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Leases"}},
		Charts: []chartDescription{
			{
				Title:   "Stuck Acquisition Count",
				Metrics: []string{"requests.slow.lease"},
			},
			{
				Title: "Succcess Rate",
				Metrics: []string{
					"leases.error",
					"leases.success",
				},
			},
			{
				Title: "Total",
				Metrics: []string{
					"leases.epoch",
					"leases.expiration",
					"replicas.leaseholders",
					"replicas.leaders_not_leaseholders",
				},
			},
			{
				Title: "Transfer Success Rate",
				Metrics: []string{
					"leases.transfers.error",
					"leases.transfers.success",
				},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Node Liveness"}},
		Charts: []chartDescription{
			{
				Title:   "Epoch Increment Count",
				Metrics: []string{"liveness.epochincrements"},
			},
			{
				Title:   "Heartbeat Latency",
				Metrics: []string{"liveness.heartbeatlatency"},
			},
			{
				Title: "Heartbeats Success",
				Metrics: []string{
					"liveness.heartbeatfailures",
					"liveness.heartbeatsuccesses",
				},
			},
			{
				Title:       "Node Count",
				Downsampler: DescribeAggregator_MAX,
				Percentiles: false,
				Metrics:     []string{"liveness.livenodes"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Entry Cache"}},
		Charts: []chartDescription{
			{
				Title: "Entries",
				Metrics: []string{
					"raft.entrycache.size",
				},
			},
			{
				Title: "Hits",
				Metrics: []string{
					"raft.entrycache.accesses",
					"raft.entrycache.hits",
				},
				AxisLabel: "Entry Cache Operations",
			},
			{
				Title: "Size",
				Metrics: []string{
					"raft.entrycache.bytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Heartbeats"}},
		Charts: []chartDescription{
			{
				Title:   "MsgHeartbeatResp Count",
				Metrics: []string{"raft.rcvd.heartbeatresp"},
			},
			{
				Title:   "MsgTransferLeader Count",
				Metrics: []string{"raft.rcvd.transferleader"},
			},
			{
				Title:   "Pending",
				Metrics: []string{"raft.heartbeats.pending"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Latency"}},
		Charts: []chartDescription{
			{
				Title:   "Apply Committed",
				Metrics: []string{"raft.process.applycommitted.latency"},
			},
			{
				Title:   "Apply Committed Consistent",
				Metrics: []string{"raft.replica.consistent.latency"},
			},
			{
				Title:   "Command Commit",
				Metrics: []string{"raft.process.commandcommit.latency"},
			},
			{
				Title:   "Handle Ready",
				Metrics: []string{"raft.process.handleready.latency"},
			},
			{
				Title:   "Log Commit",
				Metrics: []string{"raft.process.logcommit.latency"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Log"}},
		Charts: []chartDescription{
			{
				Title:   "Entries Truncated",
				Metrics: []string{"raftlog.truncated"},
			},
			{
				Title:   "Followers Behind By...",
				Metrics: []string{"raftlog.behind"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Overview"}},
		Charts: []chartDescription{
			{
				Title:   "Commands Count",
				Metrics: []string{"raft.commandsapplied"},
			},
			{
				Title:   "Enqueued",
				Metrics: []string{"raft.enqueued.pending"},
			},
			{
				Title:   "Keys/Sec Avg.",
				Metrics: []string{"rebalancing.writespersecond"},
			},
			{
				Title:   "Leader Transfers",
				Metrics: []string{"range.raftleadertransfers"},
			},
			{
				Title:   "Leaders",
				Metrics: []string{"replicas.leaders"},
			},
			{
				Title:   "Stuck Request Count",
				Metrics: []string{"requests.slow.raft"},
			},
			{
				Title:   "Ticks Queued",
				Metrics: []string{"raft.ticks"},
			},
			{
				Title: "Working vs. Ticking TIme",
				Metrics: []string{
					"raft.process.tickingnanos",
					"raft.process.workingnanos",
				},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Queues"}},
		Charts: []chartDescription{
			{
				Title:   "Log Pending",
				Metrics: []string{"queue.raftlog.pending"},
			},
			{
				Title:   "Log Processing Time Spent",
				Metrics: []string{"queue.raftlog.processingnanos"},
			},
			{
				Title: "Log Successes",
				Metrics: []string{
					"queue.raftlog.process.failure",
					"queue.raftlog.process.success",
				},
			},
			{
				Title:   "Snapshot Processing Time Spent",
				Metrics: []string{"queue.raftsnapshot.processingnanos"},
			},
			{
				Title: "Snapshot Successes",
				Metrics: []string{
					"queue.raftsnapshot.process.failure",
					"queue.raftsnapshot.process.success",
				},
			},
			{
				Title:   "Snapshots Pending",
				Metrics: []string{"queue.raftsnapshot.pending"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Raft", "Received"}},
		Charts: []chartDescription{
			{
				Title:   "Dropped",
				Metrics: []string{"raft.rcvd.dropped"},
			},
			{
				Title:   "Heartbeat Count",
				Metrics: []string{"raft.rcvd.heartbeat"},
			},
			{
				Title:   "MsgApp Count",
				Metrics: []string{"raft.rcvd.app"},
			},
			{
				Title:   "MsgAppResp Count",
				Metrics: []string{"raft.rcvd.appresp"},
			},
			{
				Title:   "MsgHeartbeatResp Count",
				Metrics: []string{"raft.rcvd.heartbeatresp"},
			},
			{
				Title:   "MsgPreVote Count",
				Metrics: []string{"raft.rcvd.prevote"},
			},
			{
				Title:   "MsgPreVoteResp Count",
				Metrics: []string{"raft.rcvd.prevoteresp"},
			},
			{
				Title:   "MsgProp Count",
				Metrics: []string{"raft.rcvd.prop"},
			},
			{
				Title:   "MsgSnap Count",
				Metrics: []string{"raft.rcvd.snap"},
			},
			{
				Title:   "MsgTimeoutNow Count",
				Metrics: []string{"raft.rcvd.timeoutnow"},
			},
			{
				Title:   "MsgTransferLeader Count",
				Metrics: []string{"raft.rcvd.transferleader"},
			},
			{
				Title:   "MsgVote Count",
				Metrics: []string{"raft.rcvd.vote"},
			},
			{
				Title:   "MsgVoteResp Count",
				Metrics: []string{"raft.rcvd.voteresp"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Ranges"}},
		Charts: []chartDescription{
			{
				Title:   "Backpressued Writes Waiting on Split",
				Metrics: []string{"requests.backpressure.split"},
			},
			{
				Title:   "Raft Leader Transfers",
				Metrics: []string{"range.raftleadertransfers"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replica GC Queue"}},
		Charts: []chartDescription{
			{
				Title:   "Pending",
				Metrics: []string{"queue.replicagc.pending"},
			},
			{
				Title:   "Removal Count",
				Metrics: []string{"queue.replicagc.removereplica"},
			},
			{
				Title: "Successes",
				Metrics: []string{
					"queue.replicagc.process.failure",
					"queue.replicagc.process.success",
				},
			},
			{
				Title:   "Time Spent",
				Metrics: []string{"queue.replicagc.processingnanos"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replicas", "Overview"}},
		Charts: []chartDescription{
			{
				Title: "Count",
				Metrics: []string{
					"replicas.quiescent",
					"replicas",
					"replicas.reserved",
				},
			},
			{
				Title:   "Leaseholders",
				Metrics: []string{"replicas.leaseholders"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Charts: []chartDescription{
			{
				Title:   "Add Replica Count",
				Metrics: []string{"queue.replicate.addreplica"},
			},
			{
				Title:   "Lease Transfer Count",
				Metrics: []string{"queue.replicate.transferlease"},
			},
			{
				Title:   "Pending",
				Metrics: []string{"queue.replicate.pending"},
			},
			{
				Title:   "Purgatory",
				Metrics: []string{"queue.replicate.purgatory"},
			},
			{
				Title:   "Reblance Count",
				Metrics: []string{"queue.replicate.rebalancereplica"},
			},
			{
				Title: "Remove Replica Count",
				Metrics: []string{
					"queue.replicate.removedeadreplica",
					"queue.replicate.removereplica",
					"queue.replicate.removelearnerreplica",
				},
			},
			{
				Title: "Successes",
				Metrics: []string{
					"queue.replicate.process.failure",
					"queue.replicate.process.success",
				},
			},
			{
				Title:   "Time Spent",
				Metrics: []string{"queue.replicate.processingnanos"},
			},
		},
	},
	{
		Organization: [][]string{{ReplicationLayer, "Replication"}},
		Charts: []chartDescription{
			{
				Title:   "Currently Running",
				Metrics: []string{"replicating.running"},
			},
			{
				Title: "Event admission latency",
				Metrics: []string{
					"replicating.admit_latency",
				},
			},
			{
				Title: "Commits Latency",
				Metrics: []string{
					"replicating.commit_latency",
				},
			},
			{
				Title: "Time spent",
				Metrics: []string{
					"replicating.flush_hist_nanos",
				},
			},
			{
				Title: "Ingested Events",
				Metrics: []string{
					"replicating.events_ingested",
					"replicating.resolved_events_ingested",
				},
			},
			{
				Title: "Flushes",
				Metrics: []string{
					"replicating.flushes",
				},
			},
			{
				Title: "Ingested Bytes",
				Metrics: []string{
					"replicating.ingested_bytes",
				},
			},
			{
				Title:   "Earliest Data Processor Checkpoint Span",
				Metrics: []string{"replicating.earliest_data_checkpoint_span"},
			},
			{
				Title:   "Latest Data Processor Checkpoint Span",
				Metrics: []string{"replicating.latest_data_checkpoint_span"},
			},
			{
				Title:   "Data Checkpoint Span Count",
				Metrics: []string{"replicating.data_checkpoint_span_count"},
			},
			{
				Title:   "Frontier Checkpoint Span Count",
				Metrics: []string{"replicating.frontier_checkpoint_span_count"},
			},
			{
				Title:   "Job Progress Updates",
				Metrics: []string{"replicating.job_progress_updates"},
			},
			{
				Title:   "Replicating Txn Received Count",
				Metrics: []string{"replicating.txn_received_count"},
			},
			{
				Title:   "Replicating Txn Replayed Counts",
				Metrics: []string{"replicating.txn_replayed_counts"},
			},
			{
				Title:   "Replicating Txn Overstock Ratio",
				Metrics: []string{"replicating.txn_overstock_ratio"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "DistSQL", "Flows"}},
		Charts: []chartDescription{
			{
				Title:   "Active",
				Metrics: []string{"sql.distsql.flows.active"},
			},
			{
				Title:   "Queue Wait",
				Metrics: []string{"sql.distsql.flows.queue_wait"},
			},
			{
				Title:   "Queued",
				Metrics: []string{"sql.distsql.flows.queued"},
			},
			{
				Title:   "Total",
				Metrics: []string{"sql.distsql.flows.total"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Charts: []chartDescription{
			{
				Title:   "Active Queries",
				Metrics: []string{"sql.distsql.queries.active"},
			},
			{
				Title:   "Current Memory Usage",
				Metrics: []string{"sql.mem.distsql.current"},
			},
			{
				Title: "DML Mix",
				Metrics: []string{
					"sql.distsql.select.count",
					"sql.distsql.select.count.internal",
				},
				AxisLabel: "SQL Statements",
			},
			{
				Title: "Exec Latency",
				Metrics: []string{
					"sql.distsql.exec.latency",
					"sql.distsql.exec.latency.internal",
				},
				AxisLabel: "Latency",
			},
			{
				Title:   "Memory Usage per Statement",
				Metrics: []string{"sql.mem.distsql.max"},
			},
			{
				Title: "Service Latency",
				Metrics: []string{
					"sql.distsql.service.latency",
					"sql.distsql.service.latency.internal",
				},
				AxisLabel: "Latency",
			},
			{
				Title:   "Total Queries",
				Metrics: []string{"sql.distsql.queries.total"},
			},
			{
				Title:   "Vectorized Temporary Storage Open File Descriptors",
				Metrics: []string{"sql.distsql.vec.openfds"},
			},
			{
				Title:   "Current Disk Usage",
				Metrics: []string{"sql.disk.distsql.current"},
			},
			{
				Title:   "Disk Usage per Statement",
				Metrics: []string{"sql.disk.distsql.max"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "Bulk"}},
		Charts: []chartDescription{
			{
				Title:   "Current Memory Usage",
				Metrics: []string{"sql.mem.bulk.current"},
			},
			{
				Title:   "Memory Usage per Statement",
				Metrics: []string{"sql.mem.bulk.max"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "Optimizer"}},
		Charts: []chartDescription{
			{
				Title: "Count",
				Metrics: []string{
					"sql.optimizer.count",
					"sql.optimizer.count.internal",
				},
				AxisLabel: "SQL Statements",
			},
			{
				Title: "Fallback",
				Metrics: []string{
					"sql.optimizer.fallback.count",
					"sql.optimizer.fallback.count.internal",
				},
				AxisLabel: "Fallbacks",
			},
			{
				Title: "Plan Cache",
				Metrics: []string{
					"sql.optimizer.plan_cache.hits",
					"sql.optimizer.plan_cache.hits.internal",
					"sql.optimizer.plan_cache.misses",
					"sql.optimizer.plan_cache.misses.internal",
				},
				AxisLabel: "Plane Cache Accesses",
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Admin"}},
		Charts: []chartDescription{
			{
				Title:   "All",
				Metrics: []string{"sql.mem.admin.max"},
			},
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.admin.current"},
			},
			{
				Title:   "Session All",
				Metrics: []string{"sql.mem.admin.session.max"},
			},
			{
				Title:   "Session Current",
				Metrics: []string{"sql.mem.admin.session.current"},
			},
			{
				Title:   "Txn All",
				Metrics: []string{"sql.mem.admin.txn.max"},
			},
			{
				Title:   "Txn Current",
				Metrics: []string{"sql.mem.admin.txn.current"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Charts: []chartDescription{
			{
				Title:   "All",
				Metrics: []string{"sql.mem.conns.max"},
			},
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.conns.current"},
			},
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.conns.current"},
			},
			{
				Title:   "Session All",
				Metrics: []string{"sql.mem.conns.session.max"},
			},
			{
				Title:   "Session Current",
				Metrics: []string{"sql.mem.conns.session.current"},
			},
			{
				Title:   "Txn All",
				Metrics: []string{"sql.mem.conns.txn.max"},
			},
			{
				Title:   "Txn Current",
				Metrics: []string{"sql.mem.conns.txn.current"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "Internal"}},
		Charts: []chartDescription{
			{
				Title:   "All",
				Metrics: []string{"sql.mem.internal.max"},
			},
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.internal.current"},
			},
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.internal.current"},
			},
			{
				Title:   "Session All",
				Metrics: []string{"sql.mem.internal.session.max"},
			},
			{
				Title:   "Session Current",
				Metrics: []string{"sql.mem.internal.session.current"},
			},
			{
				Title:   "Txn All",
				Metrics: []string{"sql.mem.internal.txn.max"},
			},
			{
				Title:   "Txn Current",
				Metrics: []string{"sql.mem.internal.txn.current"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL Session"}},
		Charts: []chartDescription{
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.sql.session.current"},
			},
			{
				Title:   "Max",
				Metrics: []string{"sql.mem.sql.session.max"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL Txn"}},
		Charts: []chartDescription{
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.sql.txn.current"},
			},
			{
				Title:   "Max",
				Metrics: []string{"sql.mem.sql.txn.max"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL Memory", "SQL"}},
		Charts: []chartDescription{
			{
				Title:   "Current",
				Metrics: []string{"sql.mem.sql.current"},
			},
			{
				Title:   "Max",
				Metrics: []string{"sql.mem.sql.max"},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "Temporary Objects Cleanup"}},
		Charts: []chartDescription{
			{
				Title:   "Active Cleaners",
				Metrics: []string{"sql.temp_object_cleaner.active_cleaners"},
			},
			{
				Title: "Deletion Rate",
				Metrics: []string{
					"sql.temp_object_cleaner.schemas_to_delete",
					"sql.temp_object_cleaner.schemas_deletion_success",
					"sql.temp_object_cleaner.schemas_deletion_error",
				},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL"}},
		Charts: []chartDescription{
			{
				Title: "Active Connections",
				Metrics: []string{
					"sql.conns",
				},
			},
			{
				Title: "Active Success Connections",
				Metrics: []string{
					"sql.suc_conns",
				},
			},
			{
				Title: "New Connections",
				Metrics: []string{
					"sql.new_conns",
				},
			},
			{
				Title: "Byte I/O",
				Metrics: []string{
					"sql.bytesin",
					"sql.bytesout",
				},
			},
			{
				Title: "Exec Latency",
				Metrics: []string{
					"sql.exec.latency",
					"sql.exec.latency.internal",
				},
				AxisLabel: "Latency",
			},
			{
				Title: "Service Latency",
				Metrics: []string{
					"sql.service.latency",
					"sql.service.latency.internal",
				},
				AxisLabel: "Latency",
			},
			{
				Title: "Transaction Latency",
				Metrics: []string{
					"sql.txn.latency",
					"sql.txn.latency.internal",
				},
				AxisLabel: "Latency",
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL", "DDL"}},
		Charts: []chartDescription{
			{
				Title: "Counts",
				Metrics: []string{
					"sql.ddl.count",
					"sql.ddl.started.count",
				},
				AxisLabel: "SQL Statements",
			},
			{
				Title: "Counts (Internal)",
				Metrics: []string{
					"sql.ddl.count.internal",
					"sql.ddl.started.count.internal",
				},
				AxisLabel: "SQL Statements",
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL", "DML"}},
		Charts: []chartDescription{
			{
				Title: "Mix",
				Metrics: []string{
					"sql.query.count",
					"sql.delete.count",
					"sql.insert.count",
					"sql.misc.count",
					"sql.query.count",
					"sql.select.count",
					"sql.update.count",
					"sql.failure.count",
				},
			},
			{
				Title: "Started Mix",
				Metrics: []string{
					"sql.query.started.count",
					"sql.delete.started.count",
					"sql.insert.started.count",
					"sql.misc.started.count",
					"sql.query.started.count",
					"sql.select.started.count",
					"sql.update.started.count",
					"sql.failure.started.count",
				},
			},
			{
				Title: "Mix (Internal)",
				Metrics: []string{
					"sql.delete.count.internal",
					"sql.insert.count.internal",
					"sql.misc.count.internal",
					"sql.query.count.internal",
					"sql.select.count.internal",
					"sql.update.count.internal",
					"sql.failure.count.internal",
				},
			},
			{
				Title: "Started Mix (Internal)",
				Metrics: []string{
					"sql.delete.started.count.internal",
					"sql.insert.started.count.internal",
					"sql.misc.started.count.internal",
					"sql.query.started.count.internal",
					"sql.select.started.count.internal",
					"sql.update.started.count.internal",
					"sql.failure.started.count.internal",
				},
			},
		},
	},
	{
		Organization: [][]string{{SQLLayer, "SQL", "Transaction Control"}},
		Charts: []chartDescription{
			{
				Title: "Transaction Control Mix",
				Metrics: []string{
					"sql.txn.abort.count",
					"sql.txn.begin.count",
					"sql.txn.commit.count",
					"sql.txn.rollback.count",
					"sql.txn.begin.started.count",
					"sql.txn.commit.started.count",
					"sql.txn.rollback.started.count",
				},
			},
			{
				Title: "Transaction Control Mix (Internal)",
				Metrics: []string{
					"sql.txn.abort.count.internal",
					"sql.txn.begin.count.internal",
					"sql.txn.commit.count.internal",
					"sql.txn.rollback.count.internal",
					"sql.txn.begin.started.count.internal",
					"sql.txn.commit.started.count.internal",
					"sql.txn.rollback.started.count.internal",
				},
			},
			{
				Title: "Savepoints",
				Metrics: []string{
					"sql.savepoint.count",
					"sql.savepoint.count.internal",
					"sql.savepoint.started.count",
					"sql.savepoint.started.count.internal",
					"sql.savepoint.rollback.count",
					"sql.savepoint.rollback.count.internal",
					"sql.savepoint.rollback.started.count",
					"sql.savepoint.rollback.started.count.internal",
					"sql.savepoint.release.count",
					"sql.savepoint.release.count.internal",
					"sql.savepoint.release.started.count",
					"sql.savepoint.release.started.count.internal",
				},
				AxisLabel: "SQL Statements",
			},
			{
				Title: "Restarts",
				Metrics: []string{
					"sql.restart_savepoint.count",
					"sql.restart_savepoint.release.count",
					"sql.restart_savepoint.rollback.count",
					"sql.restart_savepoint.started.count",
					"sql.restart_savepoint.release.started.count",
					"sql.restart_savepoint.rollback.started.count",
				},
			},
			{
				Title: "Restarts (Internal)",
				Metrics: []string{
					"sql.restart_savepoint.count.internal",
					"sql.restart_savepoint.release.count.internal",
					"sql.restart_savepoint.rollback.count.internal",
					"sql.restart_savepoint.started.count.internal",
					"sql.restart_savepoint.release.started.count.internal",
					"sql.restart_savepoint.rollback.started.count.internal",
				},
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "Block Cache"}},
		Charts: []chartDescription{
			{
				Title: "Size",
				Metrics: []string{
					"rocksdb.block.cache.pinned-usage",
					"rocksdb.block.cache.usage",
				},
			},
			{
				Title: "Success",
				Metrics: []string{
					"rocksdb.block.cache.hits",
					"rocksdb.block.cache.misses",
				},
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "Encryption at Rest"}},
		Charts: []chartDescription{
			{
				Title:   "Algorithm Enum",
				Metrics: []string{"rocksdb.encryption.algorithm"},
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "Overview"}},
		Charts: []chartDescription{
			{
				Title: "Bloom Filter",
				Metrics: []string{
					"rocksdb.bloom.filter.prefix.checked",
					"rocksdb.bloom.filter.prefix.useful",
				},
			},
			{
				Title:   "Compactions",
				Metrics: []string{"rocksdb.compactions"},
			},
			{
				Title:   "Flushes",
				Metrics: []string{"rocksdb.flushes"},
			},
			{
				Title:   "Index & Filter Block Size",
				Metrics: []string{"rocksdb.table-readers-mem-estimate"},
			},
			{
				Title:   "Memtable",
				Metrics: []string{"rocksdb.memtable.total-size"},
			},
			{
				Title:   "Read Amplification",
				Metrics: []string{"rocksdb.read-amplification"},
			},
			{
				Title:   "Pending Compaction",
				Metrics: []string{"rocksdb.estimated-pending-compaction"},
			},
			{
				Title:   "Ingestion",
				Metrics: []string{"rocksdb.ingested-bytes"},
			},
			{
				Title: "Flush & Compaction",
				Metrics: []string{
					"rocksdb.compacted-bytes-read",
					"rocksdb.compacted-bytes-written",
					"rocksdb.flushed-bytes",
				},
				AxisLabel: "Bytes",
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "RocksDB", "SSTables"}},
		Charts: []chartDescription{
			{
				Title:   "Count",
				Metrics: []string{"rocksdb.num-sstables"},
			},
			{
				Title: "Ingestions",
				Metrics: []string{
					"addsstable.copies",
					"addsstable.applications",
					"addsstable.proposals",
				},
			},
			{
				Title: "Ingestion Delays",
				Metrics: []string{
					"addsstable.delay.total",
					"addsstable.delay.enginebackpressure",
				},
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "Storage", "Compactor"}},
		Charts: []chartDescription{
			{
				Title: "Overview",
				Metrics: []string{
					"compactor.suggestionbytes.compacted",
					"compactor.suggestionbytes.skipped",
				},
			},
			{
				Title: "Queued",
				Metrics: []string{
					"compactor.suggestionbytes.queued",
				},
			},
			{
				Title: "Success",
				Metrics: []string{
					"compactor.compactions.failure",
					"compactor.compactions.success",
				},
			},
			{
				Title:   "Time",
				Metrics: []string{"compactor.compactingnanos"},
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "Storage", "KV"}},
		Charts: []chartDescription{
			{
				Title:     "Counts",
				AxisLabel: "MVCC Keys & Values",
				Metrics: []string{
					"intentcount",
					"keycount",
					"livecount",
					"syscount",
					"valcount",
				},
			},
			{
				Title:   "Cumultative Age of Non-Live Data",
				Metrics: []string{"gcbytesage"},
			},
			{
				Title:   "Cumultative Intent Age",
				Metrics: []string{"intentage"},
			},
			{
				Title:   "Metric Update Frequency",
				Metrics: []string{"lastupdatenanos"},
			},
			{
				Title: "Size",
				Metrics: []string{
					"intentbytes",
					"keybytes",
					"livebytes",
					"sysbytes",
					"totalbytes",
					"valbytes",
				},
			},
		},
	},
	{
		Organization: [][]string{{StorageLayer, "Storage", "Overview"}},
		Charts: []chartDescription{
			{
				Title: "Capacity",
				Metrics: []string{
					"capacity.available",
					"capacity",
					"capacity.reserved",
					"capacity.used",
					"capacity.tsdb_used",
					"capacity.relational_used",
				},
			},
		},
	},
	{
		Organization: [][]string{{Timeseries, "Maintenance Queue"}},
		Charts: []chartDescription{
			{
				Title:   "Pending",
				Metrics: []string{"queue.tsmaintenance.pending"},
			},
			{
				Title: "Successes",
				Metrics: []string{
					"queue.tsmaintenance.process.success",
					"queue.tsmaintenance.process.failure",
				},
			},
			{
				Title:   "Time Spent",
				Metrics: []string{"queue.tsmaintenance.processingnanos"},
			},
		},
	},
	{
		Organization: [][]string{{Timeseries, "Overview"}},
		Charts: []chartDescription{
			{
				Title:   "Count",
				Metrics: []string{"timeseries.write.samples"},
			},
			{
				Title:   "Error Count",
				Metrics: []string{"timeseries.write.errors"},
			},
			{
				Title:   "Size",
				Metrics: []string{"timeseries.write.bytes"},
			},
		},
	},
	{
		Organization: [][]string{{License, "expire time", "measuring points"}},
		Charts: []chartDescription{
			{
				Title:   "license left time",
				Metrics: []string{"sys.license.left.time.sec"},
			},
			{
				Title:   "measuring points total count",
				Metrics: []string{"ts.measuring.points.total.count"},
			},
			{
				Title:   "measuring points used count",
				Metrics: []string{"ts.measuring.points.used.count"},
			},
		},
	},
	{
		Organization: [][]string{{Jobs, "Schedules", "Daemon"}},
		Charts: []chartDescription{
			{
				Title: "Round",
				Metrics: []string{
					"schedules.round.schedules-ready-to-run",
					"schedules.round.reschedule-skip",
					"schedules.round.reschedule-wait",
					"schedules.round.jobs-started",
					"schedules.round.num-jobs-running",
				},
				AxisLabel: "Count",
			},
			{
				Title: "Total",
				Metrics: []string{
					"schedules.corrupt",
				},
				AxisLabel: "Count",
			},
		},
	},
}
