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

package reports

import (
	"context"
	gosql "database/sql"
	"encoding/hex"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/keysutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Test the constraint conformance report in a real cluster.
func TestConstraintConformanceReportIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// 该测试为测试set zone config constraint 的测试，但是在 V2.1 版本中禁用了constraint的设置,故暂时跳过
	t.Skip()
	if testing.Short() {
		// This test takes seconds because of replication vagaries.
		t.Skip("short flag")
	}
	if testutils.NightlyStress() && util.RaceEnabled {
		// Under stressrace, replication changes seem to hit 1m deadline errors and
		// don't make progress.
		t.Skip("test too slow for stressrace")
	}

	ctx := context.Background()
	tc := serverutils.StartTestCluster(t, 5, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r1"}}}},
			1: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r1"}}}},
			2: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r2"}}}},
			3: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r2"}}}},
			4: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r2"}}}},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	// Speed up the generation of the
	_, err := db.Exec("set cluster setting kv.replication_reports.interval = '1ms'")
	require.NoError(t, err)

	// Create a table and a zone config for it.
	// The zone will be configured with a constraints that can't be satisfied
	// because there are not enough nodes in the requested region.
	_, err = db.Exec("create table t(x int primary key); " +
		"alter table t configure zone using constraints='[+region=r1]'")
	require.NoError(t, err)

	// Get the id of the newly created zone.
	r := db.QueryRow("select zone_id from kwdb_internal.zones where table_name = 't'")
	var zoneID int
	require.NoError(t, r.Scan(&zoneID))

	// Wait for the violation to be detected.
	testutils.SucceedsSoon(t, func() error {
		r := db.QueryRow(
			"select violating_ranges from system.replication_constraint_stats where zone_id = $1",
			zoneID)
		var numViolations int
		if err := r.Scan(&numViolations); err != nil {
			return err
		}
		if numViolations == 0 {
			return fmt.Errorf("violation not detected yet")
		}
		return nil
	})

	// Now change the constraint asking for t to be placed in r2. This time it can be satisfied.
	_, err = db.Exec("alter table t configure zone using constraints='[+region=r2]'")
	require.NoError(t, err)

	// Wait for the violation to clear.
	testutils.SucceedsSoon(t, func() error {
		// Kick the replication queues, given that our rebalancing is finicky.
		for i := 0; i < tc.NumServers(); i++ {
			if err := tc.Server(i).GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
				return s.ForceReplicationScanAndProcess()
			}); err != nil {
				t.Fatal(err)
			}
		}
		r := db.QueryRow(
			"select violating_ranges from system.replication_constraint_stats where zone_id = $1",
			zoneID)
		var numViolations int
		if err := r.Scan(&numViolations); err != nil {
			return err
		}
		if numViolations > 0 {
			return fmt.Errorf("still reporting violations")
		}
		return nil
	})
}

// Test the critical localities report in a real cluster.
func TestCriticalLocalitiesReportIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	// 2 regions, 3 dcs per region.
	tc := serverutils.StartTestCluster(t, 6, base.TestClusterArgs{
		// We're going to do our own replication.
		// All the system ranges will start with a single replica on node 1.
		ReplicationMode: base.ReplicationManual,
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r1"}, {Key: "dc", Value: "dc1"}},
				},
			},
			1: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r1"}, {Key: "dc", Value: "dc2"}},
				},
			},
			2: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r1"}, {Key: "dc", Value: "dc3"}},
				},
			},
			3: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r2"}, {Key: "dc", Value: "dc4"}},
				},
			},
			4: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r2"}, {Key: "dc", Value: "dc5"}},
				},
			},
			5: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r2"}, {Key: "dc", Value: "dc6"}},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	// Speed up the generation of the reports.
	_, err := db.Exec("set cluster setting kv.replication_reports.interval = '1ms'")
	require.NoError(t, err)

	// Since we're using ReplicationManual, all the ranges will start with a
	// single replica on node 1. So, the node's dc and the node's region are
	// critical. Let's verify that.

	// Collect all the zones that exist at cluster bootstrap.
	systemZoneIDs := make([]int, 0, 10)
	systemZones := make([]zonepb.ZoneConfig, 0, 10)
	{
		rows, err := db.Query("select id, config from system.zones")
		require.NoError(t, err)
		for rows.Next() {
			var zoneID int
			var buf []byte
			cfg := zonepb.ZoneConfig{}
			require.NoError(t, rows.Scan(&zoneID, &buf))
			buf, err := hex.DecodeString(string(buf[2:]))
			if err != nil {
				t.Fatal(err)
			}
			require.NoError(t, protoutil.Unmarshal(buf, &cfg))
			systemZoneIDs = append(systemZoneIDs, zoneID)
			systemZones = append(systemZones, cfg)
		}
		require.NoError(t, rows.Err())
	}
	require.Greater(t, len(systemZoneIDs), 0, "expected some system zones, got none")
	// Remove the entries in systemZoneIDs that don't get critical locality reports.
	i := 0
	for j, zid := range systemZoneIDs {
		if zoneChangesReplication(&systemZones[j]) {
			systemZoneIDs[i] = zid
			i++
		}
	}
	systemZoneIDs = systemZoneIDs[:i]

	expCritLoc := []string{"region=r1", "region=r1,dc=dc1"}

	// Wait for the report to be generated.
	{
		var rowCount int
		testutils.SucceedsSoon(t, func() error {
			r := db.QueryRow("select count(1) from system.replication_critical_localities")
			require.NoError(t, r.Scan(&rowCount))
			if rowCount == 0 {
				return fmt.Errorf("no report yet")
			}
			return nil
		})
		require.Equal(t, 2*len(systemZoneIDs), rowCount)
	}

	// Check that we have all the expected rows.
	for _, zid := range systemZoneIDs {
		for _, s := range expCritLoc {
			r := db.QueryRow(
				"select at_risk_ranges from system.replication_critical_localities "+
					"where zone_id=$1 and locality=$2",
				zid, s)
			var numRanges int
			require.NoError(t, r.Scan(&numRanges))
			require.NotEqual(t, 0, numRanges)
		}
	}

	// Now create a table and a zone for it. At first n1 should be critical for it.
	// Then we'll upreplicate it in different ways.

	// Create a table with a dummy zone config. Configuring the zone is useful
	// only for creating the zone; we don't actually care about the configuration.
	// Also do a split by hand. With manual replication, we're not getting the
	// split for the table automatically.
	_, err = db.Exec("create table t(x int primary key); " +
		"alter table t configure zone using num_replicas=3; " +
		"alter table t split at values (0);")
	require.NoError(t, err)
	// Get the id of the newly created zone.
	r := db.QueryRow("select zone_id from kwdb_internal.zones where table_name = 't'")
	var zoneID int
	require.NoError(t, r.Scan(&zoneID))

	// Check initial conditions.
	require.NoError(t, checkCritical(db, zoneID, "region=r1", "region=r1,dc=dc1"))

	// Upreplicate to 2 dcs. Now they're both critical.
	_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2], 1)")
	require.NoError(t, err)
	require.NoError(t, checkCritical(db, zoneID, "region=r1", "region=r1,dc=dc1", "region=r1,dc=dc2"))

	// Upreplicate to one more dc. Now no dc is critical, only the region.
	_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2,3], 1)")
	require.NoError(t, err)
	require.NoError(t, checkCritical(db, zoneID, "region=r1"))

	// Move two replicas to the other region. Now that region is critical.
	_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,4,5], 1)")
	require.NoError(t, err)
	require.NoError(t, checkCritical(db, zoneID, "region=r2"))
}

func checkCritical(db *gosql.DB, zoneID int, locs ...string) error {
	return testutils.SucceedsSoonError(func() error {
		rows, err := db.Query(
			"select locality, at_risk_ranges from system.replication_critical_localities "+
				"where zone_id=$1", zoneID)
		if err != nil {
			return err
		}
		critical := make(map[string]struct{})
		for rows.Next() {
			var numRanges int
			var loc string
			err := rows.Scan(&loc, &numRanges)
			if err != nil {
				return err
			}
			if numRanges == 0 {
				return fmt.Errorf("expected ranges_at_risk for %s", loc)
			}
			critical[loc] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(locs) != len(critical) {
			return fmt.Errorf("expected critical: %s, got: %s", locs, critical)
		}
		for _, l := range locs {
			if _, ok := critical[l]; !ok {
				return fmt.Errorf("missing critical locality: %s", l)
			}
		}
		return nil
	})
}

// Test the replication status report in a real cluster.
func TestReplicationStatusReportIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := serverutils.StartTestCluster(t, 4, base.TestClusterArgs{
		// We're going to do our own replication.
		// All the system ranges will start with a single replica on node 1.
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	// Speed up the generation of the
	_, err := db.Exec("set cluster setting kv.replication_reports.interval = '1ms'")
	require.NoError(t, err)

	// Create a table with a dummy zone config. Configuring the zone is useful
	// only for creating the zone; we don't actually care about the configuration.
	// Also do a split by hand. With manual replication, we're not getting the
	// split for the table automatically.
	_, err = db.Exec("create table t(x int primary key); " +
		"alter table t configure zone using num_replicas=3; " +
		"alter table t split at values (0);")
	require.NoError(t, err)
	// Get the id of the newly created zone.
	r := db.QueryRow("select zone_id from kwdb_internal.zones where table_name = 't'")
	var zoneID int
	require.NoError(t, r.Scan(&zoneID))

	// Upreplicate the range.
	_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2,3], 1)")
	require.NoError(t, err)
	require.NoError(t, checkZoneReplication(db, zoneID, 1, 0, 0, 0))

	// Over-replicate.
	_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2,3,4], 1)")
	require.NoError(t, err)
	require.NoError(t, checkZoneReplication(db, zoneID, 1, 0, 1, 0))

	// TODO(andrei): I'd like to downreplicate to one replica and then stop that
	// node and check that the range is counter us "unavailable", but stopping a
	// node makes the report generation simply block sometimes trying to scan
	// Meta2. I believe I believe it's due to #40529.
	// Once stopping a node works, next thing is to start it up again.
	// Take inspiration from replica_learner_test.go.

	//// Down-replicate to one node and then kill the node. Check that the range becomes unavailable.
	//_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[4], 1)")
	//require.NoError(t, err)
	//tc.StopServer(3)
	//require.NoError(t, checkZoneReplication(db, zoneID, 1, 1, 0, 1))
}

func checkZoneReplication(db *gosql.DB, zoneID, total, under, over, unavailable int) error {
	return testutils.SucceedsSoonError(func() error {
		r := db.QueryRow(
			"select total_ranges, under_replicated_ranges, over_replicated_ranges, "+
				"unavailable_ranges from system.replication_stats where zone_id=$1",
			zoneID)
		var gotTotal, gotUnder, gotOver, gotUnavailable int
		if err := r.Scan(&gotTotal, &gotUnder, &gotOver, &gotUnavailable); err != nil {
			return err
		}
		if total != gotTotal {
			return fmt.Errorf("expected total: %d, got: %d", total, gotTotal)
		}
		if under != gotUnder {
			return fmt.Errorf("expected under: %d, got: %d", total, gotUnder)
		}
		if over != gotOver {
			return fmt.Errorf("expected over: %d, got: %d", over, gotOver)
		}
		if unavailable != gotUnavailable {
			return fmt.Errorf("expected unavailable: %d, got: %d", unavailable, gotUnavailable)
		}
		return nil
	})
}

func TestMeta2RangeIter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// First make an interator with a large page size and use it to determine the numner of ranges.
	iter := makeMeta2RangeIter(db, 10000 /* batchSize */)
	numRanges := 0
	for {
		rd, err := iter.Next(ctx)
		require.NoError(t, err)
		if rd.RangeID == 0 {
			break
		}
		numRanges++
	}
	require.Greater(t, numRanges, 20, "expected over 20 ranges, got: %d", numRanges)

	// Now make an interator with a small page size and check that we get just as many ranges.
	iter = makeMeta2RangeIter(db, 2 /* batch size */)
	numRangesPaginated := 0
	for {
		rd, err := iter.Next(ctx)
		require.NoError(t, err)
		if rd.RangeID == 0 {
			break
		}
		numRangesPaginated++
	}
	require.Equal(t, numRanges, numRangesPaginated)
}

// Test that a retriable error returned from the range iterator is properly
// handled by resetting the report.
func TestRetriableErrorWhenGenerationReport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	cfg := s.GossipI().(*gossip.Gossip).GetSystemConfig()
	dummyNodeChecker := func(id roachpb.NodeID) bool { return true }

	saver := makeReplicationStatsReportSaver()
	v := makeReplicationStatsVisitor(ctx, cfg, dummyNodeChecker, &saver)
	realIter := makeMeta2RangeIter(db, 10000 /* batchSize */)
	require.NoError(t, visitRanges(ctx, &realIter, cfg, &v))
	expReport := v.report
	require.Greater(t, len(expReport.stats), 0, "unexpected empty report")

	realIter = makeMeta2RangeIter(db, 10000 /* batchSize */)
	errorIter := erroryRangeIterator{
		iter:           realIter,
		injectErrAfter: 3,
	}
	v = makeReplicationStatsVisitor(ctx, cfg, func(id roachpb.NodeID) bool { return true }, &saver)
	require.NoError(t, visitRanges(ctx, &errorIter, cfg, &v))
	require.Greater(t, len(v.report.stats), 0, "unexpected empty report")
	require.Equal(t, expReport, v.report)
}

type erroryRangeIterator struct {
	iter           meta2RangeIter
	rangesReturned int
	injectErrAfter int
}

var _ RangeIterator = &erroryRangeIterator{}

func (it *erroryRangeIterator) Next(ctx context.Context) (roachpb.RangeDescriptor, error) {
	if it.rangesReturned == it.injectErrAfter {
		// Don't inject any more errors.
		it.injectErrAfter = -1

		var err error
		err = roachpb.NewTransactionRetryWithProtoRefreshError(
			"injected err", uuid.Nil, roachpb.Transaction{})
		// Let's wrap the error to check the unwrapping.
		err = errors.Wrap(err, "dummy wrapper")
		// Feed the error to the underlying iterator to reset it.
		it.iter.handleErr(ctx, err)
		return roachpb.RangeDescriptor{}, err
	}
	it.rangesReturned++
	rd, err := it.iter.Next(ctx)
	return rd, err
}

func (it *erroryRangeIterator) Close(ctx context.Context) {
	it.iter.Close(ctx)
}

// computeConstraintConformanceReport iterates through all the ranges and
// generates the constraint conformance report.
func computeConstraintConformanceReport(
	ctx context.Context,
	rangeStore RangeIterator,
	cfg *config.SystemConfig,
	storeResolver StoreResolver,
) (*replicationConstraintStatsReportSaver, error) {
	saver := makeReplicationConstraintStatusReportSaver()
	v := makeConstraintConformanceVisitor(ctx, cfg, storeResolver, &saver)
	err := visitRanges(ctx, rangeStore, cfg, &v)
	return v.report, err
}

// computeReplicationStatsReport iterates through all the ranges and generates
// the replication stats report.
func computeReplicationStatsReport(
	ctx context.Context, rangeStore RangeIterator, checker nodeChecker, cfg *config.SystemConfig,
) (*replicationStatsReportSaver, error) {
	saver := makeReplicationStatsReportSaver()
	v := makeReplicationStatsVisitor(ctx, cfg, checker, &saver)
	err := visitRanges(ctx, rangeStore, cfg, &v)
	return v.report, err
}

func TestZoneChecker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	type tc struct {
		split          string
		newZone        bool
		newRootZoneCfg *zonepb.ZoneConfig
		newZoneKey     ZoneKey
	}
	// NB: IDs need to be beyond MaxSystemConfigDescID, otherwise special logic
	// kicks in for mapping keys to zones.
	dbID := 50
	t1ID := 51
	t1 := table{name: "t1",
		partitions: []partition{
			{
				name:  "p1",
				start: []int{100},
				end:   []int{200},
				zone:  &zone{constraints: "[+p1]"},
			},
			{
				name:  "p2",
				start: []int{300},
				end:   []int{400},
				zone:  &zone{constraints: "[+p2]"},
			},
		},
	}
	t1.addPKIdx()
	// Create a table descriptor to be used for creating the zone config.
	t1Desc, err := makeTableDesc(t1, t1ID, dbID)
	require.NoError(t, err)
	t1Zone, err := generateTableZone(t1, t1Desc)
	require.NoError(t, err)
	p1SubzoneIndex := 0
	p2SubzoneIndex := 1
	require.Equal(t, "p1", t1Zone.Subzones[p1SubzoneIndex].PartitionName)
	require.Equal(t, "p2", t1Zone.Subzones[p2SubzoneIndex].PartitionName)
	t1ZoneKey := MakeZoneKey(uint32(t1ID), NoSubzone)
	p1ZoneKey := MakeZoneKey(uint32(t1ID), base.SubzoneIDFromIndex(p1SubzoneIndex))
	p2ZoneKey := MakeZoneKey(uint32(t1ID), base.SubzoneIDFromIndex(p2SubzoneIndex))

	ranges := []tc{
		{
			split:          "/Table/t1/pk/1",
			newZone:        true,
			newZoneKey:     t1ZoneKey,
			newRootZoneCfg: t1Zone,
		},
		{
			split:   "/Table/t1/pk/2",
			newZone: false,
		},
		{
			// p1's zone
			split:          "/Table/t1/pk/100",
			newZone:        true,
			newZoneKey:     p1ZoneKey,
			newRootZoneCfg: t1Zone,
		},
		{
			split:   "/Table/t1/pk/101",
			newZone: false,
		},
		{
			// Back to t1's zone
			split:          "/Table/t1/pk/200",
			newZone:        true,
			newZoneKey:     t1ZoneKey,
			newRootZoneCfg: t1Zone,
		},
		{
			// p2's zone
			split:          "/Table/t1/pk/305",
			newZone:        true,
			newZoneKey:     p2ZoneKey,
			newRootZoneCfg: t1Zone,
		},
	}

	splits := make([]split, len(ranges))
	for i := range ranges {
		splits[i].key = ranges[i].split
	}
	keyScanner := keysutils.MakePrettyScannerForNamedTables(
		map[string]int{"t1": t1ID} /* tableNameToID */, nil /* idxNameToID */)
	rngs, err := processSplits(keyScanner, splits)
	require.NoError(t, err)

	var zc zoneResolver
	for i, tc := range ranges {
		sameZone := zc.checkSameZone(ctx, &rngs[i])
		newZone := !sameZone
		require.Equal(t, tc.newZone, newZone, "failed at: %d (%s)", i, tc.split)
		if newZone {
			objectID, _ := config.DecodeKeyIntoZoneIDAndSuffix(rngs[i].StartKey)
			zc.setZone(objectID, tc.newZoneKey, tc.newRootZoneCfg)
		}
	}
}

// TestRangeIteration checks that visitRanges() correctly informs range
// visitors whether ranges fall in the same zone vs a new zone.
func TestRangeIteration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	schema := baseReportTestCase{
		schema: []database{{
			name: "db1",
			zone: &zone{
				replicas: 3,
			},
			tables: []table{
				{
					name: "t1",
					partitions: []partition{
						{
							name:  "p1",
							start: []int{100},
							end:   []int{200},
							zone:  &zone{},
						},
						{
							name:  "p2",
							start: []int{200},
							end:   []int{300},
							zone:  &zone{},
						},
					},
				},
				{
					name: "t2",
				},
			},
		},
		},
		splits: []split{
			{key: "/Table/t1/pk/1"},
			{key: "/Table/t1/pk/2"},
			{key: "/Table/t1/pk/100"},
			{key: "/Table/t1/pk/101"},
			{key: "/Table/t1/pk/200"},
			{key: "/Table/t1/pk/305"},
			{key: "/Table/t2/pk/1"},
		},
		defaultZone: zone{},
	}

	compiled, err := compileTestCase(schema)
	require.NoError(t, err)
	v := recordingRangeVisitor{}
	require.NoError(t, visitRanges(ctx, &compiled.iter, compiled.cfg, &v))

	type entry struct {
		newZone bool
		key     string
	}
	exp := []entry{
		{newZone: true, key: "/Table/76/1/1"},
		{newZone: false, key: "/Table/76/1/2"},
		{newZone: true, key: "/Table/76/1/100"},
		{newZone: false, key: "/Table/76/1/101"},
		{newZone: true, key: "/Table/76/1/200"},
		{newZone: true, key: "/Table/76/1/305"},
		{newZone: true, key: "/Table/77/1/1"},
	}
	got := make([]entry, len(v.rngs))
	for i, r := range v.rngs {
		got[i].newZone = r.newZone
		got[i].key = r.rng.StartKey.String()
	}
	require.Equal(t, exp, got)
}

type recordingRangeVisitor struct {
	rngs []visitorEntry
}

var _ rangeVisitor = &recordingRangeVisitor{}

func (r *recordingRangeVisitor) visitNewZone(
	_ context.Context, rng *roachpb.RangeDescriptor,
) error {
	r.rngs = append(r.rngs, visitorEntry{newZone: true, rng: *rng})
	return nil
}

func (r *recordingRangeVisitor) visitSameZone(
	_ context.Context, rng *roachpb.RangeDescriptor,
) error {
	r.rngs = append(r.rngs, visitorEntry{newZone: false, rng: *rng})
	return nil
}

func (r *recordingRangeVisitor) failed() bool {
	return false
}

func (r *recordingRangeVisitor) reset(ctx context.Context) {
	r.rngs = nil
}

type visitorEntry struct {
	newZone bool
	rng     roachpb.RangeDescriptor
}
