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

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// CandidateNode nodes
type CandidateNode struct {
	NodeID       roachpb.NodeID
	DiskUsage    float64
	ReplicaCount int
	LeaseCount   int
}

// CurrentRange ranges
type CurrentRange struct {
	RangeID   roachpb.RangeID
	Lease     roachpb.NodeID
	Replicas  []roachpb.NodeID
	RangeSize float64
	StartKey  roachpb.RKey
	EndKey    roachpb.RKey
}

// RebalancedPlan means migration plan
type RebalancedPlan struct {
	RangeID roachpb.RangeID
	size    float64
	src     roachpb.NodeID
	dst     roachpb.NodeID
}

type clusterState struct {
	Nodes  []CandidateNode
	Ranges []CurrentRange
}

// BalanceReplica set zone config to balance the replicas in the cluster.
func BalanceReplica(
	ctx context.Context,
	txn *kv.Txn,
	execCfg *ExecutorConfig,
	tc *TableCollection,
	zs *tree.ZoneSpecifier,
	table *TableDescriptor,
	user, stmt string,
) error {
	if table != nil && !table.IsTSTable() {
		return errors.New("rebalance is not support on relational table")
	}
	var nodes []CandidateNode
	resp, err := execCfg.StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return err
	}
	for _, ns := range resp.Nodes {
		id := ns.Desc.NodeID
		liveness, _ := resp.LivenessByNodeID[id]
		switch liveness {
		case storagepb.NodeLivenessStatus_DECOMMISSIONING:
			return errors.New("not supported when node is decommissioning")
		case storagepb.NodeLivenessStatus_LIVE:
		default:
			continue
		}
		nodes = append(nodes, CandidateNode{
			NodeID:    id,
			DiskUsage: float64(ns.StoreStatuses[0].Desc.Capacity.Used),
		})
	}

	type TableAndHashNum struct {
		id      sqlbase.ID
		hashNum uint64
	}
	var tables []TableAndHashNum

	if table != nil && zs.TableOrIndex.Table.String() != "" {
		tables = append(tables, TableAndHashNum{
			id:      table.ID,
			hashNum: table.TsTable.HashNum,
		})
	} else if zs.NamedZone.String() == "default" {
		descriptors, err := tc.getAllDescriptors(ctx, txn)
		if err != nil {
			return err
		}
		for i := range descriptors {
			tableDesc, ok := descriptors[i].(*sqlbase.TableDescriptor)
			if ok && tableDesc.IsTSTable() && tableDesc.ID > keys.MinNonPredefinedUserDescID {
				tables = append(tables, TableAndHashNum{
					id:      tableDesc.ID,
					hashNum: tableDesc.TsTable.HashNum,
				})
			}
		}
	} else if zs.Database.String() != "" {
		targetID, err := resolveZone(ctx, execCfg.Settings, txn, zs)
		if err != nil {
			return err
		}
		dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, txn, targetID)
		if err != nil {
			return err
		}
		if dbDesc.EngineType == tree.EngineTypeRelational {
			return errors.New("rebalance is not support on relational database")
		}
		tableNameSpace, err := GetNameSpaceByParentID(ctx, txn, targetID, keys.PublicSchemaID)
		if err != nil {
			return err
		}
		for _, n := range tableNameSpace {
			tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, sqlbase.ID(n.ID))
			if err != nil {
				return err
			}
			tables = append(tables, TableAndHashNum{
				id:      tableDesc.ID,
				hashNum: tableDesc.TsTable.HashNum,
			})
		}
	} else {
		return errors.Errorf("cannot find target to rebalance with %s", zs.String())
	}
	//tableIDs := s.server.nodeLiveness.GetAllTsTableID(ctx)
	var ranges []CurrentRange
	var relocateTargets []jobspb.RelocateTargets
	for _, tableAndNum := range tables {
		var numRepl int
		ranges, numRepl, err = getTableRanges(ctx, execCfg, tableAndNum.id, tableAndNum.hashNum, len(nodes))
		if err != nil {
			return err
		}
		if len(ranges) < 1 {
			// no need to balance the table
			continue
		}
		// assign nodes replicaCount
		var nodeReplica = make(map[roachpb.NodeID]int, len(nodes))
		var nodeLeases = make(map[roachpb.NodeID]int, len(nodes))
		for _, ra := range ranges {
			nodeLeases[ra.Lease]++
			for _, re := range ra.Replicas {
				nodeReplica[re]++
			}
		}
		for i := range nodes {
			nodes[i].ReplicaCount = nodeReplica[nodes[i].NodeID]
			nodes[i].LeaseCount = nodeLeases[nodes[i].NodeID]
		}

		initialState := clusterState{
			Nodes:  nodes,
			Ranges: ranges,
		}
		// todo(qzy): 1/3 replicas the most?
		for i := 0; i < len(initialState.Ranges); i++ {
			plan, score := greedyFindBestPlan(initialState, numRepl)
			if plan.RangeID == 0 {
				break
			}
			log.Infof(ctx, "replica rebalance of table %v and calculate plan %+v score %f",
				tableAndNum, plan, score)
			initialState = applyBalancePlan(initialState, plan)
		}
		initialState = rebalanceLeaseHolder(initialState)

		// alter zone config
		for _, r := range initialState.Ranges {
			if numRepl < len(r.Replicas) {
				r.Replicas = getRandomNTargets(r.Replicas, r.Lease, numRepl)
			}
			var targets []roachpb.ReplicationTarget
			for _, id := range r.Replicas {
				targets = append(targets, roachpb.ReplicationTarget{
					NodeID:  id,
					StoreID: roachpb.StoreID(id), // assume storeID is equal nodeID
				})
			}
			relocateTargets = append(relocateTargets, jobspb.RelocateTargets{
				TableID:  tableAndNum.id,
				RangeID:  r.RangeID,
				StartKey: r.StartKey,
				EndKey:   r.EndKey,
				Targets:  targets,
				PreLease: r.Lease,
				NumRepl:  int32(numRepl),
			})
			if err != nil {
				return err
			}
		}
	}
	var rebalanceDetails = jobspb.ReplicaRebanalceDetails{
		RelocateTargets: relocateTargets,
	}
	jr := jobs.Record{
		Statement:   stmt,
		Details:     rebalanceDetails,
		Description: "replica rebalance job",
		Progress:    jobspb.ReplicaRebanalceProgress{},
		Username:    user,
		CreatedBy:   &jobs.CreatedByInfo{Name: "REBALANCE"},
	}
	var j *jobs.Job
	j, _, err = execCfg.JobRegistry.CreateAndStartJob(ctx, nil, jr)
	if err != nil {
		return err
	}
	log.Infof(ctx, "replica rebalance job %v start with details %v", *j.ID(), rebalanceDetails)
	return nil
}

func getRandomNTargets(targets []roachpb.NodeID, lease roachpb.NodeID, num int) []roachpb.NodeID {
	rand.Seed(timeutil.Now().UnixNano())

	n := len(targets)
	if num <= 0 || n == 0 {
		return nil
	}
	if num >= n {
		return targets
	}
	// copy
	tmp := make([]roachpb.NodeID, n)
	copy(tmp, targets)

	// select random targets
	for i := 0; i < num; i++ {
		r := rand.Intn(n-i) + i
		tmp[i], tmp[r] = tmp[r], tmp[i]
	}
	if !inReplica(tmp, lease) {
		tmp[0] = lease
	}
	return tmp[:num]
}

func rebalanceLeaseHolder(state clusterState) clusterState {
	if len(state.Nodes) == 0 || len(state.Ranges) == 0 {
		return state
	}
	expectLeaseCount := len(state.Ranges) / len(state.Nodes)
	nodeLeaseCount := make(map[roachpb.NodeID]int)
	for _, node := range state.Nodes {
		nodeLeaseCount[node.NodeID] = node.LeaseCount
	}
	rangeLeases := getBestRangeLease(state.Ranges, nodeLeaseCount, expectLeaseCount)

	for i := range state.Ranges {
		state.Ranges[i].Lease = rangeLeases[state.Ranges[i].RangeID]
	}
	return state
}

func getBestRangeLease(
	ranges []CurrentRange, leaseCount map[roachpb.NodeID]int, expect int,
) map[roachpb.RangeID]roachpb.NodeID {
	var rangeLeases = make(map[roachpb.RangeID]roachpb.NodeID)
	for _, ra := range ranges {
		var minLeasesNode = ra.Replicas[0]
		if !inReplica(ra.Replicas, ra.Lease) {
			for _, re := range ra.Replicas[1:] {
				if leaseCount[re] < leaseCount[minLeasesNode] {
					minLeasesNode = re
				}
			}
			leaseCount[ra.Lease]--
			leaseCount[minLeasesNode]++
			rangeLeases[ra.RangeID] = minLeasesNode
		} else {
			var more, less roachpb.NodeID
			for _, re := range ra.Replicas {
				if leaseCount[re] > expect && leaseCount[re] > leaseCount[more] {
					more = re
				} else if leaseCount[re] < expect && (less == 0 || leaseCount[re] < leaseCount[less]) {
					less = re
				}
			}
			target := ra.Lease
			if more != 0 && less != 0 && more == ra.Lease {
				leaseCount[more]--
				leaseCount[less]++
				target = less
			}
			rangeLeases[ra.RangeID] = target
		}
	}
	return rangeLeases
}

// find the best plan greedily
func greedyFindBestPlan(state clusterState, numRepl int) (RebalancedPlan, float64) {
	bestScore := evaluateBalancePlan(state, RebalancedPlan{}, numRepl)

	plans := generateOneStepBalancePlans(state)

	var bestMigration RebalancedPlan

	for _, plan := range plans {
		score := evaluateBalancePlan(state, plan, numRepl /*, currentBalance*/)
		if score > bestScore {
			bestScore = score
			bestMigration = plan
		}
	}

	return bestMigration, bestScore
}

const (
	replicaBalanceWeight = 0.4
	migrationCostWeight  = 0.5
	diskBalanceWeight    = 0.1
)

// evaluateBalancePlan evaluate the score of the plan
func evaluateBalancePlan(state clusterState, p RebalancedPlan, numRepl int) float64 {
	// apply the plan
	newState := applyBalancePlan(state, p)

	// calculate the different scores
	replicaScore := calculateReplicaBalance(newState, numRepl)
	diskScore := calculateDiskUsageBalance(newState)

	maxData := 0.0
	for _, r := range state.Ranges {
		maxData += r.RangeSize
	}
	migrationScore := calculateMigrationTotal([]RebalancedPlan{p}, 3*maxData)
	// calculate score
	return replicaBalanceWeight*replicaScore +
		migrationCostWeight*migrationScore +
		diskBalanceWeight*diskScore
}

// deepCopyClusterState copy clusterState
func deepCopyClusterState(state clusterState) clusterState {
	newState := clusterState{
		Nodes:  make([]CandidateNode, len(state.Nodes)),
		Ranges: make([]CurrentRange, len(state.Ranges)),
	}
	copy(newState.Nodes, state.Nodes)
	for i, r := range state.Ranges {
		newState.Ranges[i].RangeID = r.RangeID
		newState.Ranges[i].Lease = r.Lease
		newState.Ranges[i].RangeSize = r.RangeSize
		newState.Ranges[i].StartKey = r.StartKey
		newState.Ranges[i].EndKey = r.EndKey
		// deep copy Replicas slice
		newState.Ranges[i].Replicas = make([]roachpb.NodeID, len(r.Replicas))
		copy(newState.Ranges[i].Replicas, r.Replicas)
	}
	return newState
}

// apply and get new clusterState
func applyBalancePlan(state clusterState, p RebalancedPlan) clusterState {
	newState := clusterState{
		Nodes:  make([]CandidateNode, len(state.Nodes)),
		Ranges: make([]CurrentRange, len(state.Ranges)),
	}
	if p.RangeID == 0 {
		newState = state
		return newState
	}

	// copy State
	newState = deepCopyClusterState(state)
	// update node message
	for i, node := range newState.Nodes {
		if node.NodeID == p.src {
			newState.Nodes[i].ReplicaCount--
			newState.Nodes[i].DiskUsage -= p.size
		}
		if node.NodeID == p.dst {
			newState.Nodes[i].ReplicaCount++
			newState.Nodes[i].DiskUsage += p.size
		}
	}

	for i, r := range newState.Ranges {
		if r.RangeID == p.RangeID {
			for j := range newState.Ranges[i].Replicas {
				if newState.Ranges[i].Replicas[j] == p.src {
					newState.Ranges[i].Replicas[j] = p.dst
				}
			}
			break
		}
	}

	return newState
}

func getTableRanges(
	ctx context.Context, exec *ExecutorConfig, tableID sqlbase.ID, hashNum uint64, nodeNum int,
) ([]CurrentRange, int, error) {
	var curRanges []CurrentRange
	var numRepl = 3
	if err := exec.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		ranges, err := ScanMetaKVs(ctx, txn, roachpb.Span{
			Key:    sqlbase.MakeTsRangeKey(tableID, 0, math.MinInt64, hashNum),
			EndKey: sqlbase.MakeTsRangeKey(tableID, hashNum-1, math.MaxInt64, hashNum),
		})
		if err != nil {
			return err
		}
		if len(ranges) == 0 {
			return pgerror.Newf(pgcode.Warning, "can not get ranges of the table %v", tableID)
		}
		for _, r := range ranges {
			var desc roachpb.RangeDescriptor
			if err := r.ValueProto(&desc); err != nil {
				return err
			}
			var replicas []roachpb.NodeID
			for _, replica := range desc.InternalReplicas {
				replicas = append(replicas, replica.NodeID)
			}
			// get range size
			b := &kv.Batch{}
			b.AddRawRequest(&roachpb.RangeStatsRequest{
				RequestHeader: roachpb.RequestHeader{
					Key: desc.StartKey.AsRawKey(),
				},
			})
			b.AddRawRequest(&roachpb.LeaseInfoRequest{
				RequestHeader: roachpb.RequestHeader{
					Key: desc.StartKey.AsRawKey(),
				},
			})
			if err = txn.Run(ctx, b); err != nil {
				return pgerror.Newf(pgcode.InvalidParameterValue, "message: %s", err)
			}
			resp := b.RawResponse().Responses[0].GetInner().(*roachpb.RangeStatsResponse).MVCCStats
			lease := b.RawResponse().Responses[1].GetInner().(*roachpb.LeaseInfoResponse).Lease
			var zone *zonepb.ZoneConfig
			zone, err = exec.Gossip.GetSystemConfig().GetZoneConfigForTSKey(desc.StartKey, desc.HashNum)
			if err != nil {
				return err
			}
			numReplicas := *zone.NumReplicas
			if int(numReplicas) > nodeNum {
				return errors.Errorf("available nodes is not enough for table %d range %d with "+
					"num_replicas = %d", desc.TableId, desc.TableId, numReplicas)
			}

			curRanges = append(curRanges, CurrentRange{
				RangeID:   desc.RangeID,
				Replicas:  replicas,
				RangeSize: float64(resp.ValBytes),
				StartKey:  desc.StartKey,
				EndKey:    desc.EndKey,
				Lease:     lease.Replica.NodeID,
			})
			numRepl = int(numReplicas)
		}
		return nil
	}); err != nil {
		return nil, 0, err
	}
	return curRanges, numRepl, nil
}

// generate all plans
func generateOneStepBalancePlans(state clusterState) []RebalancedPlan {
	var options []RebalancedPlan

	for _, r := range state.Ranges {
		for _, nodeID := range r.Replicas {
			for _, targetNode := range state.Nodes {
				if inReplica(r.Replicas, targetNode.NodeID) {
					continue
				}
				if targetNode.NodeID != nodeID {
					options = append(options, RebalancedPlan{
						RangeID: r.RangeID,
						size:    r.RangeSize,
						src:     nodeID,
						dst:     targetNode.NodeID,
					})
				}
			}
		}
	}

	return options
}

func inReplica(replicas []roachpb.NodeID, node roachpb.NodeID) bool {
	for _, n := range replicas {
		if node == n {
			return true
		}
	}
	return false
}

// calculateReplicaBalance calculate the score replica
func calculateReplicaBalance(state clusterState, numRepl int) float64 {
	nodes, ranges := state.Nodes, state.Ranges
	// get replica num
	var nodeReplica = make(map[roachpb.NodeID]int, len(nodes))
	for _, n := range nodes {
		nodeReplica[n.NodeID] = 0
	}
	for _, ra := range ranges {
		for _, re := range ra.Replicas {
			nodeReplica[re]++
		}
	}

	sum := numRepl * len(ranges)
	avg := float64(sum) / float64(len(nodes))

	variance := 0.0
	for _, countNum := range nodeReplica {
		variance += math.Pow(float64(countNum)-avg, 2)
	}
	variance /= float64(len(nodeReplica))

	return 1 - math.Sqrt(variance)/avg
}

// calculateMigrationTotal calculate the sore migration
func calculateMigrationTotal(migrations []RebalancedPlan, maxData float64) float64 {
	if maxData <= 0 {
		return 0 // 避免除以0
	}

	totalMigration := 0.0
	for _, migration := range migrations {
		totalMigration += migration.size
	}
	if totalMigration > maxData {
		totalMigration = maxData
	}

	return 1 - totalMigration/maxData
}

// calculateDiskUsageBalance calculate the score node diskusage
func calculateDiskUsageBalance(state clusterState) float64 {
	var nodeStorages = make(map[roachpb.NodeID]float64, len(state.Nodes))
	for _, node := range state.Nodes {
		nodeStorages[node.NodeID] = node.DiskUsage
	}

	var sum float64
	for _, nodeStorage := range nodeStorages {
		sum += nodeStorage
	}
	avg := sum / float64(len(nodeStorages))

	variance := 0.0
	for _, storage := range nodeStorages {
		variance += math.Pow(storage-avg, 2)
	}
	variance /= float64(len(nodeStorages))

	return 1 - math.Sqrt(variance)/avg
}

type replicaRebalanceResumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = &replicaRebalanceResumer{}

func removeExtraTarget(target jobspb.RelocateTargets) []roachpb.ReplicationTarget {
	var newTarget []roachpb.ReplicationTarget
	var haveLease bool
	for _, t := range target.Targets {
		if t.NodeID == target.PreLease {
			haveLease = true
		} else if !haveLease && len(newTarget) == int(target.NumRepl)-1 {
			continue
		}
		newTarget = append(newTarget, t)
		if len(newTarget) >= int(target.NumRepl) {
			break
		}
	}
	return newTarget
}

func (r *replicaRebalanceResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	p := phs.(PlanHookState)
	details := r.job.Details().(jobspb.ReplicaRebanalceDetails)
	var tableTargets = make(map[sqlbase.ID][]jobspb.RelocateTargets)
	for _, target := range details.RelocateTargets {
		if len(target.Targets) > int(target.NumRepl) {
			target.Targets = removeExtraTarget(target)
		}
		tableTargets[target.TableID] = append(tableTargets[target.TableID], target)
	}

	var nodesRegion = make(map[roachpb.NodeID]string)
	if len(tableTargets) == 0 {
		return nil
	}
	resp, err := p.ExecCfg().StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return err
	}
	for _, ns := range resp.Nodes {
		var region string
		for _, tier := range ns.Desc.Locality.Tiers {
			if tier.Key == "region" {
				region = tier.Value
			}
		}
		nodesRegion[ns.Desc.NodeID] = region
	}

	for tableID, tableTarget := range tableTargets {
		var tbDesc *TableDescriptor
		var dbDesc *DatabaseDescriptor
		err = p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			tbDesc, err = sqlbase.GetTableDescFromID(ctx, txn, tableID)
			if err != nil {
				return err
			}
			dbDesc, err = getDatabaseDescByID(ctx, txn, tbDesc.ParentID)
			return err
		})
		var startPoint, endPoint uint64
		// create partition for the range
		tableName := dbDesc.Name + "." + tbDesc.Name
		var strs []string
		for _, target := range tableTarget {
			_, startPoint, _, err = sqlbase.DecodeTsRangeKey(target.StartKey,
				true, tbDesc.TsTable.HashNum)
			if err != nil {
				return err
			}
			_, endPoint, _, err = sqlbase.DecodeTsRangeKey(target.EndKey,
				false, tbDesc.TsTable.HashNum)
			if err != nil {
				return err
			}
			strs = append(strs, fmt.Sprintf(
				`partition rebalance_replica%d values from (%d) to (%d)`,
				target.RangeID, startPoint, endPoint))
		}

		stmt1 := fmt.Sprintf(`alter table %s partition by hashpoint(%s)`,
			tableName, strings.Join(strs, ","))
		log.Infof(ctx, "create partition with stmt %s", stmt1)
		_, err = p.ExecCfg().InternalExecutor.Exec(ctx, "create-partition", nil, stmt1)
		if err != nil {
			return err
		}

		for _, target := range tableTarget {
			// alter partition set configure zone
			var constraints []string
			for _, t := range target.Targets {
				constraints = append(constraints,
					fmt.Sprintf("\"+region=%s\":1", nodesRegion[t.NodeID]))
			}
			stmt2 := fmt.Sprintf(`ALTER PARTITION rebalance_replica%d OF TABLE %s CONFIGURE ZONE
				USING lease_preferences='[[+region=%s]]', constraints='{%s}', num_replicas=%d;`,
				target.RangeID, tableName, nodesRegion[target.PreLease],
				strings.Join(constraints, ","), target.NumRepl)
			log.Infof(ctx, "alter partition with stmt %s", stmt2)
			_, err = p.ExecCfg().InternalExecutor.Exec(ctx, "alter-partition", nil, stmt2)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *replicaRebalanceResumer) OnFailOrCancel(ctx context.Context, phs interface{}) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeReplicaRebanalce,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &replicaRebalanceResumer{job: job}
		})
}
