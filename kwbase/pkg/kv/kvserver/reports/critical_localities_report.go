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
	"fmt"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"github.com/cockroachdb/errors"
)

// criticalLocalitiesReportID is the id of the row in the system. reports_meta
// table corresponding to the critical localities report (i.e. the
// system.replication_critical_localities table).
const criticalLocalitiesReportID reportID = 2

type localityKey struct {
	ZoneKey
	locality LocalityRepr
}

// LocalityRepr is a representation of a locality.
type LocalityRepr string

type localityStatus struct {
	atRiskRanges int32
}

// LocalityReport stores the range status information for each locality and
// applicable zone.
type LocalityReport map[localityKey]localityStatus

// ReplicationCriticalLocalitiesReportSaver manages the content and the saving
// of the report.
type replicationCriticalLocalitiesReportSaver struct {
	localities          LocalityReport
	previousVersion     LocalityReport
	lastGenerated       time.Time
	lastUpdatedRowCount int
}

// makeReplicationCriticalLocalitiesReportSaver creates a new report saver.
func makeReplicationCriticalLocalitiesReportSaver() replicationCriticalLocalitiesReportSaver {
	return replicationCriticalLocalitiesReportSaver{
		localities: LocalityReport{},
	}
}

// resetReport resets the report to an empty state.
func (r *replicationCriticalLocalitiesReportSaver) resetReport() {
	r.localities = LocalityReport{}
}

// LastUpdatedRowCount is the count of the rows that were touched during the last save.
func (r *replicationCriticalLocalitiesReportSaver) LastUpdatedRowCount() int {
	return r.lastUpdatedRowCount
}

// AddCriticalLocality will add locality to the list of the critical localities.
func (r *replicationCriticalLocalitiesReportSaver) AddCriticalLocality(
	zKey ZoneKey, loc LocalityRepr,
) {
	lKey := localityKey{
		ZoneKey:  zKey,
		locality: loc,
	}
	if _, ok := r.localities[lKey]; !ok {
		r.localities[lKey] = localityStatus{}
	}
	lStat := r.localities[lKey]
	lStat.atRiskRanges++
	r.localities[lKey] = lStat
}

func (r *replicationCriticalLocalitiesReportSaver) loadPreviousVersion(
	ctx context.Context, ex sqlutil.InternalExecutor, txn *kv.Txn,
) error {
	// The data for the previous save needs to be loaded if:
	// - this is the first time that we call this method and lastUpdatedAt has never been set
	// - in case that the lastUpdatedAt is set but is different than the timestamp in reports_meta
	//   this indicates that some other worker wrote after we did the write.
	if !r.lastGenerated.IsZero() {
		generated, err := getReportGenerationTime(ctx, criticalLocalitiesReportID, ex, txn)
		if err != nil {
			return err
		}
		// If the report is missing, this is the first time we are running and the
		// reload is needed. In that case, generated will be the zero value.
		if generated == r.lastGenerated {
			// We have the latest report; reload not needed.
			return nil
		}
	}
	const prevViolations = "select zone_id, subzone_id, locality, at_risk_ranges " +
		"from system.replication_critical_localities"
	rows, err := ex.Query(
		ctx, "get-previous-replication-critical-localities", txn, prevViolations,
	)
	if err != nil {
		return err
	}

	r.previousVersion = make(LocalityReport, len(rows))
	for _, row := range rows {
		key := localityKey{}
		key.ZoneID = (uint32)(*row[0].(*tree.DInt))
		key.SubzoneID = base.SubzoneID(*row[1].(*tree.DInt))
		key.locality = (LocalityRepr)(*row[2].(*tree.DString))
		r.previousVersion[key] = localityStatus{(int32)(*row[3].(*tree.DInt))}
	}

	return nil
}

func (r *replicationCriticalLocalitiesReportSaver) updatePreviousVersion() {
	r.previousVersion = r.localities
	r.localities = make(LocalityReport, len(r.previousVersion))
}

func (r *replicationCriticalLocalitiesReportSaver) updateTimestamp(
	ctx context.Context, ex sqlutil.InternalExecutor, txn *kv.Txn, reportTS time.Time,
) error {
	if !r.lastGenerated.IsZero() && reportTS == r.lastGenerated {
		return errors.Errorf(
			"The new time %s is the same as the time of the last update %s",
			reportTS.String(),
			r.lastGenerated.String(),
		)
	}

	_, err := ex.Exec(
		ctx,
		"timestamp-upsert-replication-critical-localities",
		txn,
		"upsert into system.reports_meta(id, generated) values($1, $2)",
		criticalLocalitiesReportID,
		reportTS,
	)
	return err
}

// Save the report.
//
// reportTS is the time that will be set in the updated_at column for every row.
func (r *replicationCriticalLocalitiesReportSaver) Save(
	ctx context.Context, reportTS time.Time, db *kv.DB, ex sqlutil.InternalExecutor,
) error {
	r.lastUpdatedRowCount = 0
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		err := r.loadPreviousVersion(ctx, ex, txn)
		if err != nil {
			return err
		}

		err = r.updateTimestamp(ctx, ex, txn, reportTS)
		if err != nil {
			return err
		}

		for key, status := range r.localities {
			if err := r.upsertLocality(
				ctx, reportTS, txn, key, status, db, ex,
			); err != nil {
				return err
			}
		}

		for key := range r.previousVersion {
			if _, ok := r.localities[key]; !ok {
				_, err := ex.Exec(
					ctx,
					"delete-old-replication-critical-localities",
					txn,
					"delete from system.replication_critical_localities "+
						"where zone_id = $1 and subzone_id = $2 and locality = $3",
					key.ZoneID,
					key.SubzoneID,
					key.locality,
				)

				if err != nil {
					return err
				}
				r.lastUpdatedRowCount++
			}
		}

		return nil
	}); err != nil {
		return err
	}

	r.lastGenerated = reportTS
	r.updatePreviousVersion()

	return nil
}

// upsertLocality upserts a row into system.replication_critical_localities.
//
// existing is used to decide is this is a new violation.
func (r *replicationCriticalLocalitiesReportSaver) upsertLocality(
	ctx context.Context,
	reportTS time.Time,
	txn *kv.Txn,
	key localityKey,
	status localityStatus,
	db *kv.DB,
	ex sqlutil.InternalExecutor,
) error {
	var err error
	previousStatus, hasOldVersion := r.previousVersion[key]
	if hasOldVersion && previousStatus.atRiskRanges == status.atRiskRanges {
		// No change in the status so no update.
		return nil
	}

	// Updating an old row.
	_, err = ex.Exec(
		ctx, "upsert-replication-critical-localities", txn,
		"upsert into system.replication_critical_localities(report_id, zone_id, subzone_id, "+
			"locality, at_risk_ranges) values($1, $2, $3, $4, $5)",
		criticalLocalitiesReportID,
		key.ZoneID, key.SubzoneID, key.locality, status.atRiskRanges,
	)

	if err != nil {
		return err
	}

	r.lastUpdatedRowCount++
	return nil
}

// criticalLocalitiesVisitor is a visitor that, when passed to visitRanges(), builds
// a LocalityReport.
type criticalLocalitiesVisitor struct {
	localityConstraints []zonepb.Constraints
	cfg                 *config.SystemConfig
	storeResolver       StoreResolver
	nodeChecker         nodeChecker

	report   *replicationCriticalLocalitiesReportSaver
	visitErr bool

	// prevZoneKey maintains state from one range to the next. This state can be
	// reused when a range is covered by the same zone config as the previous one.
	// Reusing it speeds up the report generation.
	prevZoneKey ZoneKey
}

var _ rangeVisitor = &criticalLocalitiesVisitor{}

func makeLocalityStatsVisitor(
	ctx context.Context,
	localityConstraints []zonepb.Constraints,
	cfg *config.SystemConfig,
	storeResolver StoreResolver,
	nodeChecker nodeChecker,
	saver *replicationCriticalLocalitiesReportSaver,
) criticalLocalitiesVisitor {
	v := criticalLocalitiesVisitor{
		localityConstraints: localityConstraints,
		cfg:                 cfg,
		storeResolver:       storeResolver,
		nodeChecker:         nodeChecker,
		report:              saver,
	}
	return v
}

// failed is part of the rangeVisitor interface.
func (v *criticalLocalitiesVisitor) failed() bool {
	return v.visitErr
}

// reset is part of the rangeVisitor interface.
func (v *criticalLocalitiesVisitor) reset(ctx context.Context) {
	v.visitErr = false
	v.report.resetReport()
}

// visitNewZone is part of the rangeVisitor interface.
func (v *criticalLocalitiesVisitor) visitNewZone(
	ctx context.Context, r *roachpb.RangeDescriptor,
) (retErr error) {

	defer func() {
		v.visitErr = retErr != nil
	}()

	// Get the zone.
	var zKey ZoneKey
	found, err := visitZones(ctx, r, v.cfg, ignoreSubzonePlaceholders,
		func(_ context.Context, zone *zonepb.ZoneConfig, key ZoneKey) bool {
			if !zoneChangesReplication(zone) {
				return false
			}
			zKey = key
			return true
		})
	if err != nil {
		return errors.AssertionFailedf("unexpected error visiting zones: %s", err)
	}
	if !found {
		return errors.AssertionFailedf("no suitable zone config found for range: %s", r)
	}
	v.prevZoneKey = zKey

	return v.countRange(ctx, zKey, r)
}

// visitSameZone is part of the rangeVisitor interface.
func (v *criticalLocalitiesVisitor) visitSameZone(
	ctx context.Context, r *roachpb.RangeDescriptor,
) (retErr error) {
	defer func() {
		if retErr != nil {
			v.visitErr = true
		}
	}()
	return v.countRange(ctx, v.prevZoneKey, r)
}

func (v *criticalLocalitiesVisitor) countRange(
	ctx context.Context, zoneKey ZoneKey, r *roachpb.RangeDescriptor,
) error {
	stores := v.storeResolver(r)
	for _, c := range v.localityConstraints {
		if err := processLocalityForRange(
			ctx, r, zoneKey, v.report, &c, v.cfg, v.nodeChecker, stores,
		); err != nil {
			return err
		}
	}
	return nil
}

// processLocalityForRange checks a single locality constraint against a
// range with replicas in each of the stores given, contributing to rep.
func processLocalityForRange(
	ctx context.Context,
	r *roachpb.RangeDescriptor,
	zoneKey ZoneKey,
	rep *replicationCriticalLocalitiesReportSaver,
	c *zonepb.Constraints,
	cfg *config.SystemConfig,
	nodeChecker nodeChecker,
	storeDescs []roachpb.StoreDescriptor,
) error {
	// Compute the required quorum and the number of live nodes. If the number of
	// live nodes gets lower than the required quorum then the range is already
	// unavailable.
	quorumCount := len(r.Replicas().Voters())/2 + 1
	liveNodeCount := len(storeDescs)
	for _, storeDesc := range storeDescs {
		isStoreLive := nodeChecker(storeDesc.Node.NodeID)
		if !isStoreLive {
			if liveNodeCount >= quorumCount {
				liveNodeCount--
				if liveNodeCount < quorumCount {
					break
				}
			}
		}
	}

	cstrs := make([]string, 0, len(c.Constraints))
	for _, con := range c.Constraints {
		cstrs = append(cstrs, fmt.Sprintf("%s=%s", con.Key, con.Value))
	}
	loc := LocalityRepr(strings.Join(cstrs, ","))

	passCount := 0
	for _, storeDesc := range storeDescs {
		storeHasConstraint := true
		for _, constraint := range c.Constraints {
			// For required constraints - consider unavailable nodes as not matching.
			if !zonepb.StoreMatchesConstraint(storeDesc, constraint) {
				storeHasConstraint = false
				break
			}
		}

		if storeHasConstraint && nodeChecker(storeDesc.Node.NodeID) {
			passCount++
		}
	}

	// If the live nodes outside of the given locality are not enough to
	// form quorum then this locality is critical.
	if quorumCount > liveNodeCount-passCount {
		rep.AddCriticalLocality(zoneKey, loc)
	}
	return nil
}
