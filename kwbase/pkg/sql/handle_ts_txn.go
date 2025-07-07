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

package sql

import (
	"context"
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/txnwait"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
)

// tsTxnResumer implements the jobs.Resumer interface for ts txn
// jobs. A new instance is created for each job.
type tsTxnResumer struct {
	job *jobs.Job
}

func (r *tsTxnResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	p := phs.(PlanHookState).(*planner)
	// handle ts txn record in job, and this job will be always running
	for timer := time.NewTimer(0); ; {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			// get all ts txn record every minute
			timer.Reset(time.Minute)
			if err := p.handleTsTxnRecord(ctx); err != nil {
				log.Error(ctx, err.Error())
			}
		}
	}
}

// handleTsTxnRecord handles ts txn by txn record status.
func (p *planner) handleTsTxnRecord(ctx context.Context) error {
	// make ts txn record start key
	startKey := roachpb.Key(keys.MakeTablePrefix(keys.TsTxnTableID))
	endKey := startKey.PrefixEnd()
	keyValues, err := p.ExecCfg().DB.Scan(ctx, startKey, endKey, 0)
	if err != nil {
		return err
	}
	txn := p.execCfg.DB.NewTxn(ctx, "ts txn job")
	p.txn = txn
	for _, keyValue := range keyValues {
		if !keyValue.Exists() {
			continue
		}
		var res roachpb.TsTxnRecord
		if err = protoutil.Unmarshal(keyValue.ValueBytes(), &res); err != nil {
			return err
		}
		tsTran := roachpb.TsTransaction{
			ID: res.ID,
		}
		// get txn expired time, the default is the last heartbeat time plus five seconds
		txnExpiredTime := res.LastHeartbeat.Add(txnwait.TxnLivenessThreshold.Nanoseconds(), 0)
		now := p.ExecCfg().DB.Clock().Now()
		// handel txn by txn status if txn is expired
		if txnExpiredTime.Less(now) {
			if res.Status == roachpb.PENDING {
				// txn should be roll back
				ba := roachpb.BatchRequest{}
				for _, span := range res.Spans {
					_, tableID, err := keys.DecodeTablePrefix(span.Key)
					if err != nil {
						return err
					}
					_, err = sqlbase.GetTableDescFromID(ctx, p.txn, sqlbase.ID(tableID))
					if err != nil {
						return err
					}
					ba.Add(&roachpb.TsRollbackRequest{
						RequestHeader: roachpb.RequestHeader{
							Key:    span.Key,
							EndKey: span.EndKey,
						},
						TsTransaction: &tsTran,
					})
					ba.Header.ReadConsistency = roachpb.READ_UNCOMMITTED
				}
				_, pErr := p.ExecCfg().DistSender.Send(ctx, ba)
				if pErr != nil {
					return pErr.GoError()
				}

			} else if res.Status == roachpb.PREPARED {
				// txn is prepared, we should commit this txn

				ba := roachpb.BatchRequest{}
				for _, span := range res.Spans {
					_, tableID, err := keys.DecodeTablePrefix(span.Key)
					if err != nil {
						return err
					}
					_, err = sqlbase.GetTableDescFromID(ctx, p.txn, sqlbase.ID(tableID))
					if err != nil {
						return err
					}
					ba.Add(&roachpb.TsCommitRequest{
						RequestHeader: roachpb.RequestHeader{
							Key:    span.Key,
							EndKey: span.EndKey,
						},
						TsTransaction: &tsTran,
					})
					ba.Header.ReadConsistency = roachpb.READ_UNCOMMITTED
				}
				_, pErr := p.ExecCfg().DistSender.Send(ctx, ba)
				if pErr != nil {
					return pErr.GoError()
				}
			}
			// txn is already completed, we should delete this txn record
			if err = p.ExecCfg().DB.Del(ctx, keyValue.Key); err != nil {
				return err
			}

		}
	}
	return nil
}

func (r *tsTxnResumer) OnFailOrCancel(ctx context.Context, phs interface{}) error { return nil }

var _ jobs.Resumer = &tsTxnResumer{}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &tsTxnResumer{job: job}
	}

	jobs.RegisterConstructor(jobspb.TypeTsTxn, createResumerFn)
}
