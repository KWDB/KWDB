// Copyright 2017 The Cockroach Authors.
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

package tpcc

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/workload"
	"github.com/cockroachdb/cockroach-go/crdb"
	"golang.org/x/exp/rand"
)

// 2.8 The Stock-Level Transaction
//
// The Stock-Level business transaction determines the number of recently sold
// items that have a stock level below a specified threshold. It represents a
// heavy read-only database transaction with a low frequency of execution, a
// relaxed response time requirement, and relaxed consistency requirements.

// 2.8.2.3 states:
// Full serializability and repeatable reads are not required for the
// Stock-Level business transaction. All data read must be committed and no
// older than the most recently committed data prior to the time this business
// transaction was initiated. All other ACID properties must be maintained.
// TODO(jordan): can we take advantage of this?

type stockLevelData struct {
	// This data must all be returned by the transaction. See 2.8.3.4.
	dID       int
	threshold int
	lowStock  int
}

type stockLevel struct {
	config *tpcc
	mcp    *workload.MultiConnPool
	sr     workload.SQLRunner

	selectDNextOID    workload.StmtHandle
	countRecentlySold workload.StmtHandle
}

var _ tpccTx = &stockLevel{}

func createStockLevel(
	ctx context.Context, config *tpcc, mcp *workload.MultiConnPool,
) (tpccTx, error) {
	s := &stockLevel{
		config: config,
		mcp:    mcp,
	}

	s.selectDNextOID = s.sr.Define(`
		SELECT d_next_o_id
		FROM district
		WHERE d_w_id = $1 AND d_id = $2`,
	)

	// Count the number of recently sold items that have a stock level below
	// the threshold.
	// TODO(radu): we use count(DISTINCT s_i_id) because DISTINCT inside
	// aggregates was not supported by the optimizer. This can be cleaned up.
	s.countRecentlySold = s.sr.Define(`
		SELECT count(*) FROM (
			SELECT DISTINCT s_i_id
			FROM order_line
			JOIN stock
			ON s_i_id=ol_i_id AND s_w_id=ol_w_id
			WHERE ol_w_id = $1
				AND ol_d_id = $2
				AND ol_o_id BETWEEN $3 - 20 AND $3 - 1
				AND s_quantity < $4
		)`,
	)

	if err := s.sr.Init(ctx, "stock-level", mcp, config.connFlags); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *stockLevel) run(ctx context.Context, wID int) (interface{}, error) {
	rng := rand.New(rand.NewSource(uint64(timeutil.Now().UnixNano())))

	// 2.8.1.2: The threshold of minimum quantity in stock is selected at random
	// within [10..20].
	d := stockLevelData{
		threshold: int(randInt(rng, 10, 20)),
		dID:       rng.Intn(10) + 1,
	}

	tx, err := s.mcp.Get().BeginEx(ctx, s.config.txOpts)
	if err != nil {
		return nil, err
	}
	if err := kwdb.ExecuteInTx(
		ctx, (*workload.PgxTx)(tx),
		func() error {
			var dNextOID int
			if err := s.selectDNextOID.QueryRowTx(
				ctx, tx, wID, d.dID,
			).Scan(&dNextOID); err != nil {
				return err
			}

			// Count the number of recently sold items that have a stock level below
			// the threshold.
			return s.countRecentlySold.QueryRowTx(
				ctx, tx, wID, d.dID, dNextOID, d.threshold,
			).Scan(&d.lowStock)
		}); err != nil {
		return nil, err
	}
	return d, nil
}
