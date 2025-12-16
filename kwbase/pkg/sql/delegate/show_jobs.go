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

package delegate

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
)

// delegateShowJobs rewrites ShowJobs statement to select statement which returns
// job_id, job_type, description... from kwdb_internal.jobs
func (d *delegator) delegateShowJobs(n *tree.ShowJobs) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Jobs)
	const (
		selectClause = `SELECT job_id, job_type, description, statement, user_name, status,
				       running_status, created, started, finished, modified, errord,
				       fraction_completed, error, coordinator_id, total_num_of_ex, total_num_of_success, total_num_of_fail, time_of_last_success
				FROM kwdb_internal.jobs`
	)
	var typePredicate, whereClause, orderbyClause string
	if n.Jobs == nil {
		// Display all [only automatic] jobs without selecting specific jobs.
		if n.Automatic {
			typePredicate = fmt.Sprintf("job_type = '%s'", jobspb.TypeAutoCreateStats)
		} else {
			typePredicate = fmt.Sprintf(
				"(job_type IS NULL OR job_type != '%s')", jobspb.TypeAutoCreateStats,
			)
		}
		// The query intends to present:
		// - first all the running jobs sorted in order of start time,
		// - then all completed jobs sorted in order of completion time.
		if n.Where == nil {
			whereClause = fmt.Sprintf(
				`WHERE %s AND (finished IS NULL OR finished > now() - '7d':::interval)`, typePredicate)
		} else {
			whereClause = fmt.Sprintf(
				`WHERE %s AND %s AND (finished IS NULL OR finished > now() - '7d':::interval)`, n.Where.Expr.String(), typePredicate)
		}
		// The "ORDER BY" clause below exploits the fact that all
		// running jobs have finished = NULL.
		orderbyClause = `ORDER BY COALESCE(finished, now()) DESC, started DESC`
	} else {
		// Limit the jobs displayed to the select statement in n.Jobs.
		whereClause = fmt.Sprintf(`WHERE job_id in (%s)`, n.Jobs.String())
	}

	sqlStmt := fmt.Sprintf("%s %s %s", selectClause, whereClause, orderbyClause)
	if n.Block {
		sqlStmt = fmt.Sprintf(
			`WITH jobs AS (SELECT * FROM [%s]),
       sleep_and_restart_if_unfinished AS (
              SELECT IF(pg_sleep(1), kwdb_internal.force_retry('24h'), 1)
                     = 0 AS timed_out
                FROM (SELECT job_id FROM jobs WHERE finished IS NULL LIMIT 1)
             ),
       fail_if_slept_too_long AS (
                SELECT kwdb_internal.force_error('55000', 'timed out waiting for jobs')
                  FROM sleep_and_restart_if_unfinished
                 WHERE timed_out
              )
SELECT *
  FROM jobs
 WHERE NOT EXISTS(SELECT * FROM fail_if_slept_too_long)`, sqlStmt)
	}
	if n.Name != "" {
		queryRow := `SELECT schedule_id FROM system.scheduled_jobs WHERE schedule_name = $1`
		row, err := d.evalCtx.InternalExecutor.QueryRow(d.ctx, "get-schedule-id", d.evalCtx.Txn, queryRow, n.Name)
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, pgerror.Newf(pgcode.UndefinedObject, "schedule %s does not exist", n.Name)
		}
		scheduleID := int(tree.MustBeDInt(row[0]))
		sqlStmt = fmt.Sprintf(
			`SELECT job_id, job_type, description, statement, user_name, status, running_status, 
			  created, started, finished, modified, fraction_completed, error, coordinator_id
	          FROM kwdb_internal.jobs where created_by_id = %d AND (finished IS NULL OR finished > now() - '7d':::interval)`, scheduleID)
	}
	return parse(sqlStmt)
}
