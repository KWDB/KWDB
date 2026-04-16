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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/pkg/errors"
)

func TestCreateStatsPlanNoStatsRequested(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dsp := &DistSQLPlanner{}
	planCtx := &PlanningCtx{}

	_, err := dsp.createStatsPlan(planCtx, nil /* desc */, nil /* reqStats */, nil /* job */)
	if !testutils.IsError(err, "no stats requested") {
		t.Fatalf("expected no stats requested error, got %v", err)
	}
}

func TestCreateStatisticsRelationalEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	db.SetMaxOpenConns(1)
	r := sqlutils.MakeSQLRunner(db)

	r.Exec(t, `CREATE DATABASE test`)
	r.Exec(t, `SET DATABASE = test`)
	r.Exec(t, `SET DISTSQL = ALWAYS`)

	r.Exec(t, `
		CREATE TABLE t (
			a INT PRIMARY KEY,
			b INT,
			c STRING
		)
	`)
	r.Exec(t, `
		INSERT INTO t (a, b, c) VALUES
			(1, 10, 'x'),
			(2, 20, 'y'),
			(3, 20, 'z'),
			(4, NULL, 'z')
	`)

	// First CREATE STATISTICS exercises the normal relational stats path.
	r.Exec(t, `CREATE STATISTICS s_first ON a FROM t`)

	// This branch does not support multi-column statistics yet, so keep the
	// second CREATE STATISTICS single-column while still exercising repeated
	// stats creation on a table that already has stats.
	r.Exec(t, `CREATE STATISTICS s_second ON b FROM t`)

	testutils.SucceedsSoon(t, func() error {
		var cnt int
		if err := db.QueryRow(`SELECT count(*) FROM [SHOW STATISTICS FOR TABLE t]`).Scan(&cnt); err != nil {
			return err
		}
		if cnt < 2 {
			return errors.Errorf("expected at least 2 statistics rows, got %d", cnt)
		}
		return nil
	})
}

func TestCreateStatisticsOnEmptyTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	db.SetMaxOpenConns(1)
	r := sqlutils.MakeSQLRunner(db)

	r.Exec(t, `CREATE DATABASE test`)
	r.Exec(t, `SET DATABASE = test`)
	r.Exec(t, `SET DISTSQL = ALWAYS`)

	r.Exec(t, `
		CREATE TABLE empty_t (
			a INT PRIMARY KEY,
			b INT
		)
	`)

	r.Exec(t, `CREATE STATISTICS s_empty ON a FROM empty_t`)

	testutils.SucceedsSoon(t, func() error {
		var cnt int
		if err := db.QueryRow(`SELECT count(*) FROM [SHOW STATISTICS FOR TABLE empty_t]`).Scan(&cnt); err != nil {
			return err
		}
		if cnt < 1 {
			return errors.Errorf("expected at least 1 statistics row, got %d", cnt)
		}
		return nil
	})
}

func TestCreateStatisticsOnDifferentColumnTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	db.SetMaxOpenConns(1)
	r := sqlutils.MakeSQLRunner(db)

	r.Exec(t, `CREATE DATABASE test`)
	r.Exec(t, `SET DATABASE = test`)
	r.Exec(t, `SET DISTSQL = ALWAYS`)

	r.Exec(t, `
		CREATE TABLE typed_t (
			id INT PRIMARY KEY,
			i  INT,
			s  STRING,
			b  BOOL
		)
	`)
	r.Exec(t, `
		INSERT INTO typed_t (id, i, s, b) VALUES
			(1, 10, 'a', true),
			(2, 20, 'b', false),
			(3, NULL, 'b', true),
			(4, 40, NULL, false)
	`)

	r.Exec(t, `CREATE STATISTICS s_int ON i FROM typed_t`)
	r.Exec(t, `CREATE STATISTICS s_str ON s FROM typed_t`)
	r.Exec(t, `CREATE STATISTICS s_bool ON b FROM typed_t`)

	testutils.SucceedsSoon(t, func() error {
		rows, err := db.Query(`SELECT statistics_name FROM [SHOW STATISTICS FOR TABLE typed_t]`)
		if err != nil {
			return err
		}
		defer rows.Close()

		found := map[string]bool{}
		for rows.Next() {
			var statName string
			if err := rows.Scan(&statName); err != nil {
				return err
			}
			found[statName] = true
		}
		if err := rows.Err(); err != nil {
			return err
		}

		for _, name := range []string{"s_int", "s_str", "s_bool"} {
			if !found[name] {
				return errors.Errorf("expected statistic %s to exist, found=%v", name, found)
			}
		}
		return nil
	})
}

func TestCreateStatisticsTwiceOnSameColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	db.SetMaxOpenConns(1)
	r := sqlutils.MakeSQLRunner(db)

	r.Exec(t, `CREATE DATABASE test`)
	r.Exec(t, `SET DATABASE = test`)
	r.Exec(t, `SET DISTSQL = ALWAYS`)

	r.Exec(t, `
		CREATE TABLE dup_t (
			id INT PRIMARY KEY,
			v  INT
		)
	`)
	r.Exec(t, `
		INSERT INTO dup_t (id, v) VALUES
			(1, 10), (2, 20), (3, 30), (4, NULL)
	`)

	r.Exec(t, `CREATE STATISTICS s_first_dup ON v FROM dup_t`)
	r.Exec(t, `CREATE STATISTICS s_second_dup ON v FROM dup_t`)

	testutils.SucceedsSoon(t, func() error {
		var cnt int
		err := db.QueryRow(`
			SELECT count(*)
			FROM [SHOW STATISTICS FOR TABLE dup_t]
			WHERE statistics_name = 's_second_dup'
		`).Scan(&cnt)
		if err != nil {
			return err
		}
		if cnt != 1 {
			return errors.Errorf("expected 1 stat row for s_second_dup, got %d", cnt)
		}
		return nil
	})
}

func TestCreateStatisticsAfterMoreInserts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	db.SetMaxOpenConns(1)
	r := sqlutils.MakeSQLRunner(db)

	r.Exec(t, `CREATE DATABASE test`)
	r.Exec(t, `SET DATABASE = test`)
	r.Exec(t, `SET DISTSQL = ALWAYS`)

	r.Exec(t, `
		CREATE TABLE grow_t (
			id INT PRIMARY KEY,
			v  INT
		)
	`)
	r.Exec(t, `INSERT INTO grow_t (id, v) VALUES (1, 10), (2, 20)`)

	r.Exec(t, `CREATE STATISTICS s_before_growth ON v FROM grow_t`)

	r.Exec(t, `INSERT INTO grow_t (id, v) VALUES (3, 30), (4, 40), (5, NULL)`)

	r.Exec(t, `CREATE STATISTICS s_after_growth ON v FROM grow_t`)

	testutils.SucceedsSoon(t, func() error {
		var cnt int
		err := db.QueryRow(`
			SELECT count(*)
			FROM [SHOW STATISTICS FOR TABLE grow_t]
			WHERE statistics_name = 's_after_growth'
		`).Scan(&cnt)
		if err != nil {
			return err
		}
		if cnt != 1 {
			return errors.Errorf("expected 1 stat row for s_after_growth, got %d", cnt)
		}
		return nil
	})
}

func TestCreateStatisticsByColumnIDSyntax(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	db.SetMaxOpenConns(1)
	r := sqlutils.MakeSQLRunner(db)

	r.Exec(t, `CREATE DATABASE test`)
	r.Exec(t, `SET DATABASE = test`)
	r.Exec(t, `SET DISTSQL = ALWAYS`)

	r.Exec(t, `
		CREATE TABLE id_syntax_t (
			id      INT PRIMARY KEY,
			v       INT,
			tag_col STRING
		)
	`)
	r.Exec(t, `INSERT INTO id_syntax_t (id, v, tag_col) VALUES (1, 10, 'a'), (2, 20, 'b')`)

	desc := sqlbase.GetTableDescriptor(kvDB, "test", "id_syntax_t")

	var vColID sqlbase.ColumnID
	for _, c := range desc.Columns {
		if c.Name == "v" {
			vColID = c.ID
			break
		}
	}
	if vColID == 0 {
		t.Fatal("failed to find column id for v")
	}

	r.Exec(t, fmt.Sprintf(
		`CREATE STATISTICS s_by_id ON [%d] FROM [%d]`,
		vColID, desc.ID,
	))

	testutils.SucceedsSoon(t, func() error {
		var cnt int
		err := db.QueryRow(`
			SELECT count(*)
			FROM [SHOW STATISTICS FOR TABLE id_syntax_t]
			WHERE statistics_name = 's_by_id'
		`).Scan(&cnt)
		if err != nil {
			return err
		}
		if cnt != 1 {
			return errors.Errorf("expected 1 stat row for s_by_id, got %d", cnt)
		}
		return nil
	})
}

func tsStatTestColumnIDByName(
	t *testing.T, desc *sqlbase.ImmutableTableDescriptor, name string,
) sqlbase.ColumnID {
	t.Helper()
	for _, c := range desc.Columns {
		if c.Name == name {
			return c.ID
		}
	}
	t.Fatalf("column %q not found", name)
	return 0
}

func TestCreateTsStatsPlanDirect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	execCfg := s.ExecutorConfig().(ExecutorConfig)
	r := sqlutils.MakeSQLRunner(sqlDB)

	// mustLoadLatestRealJob() expects this DB to exist.
	r.Exec(t, `CREATE DATABASE test`)

	// Create a minimal TS table, matching the style of the working reference tests.
	r.Exec(t, `CREATE TS DATABASE IF NOT EXISTS test_ts_stats_db`)
	r.Exec(t, `
		CREATE TABLE test_ts_stats_db.test_ts_table (
			ts TIMESTAMP NOT NULL,
			a  INT
		) TAGS (
			tag1 INT NOT NULL
		) PRIMARY TAGS (tag1)
	`)

	desc := sqlbase.GetTableDescriptor(kvDB, "test_ts_stats_db", "test_ts_table")
	immDesc := sqlbase.NewImmutableTableDescriptor(*desc)

	tag1ColID := tsStatTestColumnIDByName(t, immDesc, "tag1")

	// Reuse an existing real CreateStats job, since createTsStatsPlan() reads
	// job.ID() and CreateStatsDetails.TimeZone.
	job := mustLoadLatestRealJob(ctx, t, &execCfg, sqlDB)

	txn := kv.NewTxn(ctx, s.DB(), s.NodeID())
	plannerI, cleanup := NewInternalPlanner(
		"create-ts-stats-plan-direct",
		txn,
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()

	planner := plannerI.(*planner)
	dsp := planner.DistSQLPlanner()
	planCtx := dsp.NewPlanningCtx(ctx, planner.ExtendedEvalContext(), txn)
	planCtx.planner = planner

	reqStats := []requestedStat{
		{
			name:    "ts_stats_tag1",
			columns: []sqlbase.ColumnID{tag1ColID},
		},
	}

	p, err := dsp.createTsStatsPlan(planCtx, immDesc, reqStats, job)
	if err != nil {
		t.Fatalf("unexpected createTsStatsPlan error: %v", err)
	}
	if len(p.Processors) == 0 {
		t.Fatal("expected non-empty physical plan")
	}

	var foundSampler bool
	var foundAgg bool
	for _, proc := range p.Processors {
		if proc.Spec.Core.TsSampler != nil {
			foundSampler = true
		}
		if proc.Spec.Core.SampleAggregator != nil {
			foundAgg = true
			if !proc.Spec.Core.SampleAggregator.IsTsStats {
				t.Fatal("expected SampleAggregator.IsTsStats=true")
			}
			if proc.Spec.Core.SampleAggregator.TableID != immDesc.ID {
				t.Fatalf(
					"expected SampleAggregator.TableID=%d, got %d",
					immDesc.ID, proc.Spec.Core.SampleAggregator.TableID,
				)
			}
		}
	}

	if !foundSampler {
		t.Fatal("expected plan to contain a TsSampler stage")
	}
	if !foundAgg {
		t.Fatal("expected plan to contain a SampleAggregator stage")
	}
}

func TestCreateTsStatsPlanDirectInvalidColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.TSSchemaChanger = &TSSchemaChangerTestingKnobs{}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	execCfg := s.ExecutorConfig().(ExecutorConfig)
	r := sqlutils.MakeSQLRunner(sqlDB)

	r.Exec(t, `CREATE DATABASE test`)
	r.Exec(t, `CREATE TS DATABASE IF NOT EXISTS test_ts_stats_db`)
	r.Exec(t, `
		CREATE TABLE test_ts_stats_db.test_ts_table (
			ts TIMESTAMP NOT NULL,
			a  INT
		) TAGS (
			tag1 INT NOT NULL
		) PRIMARY TAGS (tag1)
	`)

	desc := sqlbase.GetTableDescriptor(kvDB, "test_ts_stats_db", "test_ts_table")
	immDesc := sqlbase.NewImmutableTableDescriptor(*desc)

	job := mustLoadLatestRealJob(ctx, t, &execCfg, sqlDB)

	txn := kv.NewTxn(ctx, s.DB(), s.NodeID())
	plannerI, cleanup := NewInternalPlanner(
		"create-ts-stats-plan-direct-invalid-column",
		txn,
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()

	planner := plannerI.(*planner)
	dsp := planner.DistSQLPlanner()
	planCtx := dsp.NewPlanningCtx(ctx, planner.ExtendedEvalContext(), txn)
	planCtx.planner = planner

	reqStats := []requestedStat{
		{
			name:    "ts_stats_bad",
			columns: []sqlbase.ColumnID{sqlbase.ColumnID(999999)},
		},
	}

	if _, err := dsp.createTsStatsPlan(planCtx, immDesc, reqStats, job); err == nil {
		t.Fatal("expected error for invalid TS statistics column, got nil")
	}
}
