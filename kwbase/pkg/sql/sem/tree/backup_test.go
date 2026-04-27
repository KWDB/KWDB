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

package tree

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestBackupFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tbl1 := &TableName{tblName: tblName{TableName: "tbl1"}}

	testCases := []struct {
		node     *Backup
		expected string
	}{
		{
			node: &Backup{
				Targets:            TargetList{},
				DescriptorCoverage: AllDescriptors,
				To:                 PartitionedBackup{NewDString("s3://bucket/backup")},
			},
			expected: `BACKUP TO 's3://bucket/backup'`},
		{
			node: &Backup{
				Targets:            TargetList{Databases: NameList{"db1"}},
				DescriptorCoverage: RequestedDescriptors,
				To:                 PartitionedBackup{NewDString("s3://bucket/backup")},
			},
			expected: `BACKUP DATABASE db1 TO 's3://bucket/backup'`},
		{
			node: &Backup{
				Targets:            TargetList{Tables: TablePatterns{tbl1}},
				DescriptorCoverage: RequestedDescriptors,
				To:                 PartitionedBackup{NewDString("s3://bucket/backup")},
			},
			expected: `BACKUP TABLE tbl1 TO 's3://bucket/backup'`},
		{
			node: &Backup{
				Targets:            TargetList{},
				DescriptorCoverage: AllDescriptors,
				To:                 PartitionedBackup{NewDString("s3://bucket/backup")},
				AsOf:               AsOfClause{Expr: NewDString("2023-01-01 00:00:00")},
			},
			expected: `BACKUP TO 's3://bucket/backup' AS OF SYSTEM TIME '2023-01-01 00:00:00'`},
		{
			node: &Backup{
				Targets:            TargetList{},
				DescriptorCoverage: AllDescriptors,
				To:                 PartitionedBackup{NewDString("s3://bucket/backup")},
				Options:            KVOptions{{Key: "revision_history", Value: nil}},
			},
			expected: `BACKUP TO 's3://bucket/backup' WITH revision_history`},
		{
			node: &Backup{
				Targets:            TargetList{},
				DescriptorCoverage: AllDescriptors,
				To:                 PartitionedBackup{NewDString("s3://bucket/backup")},
				IncrementalFrom:    Exprs{NewDString("s3://bucket/backup1")},
			},
			expected: `BACKUP TO 's3://bucket/backup' INCREMENTAL FROM 's3://bucket/backup1'`},
	}

	for i, tc := range testCases {
		ctx := NewFmtCtx(FmtSimple)
		tc.node.Format(ctx)
		result := ctx.CloseAndGetString()
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}

func TestRestoreFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		node     *Restore
		expected string
	}{
		{
			node: &Restore{
				Targets:            TargetList{},
				DescriptorCoverage: AllDescriptors,
				From:               []PartitionedBackup{{NewDString("s3://bucket/backup")}},
			},
			expected: `RESTORE FROM 's3://bucket/backup'`},
		{
			node: &Restore{
				Targets:            TargetList{Databases: NameList{"db1"}},
				DescriptorCoverage: RequestedDescriptors,
				From:               []PartitionedBackup{{NewDString("s3://bucket/backup")}},
			},
			expected: `RESTORE DATABASE db1 FROM 's3://bucket/backup'`},
		{
			node: &Restore{
				Targets:            TargetList{},
				DescriptorCoverage: AllDescriptors,
				From:               []PartitionedBackup{{NewDString("s3://bucket/backup")}},
				AsOf:               AsOfClause{Expr: NewDString("2023-01-01 00:00:00")},
			},
			expected: `RESTORE FROM 's3://bucket/backup' AS OF SYSTEM TIME '2023-01-01 00:00:00'`},
		{
			node: &Restore{
				Targets:            TargetList{},
				DescriptorCoverage: AllDescriptors,
				From:               []PartitionedBackup{{NewDString("s3://bucket/backup")}},
				Options:            KVOptions{{Key: "skip_missing_foreign_keys", Value: nil}},
			},
			expected: `RESTORE FROM 's3://bucket/backup' WITH skip_missing_foreign_keys`},
		{
			node: &Restore{
				Targets:            TargetList{},
				DescriptorCoverage: AllDescriptors,
				From:               []PartitionedBackup{{NewDString("s3://bucket/backup1")}, {NewDString("s3://bucket/backup2")}},
			},
			expected: `RESTORE FROM 's3://bucket/backup1', 's3://bucket/backup2'`},
	}

	for i, tc := range testCases {
		ctx := NewFmtCtx(FmtSimple)
		tc.node.Format(ctx)
		result := ctx.CloseAndGetString()
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}

func TestKVOptionsFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		node     *KVOptions
		expected string
	}{
		{
			node:     &KVOptions{},
			expected: "",
		},
		{
			node:     &KVOptions{{Key: "revision_history", Value: nil}},
			expected: "revision_history",
		},
		{
			node:     &KVOptions{{Key: "into_db", Value: NewDString("db1")}},
			expected: "into_db = 'db1'",
		},
		{
			node:     &KVOptions{{Key: "option1", Value: nil}, {Key: "option2", Value: NewDInt(42)}},
			expected: "option1, option2 = 42",
		},
	}

	for i, tc := range testCases {
		ctx := NewFmtCtx(FmtSimple)
		tc.node.Format(ctx)
		result := ctx.CloseAndGetString()
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}

func TestPartitionedBackupFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		node     *PartitionedBackup
		expected string
	}{
		{
			node:     &PartitionedBackup{NewDString("s3://bucket/backup")},
			expected: `'s3://bucket/backup'`},
		{
			node:     &PartitionedBackup{NewDString("s3://bucket/backup1"), NewDString("s3://bucket/backup2")},
			expected: `('s3://bucket/backup1', 's3://bucket/backup2')`},
	}

	for i, tc := range testCases {
		ctx := NewFmtCtx(FmtSimple)
		tc.node.Format(ctx)
		result := ctx.CloseAndGetString()
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}
