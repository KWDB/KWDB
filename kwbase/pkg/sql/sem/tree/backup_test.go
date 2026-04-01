// Copyright 2016 The Cockroach Authors.
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
