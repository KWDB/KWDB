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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestShowCreateTableTSTable directly tests the ShowCreateTable function
// when desc.IsTSTable() returns true
func TestShowCreateTableTSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tests := []struct {
		name     string
		setup    func() *sqlbase.TableDescriptor
		validate func(t *testing.T, result string)
	}{
		{
			name: "basic timeseries table with primary tags",
			setup: func() *sqlbase.TableDescriptor {
				return &sqlbase.TableDescriptor{
					Name:      "ts_table",
					TableType: tree.TimeseriesTable,
					Columns: []sqlbase.ColumnDescriptor{
						{
							ID:       1,
							Name:     "k_timestamp",
							Type:     *types.Timestamp,
							Nullable: false,
							TsCol: sqlbase.TSCol{
								ColumnType: sqlbase.ColumnType_TYPE_DATA,
							},
						},
						{
							ID:       2,
							Name:     "device_id",
							Type:     *types.Int,
							Nullable: false,
							TsCol: sqlbase.TSCol{
								ColumnType: sqlbase.ColumnType_TYPE_PTAG,
							},
						},
						{
							ID:       3,
							Name:     "location",
							Type:     *types.String,
							Nullable: true,
							TsCol: sqlbase.TSCol{
								ColumnType: sqlbase.ColumnType_TYPE_TAG,
							},
						},
					},
					TsTable: sqlbase.TSTable{
						Lifetime:          86400 * 30,
						PartitionInterval: 86400,
						ActiveTime:        86400,
						Sde:               false,
					},
					PrimaryIndex: sqlbase.IndexDescriptor{
						ID:          1,
						Name:        "primary",
						ColumnIDs:   []sqlbase.ColumnID{1},
						ColumnNames: []string{"k_timestamp"},
					},
				}
			},
			validate: func(t *testing.T, result string) {
				require.Contains(t, result, "CREATE TABLE ts_table")
				require.Contains(t, result, "TAGS")
				require.Contains(t, result, "device_id")
				require.Contains(t, result, "location")
				require.Contains(t, result, "PRIMARY TAGS")
				require.Contains(t, result, "retentions")
				require.Contains(t, result, "2592000s")
				require.Contains(t, result, "activetime")
				require.Contains(t, result, "1d")
			},
		},
		{
			name: "timeseries table with SDE",
			setup: func() *sqlbase.TableDescriptor {
				return &sqlbase.TableDescriptor{
					Name:      "ts_table_sde",
					TableType: tree.TimeseriesTable,
					Columns: []sqlbase.ColumnDescriptor{
						{
							ID:       1,
							Name:     "k_timestamp",
							Type:     *types.Timestamp,
							Nullable: false,
							TsCol: sqlbase.TSCol{
								ColumnType: sqlbase.ColumnType_TYPE_DATA,
							},
						},
						{
							ID:       2,
							Name:     "tag1",
							Type:     *types.String,
							Nullable: false,
							TsCol: sqlbase.TSCol{
								ColumnType: sqlbase.ColumnType_TYPE_PTAG,
							},
						},
					},
					TsTable: sqlbase.TSTable{
						Lifetime:          86400,
						PartitionInterval: 86400,
						ActiveTime:        86400,
						Sde:               true,
					},
					PrimaryIndex: sqlbase.IndexDescriptor{
						ID:          1,
						Name:        "primary",
						ColumnIDs:   []sqlbase.ColumnID{1},
						ColumnNames: []string{"k_timestamp"},
					},
				}
			},
			validate: func(t *testing.T, result string) {
				require.Contains(t, result, "DICT ENCODING")
			},
		},
		{
			name: "template table",
			setup: func() *sqlbase.TableDescriptor {
				return &sqlbase.TableDescriptor{
					Name:      "template_table",
					TableType: tree.TemplateTable,
					Columns: []sqlbase.ColumnDescriptor{
						{
							ID:       1,
							Name:     "k_timestamp",
							Type:     *types.Timestamp,
							Nullable: false,
							TsCol: sqlbase.TSCol{
								ColumnType: sqlbase.ColumnType_TYPE_DATA,
							},
						},
						{
							ID:       2,
							Name:     "ptag1",
							Type:     *types.Int,
							Nullable: false,
							TsCol: sqlbase.TSCol{
								ColumnType: sqlbase.ColumnType_TYPE_PTAG,
							},
						},
						{
							ID:       3,
							Name:     "tag1",
							Type:     *types.String,
							Nullable: true,
							TsCol: sqlbase.TSCol{
								ColumnType: sqlbase.ColumnType_TYPE_TAG,
							},
						},
					},
					TsTable: sqlbase.TSTable{
						Lifetime:          86400 * 7,
						PartitionInterval: 86400,
						ActiveTime:        86400,
					},
					PrimaryIndex: sqlbase.IndexDescriptor{
						ID:          1,
						Name:        "primary",
						ColumnIDs:   []sqlbase.ColumnID{1},
						ColumnNames: []string{"k_timestamp"},
					},
				}
			},
			validate: func(t *testing.T, result string) {
				require.NotContains(t, result, "PRIMARY TAGS")
				require.NotContains(t, result, "ptag1")
				require.Contains(t, result, "tag1")
			},
		},
		{
			name: "timeseries table with downsampling",
			setup: func() *sqlbase.TableDescriptor {
				activeTimeInput := "2d"
				partitionIntervalInput := "7d"
				return &sqlbase.TableDescriptor{
					Name:      "ts_table_downsample",
					TableType: tree.TimeseriesTable,
					Columns: []sqlbase.ColumnDescriptor{
						{
							ID:       1,
							Name:     "k_timestamp",
							Type:     *types.Timestamp,
							Nullable: false,
							TsCol: sqlbase.TSCol{
								ColumnType: sqlbase.ColumnType_TYPE_DATA,
							},
						},
						{
							ID:       2,
							Name:     "tag1",
							Type:     *types.String,
							Nullable: false,
							TsCol: sqlbase.TSCol{
								ColumnType: sqlbase.ColumnType_TYPE_PTAG,
							},
						},
					},
					TsTable: sqlbase.TSTable{
						Downsampling:           []string{"1h"},
						PartitionInterval:      86400,
						ActiveTime:             86400,
						ActiveTimeInput:        &activeTimeInput,
						PartitionIntervalInput: &partitionIntervalInput,
					},
					PrimaryIndex: sqlbase.IndexDescriptor{
						ID:          1,
						Name:        "primary",
						ColumnIDs:   []sqlbase.ColumnID{1},
						ColumnNames: []string{"k_timestamp"},
					},
				}
			},
			validate: func(t *testing.T, result string) {
				require.Contains(t, result, "retentions 1h")
				require.Contains(t, result, "activetime 2d")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			desc := tt.setup()
			require.True(t, desc.IsTSTable())

			tn := (*tree.Name)(&desc.Name)
			dbPrefix := ""
			lCtx := &internalLookupCtx{}
			displayOptions := ShowCreateDisplayOptions{
				IgnoreComments: true,
			}

			result, err := ShowCreateTable(ctx, nil, tn, dbPrefix, desc, lCtx, displayOptions)
			require.NoError(t, err)
			tt.validate(t, result)
		})
	}
}
