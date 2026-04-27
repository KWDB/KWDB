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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestProcessDBDistributionResponse tests the processDBDistributionResponse function
func TestProcessDBDistributionResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name             string
		response         *serverpb.DistributionResponse
		expectedRows     int
		expectedCapacity int
		checkTotalRow    bool
	}{
		{
			name: "single node with single block",
			response: &serverpb.DistributionResponse{
				Distribution: []*serverpb.DistributionInfo{
					{
						NodeID: "node1",
						BlockInfo: []sqlbase.BlockInfo{
							{
								Level:         "total",
								BlocksNum:     100,
								BlocksSize:    1000000,
								AvgSize:       10000.0,
								LastSegLevel0: 10,
								LastSegLevel1: 20,
								LastSegLevel2: 30,
								OriginalSize:  2500000,
							},
						},
					},
				},
			},
			expectedRows:     2, // 1 data row + 1 total row
			expectedCapacity: 2,
			checkTotalRow:    true,
		},
		{
			name: "multiple nodes with multiple blocks",
			response: &serverpb.DistributionResponse{
				Distribution: []*serverpb.DistributionInfo{
					{
						NodeID: "node1",
						BlockInfo: []sqlbase.BlockInfo{
							{
								Level:         "total",
								BlocksNum:     100,
								BlocksSize:    1000000,
								AvgSize:       10000.0,
								LastSegLevel0: 10,
								LastSegLevel1: 20,
								LastSegLevel2: 30,
								OriginalSize:  2500000,
							},
						},
					},
					{
						NodeID: "node2",
						BlockInfo: []sqlbase.BlockInfo{
							{
								Level:         "total",
								BlocksNum:     200,
								BlocksSize:    2000000,
								AvgSize:       10000.0,
								LastSegLevel0: 20,
								LastSegLevel1: 40,
								LastSegLevel2: 60,
								OriginalSize:  6000000,
							},
						},
					},
				},
			},
			expectedRows:     3, // 2 data rows + 1 total row
			expectedCapacity: 3,
			checkTotalRow:    true,
		},
		{
			name: "empty response",
			response: &serverpb.DistributionResponse{
				Distribution: []*serverpb.DistributionInfo{},
			},
			expectedRows:     0,
			expectedCapacity: 0,
			checkTotalRow:    false,
		},
		{
			name: "single node with multiple blocks",
			response: &serverpb.DistributionResponse{
				Distribution: []*serverpb.DistributionInfo{
					{
						NodeID: "node1",
						BlockInfo: []sqlbase.BlockInfo{
							{
								Level:         "level0",
								BlocksNum:     50,
								BlocksSize:    500000,
								AvgSize:       10000.0,
								LastSegLevel0: 5,
								LastSegLevel1: 10,
								LastSegLevel2: 15,
								OriginalSize:  1000000,
							},
							{
								Level:         "level1",
								BlocksNum:     30,
								BlocksSize:    300000,
								AvgSize:       10000.0,
								LastSegLevel0: 3,
								LastSegLevel1: 6,
								LastSegLevel2: 9,
								OriginalSize:  450000,
							},
						},
					},
				},
			},
			expectedRows:     3, // 2 data rows + 1 total row
			expectedCapacity: 3,
			checkTotalRow:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, capacity, err := processDBDistributionResponse(tt.response)
			require.NoError(t, err)
			require.Equal(t, tt.expectedRows, len(rows))
			require.Equal(t, tt.expectedCapacity, capacity)

			if tt.checkTotalRow && len(rows) > 0 {
				// Verify the last row is the total summary row
				totalRow := rows[len(rows)-1]
				require.Equal(t, 8, len(totalRow))
				require.Equal(t, "total", string(*totalRow[0].(*tree.DString)))
			}
		})
	}
}

// TestProcessDBDistributionResponseWithCompressionRatio tests database distribution response processing with compression ratio
func TestProcessDBDistributionResponseWithCompressionRatio(t *testing.T) {
	defer leaktest.AfterTest(t)()

	compressionRatio := float32(2.5)
	response := &serverpb.DistributionResponse{
		Distribution: []*serverpb.DistributionInfo{
			{
				NodeID: "node1",
				BlockInfo: []sqlbase.BlockInfo{
					{
						Level:            "total",
						BlocksNum:        100,
						BlocksSize:       1000000,
						AvgSize:          10000.0,
						CompressionRatio: &compressionRatio,
						LastSegLevel0:    10,
						LastSegLevel1:    20,
						LastSegLevel2:    30,
						OriginalSize:     2500000,
					},
				},
			},
		},
	}

	rows, capacity, err := processDBDistributionResponse(response)
	require.NoError(t, err)
	require.Equal(t, 2, len(rows)) // 1 data row + 1 total row
	require.Equal(t, 2, capacity)

	// Verify data row
	dataRow := rows[0]
	require.Equal(t, 8, len(dataRow))
	require.Equal(t, "node1", string(*dataRow[0].(*tree.DString)))
	require.Equal(t, tree.DInt(100), *dataRow[1].(*tree.DInt))
	require.Equal(t, tree.DInt(1000000), *dataRow[2].(*tree.DInt))
	require.Equal(t, "10000.00", string(*dataRow[3].(*tree.DString)))
	require.Equal(t, "2.50", string(*dataRow[4].(*tree.DString)))

	// Verify total row
	totalRow := rows[1]
	require.Equal(t, "total", string(*totalRow[0].(*tree.DString)))
	require.Equal(t, tree.DInt(100), *totalRow[1].(*tree.DInt))
	require.Equal(t, tree.DInt(1000000), *totalRow[2].(*tree.DInt))
}

// TestProcessDBDistributionResponseWithNilCompressionRatio tests nil compression ratio handling
func TestProcessDBDistributionResponseWithNilCompressionRatio(t *testing.T) {
	defer leaktest.AfterTest(t)()

	response := &serverpb.DistributionResponse{
		Distribution: []*serverpb.DistributionInfo{
			{
				NodeID: "node1",
				BlockInfo: []sqlbase.BlockInfo{
					{
						Level:         "total",
						BlocksNum:     100,
						BlocksSize:    1000000,
						AvgSize:       10000.0,
						LastSegLevel0: 10,
						LastSegLevel1: 20,
						LastSegLevel2: 30,
						OriginalSize:  1000000,
					},
				},
			},
		},
	}

	rows, _, err := processDBDistributionResponse(response)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(rows), 1)

	// Verify compression ratio is set to "0"
	dataRow := rows[0]
	require.Equal(t, "0", string(*dataRow[4].(*tree.DString)))
}

// TestProcessTableDistributionResponse tests the processTableDistributionResponse function
func TestProcessTableDistributionResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name             string
		response         *serverpb.DistributionResponse
		expectedRows     int
		expectedCapacity int
	}{
		{
			name: "single node with single block",
			response: &serverpb.DistributionResponse{
				Distribution: []*serverpb.DistributionInfo{
					{
						NodeID: "node1",
						BlockInfo: []sqlbase.BlockInfo{
							{
								Level:      "level0",
								BlocksNum:  50,
								BlocksSize: 500000,
								AvgSize:    10000.0,
							},
						},
					},
				},
			},
			expectedRows:     1,
			expectedCapacity: 1,
		},
		{
			name: "multiple nodes with multiple blocks",
			response: &serverpb.DistributionResponse{
				Distribution: []*serverpb.DistributionInfo{
					{
						NodeID: "node1",
						BlockInfo: []sqlbase.BlockInfo{
							{
								Level:      "level0",
								BlocksNum:  50,
								BlocksSize: 500000,
								AvgSize:    10000.0,
							},
							{
								Level:      "level1",
								BlocksNum:  30,
								BlocksSize: 300000,
								AvgSize:    10000.0,
							},
						},
					},
					{
						NodeID: "node2",
						BlockInfo: []sqlbase.BlockInfo{
							{
								Level:      "level0",
								BlocksNum:  40,
								BlocksSize: 400000,
								AvgSize:    10000.0,
							},
						},
					},
				},
			},
			expectedRows:     3,
			expectedCapacity: 3,
		},
		{
			name: "empty response",
			response: &serverpb.DistributionResponse{
				Distribution: []*serverpb.DistributionInfo{},
			},
			expectedRows:     0,
			expectedCapacity: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, capacity, err := processTableDistributionResponse(tt.response)
			require.NoError(t, err)
			require.Equal(t, tt.expectedRows, len(rows))
			require.Equal(t, tt.expectedCapacity, capacity)
		})
	}
}

// TestProcessTableDistributionResponseWithCompressionRatio tests table distribution response processing with compression ratio
func TestProcessTableDistributionResponseWithCompressionRatio(t *testing.T) {
	defer leaktest.AfterTest(t)()

	compressionRatio := float32(1.8)
	response := &serverpb.DistributionResponse{
		Distribution: []*serverpb.DistributionInfo{
			{
				NodeID: "node1",
				BlockInfo: []sqlbase.BlockInfo{
					{
						Level:            "level0",
						BlocksNum:        50,
						BlocksSize:       500000,
						AvgSize:          10000.0,
						CompressionRatio: &compressionRatio,
					},
				},
			},
		},
	}

	rows, capacity, err := processTableDistributionResponse(response)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))
	require.Equal(t, 1, capacity)

	// Verify data row
	dataRow := rows[0]
	require.Equal(t, 6, len(dataRow))
	require.Equal(t, "node1", string(*dataRow[0].(*tree.DString)))
	require.Equal(t, "level0", string(*dataRow[1].(*tree.DString)))
	require.Equal(t, tree.DInt(50), *dataRow[2].(*tree.DInt))
	require.Equal(t, tree.DInt(500000), *dataRow[3].(*tree.DInt))
	require.Equal(t, "10000.00", string(*dataRow[4].(*tree.DString)))
	require.Equal(t, "1.80", string(*dataRow[5].(*tree.DString)))
}

// TestProcessTableDistributionResponseWithNilCompressionRatio tests nil compression ratio handling
func TestProcessTableDistributionResponseWithNilCompressionRatio(t *testing.T) {
	defer leaktest.AfterTest(t)()

	response := &serverpb.DistributionResponse{
		Distribution: []*serverpb.DistributionInfo{
			{
				NodeID: "node1",
				BlockInfo: []sqlbase.BlockInfo{
					{
						Level:      "level0",
						BlocksNum:  50,
						BlocksSize: 500000,
						AvgSize:    10000.0,
					},
				},
			},
		},
	}

	rows, _, err := processTableDistributionResponse(response)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(rows), 1)

	// Verify compression ratio is set to "0"
	dataRow := rows[0]
	require.Equal(t, "0", string(*dataRow[5].(*tree.DString)))
}

// TestShowDBDistributionColumns tests database distribution column definitions
func TestShowDBDistributionColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	require.Equal(t, 8, len(showDBDistributionColumns))
	require.Equal(t, "node_id", showDBDistributionColumns[0].Name)
	require.Equal(t, "block_num", showDBDistributionColumns[1].Name)
	require.Equal(t, "block_size", showDBDistributionColumns[2].Name)
	require.Equal(t, "avg_size", showDBDistributionColumns[3].Name)
	require.Equal(t, "compression_ratio", showDBDistributionColumns[4].Name)
	require.Equal(t, "last_segment_num_level0", showDBDistributionColumns[5].Name)
	require.Equal(t, "last_segment_num_level1", showDBDistributionColumns[6].Name)
	require.Equal(t, "last_segment_num_level2", showDBDistributionColumns[7].Name)

	// Verify column types
	require.Equal(t, types.String, showDBDistributionColumns[0].Typ)
	require.Equal(t, types.Int, showDBDistributionColumns[1].Typ)
	require.Equal(t, types.Int, showDBDistributionColumns[2].Typ)
	require.Equal(t, types.String, showDBDistributionColumns[3].Typ)
	require.Equal(t, types.String, showDBDistributionColumns[4].Typ)
	require.Equal(t, types.Int, showDBDistributionColumns[5].Typ)
	require.Equal(t, types.Int, showDBDistributionColumns[6].Typ)
	require.Equal(t, types.Int, showDBDistributionColumns[7].Typ)
}

// TestShowTableDistributionColumns tests table distribution column definitions
func TestShowTableDistributionColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	require.Equal(t, 6, len(showTableDistributionColumns))
	require.Equal(t, "node_id", showTableDistributionColumns[0].Name)
	require.Equal(t, "level", showTableDistributionColumns[1].Name)
	require.Equal(t, "block_num", showTableDistributionColumns[2].Name)
	require.Equal(t, "block_size", showTableDistributionColumns[3].Name)
	require.Equal(t, "avg_size", showTableDistributionColumns[4].Name)
	require.Equal(t, "compression_ratio", showTableDistributionColumns[5].Name)

	// Verify column types
	require.Equal(t, types.String, showTableDistributionColumns[0].Typ)
	require.Equal(t, types.String, showTableDistributionColumns[1].Typ)
	require.Equal(t, types.Int, showTableDistributionColumns[2].Typ)
	require.Equal(t, types.Int, showTableDistributionColumns[3].Typ)
	require.Equal(t, types.String, showTableDistributionColumns[4].Typ)
	require.Equal(t, types.String, showTableDistributionColumns[5].Typ)
}

// TestProcessDBDistributionResponseCalculations tests database distribution response calculations
func TestProcessDBDistributionResponseCalculations(t *testing.T) {
	defer leaktest.AfterTest(t)()

	compressionRatio := float32(2.5)
	response := &serverpb.DistributionResponse{
		Distribution: []*serverpb.DistributionInfo{
			{
				NodeID: "node1",
				BlockInfo: []sqlbase.BlockInfo{
					{
						Level:            "total",
						BlocksNum:        100,
						BlocksSize:       1000000,
						AvgSize:          10000.0,
						CompressionRatio: &compressionRatio,
						LastSegLevel0:    10,
						LastSegLevel1:    20,
						LastSegLevel2:    30,
						OriginalSize:     2500000,
					},
				},
			},
		},
	}

	rows, _, err := processDBDistributionResponse(response)
	require.NoError(t, err)
	require.Equal(t, 2, len(rows))

	// Verify total row calculations
	totalRow := rows[1]
	require.Equal(t, "total", string(*totalRow[0].(*tree.DString)))
	require.Equal(t, tree.DInt(100), *totalRow[1].(*tree.DInt))        // totalNum
	require.Equal(t, tree.DInt(1000000), *totalRow[2].(*tree.DInt))    // totalSize
	require.Equal(t, "10000.00", string(*totalRow[3].(*tree.DString))) // totalAvgSize
	require.Equal(t, "2.50", string(*totalRow[4].(*tree.DString)))     // totalCompressionRatio
	require.Equal(t, tree.DInt(10), *totalRow[5].(*tree.DInt))         // totalLevel0
	require.Equal(t, tree.DInt(20), *totalRow[6].(*tree.DInt))         // totalLevel1
	require.Equal(t, tree.DInt(30), *totalRow[7].(*tree.DInt))         // totalLevel2
}

// TestProcessDBDistributionResponseMultipleNodesCalculations tests multi-node calculations
func TestProcessDBDistributionResponseMultipleNodesCalculations(t *testing.T) {
	defer leaktest.AfterTest(t)()

	compressionRatio1 := float32(2.0)
	compressionRatio2 := float32(3.0)
	response := &serverpb.DistributionResponse{
		Distribution: []*serverpb.DistributionInfo{
			{
				NodeID: "node1",
				BlockInfo: []sqlbase.BlockInfo{
					{
						Level:            "total",
						BlocksNum:        100,
						BlocksSize:       1000000,
						AvgSize:          10000.0,
						CompressionRatio: &compressionRatio1,
						LastSegLevel0:    10,
						LastSegLevel1:    20,
						LastSegLevel2:    30,
						OriginalSize:     2000000,
					},
				},
			},
			{
				NodeID: "node2",
				BlockInfo: []sqlbase.BlockInfo{
					{
						Level:            "total",
						BlocksNum:        200,
						BlocksSize:       2000000,
						AvgSize:          10000.0,
						CompressionRatio: &compressionRatio2,
						LastSegLevel0:    20,
						LastSegLevel1:    40,
						LastSegLevel2:    60,
						OriginalSize:     6000000,
					},
				},
			},
		},
	}

	rows, _, err := processDBDistributionResponse(response)
	require.NoError(t, err)
	require.Equal(t, 3, len(rows)) // 2 data rows + 1 total row

	// Verify total row calculations
	totalRow := rows[2]
	require.Equal(t, "total", string(*totalRow[0].(*tree.DString)))
	require.Equal(t, tree.DInt(300), *totalRow[1].(*tree.DInt))     // totalNum = 100 + 200
	require.Equal(t, tree.DInt(3000000), *totalRow[2].(*tree.DInt)) // totalSize = 1000000 + 2000000
	// totalAvgSize = 3000000 / 300 = 10000.00
	require.Equal(t, "10000.00", string(*totalRow[3].(*tree.DString)))
	// totalCompressionRatio = 8000000 / 3000000 = 2.67
	require.Equal(t, "2.67", string(*totalRow[4].(*tree.DString)))
	require.Equal(t, tree.DInt(30), *totalRow[5].(*tree.DInt)) // totalLevel0 = 10 + 20
	require.Equal(t, tree.DInt(60), *totalRow[6].(*tree.DInt)) // totalLevel1 = 20 + 40
	require.Equal(t, tree.DInt(90), *totalRow[7].(*tree.DInt)) // totalLevel2 = 30 + 60
}
