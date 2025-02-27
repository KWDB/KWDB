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

package tpcc

import (
	"encoding/json"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/workload"
)

func TestTPCCStats(t *testing.T) {
	const warehouses = 100
	gen := FromWarehouses(warehouses).(*tpcc)

	// Note: These are real stats collected from running CREATE STATISTICS on
	// a database that has been loaded with the initial rows for TPC-C with 100
	// warehouses. We check that the stats generated for the TPC-C 100 workload
	// fixture are within 5% of this sample.

	itemStats := `
    [{"columns": ["i_id"], "distinct_count": 101273, "null_count": 0, "row_count": 100000},
     {"columns": ["i_im_id"], "distinct_count": 10032, "null_count": 0, "row_count": 100000},
     {"columns": ["i_name"], "distinct_count": 98749, "null_count": 0, "row_count": 100000},
     {"columns": ["i_price"], "distinct_count": 9855, "null_count": 0, "row_count": 100000},
     {"columns": ["i_data"], "distinct_count": 98582, "null_count": 0, "row_count": 100000}]`

	warehouseStats := `
    [{"columns": ["w_id"], "distinct_count": 100, "null_count": 0, "row_count": 100},
     {"columns": ["w_name"], "distinct_count": 5, "null_count": 0, "row_count": 100},
     {"columns": ["w_street_1"], "distinct_count": 11, "null_count": 0, "row_count": 100},
     {"columns": ["w_street_2"], "distinct_count": 11, "null_count": 0, "row_count": 100},
     {"columns": ["w_city"], "distinct_count": 11, "null_count": 0, "row_count": 100},
     {"columns": ["w_state"], "distinct_count": 92, "null_count": 0, "row_count": 100},
     {"columns": ["w_zip"], "distinct_count": 100, "null_count": 0, "row_count": 100},
     {"columns": ["w_tax"], "distinct_count": 100, "null_count": 0, "row_count": 100},
     {"columns": ["w_ytd"], "distinct_count": 1, "null_count": 0, "row_count": 100}]`

	stockStats := `
    [{"columns": ["s_i_id"], "distinct_count": 101273, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_w_id"], "distinct_count": 100, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_quantity"], "distinct_count": 91, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_dist_01"], "distinct_count": 10147435, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_dist_02"], "distinct_count": 9896785, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_dist_03"], "distinct_count": 9932517, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_dist_04"], "distinct_count": 10048131, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_dist_05"], "distinct_count": 10045535, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_dist_06"], "distinct_count": 9801428, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_dist_07"], "distinct_count": 10074492, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_dist_08"], "distinct_count": 9943047, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_dist_09"], "distinct_count": 10001651, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_dist_10"], "distinct_count": 10097341, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_ytd"], "distinct_count": 1, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_order_cnt"], "distinct_count": 1, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_remote_cnt"], "distinct_count": 1, "null_count": 0, "row_count": 10000000},
     {"columns": ["s_data"], "distinct_count": 10151633, "null_count": 0, "row_count": 10000000}]`

	districtStats := `
    [{"columns": ["d_id"], "distinct_count": 10, "null_count": 0, "row_count": 1000},
     {"columns": ["d_w_id"], "distinct_count": 100, "null_count": 0, "row_count": 1000},
     {"columns": ["d_name"], "distinct_count": 1000, "null_count": 0, "row_count": 1000},
     {"columns": ["d_street_1"], "distinct_count": 1000, "null_count": 0, "row_count": 1000},
     {"columns": ["d_street_2"], "distinct_count": 1000, "null_count": 0, "row_count": 1000},
     {"columns": ["d_city"], "distinct_count": 1000, "null_count": 0, "row_count": 1000},
     {"columns": ["d_state"], "distinct_count": 506, "null_count": 0, "row_count": 1000},
     {"columns": ["d_zip"], "distinct_count": 951, "null_count": 0, "row_count": 1000},
     {"columns": ["d_tax"], "distinct_count": 780, "null_count": 0, "row_count": 1000},
     {"columns": ["d_ytd"], "distinct_count": 1, "null_count": 0, "row_count": 1000},
     {"columns": ["d_next_o_id"], "distinct_count": 1, "null_count": 0, "row_count": 1000}]`

	customerStats := `
    [{"columns": ["c_id"], "distinct_count": 3000, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_d_id"], "distinct_count": 10, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_w_id"], "distinct_count": 100, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_first"], "distinct_count": 3024712, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_middle"], "distinct_count": 1, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_last"], "distinct_count": 1000, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_street_1"], "distinct_count": 3007563, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_street_2"], "distinct_count": 3000445, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_city"], "distinct_count": 2994387, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_state"], "distinct_count": 676, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_zip"], "distinct_count": 10018, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_phone"], "distinct_count": 2972913, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_since"], "distinct_count": 1, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_credit"], "distinct_count": 2, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_credit_lim"], "distinct_count": 1, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_discount"], "distinct_count": 5000, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_balance"], "distinct_count": 1, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_ytd_payment"], "distinct_count": 1, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_payment_cnt"], "distinct_count": 1, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_delivery_cnt"], "distinct_count": 1, "null_count": 0, "row_count": 3000000},
     {"columns": ["c_data"], "distinct_count": 2967420, "null_count": 0, "row_count": 3000000}]`

	historyStats := `
    [{"columns": ["rowid"], "distinct_count": 2985531, "null_count": 0, "row_count": 3000000},
     {"columns": ["h_c_id"], "distinct_count": 3000, "null_count": 0, "row_count": 3000000},
     {"columns": ["h_c_d_id"], "distinct_count": 10, "null_count": 0, "row_count": 3000000},
     {"columns": ["h_c_w_id"], "distinct_count": 100, "null_count": 0, "row_count": 3000000},
     {"columns": ["h_d_id"], "distinct_count": 10, "null_count": 0, "row_count": 3000000},
     {"columns": ["h_w_id"], "distinct_count": 100, "null_count": 0, "row_count": 3000000},
     {"columns": ["h_date"], "distinct_count": 1, "null_count": 0, "row_count": 3000000},
     {"columns": ["h_amount"], "distinct_count": 1, "null_count": 0, "row_count": 3000000},
     {"columns": ["h_data"], "distinct_count": 2979454, "null_count": 0, "row_count": 3000000}]`

	orderStats := `
    [{"columns": ["o_id"], "distinct_count": 3000, "null_count": 0, "row_count": 3000000},
     {"columns": ["o_d_id"], "distinct_count": 10, "null_count": 0, "row_count": 3000000},
     {"columns": ["o_w_id"], "distinct_count": 100, "null_count": 0, "row_count": 3000000},
     {"columns": ["o_c_id"], "distinct_count": 3000, "null_count": 0, "row_count": 3000000},
     {"columns": ["o_entry_d"], "distinct_count": 1, "null_count": 0, "row_count": 3000000},
     {"columns": ["o_carrier_id"], "distinct_count": 10, "null_count": 900000, "row_count": 3000000},
     {"columns": ["o_ol_cnt"], "distinct_count": 11, "null_count": 0, "row_count": 3000000},
     {"columns": ["o_all_local"], "distinct_count": 1, "null_count": 0, "row_count": 3000000}]`

	newOrderStats := `
    [{"columns": ["no_o_id"], "distinct_count": 900, "null_count": 0, "row_count": 900000},
     {"columns": ["no_d_id"], "distinct_count": 10, "null_count": 0, "row_count": 900000},
     {"columns": ["no_w_id"], "distinct_count": 100, "null_count": 0, "row_count": 900000}]`

	orderLineStats := `
    [{"columns": ["ol_o_id"], "distinct_count": 3000, "null_count": 0, "row_count": 30005985},
     {"columns": ["ol_d_id"], "distinct_count": 10, "null_count": 0, "row_count": 30005985},
     {"columns": ["ol_w_id"], "distinct_count": 100, "null_count": 0, "row_count": 30005985},
     {"columns": ["ol_number"], "distinct_count": 15, "null_count": 0, "row_count": 30005985},
     {"columns": ["ol_i_id"], "distinct_count": 101273, "null_count": 0, "row_count": 30005985},
     {"columns": ["ol_supply_w_id"], "distinct_count": 100, "null_count": 0, "row_count": 30005985},
     {"columns": ["ol_delivery_d"], "distinct_count": 1, "null_count": 9003667, "row_count": 30005985},
     {"columns": ["ol_quantity"], "distinct_count": 1, "null_count": 0, "row_count": 30005985},
     {"columns": ["ol_amount"], "distinct_count": 988202, "null_count": 0, "row_count": 30005985},
     {"columns": ["ol_dist_info"], "distinct_count": 30179646, "null_count": 0, "row_count": 30005985}]`

	testStats := func(table, expectedStr string, actual []workload.JSONStatistic) {
		var expected []workload.JSONStatistic
		if err := json.Unmarshal([]byte(expectedStr), &expected); err != nil {
			t.Fatal(err)
		}
		if len(actual) != len(expected) {
			t.Fatalf("for table %s, expected %d stats but found %d", table, len(expected), len(actual))
		}
		testWithinFivePercent := func(columns []string, a, b uint64) {
			t.Helper()
			larger, smaller := float64(a), float64(b)
			if larger < smaller {
				larger, smaller = smaller, larger
			}
			if (larger-smaller)/smaller > 0.05 {
				t.Fatalf("for table %s, columns %v, %d and %d are more than 5%% apart",
					table, columns, a, b)
			}

		}
		for i := 0; i < len(actual); i++ {
			if !reflect.DeepEqual(expected[i].Columns, actual[i].Columns) {
				t.Fatalf("for table %s, expected columns %v but found %v",
					table, expected[i].Columns, actual[i].Columns)
			}
			testWithinFivePercent(expected[i].Columns, expected[i].RowCount, actual[i].RowCount)
			testWithinFivePercent(expected[i].Columns, expected[i].DistinctCount, actual[i].DistinctCount)
			testWithinFivePercent(expected[i].Columns, expected[i].NullCount, actual[i].NullCount)
			if actual[i].Name != workload.AutoStatsName {
				t.Fatalf("for table %s, expected name %s but found %s",
					table, workload.AutoStatsName, actual[i].Name)
			}
		}
	}

	testStats("item", itemStats, gen.tpccItemStats())
	testStats("warehouse", warehouseStats, gen.tpccWarehouseStats())
	testStats("stock", stockStats, gen.tpccStockStats())
	testStats("district", districtStats, gen.tpccDistrictStats())
	testStats("customer", customerStats, gen.tpccCustomerStats())
	testStats("history", historyStats, gen.tpccHistoryStats())
	testStats("order", orderStats, gen.tpccOrderStats())
	testStats("new_order", newOrderStats, gen.tpccNewOrderStats())
	testStats("order_line", orderLineStats, gen.tpccOrderLineStats())
}
