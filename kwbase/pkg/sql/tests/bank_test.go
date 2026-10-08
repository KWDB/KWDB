// Copyright 2015 The Cockroach Authors.
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

package tests_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
)

// maxTransfer is the maximum amount to transfer in one transaction.
const maxTransfer = 999

// runBenchmarkBank mirrors the SQL performed by examples/sql_bank, but
// structured as a benchmark for easier usage of the Go performance analysis
// tools like pprof, memprof and trace.
func runBenchmarkBank(b *testing.B, db *sqlutils.SQLRunner, numAccounts int) {
	{
		// Initialize the "bank" table.
		schema := `
CREATE TABLE IF NOT EXISTS bench.bank (
  id INT8 PRIMARY KEY,
  balance INT8 NOT NULL
)`
		db.Exec(b, schema)
		db.Exec(b, "TRUNCATE TABLE bench.bank")

		var placeholders bytes.Buffer
		var values []interface{}
		for i := 0; i < numAccounts; i++ {
			if i > 0 {
				placeholders.WriteString(", ")
			}
			fmt.Fprintf(&placeholders, "($%d, 10000)", i+1)
			values = append(values, i)
		}
		stmt := `INSERT INTO bench.bank (id, balance) VALUES ` + placeholders.String()
		db.Exec(b, stmt, values...)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			from := rand.Intn(numAccounts)
			to := rand.Intn(numAccounts - 1)
			for from == to {
				to = numAccounts - 1
			}

			amount := rand.Intn(maxTransfer)

			const update = `
UPDATE bench.bank
  SET balance = CASE id WHEN $1 THEN balance-$3 WHEN $2 THEN balance+$3 END
  WHERE id IN ($1, $2) AND (SELECT balance >= $3 FROM bench.bank WHERE id = $1)`
			db.Exec(b, update, from, to, amount)
		}
	})
	b.StopTimer()
}
