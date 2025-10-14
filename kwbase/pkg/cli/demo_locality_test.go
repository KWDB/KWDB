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

//go:build !race
// +build !race

package cli

import "gitee.com/kwbasedb/kwbase/pkg/security"

func Example_demo_locality() {
	c := newCLITest(cliTestParams{noServer: true})
	defer c.cleanup()

	testData := [][]string{
		{`demo`, `--nodes`, `3`, `-e`, `select node_id, locality from kwdb_internal.gossip_nodes order by node_id`},
		{`demo`, `--nodes`, `9`, `-e`, `select node_id, locality from kwdb_internal.gossip_nodes order by node_id`},
		{`demo`, `--nodes`, `3`, `--demo-locality=region=us-east1:region=us-east2:region=us-east3`,
			`-e`, `select node_id, locality from kwdb_internal.gossip_nodes order by node_id`},
	}
	setCLIDefaultsForTests()
	// We must reset the security asset loader here, otherwise the dummy
	// asset loader that is set by default in tests will not be able to
	// find the certs that demo sets up.
	security.ResetAssetLoader()
	for _, cmd := range testData {
		c.RunWithArgs(cmd)
	}

	// Output:
	// demo --nodes 3 -e select node_id, locality from kwdb_internal.gossip_nodes order by node_id
	// node_id	locality
	// 1	region=us-east1,az=b
	// 2	region=us-east1,az=c
	// 3	region=us-east1,az=d
	// demo --nodes 9 -e select node_id, locality from kwdb_internal.gossip_nodes order by node_id
	// node_id	locality
	// 1	region=us-east1,az=b
	// 2	region=us-east1,az=c
	// 3	region=us-east1,az=d
	// 4	region=us-west1,az=a
	// 5	region=us-west1,az=b
	// 6	region=us-west1,az=c
	// 7	region=europe-west1,az=b
	// 8	region=europe-west1,az=c
	// 9	region=europe-west1,az=d
	// demo --nodes 3 --demo-locality=region=us-east1:region=us-east2:region=us-east3 -e select node_id, locality from kwdb_internal.gossip_nodes order by node_id
	// node_id	locality
	// 1	region=us-east1
	// 2	region=us-east2
	// 3	region=us-east3
}
