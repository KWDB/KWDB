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
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// delegateShowSessions rewrites ShowSessions statement to select statement which returns
// node_id, session_id, user_name... from kwdb_internal.node_sessions or kwdb_internal.cluster_sessions
func (d *delegator) delegateShowSessions(n *tree.ShowSessions) (tree.Statement, error) {
	const query = `SELECT node_id, session_id, goroutine_id, user_name, client_address, application_name, active_queries, last_active_query, session_start, oldest_query_start FROM kwdb_internal.`
	table := `node_sessions`
	if n.Cluster {
		table = `cluster_sessions`
	}
	var filter string
	if !n.All {
		filter = " WHERE application_name NOT LIKE '" + sqlbase.InternalAppNamePrefix + "%'"
	}
	return parse(query + table + filter)
}

// delegateShowApplications rewrites ShowApplications statement to select statement
// which returns application_name... from kwdb_internal.cluster_sessions
func (d *delegator) delegateShowApplications() (tree.Statement, error) {
	const query = `SELECT application_name FROM kwdb_internal.cluster_sessions`
	return parse(query)
}
