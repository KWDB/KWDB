// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
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

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowStreams(n *tree.ShowStreams) (tree.Statement, error) {
	var query string
	if n.ShowAll {
		query = `SELECT * FROM kwdb_internal.kwdb_streams`
	} else {
		query = fmt.Sprintf(`SELECT * FROM kwdb_internal.kwdb_streams WHERE name='%s'`, n.StreamName)
	}
	return parse(query)
}
