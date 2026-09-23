// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
//
// Shanghai Yunxi Technology Co, Ltd makes no representations or warranties about the suitability
// of the software, either express or implied, including but not limited to the implied warranties
// of merchantability, fitness for a particular purpose, or non-infringement. Shanghai Yunxi
// Technology Co, Ltd shall not be liable for any damages suffered by licensee as a result
// of using, modifying or distributing this software or its derivatives.
//

package delegate

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// delegateShowPublications rewrites ShowPublications statement to select statement which returns
// name, pub_objects, parameter, create_at, create_by, subscription from system.kwdb_publications.
func (d *delegator) delegateShowPublications(stmt *tree.ShowPublications) (tree.Statement, error) {
	query := `SELECT name, pub_objects, parameter, create_at, create_by, subscription FROM kwdb_internal.publications`

	if !stmt.ShowAll {
		query += fmt.Sprintf(` WHERE name='%s'`, stmt.PubName)
	}
	query += " ORDER BY create_at ASC"
	return parse(query)
}
