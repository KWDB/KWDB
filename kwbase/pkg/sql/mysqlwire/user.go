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

package mysqlwire

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// lookupUser reads an account's password material.
//
// The two MySQL digest columns are nullable and are populated only when a
// password is set through a KWDB version that knows about them, so an absent
// digest is an ordinary state rather than an error: it means the account can
// authenticate through the cleartext plugins but not through the
// challenge-response ones. See the authenticator's comment for why that
// distinction is not surfaced to the client.
func (s *Server) lookupUser(ctx context.Context, username string) (userDigests, error) {
	const query = `SELECT "hashedPassword", "mysqlNativeDigest", "mysqlSHA2Digest" ` +
		`FROM system.users WHERE username = $1 AND "isRole" = false`

	row, err := s.execCfg.InternalExecutor.QueryRowEx(
		ctx, "mysqlwire-lookup-user", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		query, username,
	)
	if err != nil {
		return userDigests{}, err
	}
	if row == nil {
		// No such account. The zero value carries exists=false, and every
		// verification path rejects it; the client is told only "access
		// denied", which is what keeps account enumeration out of reach.
		return userDigests{}, nil
	}

	digests := userDigests{exists: true}
	digests.bcryptHash = bytesOrNil(row[0])
	digests.nativeDigest = bytesOrNil(row[1])
	digests.sha2Digest = bytesOrNil(row[2])

	if len(digests.nativeDigest) == 0 && len(digests.sha2Digest) == 0 {
		log.VEventf(ctx, 2,
			"mysqlwire: %q has no MySQL password digests; only the cleartext plugins can authenticate it",
			username)
	}
	return digests, nil
}

// bytesOrNil reads a nullable BYTES column, returning nil for SQL NULL.
func bytesOrNil(d tree.Datum) []byte {
	if d == tree.DNull {
		return nil
	}
	if b, ok := d.(*tree.DBytes); ok {
		return []byte(*b)
	}
	return nil
}
