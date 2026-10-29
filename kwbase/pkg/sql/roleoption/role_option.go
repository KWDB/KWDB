// Copyright 2020 The Cockroach Authors.
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

package roleoption

import (
	"errors"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mysqlwire/mysqlwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
)

//go:generate stringer -type=Option

// Option defines a role option. This is output by the parser
type Option uint32

// RoleOption represents an Option with a value.
type RoleOption struct {
	Option
	HasValue bool
	// Need to resolve value in Exec for the case of placeholders.
	Value func() (bool, string, error)
}

// KindList of role options.
const (
	_ Option = iota
	CREATEROLE
	NOCREATEROLE
	PASSWORD
	LOGIN
	NOLOGIN
	VALIDUNTIL
)

// toSQLStmts is a map of Kind -> SQL statement string for applying the
// option to the role.
var toSQLStmts = map[Option]string{
	CREATEROLE:   `UPSERT INTO system.role_options (username, option) VALUES ($1, 'CREATEROLE')`,
	NOCREATEROLE: `DELETE FROM system.role_options WHERE username = $1 AND option = 'CREATEROLE'`,
	LOGIN:        `DELETE FROM system.role_options WHERE username = $1 AND option = 'NOLOGIN'`,
	NOLOGIN:      `UPSERT INTO system.role_options (username, option) VALUES ($1, 'NOLOGIN')`,
	VALIDUNTIL:   `UPSERT INTO system.role_options (username, option, value) VALUES ($1, 'VALID UNTIL', $2::timestamptz::string)`,
}

// Mask returns the bitmask for a given role option.
func (o Option) Mask() uint32 {
	return 1 << o
}

// ByName is a map of string -> kind value.
var ByName = map[string]Option{
	"CREATEROLE":   CREATEROLE,
	"NOCREATEROLE": NOCREATEROLE,
	"PASSWORD":     PASSWORD,
	"LOGIN":        LOGIN,
	"NOLOGIN":      NOLOGIN,
	"VALID_UNTIL":  VALIDUNTIL,
}

// ToOption takes a string and returns the corresponding Option.
func ToOption(str string) (Option, error) {
	ret := ByName[strings.ToUpper(str)]
	if ret == 0 {
		return 0, pgerror.New(pgcode.Syntax, "option does not exist")
	}

	return ret, nil
}

// List is a list of role options.
type List []RoleOption

// GetSQLStmts returns a map of SQL stmts to apply each role option.
// Maps stmts to values (value of the role option).
func (rol List) GetSQLStmts(op string) (map[string]func() (bool, string, error), error) {
	if len(rol) <= 0 {
		return nil, nil
	}

	stmts := make(map[string]func() (bool, string, error), len(rol))

	err := rol.CheckRoleOptionConflicts()
	if err != nil {
		return stmts, err
	}

	for _, ro := range rol {
		sqltelemetry.IncIAMOptionCounter(
			op,
			strings.ToLower(ro.Option.String()),
		)
		// Skip PASSWORD option.
		// Since PASSWORD still resides in system.users, we handle setting PASSWORD
		// outside of this set stmt.
		// TODO(richardjcai): migrate password to system.role_options
		if ro.Option == PASSWORD {
			continue
		}

		stmt := toSQLStmts[ro.Option]
		if ro.HasValue {
			stmts[stmt] = ro.Value
		} else {
			stmts[stmt] = nil
		}
	}

	return stmts, nil
}

// ToBitField returns the bitfield representation of
// a list of role options.
func (rol List) ToBitField() (uint32, error) {
	var ret uint32
	for _, p := range rol {
		if ret&p.Option.Mask() != 0 {
			return 0, pgerror.Newf(pgcode.Syntax, "redundant role options")
		}
		ret |= p.Option.Mask()
	}
	return ret, nil
}

// Contains returns true if List contains option, false otherwise.
func (rol List) Contains(p Option) bool {
	for _, ro := range rol {
		if ro.Option == p {
			return true
		}
	}

	return false
}

// CheckRoleOptionConflicts returns an error if two or more options conflict with each other.
func (rol List) CheckRoleOptionConflicts() error {
	roleOptionBits, err := rol.ToBitField()

	if err != nil {
		return err
	}

	if (roleOptionBits&CREATEROLE.Mask() != 0 &&
		roleOptionBits&NOCREATEROLE.Mask() != 0) ||
		(roleOptionBits&LOGIN.Mask() != 0 &&
			roleOptionBits&NOLOGIN.Mask() != 0) {
		return pgerror.Newf(pgcode.Syntax, "conflicting role options")
	}
	return nil
}

// GetHashedPassword returns the value of the password after hashing it.
// Returns error if no password option is found or if password is invalid.
func (rol List) GetHashedPassword() ([]byte, error) {
	var hashedPassword []byte
	for _, ro := range rol {
		if ro.Option == PASSWORD {
			isNull, password, err := ro.Value()
			if isNull {
				// Use empty byte array for hashedPassword.
				return hashedPassword, nil
			}
			if err != nil {
				return hashedPassword, err
			}
			if password == "" {
				return hashedPassword, security.ErrEmptyPassword
			}
			hashedPassword, err = security.HashPassword(password)
			if err != nil {
				return hashedPassword, err
			}

			return hashedPassword, nil
		}
	}
	// Password option not found.
	return hashedPassword, errors.New("password not found in role options")
}

// GetMySQLPasswordDigests returns the two digests the MySQL authentication
// plugins require: SHA1(SHA1(password)) for mysql_native_password, and
// SHA256(SHA256(password)) for the caching_sha2_password fast path.
//
// These exist because neither can be derived from the bcrypt hash KWDB stores,
// and both plugins are challenge-response: the client never sends anything the
// server could compare against bcrypt. Recording them at the moment the
// password is set is the only point where the cleartext is available.
//
// Both digests are password-equivalent for their respective plugins, and are
// far cheaper to attack offline than bcrypt. They are stored because the
// protocol leaves no alternative, and this is the same exposure a real MySQL
// server carries for the same accounts.
//
// A NULL password yields nil digests, matching GetHashedPassword's treatment.
func (rol List) GetMySQLPasswordDigests() (native []byte, sha2 []byte, err error) {
	for _, ro := range rol {
		if ro.Option != PASSWORD {
			continue
		}
		isNull, password, err := ro.Value()
		if isNull {
			return nil, nil, nil
		}
		if err != nil {
			return nil, nil, err
		}
		if password == "" {
			return nil, nil, security.ErrEmptyPassword
		}
		return mysqlwirebase.NativePasswordDigest(password),
			mysqlwirebase.CachingSHA2Digest(password), nil
	}
	return nil, nil, errors.New("password not found in role options")
}
