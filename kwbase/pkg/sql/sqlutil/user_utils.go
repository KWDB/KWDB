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

package sqlutil

import (
	"regexp"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

const usernameHelp = "Usernames are case insensitive, must start with a letter, " +
	"digit or underscore, may contain letters, digits, dashes, periods, or underscores, and must not exceed 63 characters."

var usernameRE = regexp.MustCompile(`^[\p{Ll}0-9_][---\p{Ll}0-9_.]*$`)

var blacklistedUsernames = map[string]struct{}{
	security.NodeUser: {},
}

// NormalizeAndValidateUsername case folds the specified username and verifies
// it validates according to the usernameRE regular expression.
// It rejects reserved user names.
func NormalizeAndValidateUsername(username string) (string, error) {
	username, err := NormalizeAndValidateUsernameNoBlacklist(username)
	if err != nil {
		return "", err
	}
	if _, ok := blacklistedUsernames[username]; ok {
		return "", pgerror.Newf(pgcode.ReservedName, "username %q reserved", username)
	}
	return username, nil
}

// NormalizeAndValidateUsernameNoBlacklist case folds the specified username and verifies
// it validates according to the usernameRE regular expression.
func NormalizeAndValidateUsernameNoBlacklist(username string) (string, error) {
	username = tree.Name(username).Normalize()
	if !usernameRE.MatchString(username) {
		return "", errors.WithHint(pgerror.Newf(pgcode.InvalidName, "username %q invalid", username), usernameHelp)
	}
	if len(username) > 63 {
		return "", errors.WithHint(pgerror.Newf(pgcode.NameTooLong, "username %q is too long", username), usernameHelp)
	}
	return username, nil
}
