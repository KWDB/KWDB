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

package tree

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestGrantFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *Grant
		expected string
	}{
		{
			name: "grant on database",
			node: &Grant{
				Privileges: privilege.List{privilege.SELECT, privilege.INSERT},
				Targets: TargetList{
					Databases: NameList{"db1"},
				},
				Grantees: NameList{"user1"},
			},
			expected: `GRANT SELECT, INSERT ON DATABASE db1 TO user1`,
		},
		{
			name: "grant on schema",
			node: &Grant{
				Privileges: privilege.List{privilege.CREATE},
				Targets: TargetList{
					Schemas: NameList{"public"},
				},
				Grantees: NameList{"user2"},
			},
			expected: `GRANT CREATE ON SCHEMA public TO user2`,
		},
		{
			name: "grant on tables",
			node: &Grant{
				Privileges: privilege.List{privilege.ALL},
				Targets: TargetList{
					Tables: TablePatterns{
						&TableName{
							tblName: tblName{
								TableName: Name("mytable"),
							},
						},
					},
				},
				Grantees: NameList{"user3", "user4"},
			},
			expected: `GRANT ALL ON TABLE mytable TO user3, user4`,
		},
		{
			name: "grant on procedures",
			node: &Grant{
				Privileges: privilege.List{privilege.EXECUTE},
				Targets: TargetList{
					Procedures: TableNames{
						func() TableName { return MakeTableName("mydb", "proc1") }(),
					},
				},
				Grantees: NameList{"user5"},
			},
			expected: `GRANT EXECUTE ON PROCEDURE mydb.public.proc1 TO user5`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestGrantRoleFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *GrantRole
		expected string
	}{
		{
			name: "grant role basic",
			node: &GrantRole{
				Roles:   NameList{"role1"},
				Members: NameList{"user1"},
			},
			expected: `GRANT role1 TO user1`,
		},
		{
			name: "grant role with admin option",
			node: &GrantRole{
				Roles:       NameList{"admin", "developer"},
				Members:     NameList{"user2", "user3"},
				AdminOption: true,
			},
			expected: `GRANT admin, developer TO user2, user3 WITH ADMIN OPTION`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}
