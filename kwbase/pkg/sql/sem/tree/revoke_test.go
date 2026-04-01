package tree

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestRevokeFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		stmt     *Revoke
		expected string
	}{
		{
			name: "revoke all on table",
			stmt: &Revoke{
				Privileges: privilege.List{privilege.ALL},
				Targets: TargetList{
					Tables: TablePatterns{
						&TableName{
							tblName: tblName{
								TableName: Name("t1"),
							},
						},
					},
				},
				Grantees: NameList{Name("user1"), Name("user2")},
			},
			expected: `REVOKE ALL ON TABLE t1 FROM user1, user2`,
		},
		{
			name: "revoke select, insert on database",
			stmt: &Revoke{
				Privileges: privilege.List{privilege.SELECT, privilege.INSERT},
				Targets: TargetList{
					Databases: NameList{Name("db1")},
				},
				Grantees: NameList{Name("user1")},
			},
			expected: `REVOKE SELECT, INSERT ON DATABASE db1 FROM user1`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tt.stmt.Format(ctx)
			require.Equal(t, tt.expected, ctx.CloseAndGetString())
		})
	}
}

func TestRevokeRoleFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		stmt     *RevokeRole
		expected string
	}{
		{
			name: "revoke role without admin option",
			stmt: &RevokeRole{
				Roles:   NameList{Name("role1"), Name("role2")},
				Members: NameList{Name("user1")},
			},
			expected: `REVOKE role1, role2 FROM user1`,
		},
		{
			name: "revoke role with admin option",
			stmt: &RevokeRole{
				Roles:       NameList{Name("role1")},
				Members:     NameList{Name("user1"), Name("user2")},
				AdminOption: true,
			},
			expected: `REVOKE ADMIN OPTION FOR role1 FROM user1, user2`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tt.stmt.Format(ctx)
			require.Equal(t, tt.expected, ctx.CloseAndGetString())
		})
	}
}
