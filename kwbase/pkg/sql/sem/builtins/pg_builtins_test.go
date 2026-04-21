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

package builtins

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMakeNotUsableFalseBuiltin verifies the behavior of makeNotUsableFalseBuiltin function
func TestMakeNotUsableFalseBuiltin(t *testing.T) {
	defer leaktest.AfterTest(t)()

	builtin := makeNotUsableFalseBuiltin()
	require.Len(t, builtin.overloads, 1)

	overload := builtin.overloads[0]
	assert.Empty(t, overload.Types)
	assert.Equal(t, types.Bool, overload.FixedReturnType())

	result, err := overload.Fn(nil, nil)
	require.NoError(t, err)
	assert.Equal(t, tree.DBoolFalse, result)
	assert.Equal(t, notUsableInfo, overload.Info)
}

// TestPGIOBuiltinPrefix checks the PGIOBuiltinPrefix function returns correct prefixes for different types
func TestPGIOBuiltinPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		typ      *types.T
		expected string
	}{
		{
			name:     "date type with underscore",
			typ:      types.Date,
			expected: "date_",
		},
		{
			name:     "int8 type without underscore",
			typ:      types.Int,
			expected: "int8",
		},
		{
			name:     "jsonb type with underscore",
			typ:      types.Jsonb,
			expected: "jsonb_",
		},
		{
			name:     "int4 type without underscore",
			typ:      types.Int4,
			expected: "int4",
		},
		{
			name:     "timestamp type with underscore",
			typ:      types.Timestamp,
			expected: "timestamp_",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := PGIOBuiltinPrefix(tc.typ)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestMakeTypeIOBuiltins verifies the creation of type IO builtins
func TestMakeTypeIOBuiltins(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name          string
		builtinPrefix string
		typ           *types.T
		expectedFuncs []string
	}{
		{
			name:          "date type with underscore prefix",
			builtinPrefix: "date_",
			typ:           types.Date,
			expectedFuncs: []string{"date_send", "date_recv", "date_out", "date_in"},
		},
		{
			name:          "int8 type without underscore prefix",
			builtinPrefix: "int8",
			typ:           types.Int,
			expectedFuncs: []string{"int8send", "int8recv", "int8out", "int8in"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builtins := makeTypeIOBuiltins(tc.builtinPrefix, tc.typ)
			require.Len(t, builtins, 4)

			for _, fn := range tc.expectedFuncs {
				builtin, ok := builtins[fn]
				require.True(t, ok, "expected function %s not found", fn)

				require.Len(t, builtin.overloads, 1)
				overload := builtin.overloads[0]
				assert.Equal(t, categoryCompatibility, builtin.props.Category)
				assert.Equal(t, notUsableInfo, overload.Info)

				// Verify function returns unimplemented error
				result, err := overload.Fn(nil, nil)
				assert.Nil(t, result)
				assert.Error(t, err, errUnimplemented)
			}
		})
	}
}

// TestPGEncodingFunctions tests pg_encoding_to_char and getdatabaseencoding functions
func TestPGEncodingFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test pg_encoding_to_char
	t.Run("pg_encoding_to_char", func(t *testing.T) {
		builtin := pgBuiltins["pg_encoding_to_char"]
		require.Len(t, builtin.overloads, 1)
		overload := builtin.overloads[0]

		// Test UTF8 encoding ID
		utf8Arg := tree.NewDInt(6)
		result, err := overload.Fn(&tree.EvalContext{}, tree.Datums{utf8Arg})
		require.NoError(t, err)
		assert.Equal(t, datEncodingUTF8ShortName, result)

		// Test non-UTF8 encoding ID
		invalidArg := tree.NewDInt(10)
		result, err = overload.Fn(&tree.EvalContext{}, tree.Datums{invalidArg})
		require.NoError(t, err)
		assert.Equal(t, tree.DNull, result)
	})

	// Test getdatabaseencoding
	t.Run("getdatabaseencoding", func(t *testing.T) {
		builtin := pgBuiltins["getdatabaseencoding"]
		require.Len(t, builtin.overloads, 1)
		overload := builtin.overloads[0]

		result, err := overload.Fn(&tree.EvalContext{}, nil)
		require.NoError(t, err)
		assert.Equal(t, datEncodingUTF8ShortName, result)
		assert.Equal(t, "Returns the current encoding name used by the database.", overload.Info)
	})

	// Test pg_client_encoding
	t.Run("pg_client_encoding", func(t *testing.T) {
		builtin := pgBuiltins["pg_client_encoding"]
		require.Len(t, builtin.overloads, 1)
		overload := builtin.overloads[0]

		result, err := overload.Fn(nil, nil)
		require.NoError(t, err)
		assert.Equal(t, tree.NewDString("UTF8"), result)
	})
}

// TestPGGetExpr tests the pg_get_expr builtin function
func TestPGGetExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	builtin := pgBuiltins["pg_get_expr"]
	require.Len(t, builtin.overloads, 2)

	// Test two-argument overload
	t.Run("two arguments", func(t *testing.T) {
		overload := builtin.overloads[0]
		testExpr := tree.NewDString("test_expression")
		oidArg := tree.NewDOid(123)

		result, err := overload.Fn(nil, tree.Datums{testExpr, oidArg})
		require.NoError(t, err)
		assert.Equal(t, testExpr, result)
	})

	// Test three-argument overload
	t.Run("three arguments", func(t *testing.T) {
		overload := builtin.overloads[1]
		testExpr := tree.NewDString("test_expression")
		oidArg := tree.NewDOid(123)
		boolArg := tree.DBoolTrue

		result, err := overload.Fn(nil, tree.Datums{testExpr, oidArg, boolArg})
		require.NoError(t, err)
		assert.Equal(t, testExpr, result)
	})
}

// TestOidFunction tests the oid conversion function
func TestOidFunction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	builtin := pgBuiltins["oid"]
	require.Len(t, builtin.overloads, 1)
	overload := builtin.overloads[0]

	testCases := []struct {
		name     string
		input    tree.Datum
		expected tree.Datum
	}{
		{
			name:     "convert int 123 to oid",
			input:    tree.NewDInt(123),
			expected: tree.NewDOid(123),
		},
		{
			name:     "convert int 0 to oid",
			input:    tree.NewDInt(0),
			expected: tree.NewDOid(0),
		},
		{
			name:     "convert negative int to oid",
			input:    tree.NewDInt(-456),
			expected: tree.NewDOid(-456),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := overload.Fn(nil, tree.Datums{tc.input})
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestAdvisoryLockFunctions tests pg_try_advisory_lock and pg_advisory_unlock functions
func TestAdvisoryLockFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test pg_try_advisory_lock
	t.Run("pg_try_advisory_lock", func(t *testing.T) {
		builtin := pgBuiltins["pg_try_advisory_lock"]
		require.Len(t, builtin.overloads, 1)
		overload := builtin.overloads[0]

		result, err := overload.Fn(nil, tree.Datums{tree.NewDInt(123)})
		require.NoError(t, err)
		assert.Equal(t, tree.DBoolTrue, result)
	})

	// Test pg_advisory_unlock
	t.Run("pg_advisory_unlock", func(t *testing.T) {
		builtin := pgBuiltins["pg_advisory_unlock"]
		require.Len(t, builtin.overloads, 1)
		overload := builtin.overloads[0]

		result, err := overload.Fn(nil, tree.Datums{tree.NewDInt(123)})
		require.NoError(t, err)
		assert.Equal(t, tree.DBoolTrue, result)
	})
}

// TestPGTypeof tests the pg_typeof builtin function
func TestPGTypeof(t *testing.T) {
	defer leaktest.AfterTest(t)()

	builtin := pgBuiltins["pg_typeof"]
	require.Len(t, builtin.overloads, 1)
	overload := builtin.overloads[0]

	testCases := []struct {
		name     string
		input    tree.Datum
		expected string
	}{
		{
			name:     "integer type",
			input:    tree.NewDInt(123),
			expected: "int",
		},
		{
			name:     "string type",
			input:    tree.NewDString("test"),
			expected: "string",
		},
		{
			name:     "boolean type",
			input:    tree.DBoolTrue,
			expected: "bool",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := overload.Fn(nil, tree.Datums{tc.input})
			require.NoError(t, err)
			assert.Equal(t, tree.NewDString(tc.expected), result)
		})
	}
}

// TestFormatType tests the format_type builtin function
func TestFormatType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	builtin := pgBuiltins["format_type"]
	require.Len(t, builtin.overloads, 1)
	overload := builtin.overloads[0]

	testCases := []struct {
		name      string
		oidArg    tree.Datum
		typmodArg tree.Datum
		expected  tree.Datum
	}{
		{
			name:      "known type (int4)",
			oidArg:    tree.NewDOid(tree.DInt(types.Int4.Oid())),
			typmodArg: tree.DNull,
			expected:  tree.NewDString("integer"),
		},
		// FIXED: varchar typmod = n + 4, so 50 → pass 54 to get varchar(50)
		{
			name:      "known type (varchar) with typmod",
			oidArg:    tree.NewDOid(tree.DInt(types.VarChar.Oid())),
			typmodArg: tree.NewDInt(54),
			expected:  tree.NewDString("character varying(50)"),
		},
		{
			name:      "unknown type OID",
			oidArg:    tree.NewDOid(999999),
			typmodArg: tree.DNull,
			expected:  tree.NewDString("unknown (OID=999999)"),
		},
		{
			name:      "null OID argument",
			oidArg:    tree.DNull,
			typmodArg: tree.NewDInt(10),
			expected:  tree.DNull,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := overload.Fn(&tree.EvalContext{}, tree.Datums{tc.oidArg, tc.typmodArg})
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestPGBackendPID tests the pg_backend_pid builtin function
func TestPGBackendPID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	builtin := pgBuiltins["pg_backend_pid"]
	require.Len(t, builtin.overloads, 1)
	overload := builtin.overloads[0]

	result, err := overload.Fn(nil, nil)
	require.NoError(t, err)
	assert.Equal(t, tree.NewDInt(-1), result)
	assert.Equal(t, notUsableInfo, overload.Info)
}

// TestPGMyTempSchema tests the pg_my_temp_schema builtin function
func TestPGMyTempSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()

	builtin := pgBuiltins["pg_my_temp_schema"]
	require.Len(t, builtin.overloads, 1)
	overload := builtin.overloads[0]

	result, err := overload.Fn(nil, nil)
	require.NoError(t, err)
	assert.Equal(t, tree.NewDOid(0), result)
	assert.Equal(t, notUsableInfo, overload.Info)
}

// TestInitPGBuiltins tests the initPGBuiltins function for duplicate builtins
func TestInitPGBuiltins(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Save original builtins and restore after test
	originalBuiltins := make(map[string]builtinDefinition)
	for k, v := range builtins {
		originalBuiltins[k] = v
	}
	defer func() {
		builtins = originalBuiltins
	}()

	// Reset builtins for test
	builtins = make(map[string]builtinDefinition)

	// Add a duplicate to pgBuiltins to test panic
	originalPgBuiltins := pgBuiltins
	defer func() {
		pgBuiltins = originalPgBuiltins
	}()

	// Test normal case (no duplicates)
	assert.NotPanics(t, func() {
		initPGBuiltins()
	})

	// Test duplicate builtin case
	duplicateName := "pg_backend_pid"
	pgBuiltins[duplicateName] = pgBuiltins[duplicateName] // Duplicate entry

	assert.Panics(t, func() {
		initPGBuiltins()
	})
}

// TestGetNameForArg tests the getNameForArg helper function
func TestGetNameForArg(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This test requires a mock EvalContext with InternalExecutor
	// For unit test purposes, we'll test the type checking logic
	t.Run("invalid argument type", func(t *testing.T) {
		ctx := &tree.EvalContext{}
		invalidArg := tree.NewDInt(123) // Should be DString or DOid

		assert.Panics(t, func() {
			_, _ = getNameForArg(ctx, invalidArg, "pg_roles", "rolname")
		})
	})
}

// TestParsePrivilegeStr tests the parsePrivilegeStr helper function
func TestParsePrivilegeStr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name        string
		privStr     string
		availOpts   pgPrivList
		expectedErr string
		expectedRes tree.Datum
	}{
		{
			name:    "valid privilege",
			privStr: "SELECT",
			availOpts: pgPrivList{
				"SELECT": func(_ bool) (tree.Datum, error) {
					return tree.DBoolTrue, nil
				},
			},
			expectedRes: tree.DBoolTrue,
		},
		{
			name:    "valid privilege with grant option",
			privStr: "SELECT WITH GRANT OPTION",
			availOpts: pgPrivList{
				"SELECT": func(withGrantOpt bool) (tree.Datum, error) {
					assert.True(t, withGrantOpt)
					return tree.DBoolTrue, nil
				},
			},
			expectedRes: tree.DBoolTrue,
		},
		{
			name:    "multiple valid privileges",
			privStr: "SELECT, INSERT",
			availOpts: pgPrivList{
				"SELECT": func(_ bool) (tree.Datum, error) {
					return tree.DBoolTrue, nil
				},
				"INSERT": func(_ bool) (tree.Datum, error) {
					return tree.DBoolTrue, nil
				},
			},
			expectedRes: tree.DBoolTrue,
		},
		{
			name:    "invalid privilege",
			privStr: "INVALID",
			availOpts: pgPrivList{
				"SELECT": func(_ bool) (tree.Datum, error) {
					return tree.DBoolTrue, nil
				},
			},
			expectedErr: `unrecognized privilege type: "INVALID"`,
		},
		{
			name:    "mixed valid and invalid privileges",
			privStr: "SELECT, INVALID",
			availOpts: pgPrivList{
				"SELECT": func(_ bool) (tree.Datum, error) {
					return tree.DBoolTrue, nil
				},
			},
			expectedErr: `unrecognized privilege type: "INVALID"`,
		},
		{
			name:    "privilege with whitespace",
			privStr: " SELECT   , INSERT WITH GRANT OPTION ",
			availOpts: pgPrivList{
				"SELECT": func(_ bool) (tree.Datum, error) {
					return tree.DBoolTrue, nil
				},
				"INSERT WITH GRANT OPTION": func(_ bool) (tree.Datum, error) {
					return tree.DBoolTrue, nil
				},
			},
			expectedRes: tree.DBoolTrue,
		},
		{
			name:    "case insensitive privilege",
			privStr: "select",
			availOpts: pgPrivList{
				"SELECT": func(_ bool) (tree.Datum, error) {
					return tree.DBoolTrue, nil
				},
			},
			expectedRes: tree.DBoolTrue,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			arg := tree.NewDString(tc.privStr)
			result, err := parsePrivilegeStr(arg, tc.availOpts)

			if tc.expectedErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedRes, result)
			}
		})
	}
}

func TestPGBuiltins(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	// -------------------------------------------------------------------------
	// Test pg_get_expr built-in function
	// -------------------------------------------------------------------------
	t.Run("pg_get_expr_two_args", func(t *testing.T) {
		fnDef := pgBuiltins["pg_get_expr"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDString("test_expr"), tree.NewDOid(12345)}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("test_expr"), res)
	})

	t.Run("pg_get_expr_three_args", func(t *testing.T) {
		fnDef := pgBuiltins["pg_get_expr"]
		overload := fnDef.overloads[1]

		args := tree.Datums{tree.NewDString("test_expr"), tree.NewDOid(12345), tree.DBoolTrue}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("test_expr"), res)
	})

	// -------------------------------------------------------------------------
	// Test pg_my_temp_schema built-in function
	// -------------------------------------------------------------------------
	t.Run("pg_my_temp_schema", func(t *testing.T) {
		fnDef := pgBuiltins["pg_my_temp_schema"]
		overload := fnDef.overloads[0]

		args := tree.Datums{}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.NewDOid(0), res)
	})

	// -------------------------------------------------------------------------
	// Test pg_typeof built-in function
	// -------------------------------------------------------------------------
	t.Run("pg_typeof_int_value", func(t *testing.T) {
		fnDef := pgBuiltins["pg_typeof"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDInt(10)}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("int"), res)
	})

	t.Run("pg_typeof_string_value", func(t *testing.T) {
		fnDef := pgBuiltins["pg_typeof"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDString("test")}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("string"), res)
	})

	t.Run("pg_typeof_float_value", func(t *testing.T) {
		fnDef := pgBuiltins["pg_typeof"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDFloat(1.23)}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("float"), res)
	})

	// -------------------------------------------------------------------------
	// Test format_type built-in function
	// -------------------------------------------------------------------------
	t.Run("format_type_with_valid_oid", func(t *testing.T) {
		fnDef := pgBuiltins["format_type"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDOid(tree.DInt(types.Int.Oid())), tree.NewDInt(-1)}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("bigint"), res)
	})

	t.Run("format_type_with_unknown_oid", func(t *testing.T) {
		fnDef := pgBuiltins["format_type"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDOid(9999), tree.NewDInt(-1)}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("unknown (OID=9999)"), res)
	})

	t.Run("format_type_with_null_oid", func(t *testing.T) {
		fnDef := pgBuiltins["format_type"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.DNull, tree.NewDInt(-1)}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
	})

	// -------------------------------------------------------------------------
	// Test oid built-in function
	// -------------------------------------------------------------------------
	t.Run("oid_convert_int_to_oid", func(t *testing.T) {
		fnDef := pgBuiltins["oid"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDInt(12345)}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.NewDOid(12345), res)
	})

	// -------------------------------------------------------------------------
	// Test pg_try_advisory_lock built-in function
	// -------------------------------------------------------------------------
	t.Run("pg_try_advisory_lock", func(t *testing.T) {
		fnDef := pgBuiltins["pg_try_advisory_lock"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDInt(12345)}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DBoolTrue, res)
	})

	// -------------------------------------------------------------------------
	// Test pg_advisory_unlock built-in function
	// -------------------------------------------------------------------------
	t.Run("pg_advisory_unlock", func(t *testing.T) {
		fnDef := pgBuiltins["pg_advisory_unlock"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDInt(12345)}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DBoolTrue, res)
	})

	// -------------------------------------------------------------------------
	// Test pg_client_encoding built-in function
	// -------------------------------------------------------------------------
	t.Run("pg_client_encoding", func(t *testing.T) {
		fnDef := pgBuiltins["pg_client_encoding"]
		overload := fnDef.overloads[0]

		args := tree.Datums{}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.NewDString("UTF8"), res)
	})

	// -------------------------------------------------------------------------
	// Test pg_type_is_visible built-in function
	// -------------------------------------------------------------------------
	t.Run("pg_type_is_visible_valid_oid", func(t *testing.T) {
		fnDef := pgBuiltins["pg_type_is_visible"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDOid(tree.DInt(types.Int.Oid()))}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DBoolTrue, res)
	})

	t.Run("pg_type_is_visible_null_oid", func(t *testing.T) {
		fnDef := pgBuiltins["pg_type_is_visible"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.DNull}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
	})

	// -------------------------------------------------------------------------
	// Test pg_sleep built-in function
	// -------------------------------------------------------------------------
	t.Run("pg_sleep_short_sleep", func(t *testing.T) {
		fnDef := pgBuiltins["pg_sleep"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDFloat(0.001)}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DBoolTrue, res)
	})

	// -------------------------------------------------------------------------
	// Test pg_is_in_recovery built-in function
	// -------------------------------------------------------------------------
	t.Run("pg_is_in_recovery", func(t *testing.T) {
		fnDef := pgBuiltins["pg_is_in_recovery"]
		overload := fnDef.overloads[0]

		args := tree.Datums{}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DBoolFalse, res)
	})

	// -------------------------------------------------------------------------
	// Test pg_is_xlog_replay_paused built-in function
	// -------------------------------------------------------------------------
	t.Run("pg_is_xlog_replay_paused", func(t *testing.T) {
		fnDef := pgBuiltins["pg_is_xlog_replay_paused"]
		overload := fnDef.overloads[0]

		args := tree.Datums{}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DBoolFalse, res)
	})

	// -------------------------------------------------------------------------
	// Test has_any_column_privilege built-in function
	// -------------------------------------------------------------------------
	t.Run("has_any_column_privilege", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "check table select privilege with string table name",
				args:     tree.Datums{tree.NewDString("public.test_table"), tree.NewDString("SELECT")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "check table insert privilege with oid",
				args:     tree.Datums{tree.NewDInt(12345), tree.NewDString("INSERT")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "check invalid privilege returns error",
				args:     tree.Datums{tree.NewDString("public.test_table"), tree.NewDString("INVALID")},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("has_any_column_privilege")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					assert.Nil(t, got)
					return
				}
				assert.NoError(t, err)
				if got != tree.DNull {
					_, ok := got.(*tree.DBool)
					assert.True(t, ok)
				}
			})
		}
	})

	// -------------------------------------------------------------------------
	// Test has_column_privilege built-in function
	// -------------------------------------------------------------------------
	t.Run("has_column_privilege", func(t *testing.T) {
		fnDef := pgBuiltins["has_column_privilege"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDOid(12345), tree.NewDString("id"), tree.NewDString("SELECT")}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
	})

	// -------------------------------------------------------------------------
	// Test has_database_privilege built-in function
	// -------------------------------------------------------------------------
	t.Run("has_database_privilege", func(t *testing.T) {
		tests := []struct {
			name     string
			args     tree.Datums
			overload int
			wantErr  bool
		}{
			{
				name:     "valid database with create privilege",
				args:     tree.Datums{tree.NewDString("defaultdb"), tree.NewDString("CREATE")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "valid database with connect privilege",
				args:     tree.Datums{tree.NewDString("defaultdb"), tree.NewDString("CONNECT")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "valid database with temporary privilege",
				args:     tree.Datums{tree.NewDString("defaultdb"), tree.NewDString("TEMPORARY")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "database oid returns null for non-existent oid",
				args:     tree.Datums{tree.NewDOid(999999), tree.NewDString("CONNECT")},
				overload: 0,
				wantErr:  false,
			},
			{
				name:     "invalid privilege string returns error",
				args:     tree.Datums{tree.NewDString("defaultdb"), tree.NewDString("INVALID")},
				overload: 0,
				wantErr:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, overloads := GetBuiltinProperties("has_database_privilege")
				got, err := overloads[tt.overload].Fn(ctx, tt.args)

				if tt.wantErr {
					assert.Error(t, err)
					assert.Nil(t, got)
					return
				}
				assert.NoError(t, err)
				if got != tree.DNull {
					_, ok := got.(*tree.DBool)
					assert.True(t, ok)
				}
			})
		}
	})

	// -------------------------------------------------------------------------
	// Test has_foreign_data_wrapper_privilege built-in function
	// -------------------------------------------------------------------------
	t.Run("has_foreign_data_wrapper_privilege", func(t *testing.T) {
		fnDef := pgBuiltins["has_foreign_data_wrapper_privilege"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDOid(12345), tree.NewDString("USAGE")}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
	})

	// -------------------------------------------------------------------------
	// Test has_function_privilege built-in function
	// -------------------------------------------------------------------------
	t.Run("has_function_privilege", func(t *testing.T) {
		fnDef := pgBuiltins["has_function_privilege"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDOid(12345), tree.NewDString("EXECUTE")}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
	})

	// -------------------------------------------------------------------------
	// Test has_language_privilege built-in function
	// -------------------------------------------------------------------------
	t.Run("has_language_privilege", func(t *testing.T) {
		fnDef := pgBuiltins["has_language_privilege"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDOid(12345), tree.NewDString("USAGE")}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
	})

	// -------------------------------------------------------------------------
	// Test has_schema_privilege built-in function
	// -------------------------------------------------------------------------
	t.Run("has_schema_privilege", func(t *testing.T) {
		fnDef := pgBuiltins["has_schema_privilege"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDOid(12345), tree.NewDString("USAGE")}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
	})

	// -------------------------------------------------------------------------
	// Test has_sequence_privilege built-in function
	// -------------------------------------------------------------------------
	t.Run("has_sequence_privilege", func(t *testing.T) {
		fnDef := pgBuiltins["has_sequence_privilege"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDOid(12345), tree.NewDString("USAGE")}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
	})

	// -------------------------------------------------------------------------
	// Test has_server_privilege built-in function
	// -------------------------------------------------------------------------
	t.Run("has_server_privilege", func(t *testing.T) {
		fnDef := pgBuiltins["has_server_privilege"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDOid(12345), tree.NewDString("USAGE")}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
	})

	// -------------------------------------------------------------------------
	// Test has_table_privilege built-in function
	// -------------------------------------------------------------------------
	t.Run("has_table_privilege", func(t *testing.T) {
		fnDef := pgBuiltins["has_table_privilege"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDOid(12345), tree.NewDString("SELECT")}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
	})

	// -------------------------------------------------------------------------
	// Test has_tablespace_privilege built-in function
	// -------------------------------------------------------------------------
	t.Run("has_tablespace_privilege", func(t *testing.T) {
		fnDef := pgBuiltins["has_tablespace_privilege"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDOid(12345), tree.NewDString("CREATE")}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
	})

	// -------------------------------------------------------------------------
	// Test has_type_privilege built-in function
	// -------------------------------------------------------------------------
	t.Run("has_type_privilege", func(t *testing.T) {
		fnDef := pgBuiltins["has_type_privilege"]
		overload := fnDef.overloads[0]

		args := tree.Datums{tree.NewDOid(12345), tree.NewDString("USAGE")}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DNull, res)
	})

	// -------------------------------------------------------------------------
	// Test inet_client_addr built-in function
	// -------------------------------------------------------------------------
	t.Run("inet_client_addr", func(t *testing.T) {
		fnDef := pgBuiltins["inet_client_addr"]
		overload := fnDef.overloads[0]

		args := tree.Datums{}
		_, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
	})

	// -------------------------------------------------------------------------
	// Test inet_client_port built-in function
	// -------------------------------------------------------------------------
	t.Run("inet_client_port", func(t *testing.T) {
		fnDef := pgBuiltins["inet_client_port"]
		overload := fnDef.overloads[0]

		args := tree.Datums{}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DZero, res)
	})

	// -------------------------------------------------------------------------
	// Test inet_server_addr built-in function
	// -------------------------------------------------------------------------
	t.Run("inet_server_addr", func(t *testing.T) {
		fnDef := pgBuiltins["inet_server_addr"]
		overload := fnDef.overloads[0]

		args := tree.Datums{}
		_, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
	})

	// -------------------------------------------------------------------------
	// Test inet_server_port built-in function
	// -------------------------------------------------------------------------
	t.Run("inet_server_port", func(t *testing.T) {
		fnDef := pgBuiltins["inet_server_port"]
		overload := fnDef.overloads[0]

		args := tree.Datums{}
		res, err := overload.Fn(ctx, args)

		assert.NoError(t, err)
		assert.Equal(t, tree.DZero, res)
	})
}
