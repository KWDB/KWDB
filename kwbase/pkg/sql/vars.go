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

package sql

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/build"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/delegate"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

const (
	// PgServerVersion is the latest version of postgres that we claim to support.
	PgServerVersion = "9.5.0"
	// PgServerVersionNum is the latest version of postgres that we claim to support in the numeric format of "server_version_num".
	PgServerVersionNum = "90500"
)

type getStringValFn = func(
	ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
) (string, error)

// sessionVar provides a unified interface for performing operations on
// variables such as the selected database, or desired syntax.
type sessionVar struct {
	// Hidden indicates that the variable should not show up in the output of SHOW ALL.
	Hidden bool

	// Get returns a string representation of a given variable to be used
	// either by SHOW or in the pg_catalog table.
	Get func(evalCtx *extendedEvalContext) string

	// GetStringVal converts the provided Expr to a string suitable
	// for Set() or RuntimeSet().
	// If this method is not provided,
	//   `getStringVal(evalCtx, varName, values)`
	// will be used instead.
	//
	// The reason why variable sets work in two phases like this is that
	// the Set() method has to operate on strings, because it can be
	// invoked at a point where there is no evalContext yet (e.g.
	// upon session initialization in pgwire).
	GetStringVal getStringValFn

	// Set performs mutations to effect the change desired by SET commands.
	// This method should be provided for variables that can be overridden
	// in pgwire.
	Set func(ctx context.Context, m *sessionDataMutator, val string) error

	// RuntimeSet is like Set except it can only be used in sessions
	// that are already running (i.e. not during session
	// initialization).  Currently only used for transaction_isolation.
	RuntimeSet func(_ context.Context, evalCtx *extendedEvalContext, s string) error

	// GlobalDefault is the string value to use as default for RESET or
	// during session initialization when no default value was provided
	// by the client.
	GlobalDefault func(sv *settings.Values) string
}

func formatBoolAsPostgresSetting(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func parsePostgresBool(s string) (bool, error) {
	s = strings.ToLower(s)
	switch s {
	case "on":
		s = "true"
	case "off":
		s = "false"
	}
	return strconv.ParseBool(s)
}

// varGen is the main definition array for all session variables.
// Note to maintainers: try to keep this sorted in the source code.
var varGen = map[string]sessionVar{
	// Set by clients to improve query logging.
	// See https://www.postgresql.org/docs/10/static/runtime-config-logging.html#GUC-APPLICATION-NAME
	`application_name`: {
		Set: func(
			ctx context.Context, m *sessionDataMutator, s string,
		) error {
			m.SetApplicationName(s)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.ApplicationName
		},
		GlobalDefault: func(_ *settings.Values) string { return "" },
	},

	`avoid_buffering`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.AvoidBuffering)
		},
		GetStringVal: makePostgresBoolGetStringValFn("avoid_buffering"),
		Set: func(ctx context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetAvoidBuffering(b)
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "false"
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html
	// and https://www.postgresql.org/docs/10/static/datatype-binary.html
	`bytea_output`: {
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			mode, ok := sessiondata.BytesEncodeFormatFromString(s)
			if !ok {
				return newVarValueError(`bytea_output`, s, "hex", "escape", "base64")
			}
			m.SetBytesEncodeFormat(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.DataConversion.BytesEncodeFormat.String()
		},
		GlobalDefault: func(sv *settings.Values) string { return sessiondata.BytesEncodeHex.String() },
	},

	// Supported for PG compatibility only.
	// Controls returned message verbosity. We don't support this.
	// See https://www.postgresql.org/docs/9.6/static/runtime-config-compatible.html
	`client_min_messages`: makeCompatStringVar(`client_min_messages`, `notice`, `debug5`, `debug4`, `debug3`, `debug2`, `debug1`, `debug`, `log`, `warning`, `error`, `fatal`, `panic`),

	// See https://www.postgresql.org/docs/9.6/static/multibyte.html
	// Also aliased to SET NAMES.
	`client_encoding`: {
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			encoding := builtins.CleanEncodingName(s)
			switch encoding {
			case "utf8", "unicode", "cp65001":
				m.data.ClientEncoding = encoding
			case "gbk", "gb18030":
				m.data.ClientEncoding = encoding
			case "big5":
				m.data.ClientEncoding = encoding
			case "":
				m.data.ClientEncoding = "UTF8"
			default:
				return unimplemented.NewWithIssueDetailf(35882,
					"client_encoding "+encoding,
					"unimplemented client encoding: %q", encoding)
			}
			m.paramStatusUpdater.AppendParamStatusUpdate("client_encoding", encoding)
			return nil
		},
		Get:           func(evalCtx *extendedEvalContext) string { return evalCtx.SessionData.ClientEncoding },
		GlobalDefault: func(_ *settings.Values) string { return "UTF8" },
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/9.6/static/multibyte.html
	`server_encoding`: makeReadOnlyVar("UTF8"),

	// CockroachDB extension.
	`database`: {
		GetStringVal: func(
			ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) (string, error) {
			dbName, err := getStringVal(&evalCtx.EvalContext, `database`, values)
			if err != nil {
				return "", err
			}

			if len(dbName) == 0 && evalCtx.SessionData.SafeUpdates {
				return "", pgerror.DangerousStatementf("SET database to empty string")
			}

			if len(dbName) != 0 {
				// Verify database descriptor exists.
				if _, err := evalCtx.schemaAccessors.logical.GetDatabaseDesc(ctx, evalCtx.Txn,
					dbName, tree.DatabaseLookupFlags{Required: true}); err != nil {
					return "", err
				}
			}
			return dbName, nil
		},
		Set: func(
			ctx context.Context, m *sessionDataMutator, dbName string,
		) error {
			m.SetDatabase(dbName)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string { return evalCtx.SessionData.Database },
		GlobalDefault: func(_ *settings.Values) string {
			// The "defaultdb" value is set as session default in the pgwire
			// connection code. The global default is the empty string,
			// which is what internal connections should pick up.
			return ""
		},
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-DATESTYLE
	`datestyle`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			s = strings.ToLower(s)
			parts := strings.Split(s, ",")
			if strings.TrimSpace(parts[0]) != "iso" ||
				(len(parts) == 2 && strings.TrimSpace(parts[1]) != "mdy") ||
				len(parts) > 2 {
				err := newVarValueError("DateStyle", s, "ISO", "ISO, MDY")
				err = errors.WithDetail(err, compatErrMsg)
				return err
			}
			return nil
		},
		Get:           func(evalCtx *extendedEvalContext) string { return "ISO, MDY" },
		GlobalDefault: func(_ *settings.Values) string { return "ISO, MDY" },
	},
	// Controls the subsequent parsing of a "naked" INT type.
	// TODO(bob): Remove or no-op this in v2.4: https://gitee.com/kwbasedb/kwbase/issues/32844
	`default_int_size`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(int64(evalCtx.SessionData.DefaultIntSize), 10)
		},
		GetStringVal: makeIntGetStringValFn("default_int_size"),
		Set: func(ctx context.Context, m *sessionDataMutator, val string) error {
			i, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return wrapSetVarError("default_int_size", val, "%v", err)
			}
			if i != 4 && i != 8 {
				return pgerror.New(pgcode.InvalidParameterValue,
					`only 4 or 8 are supported by default_int_size`)
			}
			// Only record when the value has been changed to a non-default
			// value, since we really just want to know how useful int4-mode
			// is. If we were to record counts for size.4 and size.8
			// variables, we'd have to distinguish cases in which a session
			// was opened in int8 mode and switched to int4 mode, versus ones
			// set to int4 by a connection string.
			// TODO(bob): Change to 8 in v2.3: https://gitee.com/kwbasedb/kwbase/issues/32534
			if i == 4 {
				telemetry.Inc(sqltelemetry.DefaultIntSize4Counter)
			}
			m.SetDefaultIntSize(int(i))
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(defaultIntSize.Get(sv), 10)
		},
	},
	// See https://www.postgresql.org/docs/10/runtime-config-client.html.
	// Supported only for pg compatibility - CockroachDB has no notion of
	// tablespaces.
	`default_tablespace`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			if s != "" {
				return newVarValueError(`default_tablespace`, s, "")
			}
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return ""
		},
		GlobalDefault: func(sv *settings.Values) string { return "" },
	},
	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-DEFAULT-TRANSACTION-ISOLATION
	`default_transaction_isolation`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			level, ok := tree.IsolationLevelMap[strings.ToLower(s)]
			if !ok || level == tree.UnspecifiedIsolation {
				var allowedValues = []string{"serializable", "read committed", "repeatable read"}
				return newVarValueError(`transaction_isolation`, s, allowedValues...)
			}
			m.SetDefaultTransactionIsolationLevel(level)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			level := tree.IsolationLevel(evalCtx.SessionData.DefaultTxnIsolationLevel)
			return strings.ToLower(level.String())
		},
		GlobalDefault: func(sv *settings.Values) string {
			level := tree.IsolationLevel(clusterTransactionIsolation.Get(sv)).String()
			return strings.ToLower(level)
		},
	},
	// See https://www.postgresql.org/docs/9.3/static/runtime-config-client.html#GUC-DEFAULT-TRANSACTION-READ-ONLY
	`default_transaction_read_only`: {
		GetStringVal: makePostgresBoolGetStringValFn("default_transaction_read_only"),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetDefaultReadOnly(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.DefaultReadOnly)
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`distsql`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			mode, ok := sessiondata.DistSQLExecModeFromString(s)
			if !ok {
				return newVarValueError(`distsql`, s, "on", "off", "auto", "always", "2.0-auto", "2.0-off")
			}
			m.SetDistSQLMode(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.DistSQLMode.String()
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondata.DistSQLExecMode(DistSQLClusterExecMode.Get(sv)).String()
		},
	},

	// KaiwuDB extension.
	`hash_scan_mode`: {
		GetStringVal: makeIntGetStringValFn(`hash_scan_mode`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set hash_scan_mode to a negative value: %d", b)
			}
			m.SetHashScanMode(int(b))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(int64(evalCtx.SessionData.HashScanMode), 10)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(hashScanMode.Get(sv), 10)
		},
	},

	// KaiwuDB extension.
	`enable_multimodel`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_multimodel`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetMultiModelEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.MultiModelEnabled)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(multiModelClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`enable_zigzag_join`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_zigzag_join`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetZigzagJoinEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.ZigzagJoinEnabled)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(zigzagJoinClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`reorder_joins_limit`: {
		GetStringVal: makeIntGetStringValFn(`reorder_joins_limit`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set reorder_joins_limit to a negative value: %d", b)
			}
			m.SetReorderJoinsLimit(int(b))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(int64(evalCtx.SessionData.ReorderJoinsLimit), 10)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(ReorderJoinsLimitClusterValue.Get(sv), 10)
		},
	},

	// KaiwuDB extension.
	`multi_model_reorder_joins_limit`: {
		GetStringVal: makeIntGetStringValFn(`multi_model_reorder_joins_limit`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set multi_model_reorder_joins_limit to a negative value: %d", b)
			}
			m.SetMultiModelReorderJoinsLimit(int(b))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(int64(evalCtx.SessionData.MultiModelReorderJoinsLimit), 10)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(MultiModelReorderJoinsLimitClusterValue.Get(sv), 10)
		},
	},

	// CockroachDB extension.
	`require_explicit_primary_keys`: {
		GetStringVal: makePostgresBoolGetStringValFn(`require_explicit_primary_keys`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetRequireExplicitPrimaryKeys(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.RequireExplicitPrimaryKeys)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(requireExplicitPrimaryKeysClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`vectorize`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			mode, ok := sessiondata.VectorizeExecModeFromString(s)
			if !ok {
				return newVarValueError(`vectorize`, s,
					"off", "auto", "on", "experimental_always")
			}
			m.SetVectorize(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.VectorizeMode.String()
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondata.VectorizeExecMode(
				VectorizeClusterMode.Get(sv)).String()
		},
	},

	// CockroachDB extension.
	`vectorize_row_count_threshold`: {
		GetStringVal: makeIntGetStringValFn(`vectorize_row_count_threshold`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set vectorize_row_count_threshold to a negative value: %d", b)
			}
			m.SetVectorizeRowCountThreshold(uint64(b))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(int64(evalCtx.SessionData.VectorizeRowCountThreshold), 10)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(VectorizeRowCountThresholdClusterValue.Get(sv), 10)
		},
	},

	// CockroachDB extension.
	// This is deprecated; the only allowable setting is "on".
	`optimizer`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			if strings.ToUpper(s) != "ON" {
				return newVarValueError(`optimizer`, s, "on")
			}
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return "on"
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "on"
		},
	},

	// CockroachDB extension.
	`optimizer_foreign_keys`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_foreign_keys`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetOptimizerFKs(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.OptimizerFKs)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(optDrivenFKClusterMode.Get(sv))
		},
	},

	`inside_out_row_ratio`: {
		GetStringVal: makeFloatGetStringValFn(`inside_out_row_ratio`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return err
			}
			if b > 1 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set inside_out_row_ratio to a negative value: %f, should be less then 1.0", b)
			}
			m.SetInsideOutRowRatio(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatFloat(evalCtx.SessionData.InsideOutRowRatio, 'f', -1, 64)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatFloat(TSInsideOutRowRatio.Get(sv), 'f', -1, 64)
		},
	},

	// CockroachDB extension.
	`enable_implicit_select_for_update`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_implicit_select_for_update`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetImplicitSelectForUpdate(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.ImplicitSelectForUpdate)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(implicitSelectForUpdateClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`enable_insert_fast_path`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_insert_fast_path`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetInsertFastPath(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.InsertFastPath)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(insertFastPathClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`experimental_serial_normalization`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			mode, ok := sessiondata.SerialNormalizationModeFromString(s)
			if !ok {
				return newVarValueError(`experimental_serial_normalization`, s,
					"rowid", "virtual_sequence", "sql_sequence")
			}
			m.SetSerialNormalizationMode(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.SerialNormalizationMode.String()
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondata.SerialNormalizationMode(
				SerialNormalizationMode.Get(sv)).String()
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html
	`extra_float_digits`: {
		GetStringVal: makeIntGetStringValFn(`extra_float_digits`),
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return wrapSetVarError("extra_float_digits", s, "%v", err)
			}
			// Note: this is the range allowed by PostgreSQL.
			// See also the documentation around (DataConversionConfig).GetFloatPrec()
			// in session_data.go.
			if i < -15 || i > 3 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					`%d is outside the valid range for parameter "extra_float_digits" (-15 .. 3)`, i)
			}
			m.SetExtraFloatDigits(int(i))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return fmt.Sprintf("%d", evalCtx.SessionData.DataConversion.ExtraFloatDigits)
		},
		GlobalDefault: func(sv *settings.Values) string { return "0" },
	},
	// CockroachDB extension. See docs on SessionData.ForceSavepointRestart.
	// https://gitee.com/kwbasedb/kwbase/issues/30588
	`force_savepoint_restart`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.ForceSavepointRestart)
		},
		GetStringVal: makePostgresBoolGetStringValFn("force_savepoint_restart"),
		Set: func(_ context.Context, m *sessionDataMutator, val string) error {
			b, err := parsePostgresBool(val)
			if err != nil {
				return err
			}
			if b {
				telemetry.Inc(sqltelemetry.ForceSavepointRestartCounter)
			}
			m.SetForceSavepointRestart(b)
			return nil
		},
		GlobalDefault: globalFalse,
	},
	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html
	`integer_datetimes`: makeReadOnlyVar("on"),

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-INTERVALSTYLE
	`intervalstyle`: makeCompatStringVar(`IntervalStyle`, "postgres"),

	// CockroachDB extension.
	`locality`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.Locality.String()
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-LOC-TIMEOUT
	`lock_timeout`: makeCompatIntVar(`lock_timeout`, 0),

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-IDLE-IN-TRANSACTION-SESSION-TIMEOUT
	// See also issue #5924.
	`idle_in_transaction_session_timeout`: makeCompatIntVar(`idle_in_transaction_session_timeout`, 0),

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
	`max_identifier_length`: {
		Get: func(evalCtx *extendedEvalContext) string { return "128" },
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-MAX-INDEX-KEYS
	`max_index_keys`: makeReadOnlyVar("32"),

	// CockroachDB extension.
	`node_id`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return fmt.Sprintf("%d", evalCtx.NodeID)
		},
	},

	// CockroachDB extension.
	// TODO(dan): This should also work with SET.
	`results_buffer_size`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(evalCtx.SessionData.ResultsBufferSize, 10)
		},
	},

	// CockroachDB extension (inspired by MySQL).
	// See https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_sql_safe_updates
	`sql_safe_updates`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.SafeUpdates)
		},
		GetStringVal: makePostgresBoolGetStringValFn("sql_safe_updates"),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetSafeUpdates(b)
			return nil
		},
		GlobalDefault: globalFalse,
	},

	// See https://www.postgresql.org/docs/10/static/ddl-schemas.html#DDL-SCHEMAS-PATH
	// https://www.postgresql.org/docs/9.6/static/runtime-config-client.html
	`search_path`: {
		GetStringVal: func(
			ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) (string, error) {
			comma := ""
			var buf bytes.Buffer
			dbName := evalCtx.SessionData.Database
			dbDesc, err := evalCtx.schemaAccessors.logical.GetDatabaseDesc(ctx, evalCtx.Txn,
				dbName, tree.DatabaseLookupFlags{Required: true})
			if err != nil {
				return "", err
			}
			for _, v := range values {
				s, err := datumAsString(&evalCtx.EvalContext, "search_path", v)
				if err != nil {
					return "", err
				}
				if strings.Contains(s, ",") {
					// TODO(knz): if/when we want to support this, we'll need to change
					// the interface between GetStringVal() and Set() to take string
					// arrays instead of a single string.
					return "", unimplemented.Newf("schema names containing commas in search_path",
						"schema name %q not supported in search_path", s)
				}
				found, _, err := evalCtx.schemaAccessors.logical.GetSchema(ctx, evalCtx.Txn, dbDesc.ID, s)
				if err != nil {
					return "", err
				}
				if !found {
					return "", sqlbase.NewUndefinedSchemaError(s)
				}
				buf.WriteString(comma)
				buf.WriteString(s)
				comma = ","
			}
			return buf.String(), nil
		},
		Set: func(ctx context.Context, m *sessionDataMutator, s string) error {
			paths := strings.Split(s, ",")
			m.UpdateSearchPath(paths)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.SearchPath.String()
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sqlbase.DefaultSearchPath.String()
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-SERVER-VERSION
	`server_version`: makeReadOnlyVar(PgServerVersion),

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-SERVER-VERSION-NUM
	`server_version_num`: makeReadOnlyVar(PgServerVersionNum),

	// See https://www.postgresql.org/docs/9.4/runtime-config-connection.html
	`ssl_renegotiation_limit`: {
		Hidden:        true,
		GetStringVal:  makeIntGetStringValFn(`ssl_renegotiation_limit`),
		Get:           func(_ *extendedEvalContext) string { return "0" },
		GlobalDefault: func(_ *settings.Values) string { return "0" },
		Set: func(_ context.Context, _ *sessionDataMutator, s string) error {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return wrapSetVarError("ssl_renegotiation_limit", s, "%v", err)
			}
			if i != 0 {
				// See pg src/backend/utils/misc/guc.c: non-zero values are not to be supported.
				return newVarValueError("ssl_renegotiation_limit", s, "0")
			}
			return nil
		},
	},

	// CockroachDB extension.
	`kwdb_version`: makeReadOnlyVar(build.GetInfo().Short()),

	// CockroachDB extension
	`session_id`: {
		Get: func(evalCtx *extendedEvalContext) string { return evalCtx.SessionID.String() },
	},

	// CockroachDB extension.
	// In PG this is a pseudo-function used with SELECT, not SHOW.
	// See https://www.postgresql.org/docs/10/static/functions-info.html
	`session_user`: {
		Get: func(evalCtx *extendedEvalContext) string { return evalCtx.SessionData.User },
	},

	// See pg sources src/backend/utils/misc/guc.c. The variable is defined
	// but is hidden from SHOW ALL.
	`session_authorization`: {
		Hidden: true,
		Get:    func(evalCtx *extendedEvalContext) string { return evalCtx.SessionData.User },
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/runtime-config-compatible.html#GUC-STANDARD-CONFORMING-STRINGS
	`standard_conforming_strings`: makeCompatBoolVar(`standard_conforming_strings`, true, false /* anyAllowed */),

	// See https://www.postgresql.org/docs/10/static/runtime-config-compatible.html#GUC-SYNCHRONIZE-SEQSCANS
	// The default in pg is "on" but the behavior in CockroachDB is "off". As this does not affect
	// results received by clients, we accept both values.
	`synchronize_seqscans`: makeCompatBoolVar(`synchronize_seqscans`, true, true /* anyAllowed */),

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-ROW-SECURITY
	// The default in pg is "on" but row security is not supported in CockroachDB.
	// We blindly accept both values because as long as there are now row security policies defined,
	// either value produces the same query results in PostgreSQL. That is, as long as CockroachDB
	// does not support row security, accepting either "on" and "off" but ignoring the result
	// is postgres-compatible.
	// If/when CockroachDB is extended to support row security, the default and allowed values
	// should be modified accordingly.
	`row_security`: makeCompatBoolVar(`row_security`, false, true /* anyAllowed */),

	`statement_timeout`: {
		GetStringVal: makeTimeoutVarGetter(`statement_timeout`),
		Set:          stmtTimeoutVarSet,
		Get: func(evalCtx *extendedEvalContext) string {
			ms := evalCtx.SessionData.StmtTimeout.Nanoseconds() / int64(time.Millisecond)
			return strconv.FormatInt(ms, 10)
		},
		GlobalDefault: func(sv *settings.Values) string { return "0" },
	},

	`idle_in_session_timeout`: {
		GetStringVal: makeTimeoutVarGetter(`idle_in_session_timeout`),
		Set:          idleInSessionTimeoutVarSet,
		Get: func(evalCtx *extendedEvalContext) string {
			ms := evalCtx.SessionData.IdleInSessionTimeout.Nanoseconds() / int64(time.Millisecond)
			return strconv.FormatInt(ms, 10)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return clusterIdleInSessionTimeout.String(sv)
		},
	},

	`tsinsert_direct`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.TsInsertShortcircuit)
		},
		GetStringVal: makePostgresBoolGetStringValFn("tsinsert_direct"),
		Set:          tsinsertshortcircuitSet,
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(insertShortCircuitClusterMode.Get(sv))
		},
	},
	// tsinsert_direct special handling error, needs to be used with the tsinsert_direct switch turned on
	`ts_ignore_batcherror`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.TsSupportBatch)
		},
		GetStringVal: makePostgresBoolGetStringValFn("ts_ignore_batcherror"),
		Set:          tssupportbatch,
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-TIMEZONE
	`timezone`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return sessionDataTimeZoneFormat(evalCtx.SessionData.DataConversion.Location)
		},
		GetStringVal:  timeZoneVarGetStringVal,
		Set:           timeZoneVarSet,
		GlobalDefault: func(_ *settings.Values) string { return "UTC" },
	},

	// This is not directly documented in PG's docs but does indeed behave this way.
	// See https://github.com/postgres/postgres/blob/REL_10_STABLE/src/backend/utils/misc/guc.c#L3401-L3409
	`transaction_isolation`: {
		Get: func(evalCtx *extendedEvalContext) string {
			level := tree.IsolationLevel(evalCtx.Txn.IsoLevel()).String()
			return strings.ToLower(level)
		},
		RuntimeSet: func(_ context.Context, evalCtx *extendedEvalContext, s string) error {
			level, ok := tree.IsolationLevelMap[strings.ToLower(s)]
			if !ok || level == tree.UnspecifiedIsolation {
				var allowedValues = []string{"serializable", "read committed", "repeatable read"}
				return newVarValueError(`transaction_isolation`, s, allowedValues...)
			}
			modes := tree.TransactionModes{
				Isolation: level,
			}
			if err := evalCtx.TxnModesSetter.setTransactionModes(modes, hlc.Timestamp{} /* asOfSystemTime */); err != nil {
				return err
			}
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			level := tree.IsolationLevel(clusterTransactionIsolation.Get(sv)).String()
			return strings.ToLower(level)
		},
	},

	// CockroachDB extension.
	`transaction_priority`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.Txn.UserPriority().String()
		},
	},

	// CockroachDB extension.
	`transaction_status`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.TxnState
		},
	},

	// See https://www.postgresql.org/docs/10/static/hot-standby.html#HOT-STANDBY-USERS
	`transaction_read_only`: {
		GetStringVal: makePostgresBoolGetStringValFn("transaction_read_only"),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetReadOnly(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.TxnReadOnly)
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`tracing`: {
		Get: func(evalCtx *extendedEvalContext) string {
			sessTracing := evalCtx.Tracing
			if sessTracing.Enabled() {
				val := "on"
				if sessTracing.RecordingType() == tracing.SingleNodeRecording {
					val += ", local"
				}
				if sessTracing.KVTracingEnabled() {
					val += ", kv"
				}
				return val
			}
			return "off"
		},
		// Setting is done by the SetTracing statement.
	},

	// CockroachDB extension.
	`allow_prepare_as_opt_plan`: {
		Hidden: true,
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.AllowPrepareAsOptPlan)
		},
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetAllowPrepareAsOptPlan(b)
			return nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`save_tables_prefix`: {
		Hidden: true,
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.SaveTablesPrefix
		},
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			m.SetSaveTablesPrefix(s)
			return nil
		},
		GlobalDefault: func(_ *settings.Values) string { return "" },
	},

	// CockroachDB extension.
	`experimental_enable_temp_tables`: {
		GetStringVal: makePostgresBoolGetStringValFn(`experimental_enable_temp_tables`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetTempTablesEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.TempTablesEnabled)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(temporaryTablesEnabledClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`experimental_enable_hash_sharded_indexes`: {
		GetStringVal: makePostgresBoolGetStringValFn(`experimental_enable_hash_sharded_indexes`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetHashShardedIndexesEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.HashShardedIndexesEnabled)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(hashShardedIndexesEnabledClusterMode.Get(sv))
		},
	},

	`max_push_limit_number`: {
		Hidden:       true,
		GetStringVal: makeIntGetStringValFn(`max_push_limit_number`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}

			m.SetMaxPushLimitNumber(i)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatInt(evalCtx.SessionData.MaxPushLimitNumber, 10)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "1000"
		},
	},

	`can_push_sorter`: {
		Hidden:       true,
		GetStringVal: makePostgresBoolGetStringValFn(`can_push_sorter`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetCanPushSorter(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.CanPushSorter)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "off"
		},
	},
	`need_control_inside_out`: {
		GetStringVal: makePostgresBoolGetStringValFn(`need_control_inside_out`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetNeedControlIndideOut(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.NeedControlIndideOut)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "off"
		},
	},
}

const compatErrMsg = "this parameter is currently recognized only for compatibility and has no effect in CockroachDB."

func init() {
	// Initialize delegate.ValidVars.
	for v := range varGen {
		delegate.ValidVars[v] = struct{}{}
	}
}

// makePostgresBoolGetStringValFn returns a function that evaluates and returns
// a string representation of the first argument value.
func makePostgresBoolGetStringValFn(varName string) getStringValFn {
	return func(
		ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
	) (string, error) {
		if len(values) != 1 {
			return "", newSingleArgVarError(varName)
		}
		val, err := values[0].Eval(&evalCtx.EvalContext)
		if err != nil {
			return "", err
		}
		if s, ok := val.(*tree.DString); ok {
			return string(*s), nil
		}
		s, err := getSingleBool(varName, val)
		if err != nil {
			return "", err
		}
		return strconv.FormatBool(bool(*s)), nil
	}
}

func makeReadOnlyVar(value string) sessionVar {
	return sessionVar{
		Get:           func(_ *extendedEvalContext) string { return value },
		GlobalDefault: func(_ *settings.Values) string { return value },
	}
}

func displayPgBool(val bool) func(_ *settings.Values) string {
	strVal := formatBoolAsPostgresSetting(val)
	return func(_ *settings.Values) string { return strVal }
}

var globalFalse = displayPgBool(false)

// sessionDataTimeZoneFormat returns the appropriate timezone format
// to output when the `timezone` is required output.
// If the time zone is a "fixed offset" one, initialized from an offset
// and not a standard name, then we use a magic format in the Location's
// name. We attempt to parse that here and retrieve the original offset
// specified by the user.
func sessionDataTimeZoneFormat(loc *time.Location) string {
	locStr := loc.String()
	_, origRepr, parsed := timeutil.ParseFixedOffsetTimeZone(locStr)
	if parsed {
		return origRepr
	}
	return locStr
}

func makeCompatBoolVar(varName string, displayValue, anyValAllowed bool) sessionVar {
	displayValStr := formatBoolAsPostgresSetting(displayValue)
	return sessionVar{
		Get: func(_ *extendedEvalContext) string { return displayValStr },
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
			}
			if anyValAllowed || b == displayValue {
				return nil
			}
			telemetry.Inc(sqltelemetry.UnimplementedSessionVarValueCounter(varName, s))
			allowedVals := []string{displayValStr}
			if anyValAllowed {
				allowedVals = append(allowedVals, formatBoolAsPostgresSetting(!displayValue))
			}
			err = newVarValueError(varName, s, allowedVals...)
			err = errors.WithDetail(err, compatErrMsg)
			return err
		},
		GlobalDefault: func(sv *settings.Values) string { return displayValStr },
		GetStringVal:  makePostgresBoolGetStringValFn(varName),
	}
}

func makeCompatIntVar(varName string, displayValue int, extraAllowed ...int) sessionVar {
	displayValueStr := strconv.Itoa(displayValue)
	extraAllowedStr := make([]string, len(extraAllowed))
	for i, v := range extraAllowed {
		extraAllowedStr[i] = strconv.Itoa(v)
	}
	varObj := makeCompatStringVar(varName, displayValueStr, extraAllowedStr...)
	varObj.GetStringVal = makeIntGetStringValFn(varName)
	return varObj
}

func makeCompatStringVar(varName, displayValue string, extraAllowed ...string) sessionVar {
	allowedVals := append(extraAllowed, strings.ToLower(displayValue))
	return sessionVar{
		Get: func(_ *extendedEvalContext) string {
			return displayValue
		},
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			enc := strings.ToLower(s)
			for _, a := range allowedVals {
				if enc == a {
					return nil
				}
			}
			telemetry.Inc(sqltelemetry.UnimplementedSessionVarValueCounter(varName, s))
			err := newVarValueError(varName, s, allowedVals...)
			err = errors.WithDetail(err, compatErrMsg)
			return err
		},
		GlobalDefault: func(sv *settings.Values) string { return displayValue },
	}
}

// makeIntGetStringValFn returns a getStringValFn which allows
// the user to provide plain integer values to a SET variable.
func makeIntGetStringValFn(name string) getStringValFn {
	return func(ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr) (string, error) {
		s, err := getIntVal(&evalCtx.EvalContext, name, values)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(s, 10), nil
	}
}

// makeFloatGetStringValFn returns a getStringValFn which allows
// the user to provide plain float values to a SET variable.
func makeFloatGetStringValFn(name string) getStringValFn {
	return func(ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr) (string, error) {
		s, err := getFloatVal(&evalCtx.EvalContext, name, values)
		if err != nil {
			return "", err
		}
		return strconv.FormatFloat(s, 'f', -1, 64), nil
	}
}

// IsSessionVariableConfigurable returns true iff there is a session
// variable with the given name and it is settable by a client
// (e.g. in pgwire).
func IsSessionVariableConfigurable(varName string) (exists, configurable bool) {
	v, exists := varGen[varName]
	return exists, v.Set != nil
}

var varNames = func() []string {
	res := make([]string, 0, len(varGen))
	for vName := range varGen {
		res = append(res, vName)
	}
	sort.Strings(res)
	return res
}()

// getSingleBool returns the boolean if the input Datum is a DBool,
// and returns a detailed error message if not.
func getSingleBool(name string, val tree.Datum) (*tree.DBool, error) {
	b, ok := val.(*tree.DBool)
	if !ok {
		err := pgerror.Newf(pgcode.InvalidParameterValue,
			"parameter %q requires a Boolean value", name)
		err = errors.WithDetailf(err,
			"%s is a %s", val, errors.Safe(val.ResolvedType()))
		return nil, err
	}
	return b, nil
}

func getSessionVar(name string, missingOk bool) (bool, sessionVar, error) {
	if _, ok := UnsupportedVars[name]; ok {
		return false, sessionVar{}, unimplemented.Newf("set."+name,
			"the configuration setting %q is not supported", name)
	}

	v, ok := varGen[name]
	if !ok {
		if missingOk {
			return false, sessionVar{}, nil
		}
		return false, sessionVar{}, pgerror.Newf(pgcode.UndefinedObject,
			"unrecognized configuration parameter %q", name)
	}

	return true, v, nil
}

// GetSessionVar implements the EvalSessionAccessor interface.
func (p *planner) GetSessionVar(
	_ context.Context, varName string, missingOk bool,
) (bool, string, error) {
	name := strings.ToLower(varName)
	ok, v, err := getSessionVar(name, missingOk)
	if err != nil || !ok {
		return ok, "", err
	}

	return true, v.Get(&p.extendedEvalCtx), nil
}

// GetUserDefinedVar implements the EvalSessionAccessor interface.
func (p *planner) GetUserDefinedVar(
	_ context.Context, varName string, missingOk bool,
) (bool, interface{}, error) {
	name := strings.ToLower(varName)
	if p.extendedEvalCtx.SessionData.UserDefinedVars[varName] == nil {
		return false, nil, nil
	}
	return true, p.extendedEvalCtx.SessionData.UserDefinedVars[name], nil
}

// SetSessionVar implements the EvalSessionAccessor interface.
func (p *planner) SetSessionVar(ctx context.Context, varName, newVal string) error {
	name := strings.ToLower(varName)
	_, v, err := getSessionVar(name, false /* missingOk */)
	if err != nil {
		return err
	}

	if v.Set == nil && v.RuntimeSet == nil {
		return newCannotChangeParameterError(name)
	}
	if v.RuntimeSet != nil {
		return v.RuntimeSet(ctx, &p.extendedEvalCtx, newVal)
	}
	return v.Set(ctx, p.sessionDataMutator, newVal)
}
