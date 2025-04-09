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
	"math"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	//mgr "gitee.com/kwbasedb/kwbase/pkg/replicationmgr"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/stats"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

const rangemb = 1024 * 1024

// setClusterSettingNode represents a SET CLUSTER SETTING statement.
type setClusterSettingNode struct {
	name    string
	st      *cluster.Settings
	setting settings.WritableSetting
	// If value is nil, the setting should be reset.
	value tree.TypedExpr
}

// SetClusterSetting sets session variables.
// Privileges: super user.
func (p *planner) SetClusterSetting(
	ctx context.Context, n *tree.SetClusterSetting,
) (planNode, error) {
	if err := p.RequireAdminRole(ctx, "SET CLUSTER SETTING"); err != nil {
		return nil, err
	}

	name := strings.ToLower(n.Name)
	st := p.EvalContext().Settings
	v, ok := settings.Lookup(name, settings.LookupForLocalAccess)
	if !ok {
		return nil, errors.Errorf("unknown cluster setting '%s'", name)
	}

	setting, ok := v.(settings.WritableSetting)
	if !ok {
		return nil, errors.AssertionFailedf("expected writable setting, got %T", v)
	}

	var value tree.TypedExpr
	if n.Value != nil {
		// For DEFAULT, let the value reference be nil. That's a RESET in disguise.
		if _, ok := n.Value.(tree.DefaultVal); !ok {
			expr := n.Value
			expr = unresolvedNameToStrVal(expr)

			var requiredType *types.T
			switch setting.(type) {
			case *settings.StringSetting, *settings.StateMachineSetting, *settings.ByteSizeSetting:
				requiredType = types.String
			case *settings.BoolSetting:
				requiredType = types.Bool
			case *settings.IntSetting:
				requiredType = types.Int
			case *settings.FloatSetting:
				requiredType = types.Float
			case *settings.EnumSetting:
				requiredType = types.Any
			case *settings.DurationSetting:
				requiredType = types.Interval
			default:
				return nil, errors.Errorf("unsupported setting type %T", setting)
			}

			var dummyHelper tree.IndexedVarHelper
			typed, err := p.analyzeExpr(
				ctx, expr, nil, dummyHelper, requiredType, true, "SET CLUSTER SETTING "+name)
			if err != nil {
				return nil, err
			}

			value = typed
		} else if _, isStateMachineSetting := setting.(*settings.StateMachineSetting); isStateMachineSetting {
			return nil, errors.New("cannot RESET this cluster setting")
		}
	}

	return &setClusterSettingNode{name: name, st: st, setting: setting, value: value}, nil
}

// CheckOperation method for checking cluster settings
type CheckOperation func(string) error

func checkTsDedupRule(encodedValue string) error {
	if encodedValue != "merge" && encodedValue != "keep" && encodedValue != "reject" &&
		encodedValue != "discard" && encodedValue != "override" {
		return errors.New("ts.dedup.rule setting value is not right")
	}
	return nil
}

func checkTsMountLimit(encodedValue string) error {
	value, err := strconv.ParseInt(encodedValue, 10, 64)
	if err != nil {
		return err
	}
	if value < 0 || value > math.MaxInt32 {
		return errors.New("invalid value, the range of ts.mount.max_limit is [0, 2147483647]")
	}
	return nil
}

func checkTsCachedPartitionsLimit(encodedValue string) error {
	value, err := strconv.ParseInt(encodedValue, 10, 64)
	if err != nil {
		return err
	}
	if value < 0 || value > math.MaxInt32 {
		return errors.New("invalid value, the range of ts.cached_partitions_per_subgroup.max_limit is [0, 2147483647]")
	}
	return nil
}

func checkTsEntitiesPerSubgroupLimit(encodedValue string) error {
	value, err := strconv.ParseInt(encodedValue, 10, 64)
	if err != nil {
		return err
	}
	if value < 1 || value > math.MaxInt32 {
		return errors.New("invalid value, the range of ts.entities_per_subgroup.max_limit is [1, 2147483647]")
	}
	return nil
}

func checkTsBlocksPerSegmentLimit(encodedValue string) error {
	value, err := strconv.ParseInt(encodedValue, 10, 64)
	if err != nil {
		return err
	}
	if value < 1 || value > 1000000 {
		return errors.New("invalid value, the range of ts.blocks_per_segment.max_limit is [1, 1000000]")
	}
	return nil
}

func checkTsRowsPerBlockLimit(encodedValue string) error {
	value, err := strconv.ParseInt(encodedValue, 10, 64)
	if err != nil {
		return err
	}
	if value < 10 || value > 1000 {
		return errors.New("invalid value, the range of ts.rows_per_block.max_limit is [10, 1000]")
	}
	return nil
}

func checkTsCompressType(encodedValue string) error {
	if encodedValue != "gzip" && encodedValue != "lz4" && encodedValue != "lzma" &&
		encodedValue != "lzo" && encodedValue != "xz" && encodedValue != "zstd" {
		return errors.New("ts.compression.type is incorrectly configured, and can be configured as: gzip, lz4, lzma, lzo, xz, and zstd")
	}
	return nil
}

func checkTsCompressLevel(encodedValue string) error {
	if encodedValue != "low" && encodedValue != "middle" && encodedValue != "high" {
		return errors.New("ts.compression.level is incorrectly configured, and can be configured as: low, middle, high")
	}
	return nil
}

func checkTsCompressThreads(encodedValue string) error {
	value, err := strconv.ParseInt(encodedValue, 10, 64)
	if err != nil {
		return err
	}
	if value < 1 || value > 1000 {
		return errors.New("invalid value, the range of compression threads should be [1, 1000]")
	}
	return nil
}

// checkTsQueryOptMode checks whether the mode value set is valid
func checkTsQueryOptMode(encodedValue string) error {
	value, err := strconv.ParseInt(encodedValue, 2, 64)
	if err != nil || value < 0 || value > 63 {
		return pgerror.Newf(pgcode.InvalidParameterValue, "invalid value for ts.sql.query_opt_mode")
	}
	return nil
}

func checkTsCountUseStatistics(encodedValue string) error {
	_, err := strconv.ParseBool(encodedValue)
	if err != nil {
		return err
	}
	return nil
}

// checkCapacityStatsPeriod checks whether the capacity stats period value set is valid
func checkCapacityStatsPeriod(encodedValue string) error {
	value, err := strconv.ParseInt(encodedValue, 10, 64)
	if err != nil {
		return err
	}
	if value < 1 || value > 10000 {
		return errors.New("invalid value, the range of capacity stats period should be [1, 10000]")
	}
	return nil
}

// checkTsTableCacheCapacity checks whether the table cache capacity value set is valid
func checkTsTableCacheCapacity(encodedValue string) error {
	value, err := strconv.ParseInt(encodedValue, 10, 64)
	if err != nil {
		return err
	}
	if value < 1 || value > math.MaxInt32 {
		return errors.New("invalid value, the range of ts.table_cache.capacity is [1, 2147483647]")
	}
	return nil
}

// CheckClusterSetting map of checking methods for saving cluster settings
var CheckClusterSetting = map[string]CheckOperation{
	"ts.dedup.rule":      checkTsDedupRule,
	"ts.mount.max_limit": checkTsMountLimit,
	"ts.cached_partitions_per_subgroup.max_limit": checkTsCachedPartitionsLimit,
	"ts.entities_per_subgroup.max_limit":          checkTsEntitiesPerSubgroupLimit,
	"ts.blocks_per_segment.max_limit":             checkTsBlocksPerSegmentLimit,
	"ts.rows_per_block.max_limit":                 checkTsRowsPerBlockLimit,
	"ts.compression.type":                         checkTsCompressType,
	"ts.compression.level":                        checkTsCompressLevel,
	"immediate_compression.threads":               checkTsCompressThreads,
	"ts.sql.query_opt_mode":                       checkTsQueryOptMode,
	"ts.count.use_statistics.enabled":             checkTsCountUseStatistics,
	"capacity.stats.period":                       checkCapacityStatsPeriod,
	"ts.table_cache.capacity":                     checkTsTableCacheCapacity,
}

func (n *setClusterSettingNode) startExec(params runParams) error {
	//if n.name == mgr.ClusterSettingReplicaRole &&
	//	!strings.Contains(params.SessionData().ApplicationName, mgr.SettingReplicaRoleOpName) {
	//	return errors.Errorf("CLUSTER SETTING :\"%s\" cannot be set manually", mgr.ClusterSettingReplicaRole)
	//}
	if !params.p.ExtendedEvalContext().TxnImplicit {
		return errors.Errorf("SET CLUSTER SETTING cannot be used inside a transaction")
	}
	execCfg := params.extendedEvalCtx.ExecCfg
	var expectedEncodedValue string
	if err := execCfg.DB.Txn(params.ctx, func(ctx context.Context, txn *kv.Txn) error {
		var reportedValue string
		if n.value == nil {
			reportedValue = "DEFAULT"
			expectedEncodedValue = n.setting.EncodedDefault()
			if _, err := execCfg.InternalExecutor.ExecEx(
				ctx, "reset-setting", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				"DELETE FROM system.settings WHERE name = $1", n.name,
			); err != nil {
				return err
			}
		} else {
			value, err := n.value.Eval(params.p.EvalContext())
			if err != nil {
				return err
			}
			reportedValue = tree.AsStringWithFlags(value, tree.FmtBareStrings)
			var prev tree.Datum
			if _, ok := n.setting.(*settings.StateMachineSetting); ok {
				datums, err := execCfg.InternalExecutor.QueryRowEx(
					ctx, "retrieve-prev-setting", txn,
					sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
					"SELECT value FROM system.settings WHERE name = $1", n.name,
				)
				if err != nil {
					return err
				}
				if len(datums) == 0 {
					// There is a SQL migration which adds this value. If it
					// hasn't run yet, we can't update the version as we don't
					// have good enough information about the current cluster
					// version.
					return errors.New("no persisted cluster version found, please retry later")
				}
				prev = datums[0]
			}
			encoded, err := toSettingString(ctx, n.st, n.name, n.setting, value, prev)
			expectedEncodedValue = encoded
			if err != nil {
				return err
			}
			if function, ok := CheckClusterSetting[n.name]; ok {
				if err = function(encoded); err != nil {
					return err
				}
			}
			if n.name == "ts.raftlog_combine_wal.enabled" {
				if encoded == "true" {
					if tse.TsRaftLogCombineWAL.Get(&n.st.SV) {
						return nil
					}
					if params.ExecCfg().StartMode != StartMultiReplica {
						return errors.New("set ts.raftlog_combine_wal.enabled to true is not supported in MPP mode")
					}
					if sqlbase.CheckWhetherHasTsTable(params.ctx, txn) {
						return errors.New("cluster has time-series table, cannot set ts.raftlog_combine_wal.enabled to true")
					}
					deRule := tse.DeDuplicateRule.Get(&n.st.SV)
					if deRule != "merge" && deRule != "override" {
						return errors.New("should set ts.dedup.rule to merge or override before set ts.raftlog_combine_wal.enabled to true")
					}
				} else {
					if !tse.TsRaftLogCombineWAL.Get(&n.st.SV) {
						return nil
					}
				}
			}
			if _, err = execCfg.InternalExecutor.ExecEx(
				ctx, "update-setting", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				`UPSERT INTO system.settings (name, value, "lastUpdated", "valueType") VALUES ($1, $2, now(), $3)`,
				n.name, encoded, n.setting.Typ(),
			); err != nil {
				return err
			}
		}

		// Report tracked cluster settings via telemetry.
		// TODO(justin): implement a more general mechanism for tracking these.
		switch n.name {
		case stats.AutoStatsClusterSettingName:
			switch expectedEncodedValue {
			case "true":
				telemetry.Inc(sqltelemetry.TurnAutoStatsOnUseCounter)
			case "false":
				telemetry.Inc(sqltelemetry.TurnAutoStatsOffUseCounter)
			}
		case ConnAuditingClusterSettingName:
			switch expectedEncodedValue {
			case "true":
				telemetry.Inc(sqltelemetry.TurnConnAuditingOnUseCounter)
			case "false":
				telemetry.Inc(sqltelemetry.TurnConnAuditingOffUseCounter)
			}
		case AuthAuditingClusterSettingName:
			switch expectedEncodedValue {
			case "true":
				telemetry.Inc(sqltelemetry.TurnAuthAuditingOnUseCounter)
			case "false":
				telemetry.Inc(sqltelemetry.TurnAuthAuditingOffUseCounter)
			}
		case ReorderJoinsLimitClusterSettingName:
			val, err := strconv.ParseInt(expectedEncodedValue, 10, 64)
			if err != nil {
				break
			}
			sqltelemetry.ReportJoinReorderLimit(int(val))
		case MultiModelReorderJoinsLimitClusterSettingName:
			val, err := strconv.ParseInt(expectedEncodedValue, 10, 64)
			if err != nil {
				break
			}
			sqltelemetry.ReportMultiModelJoinReorderLimit(int(val))
		case VectorizeClusterSettingName:
			val, err := strconv.Atoi(expectedEncodedValue)
			if err != nil {
				break
			}
			validatedExecMode, isValid := sessiondata.VectorizeExecModeFromString(sessiondata.VectorizeExecMode(val).String())
			if !isValid {
				break
			}
			telemetry.Inc(sqltelemetry.VecModeCounter(validatedExecMode.String()))
		}

		params.p.SetAuditTarget(0, n.name, nil)
		return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
			ctx,
			txn,
			EventLogSetClusterSetting,
			0, /* no target */
			int32(params.extendedEvalCtx.NodeID),
			EventLogSetClusterSettingDetail{n.name, reportedValue, params.SessionData().User},
		)
	}); err != nil {
		return err
	}

	if _, ok := n.setting.(*settings.StateMachineSetting); ok && n.value == nil {
		// The "version" setting doesn't have a well defined "default" since it is
		// set in a startup migration.
		return nil
	}
	errNotReady := errors.New("setting updated but timed out waiting to read new value")
	var observed string
	err := retry.ForDuration(10*time.Second, func() error {
		observed = n.setting.Encoded(&execCfg.Settings.SV)
		if observed != expectedEncodedValue {
			return errNotReady
		}
		return nil
	})
	if err != nil {
		log.Warningf(
			params.ctx, "SET CLUSTER SETTING %q timed out waiting for value %q, observed %q",
			n.name, expectedEncodedValue, observed,
		)
	}

	if n.name == "sql.add_white_list" {
		if n.value == nil {
			return errors.Errorf("unsupport reset add_white_list")
		}
		var whiteList sqlbase.WhiteList
		values := strings.Split(expectedEncodedValue, "$")
		if len(values) != 6 {
			return errors.Errorf("wrong number of parameters")
		}

		whiteList.Name = values[0]
		argNum, err := strconv.ParseInt(values[1], 10, 64)
		if err != nil {
			return err
		}
		whiteList.ArgNum = uint32(argNum)
		sArgTypes := strings.Split(values[2], ",")
		argTypes := make([]uint32, 0)
		for _, s := range sArgTypes {
			argType, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			argTypes = append(argTypes, uint32(argType))
		}
		whiteList.ArgType = argTypes
		position, err := strconv.ParseInt(values[3], 10, 64)
		if err != nil {
			return err
		}
		whiteList.Position = uint32(position)
		enabled, err := strconv.ParseBool(values[4])
		if err != nil {
			return err
		}
		whiteList.Enabled = enabled

		argOpt, err := strconv.ParseInt(values[5], 10, 64)
		if err != nil {
			return err
		}
		whiteList.ArgOpt = uint32(argOpt)

		boWhiteLists := []sqlbase.WhiteList{whiteList}

		err = WriteWhiteListDesc(params.ctx, params.p.txn, execCfg.InternalExecutor, params.p.EvalContext().NodeID, boWhiteLists, true)
		if err != nil {
			return err
		}

		err = UpdateWhiteListMap(params.ctx, params.p.txn, execCfg.InternalExecutor, params.ExecCfg().TSWhiteListMap)
		if err != nil {
			return err
		}
	}

	if n.name == "sql.delete_white_list" {
		if n.value == nil {
			return errors.Errorf("unsupport reset delete_white_list")
		}
		values := strings.Split(expectedEncodedValue, "$")
		if len(values) != 6 {
			return errors.Errorf("wrong number of parameters")
		}
		name := values[0]
		const deleteStmt = `delete from system.bo_black_list where name = $1 and `
		var buffer bytes.Buffer
		buffer.WriteString(deleteStmt)
		buffer.WriteString(` arg_type=array[`)
		buffer.WriteString(values[2])
		buffer.WriteString(` ]`)
		_, err := execCfg.InternalExecutor.Exec(
			params.ctx, `delete bo_black_list`, params.p.txn, buffer.String(), name)
		if err != nil {
			return err
		}
		err = UpdateWhiteListMap(params.ctx, params.p.txn, execCfg.InternalExecutor, params.ExecCfg().TSWhiteListMap)
		if err != nil {
			return err
		}
	}

	return err
}

func (n *setClusterSettingNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *setClusterSettingNode) Values() tree.Datums            { return nil }
func (n *setClusterSettingNode) Close(_ context.Context)        {}

// toSettingString takes in a datum that's supposed to become the value for a
// Setting and validates it, returning the string representation of the new
// value as it needs to be inserted into the system.settings table.
//
// Args:
// prev: Only specified if the setting is a StateMachineSetting. Represents the
//
//	current value of the setting, read from the system.settings table.
func toSettingString(
	ctx context.Context, st *cluster.Settings, name string, s settings.Setting, d, prev tree.Datum,
) (string, error) {
	switch setting := s.(type) {
	case *settings.StringSetting:
		if s, ok := d.(*tree.DString); ok {
			if err := setting.Validate(&st.SV, string(*s)); err != nil {
				return "", err
			}
			return string(*s), nil
		}
		return "", errors.Errorf("cannot use %s %T value for string setting", d.ResolvedType(), d)
	case *settings.StateMachineSetting:
		if s, ok := d.(*tree.DString); ok {
			dStr, ok := prev.(*tree.DString)
			if !ok {
				return "", errors.New("the existing value is not a string")
			}
			prevRawVal := []byte(string(*dStr))
			newBytes, err := setting.Validate(ctx, &st.SV, prevRawVal, string(*s))
			if err != nil {
				return "", err
			}
			return string(newBytes), nil
		}
		return "", errors.Errorf("cannot use %s %T value for string setting", d.ResolvedType(), d)
	case *settings.BoolSetting:
		if b, ok := d.(*tree.DBool); ok {
			return settings.EncodeBool(bool(*b)), nil
		}
		return "", errors.Errorf("cannot use %s %T value for bool setting", d.ResolvedType(), d)
	case *settings.IntSetting:
		if i, ok := d.(*tree.DInt); ok {
			if err := setting.Validate(int64(*i)); err != nil {
				return "", err
			}
			return settings.EncodeInt(int64(*i)), nil
		}
		return "", errors.Errorf("cannot use %s %T value for int setting", d.ResolvedType(), d)
	case *settings.FloatSetting:
		if f, ok := d.(*tree.DFloat); ok {
			if err := setting.Validate(float64(*f)); err != nil {
				return "", err
			}
			return settings.EncodeFloat(float64(*f)), nil
		}
		return "", errors.Errorf("cannot use %s %T value for float setting", d.ResolvedType(), d)
	case *settings.EnumSetting:
		if i, intOK := d.(*tree.DInt); intOK {
			v, ok := setting.ParseEnum(settings.EncodeInt(int64(*i)))
			if ok {
				return settings.EncodeInt(v), nil
			}
			return "", errors.Errorf("invalid integer value '%d' for enum setting", *i)
		} else if s, ok := d.(*tree.DString); ok {
			str := string(*s)
			v, ok := setting.ParseEnum(str)
			if ok {
				return settings.EncodeInt(v), nil
			}
			return "", errors.Errorf("invalid string value '%s' for enum setting", str)
		}
		return "", errors.Errorf("cannot use %s %T value for enum setting, must be int or string", d.ResolvedType(), d)
	case *settings.ByteSizeSetting:
		if s, ok := d.(*tree.DString); ok {
			bytes, err := humanizeutil.ParseBytes(string(*s))
			if err != nil {
				return "", err
			}
			if err := setting.Validate(bytes); err != nil {
				return "", err
			}
			return settings.EncodeInt(bytes), nil
		}
		return "", errors.Errorf("cannot use %s %T value for byte size setting", d.ResolvedType(), d)
	case *settings.DurationSetting:
		if f, ok := d.(*tree.DInterval); ok {
			if f.Duration.Months > 0 || f.Duration.Days > 0 {
				return "", errors.Errorf("cannot use day or month specifiers: %s", d.String())
			}
			d := time.Duration(f.Duration.Nanos()) * time.Nanosecond
			if err := setting.Validate(d); err != nil {
				return "", err
			}
			return settings.EncodeDuration(d), nil
		}
		return "", errors.Errorf("cannot use %s %T value for duration setting", d.ResolvedType(), d)
	default:
		return "", errors.Errorf("unsupported setting type %T", setting)
	}
}
