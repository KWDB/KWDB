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
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/stats"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
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
			switch n.name {
			case "ts.dedup.rule":
				if encoded != "merge" && encoded != "keep" && encoded != "reject" &&
					encoded != "discard" && encoded != "override" {
					return errors.New("ts.dedup.rule setting value is not right")
				}
			case "ts.mount.max_limit":
				value, err := strconv.ParseInt(expectedEncodedValue, 10, 64)
				if err != nil {
					return err
				}
				if value < 0 || value > math.MaxInt32 {
					return errors.New("invalid value, the range of ts.mount.max_limit is [0, 2147483647]")
				}
			case "ts.cached_partitions_per_subgroup.max_limit":
				value, err := strconv.ParseInt(expectedEncodedValue, 10, 64)
				if err != nil {
					return err
				}
				if value < 0 || value > math.MaxInt32 {
					return errors.New("invalid value, the range of ts.cached_partitions_per_subgroup.max_limit is [0, 2147483647]")
				}
			case "ts.entities_per_subgroup.max_limit":
				value, err := strconv.ParseInt(expectedEncodedValue, 10, 64)
				if err != nil {
					return err
				}
				if value < 1 || value > math.MaxInt32 {
					return errors.New("invalid value, the range of ts.entities_per_subgroup.max_limit is [1, 2147483647]")
				}
			case "ts.blocks_per_segment.max_limit":
				value, err := strconv.ParseInt(expectedEncodedValue, 10, 64)
				if err != nil {
					return err
				}
				if value < 1 || value > 1000000 {
					return errors.New("invalid value, the range of ts.blocks_per_segment.max_limit is [1, 1000000]")
				}
			case "ts.rows_per_block.max_limit":
				value, err := strconv.ParseInt(expectedEncodedValue, 10, 64)
				if err != nil {
					return err
				}
				if value < 10 || value > 1000 {
					return errors.New("invalid value, the range of ts.rows_per_block.max_limit is [10, 1000]")
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