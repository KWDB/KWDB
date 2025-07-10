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
	"math"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/contextutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var versionName = "version"

func (p *planner) showStateMachineSetting(
	ctx context.Context, st *cluster.Settings, s *settings.StateMachineSetting, name string,
) (string, error) {
	var res string
	// For statemachine settings (at the time of writing, this is only the cluster version setting)
	// we show the value from the KV store and additionally wait for the local Gossip instance to
	// have observed the value as well. This makes sure that cluster version bumps become visible
	// immediately while at the same time guaranteeing that a node reporting a certain version has
	// also processed the corresponding Gossip update (which is important as only then does the node
	// update its persisted state; see #22796).
	if err := contextutil.RunWithTimeout(ctx, fmt.Sprintf("show cluster setting %s", name), 2*time.Minute,
		func(ctx context.Context) error {
			tBegin := timeutil.Now()

			// The (slight ab)use of WithMaxAttempts achieves convenient context cancellation.
			return retry.WithMaxAttempts(ctx, retry.Options{}, math.MaxInt32, func() error {
				return p.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					datums, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
						ctx, "read-setting",
						txn,
						sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
						"SELECT value FROM system.settings WHERE name = $1", name,
					)
					if err != nil {
						return err
					}
					var kvRawVal []byte
					if len(datums) != 0 {
						dStr, ok := datums[0].(*tree.DString)
						if !ok {
							return errors.New("the existing value is not a string")
						}
						kvRawVal = []byte(string(*dStr))
					} else {
						// There should always be a version saved; there's a migration
						// populating it.
						return errors.AssertionFailedf("no value found for version setting")
					}

					gossipRawVal := []byte(s.Get(&st.SV))
					if !bytes.Equal(gossipRawVal, kvRawVal) {
						return errors.Errorf(
							"value differs between gossip (%v) and KV (%v); try again later (%v after %s)",
							gossipRawVal, kvRawVal, ctx.Err(), timeutil.Since(tBegin))
					}

					val, err := s.Decode(kvRawVal)
					if err != nil {
						return err
					}
					res = val.(fmt.Stringer).String()

					return nil
				})
			})
		}); err != nil {
		return "", err
	}

	return res, nil
}

func (p *planner) ShowClusterSetting(
	ctx context.Context, n *tree.ShowClusterSetting,
) (planNode, error) {

	if err := p.RequireAdminRole(ctx, "SHOW CLUSTER SETTING"); err != nil {
		return nil, err
	}

	name := strings.ToLower(n.Name)
	st := p.ExecCfg().Settings
	val, ok := settings.Lookup(name, settings.LookupForLocalAccess)
	if !ok {
		return nil, errors.Errorf("unknown setting: %q", name)
	}

	var dType *types.T
	switch val.(type) {
	case *settings.IntSetting:
		dType = types.Int
	case *settings.StringSetting, *settings.ByteSizeSetting, *settings.StateMachineSetting, *settings.EnumSetting:
		dType = types.String
	case *settings.BoolSetting:
		dType = types.Bool
	case *settings.FloatSetting:
		dType = types.Float
	case *settings.DurationSetting:
		dType = types.Interval
	default:
		return nil, errors.Errorf("unknown setting type for %s: %s", name, val.Typ())
	}

	columns := sqlbase.ResultColumns{{Name: name, Typ: dType}}
	return &delayedNode{
		name:    "SHOW CLUSTER SETTING " + name,
		columns: columns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			var d tree.Datum
			switch s := val.(type) {
			case *settings.IntSetting:
				d = tree.NewDInt(tree.DInt(s.Get(&st.SV)))
			case *settings.StringSetting:
				d = tree.NewDString(s.String(&st.SV))
			case *settings.StateMachineSetting:
				var err error
				valStr, err := p.showStateMachineSetting(ctx, st, s, name)
				if err != nil {
					return nil, err
				}
				d = tree.NewDString(valStr)
			case *settings.BoolSetting:
				d = tree.MakeDBool(tree.DBool(s.Get(&st.SV)))
			case *settings.FloatSetting:
				d = tree.NewDFloat(tree.DFloat(s.Get(&st.SV)))
			case *settings.DurationSetting:
				d = &tree.DInterval{Duration: duration.MakeDuration(s.Get(&st.SV).Nanoseconds(), 0, 0)}
			case *settings.EnumSetting:
				d = tree.NewDString(s.String(&st.SV))
			case *settings.ByteSizeSetting:
				d = tree.NewDString(s.String(&st.SV))
			default:
				return nil, errors.Errorf("unknown setting type for %s: %s", name, val.Typ())
			}

			v := p.newContainerValuesNode(columns, 0)
			if _, err := v.rows.AddRow(ctx, tree.Datums{d}); err != nil {
				v.rows.Close(ctx)
				return nil, err
			}
			return v, nil
		},
	}, nil
}
