// Copyright 2015 The Cockroach Authors.
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
	"context"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/errors"
)

// setVarNode represents a SET SESSION statement.
type setVarNode struct {
	name string
	v    sessionVar
	// typedValues == nil means RESET.
	typedValues   []tree.TypedExpr
	isUserDefined bool
}

// SetVar sets session variables.
// Privileges: None.
//
//	Notes: postgres/mysql do not require privileges for session variables (some exceptions).
func (p *planner) SetVar(ctx context.Context, n *tree.SetVar) (planNode, error) {
	if n.Name == "" {
		// A client has sent the reserved internal syntax SET ROW ...,
		// or the user entered `SET "" = foo`. Reject it.
		return nil, pgerror.Newf(pgcode.Syntax,
			"invalid variable name: %q", n.Name)
	}

	name := strings.ToLower(n.Name)
	if n.IsUserDefined {
		if len(n.Values) != 1 {
			return nil, newSingleArgVarError(name)
		}
		typedVal, err := n.Values[0].TypeCheck(&p.semaCtx, types.Any)
		if err != nil {
			return nil, err
		}
		return &setVarNode{name: name, isUserDefined: n.IsUserDefined, typedValues: []tree.TypedExpr{typedVal}}, nil
	}
	_, v, err := getSessionVar(name, false /* missingOk */)
	if err != nil {
		return nil, err
	}

	var typedValues []tree.TypedExpr
	if len(n.Values) > 0 {
		isReset := false
		if len(n.Values) == 1 {
			if _, ok := n.Values[0].(tree.DefaultVal); ok {
				// "SET var = DEFAULT" means RESET.
				// In that case, we want typedValues to remain nil, so that
				// the Start() logic recognizes the RESET too.
				isReset = true
			}
		}

		if !isReset {
			typedValues = make([]tree.TypedExpr, len(n.Values))
			for i, expr := range n.Values {
				expr = unresolvedNameToStrVal(expr)

				var dummyHelper tree.IndexedVarHelper
				typedValue, err := p.analyzeExpr(
					ctx, expr, nil, dummyHelper, types.String, false, "SET SESSION "+name)
				if err != nil {
					return nil, wrapSetVarError(name, expr.String(), "%v", err)
				}
				typedValues[i] = typedValue
			}
		}
	}

	if v.Set == nil && v.RuntimeSet == nil {
		return nil, newCannotChangeParameterError(name)
	}

	if typedValues == nil {
		// Statement is RESET. Do we have a default available?
		// We do not use getDefaultString here because we need to delay
		// the computation of the default to the execute phase.
		if _, ok := p.sessionDataMutator.defaults.SessionDefaultsMp[name]; !ok && v.GlobalDefault == nil {
			return nil, newCannotChangeParameterError(name)
		}
	}

	return &setVarNode{name: name, v: v, typedValues: typedValues}, nil
}

// Special rule for SET: because SET doesn't apply in the context
// of a table, SET ... = IDENT really means SET ... = 'IDENT'.
func unresolvedNameToStrVal(expr tree.Expr) tree.Expr {
	if s, ok := expr.(*tree.UnresolvedName); ok {
		return tree.NewStrVal(tree.AsStringWithFlags(s, tree.FmtBareIdentifiers))
	}
	return expr
}

func (n *setVarNode) startExec(params runParams) error {
	var strVal string
	var operation target.OperationType = target.Set
	if n.isUserDefined {
		val, err := n.typedValues[0].Eval(params.EvalContext())
		if err != nil {
			return err
		}
		return params.p.sessionDataMutator.SetUserDefinedVar(n.name, val)
	}
	if n.typedValues != nil {
		operation = target.Set
		for i, v := range n.typedValues {
			d, err := v.Eval(params.EvalContext())
			if err != nil {
				return err
			}
			n.typedValues[i] = d
		}
		var err error
		if n.v.GetStringVal != nil {

			strVal, err = n.v.GetStringVal(params.ctx, params.extendedEvalCtx, n.typedValues)
		} else {
			// No string converter defined, use the default one.

			strVal, err = getStringVal(params.EvalContext(), n.name, n.typedValues)
		}
		if err != nil {
			return err
		}
	} else {
		operation = target.Reset
		// Statement is RESET and we already know we have a default. Find it.
		_, strVal = getSessionVarDefaultString(n.name, n.v, params.p.sessionDataMutator)
	}

	if n.name == "database" {
		currSearchPath := params.p.sessionDataMutator.data.SearchPath
		SearchPath := sessiondata.SetSearchPath(currSearchPath, []string{tree.PublicSchema})
		params.p.sessionDataMutator.data.SearchPath = SearchPath
	}

	params.p.SetAuditTarget(0, params.extendedEvalCtx.SessionID.String(), nil)

	var err error
	if n.v.RuntimeSet != nil {
		err = n.v.RuntimeSet(params.ctx, params.extendedEvalCtx, strVal)
		defer func() {
			params.p.SetAuditInfo(params.ctx, params.p.Txn(), timeutil.Now(), target.ObjectSession, operation, err)
		}()
		return err
	}
	err = n.v.Set(params.ctx, params.p.sessionDataMutator, strVal)
	defer func() {
		params.p.SetAuditInfo(params.ctx, params.p.Txn(), timeutil.Now(), target.ObjectSession, operation, err)
	}()
	return err
}

// getSessionVarDefaultString retrieves a string suitable to pass to a
// session var's Set() method. First return value is false if there is
// no default.
func getSessionVarDefaultString(
	varName string, v sessionVar, m *sessionDataMutator,
) (bool, string) {
	if defVal, ok := m.defaults.SessionDefaultsMp[varName]; ok {
		return true, defVal
	}
	if v.GlobalDefault != nil {
		return true, v.GlobalDefault(&m.settings.SV)
	}
	return false, ""
}

func (n *setVarNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *setVarNode) Values() tree.Datums            { return nil }
func (n *setVarNode) Close(_ context.Context)        {}

func datumAsString(evalCtx *tree.EvalContext, name string, value tree.TypedExpr) (string, error) {
	val, err := value.Eval(evalCtx)
	if err != nil {
		return "", err
	}
	s, ok := tree.AsDString(val)
	if !ok {
		err = pgerror.Newf(pgcode.InvalidParameterValue,
			"parameter %q requires a string value", name)
		err = errors.WithDetailf(err,
			"%s is a %s", value, errors.Safe(val.ResolvedType()))
		return "", err
	}
	return string(s), nil
}

func getStringVal(evalCtx *tree.EvalContext, name string, values []tree.TypedExpr) (string, error) {
	if len(values) != 1 {
		return "", newSingleArgVarError(name)
	}
	return datumAsString(evalCtx, name, values[0])
}

func datumAsInt(evalCtx *tree.EvalContext, name string, value tree.TypedExpr) (int64, error) {
	val, err := value.Eval(evalCtx)
	if err != nil {
		return 0, err
	}
	iv, ok := tree.AsDInt(val)
	if !ok {
		err = pgerror.Newf(pgcode.InvalidParameterValue,
			"parameter %q requires an integer value", name)
		err = errors.WithDetailf(err,
			"%s is a %s", value, errors.Safe(val.ResolvedType()))
		return 0, err
	}
	return int64(iv), nil
}

func getIntVal(evalCtx *tree.EvalContext, name string, values []tree.TypedExpr) (int64, error) {
	if len(values) != 1 {
		return 0, newSingleArgVarError(name)
	}
	return datumAsInt(evalCtx, name, values[0])
}

func datumAsFloat(evalCtx *tree.EvalContext, name string, value tree.TypedExpr) (float64, error) {
	val, err := value.Eval(evalCtx)
	if err != nil {
		return 0, err
	}
	temp := tree.MustBeDDecimal(val)
	iv, err := temp.Decimal.Float64()
	if err != nil {
		err = pgerror.Newf(pgcode.InvalidParameterValue,
			"parameter %q requires an Float value, parse float failed", name)
		return 0, err
	}
	return float64(iv), nil
}

func getFloatVal(evalCtx *tree.EvalContext, name string, values []tree.TypedExpr) (float64, error) {
	if len(values) != 1 {
		return 0, newSingleArgVarError(name)
	}
	return datumAsFloat(evalCtx, name, values[0])
}

func timeZoneVarGetStringVal(
	_ context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
) (string, error) {
	if len(values) != 1 {
		return "", newSingleArgVarError("timezone")
	}
	d, err := values[0].Eval(&evalCtx.EvalContext)
	if err != nil {
		return "", err
	}

	var loc *time.Location
	var offset int64
	switch v := tree.UnwrapDatum(&evalCtx.EvalContext, d).(type) {
	case *tree.DString:
		location := string(*v)
		loc, err = timeutil.TimeZoneStringToLocation(
			location,
			timeutil.TimeZoneStringToLocationISO8601Standard,
		)
		if err != nil {
			return "", wrapSetVarError("timezone", values[0].String(),
				"cannot find time zone %q: %v", location, err)
		}

	case *tree.DInterval:
		offset, _, _, err = v.Duration.Encode()
		if err != nil {
			return "", wrapSetVarError("timezone", values[0].String(), "%v", err)
		}
		offset /= int64(time.Second)

	case *tree.DInt:
		offset = int64(*v) * 60 * 60

	case *tree.DFloat:
		offset = int64(float64(*v) * 60.0 * 60.0)

	case *tree.DDecimal:
		sixty := apd.New(60, 0)
		ed := apd.MakeErrDecimal(tree.ExactCtx)
		ed.Mul(sixty, sixty, sixty)
		ed.Mul(sixty, sixty, &v.Decimal)
		offset = ed.Int64(sixty)
		if ed.Err() != nil {
			return "", wrapSetVarError("timezone", values[0].String(),
				"time zone value %s would overflow an int64", sixty)
		}

	default:
		return "", newVarValueError("timezone", values[0].String())
	}
	if loc == nil {
		loc = timeutil.FixedOffsetTimeZoneToLocation(int(offset), d.String())
	}

	return loc.String(), nil
}

func timeZoneVarSet(_ context.Context, m *sessionDataMutator, s string) error {
	loc, err := timeutil.TimeZoneStringToLocation(
		s,
		timeutil.TimeZoneStringToLocationISO8601Standard,
	)
	if err != nil {
		return wrapSetVarError("timezone", s, "%v", err)
	}

	m.SetLocation(loc)
	return nil
}

func stmtTimeoutVarGetStringVal(
	ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
) (string, error) {
	if len(values) != 1 {
		return "", newSingleArgVarError("statement_timeout")
	}
	d, err := values[0].Eval(&evalCtx.EvalContext)
	if err != nil {
		return "", err
	}

	var timeout time.Duration
	switch v := tree.UnwrapDatum(&evalCtx.EvalContext, d).(type) {
	case *tree.DString:
		return string(*v), nil
	case *tree.DInterval:
		timeout, err = intervalToDuration(v)
		if err != nil {
			return "", wrapSetVarError("statement_timeout", values[0].String(), "%v", err)
		}
	case *tree.DInt:
		timeout = time.Duration(*v) * time.Millisecond
	}
	return timeout.String(), nil
}

func makeTimeoutVarGetter(varName string) getStringValFn {
	timeoutVarGetter := func(ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr) (string, error) {
		if len(values) != 1 {
			return "", newSingleArgVarError(varName)
		}
		d, err := values[0].Eval(&evalCtx.EvalContext)
		if err != nil {
			return "", err
		}

		var timeout time.Duration
		switch v := tree.UnwrapDatum(&evalCtx.EvalContext, d).(type) {
		case *tree.DString:
			return string(*v), nil
		case *tree.DInterval:
			timeout, err = intervalToDuration(v)
			if err != nil {
				return "", wrapSetVarError(varName, values[0].String(), "%v", err)
			}
		case *tree.DInt:
			timeout = time.Duration(*v) * time.Millisecond
		}
		return timeout.String(), nil
	}
	return timeoutVarGetter
}

func validateTimeoutVar(timeString string, varName string) (time.Duration, error) {
	interval, err := tree.ParseDIntervalWithTypeMetadata(timeString, types.IntervalTypeMetadata{
		DurationField: types.IntervalDurationField{
			DurationType: types.IntervalDurationType_MILLISECOND,
		},
	})
	if err != nil {
		return 0, wrapSetVarError(varName, timeString, "%v", err)
	}
	timeout, err := intervalToDuration(interval)
	if err != nil {
		return 0, wrapSetVarError(varName, timeString, "%v", err)
	}

	if timeout < 0 {
		return 0, wrapSetVarError(varName, timeString,
			"%v cannot have a negative duration", varName)
	}

	return timeout, nil
}

func stmtTimeoutVarSet(ctx context.Context, m *sessionDataMutator, s string) error {
	timeout, err := validateTimeoutVar(s, "statement_timeout")
	if err != nil {
		return err
	}

	m.SetStmtTimeout(timeout)
	return nil
}

func idleInSessionTimeoutVarSet(ctx context.Context, m *sessionDataMutator, s string) error {
	timeout, err := validateTimeoutVar(s, "idle_in_session_timeout")
	if err != nil {
		return err
	}

	m.SetIdleInSessionTimeout(timeout)
	return nil
}

func tsinsertshortcircuitSet(ctx context.Context, m *sessionDataMutator, s string) error {
	b, err := parsePostgresBool(s)
	if err != nil {
		return err
	}
	m.SetTsInsertShortcircuit(b)
	return nil
}

func tssupportbatch(ctx context.Context, m *sessionDataMutator, s string) error {
	b, err := parsePostgresBool(s)
	if err != nil {
		return err
	}
	m.SetTsSupportBatch(b)
	return nil
}

func intervalToDuration(interval *tree.DInterval) (time.Duration, error) {
	nanos, _, _, err := interval.Encode()
	if err != nil {
		return 0, err
	}
	return time.Duration(nanos), nil
}

func newSingleArgVarError(varName string) error {
	return pgerror.Newf(pgcode.InvalidParameterValue,
		"SET %s takes only one argument", varName)
}

func wrapSetVarError(varName, actualValue string, fmt string, args ...interface{}) error {
	err := pgerror.Newf(pgcode.InvalidParameterValue,
		"invalid value for parameter %q: %q", varName, actualValue)
	return errors.WithDetailf(err, fmt, args...)
}

func newVarValueError(varName, actualVal string, allowedVals ...string) (err error) {
	err = pgerror.Newf(pgcode.InvalidParameterValue,
		"invalid value for parameter %q: %q", varName, actualVal)
	if len(allowedVals) > 0 {
		err = errors.WithHintf(err, "Available values: %s", strings.Join(allowedVals, ","))
	}
	return err
}

func newCannotChangeParameterError(varName string) error {
	return pgerror.Newf(pgcode.CantChangeRuntimeParam,
		"parameter %q cannot be changed", varName)
}
