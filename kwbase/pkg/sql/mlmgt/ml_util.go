// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package mlmgt

import (
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	ejson "encoding/json"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/mlagent/client"
	pb "gitee.com/kwbasedb/kwbase/pkg/sql/mlagent/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// mlResourceType type of ML assets.
type mlResourceType int16

const (
	_ mlResourceType = iota
	mlModel
	mlTrainingPipeline
)

func (t mlResourceType) String() string {
	switch t {
	case mlModel:
		return "model"
	case mlTrainingPipeline:
		return "training pipeline"
	default:
		return ""
	}
}

var mlProblemTypeMap = map[string]string{
	"r":                     "r",
	"regression":            "r",
	"b":                     "b",
	"binary_classification": "b",
	"m":                     "m",
	"multi_classification":  "m",
}

var mlFrameworkMap = map[string]struct{}{
	"sklearn_1.3":       {},
	"xgboost_1.5":       {},
	"xgboost_1.6":       {},
	"lightgbm_3.2":      {},
	"lightgbm_3.3":      {},
	"tensorflow_2.6":    {},
	"tensorflow_2.7":    {},
	"tensorflow_2.8":    {},
	"tensorflow_2.9":    {},
	"tensorflow_2.10":   {},
	"tensorflow_2.11":   {},
	"transformers_4.40": {},
}

var mlRuntimeMap = map[string]struct{}{
	"python_3.8":  {},
	"python_3.9":  {},
	"python_3.10": {},
}

var mlTypeMap = map[string]struct{}{
	"string": {},
	"int":    {},
	"float":  {},
	"array":  {},
	"bool":   {},
}

var mlUtils = mlUtil{}

type mlUtil struct{}

func (*mlUtil) newClient(p GenericPlannerAdapter) (*client.MLGRPCClient, error) {
	tlsConfig, err := p.RPCContext().GetClientTLSConfig()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get the TLS configuration")
	}

	mlAgentAddr := p.MLAgent()
	if mlAgentAddr == "" {
		return nil, errors.New("KaiwuDB ML setting 'ml.agent.addr' does not exist")
	}

	return &client.MLGRPCClient{
		Address: mlAgentAddr,
		Config:  tlsConfig.(*tls.Config),
	}, nil
}

func (*mlUtil) buildFullName(schema, name string) string {
	return fmt.Sprintf("%s.%s", schema, name)
}

func (*mlUtil) makeVersionNumberGeneratorKey(keys ...[]byte) []byte {
	return bytes.Join(keys, nil)
}

func (*mlUtil) computeChecksum(source []byte) (string, error) {
	Md5 := md5.New()
	if _, err := Md5.Write(source); err != nil {
		return "", errors.Wrap(err, "failed to compute the ml entity's checksum")
	}

	return hex.EncodeToString(Md5.Sum(nil)), nil
}

func (*mlUtil) parameterVerify(strs ...string) bool {
	for _, v := range strs {
		if v == "" {
			return false
		}
	}

	return true
}

func (util *mlUtil) checkSupportDataType(columnName, columnType string, value tree.Datum) error {
	// double-check the support list of input data, for example, timestamp type.
	switch value.(type) {
	case *tree.DInt, *tree.DDecimal:
		if columnType != "int" && columnType != "float" {
			return util.constructDataTypeError(columnName, columnType, value)
		}
	case *tree.DString, *tree.DBytes, *tree.DBitArray, *tree.DCollatedString, *tree.DDate, *tree.DTime,
		*tree.DTimeTZ, *tree.DTimestamp, *tree.DTimestampTZ, *tree.DUuid, *tree.DIPAddr:
		if columnType != "string" {
			return util.constructDataTypeError(columnName, columnType, value)
		}
	case *tree.DFloat:
		if columnType != "float" {
			return util.constructDataTypeError(columnName, columnType, value)
		}
	case *tree.DBool:
		if columnType != "bool" {
			return util.constructDataTypeError(columnName, columnType, value)
		}
	case *tree.DArray:
		if columnType != "array" {
			return util.constructDataTypeError(columnName, columnType, value)
		}
	default:
		if value != tree.DNull {
			return util.constructDataTypeError(columnName, columnType, value)
		}
	}

	return nil
}

func (util *mlUtil) checkSupportColumnType(columnName, columnType, checkType string) error {
	// double-check the support list of input data, for example, timestamp type.
	switch checkType {
	case columnType:
		return nil
	case types.Int.Name(), types.Int4.Name(), types.Decimal.Name():
		if columnType != "int" && columnType != "float" {
			return util.constructDataTypeError(columnName, columnType, checkType)
		}
	case types.String.Name(),
		types.Bytes.Name(),
		types.VarBit.Name(),
		types.AnyCollatedString.Name(),
		types.Date.Name(),
		types.Time.Name(),
		types.TimeTZ.Name(),
		types.Timestamp.Name(),
		types.Uuid.Name(),
		types.INet.Name(),
		types.TimestampTZ.Name():
		if columnType != "string" {
			return util.constructDataTypeError(columnName, columnType, checkType)
		}
	case types.Float.Name(), types.Float4.Name():
		if columnType != "float" {
			return util.constructDataTypeError(columnName, columnType, checkType)
		}
	case types.Bool.Name():
		if columnType != "bool" {
			return util.constructDataTypeError(columnName, columnType, checkType)
		}
	case types.AnyArray.Name():
		if columnType != "array" {
			return util.constructDataTypeError(columnName, columnType, checkType)
		}
	default:
		return util.constructDataTypeError(columnName, columnType, checkType)
	}

	return nil
}

func (*mlUtil) constructDataTypeError(columnName, columnType string, value interface{}) error {
	return pgerror.Newf(
		pgcode.InvalidParameterValue,
		"the value %v is not compatible with the expected data type '%s' for column '%s'",
		value,
		columnType,
		columnName,
	)
}

func (*mlUtil) checkProblemType(source string) (string, error) {
	val, ok := mlProblemTypeMap[strings.ToLower(source)]
	if !ok {
		return "", pgerror.Newf(
			pgcode.InvalidParameterValue,
			"problem type '%s' is unsupported",
			source,
		)
	}

	return val, nil
}

func (*mlUtil) checkIsvcNameLength(isvcName string) error {
	if len(isvcName) > pb.MaxServiceNameLength {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"'%s' must be no more than %d characters",
			isvcName,
			pb.MaxServiceNameLength,
		)
	}

	return nil
}

func (*mlUtil) checkFramework(source string) (string, error) {
	_, ok := mlFrameworkMap[strings.ToLower(source)]
	if !ok {
		return "", pgerror.Newf(
			pgcode.InvalidParameterValue,
			"framework '%s' is unsupported",
			source,
		)
	}

	return strings.ToLower(source), nil
}

func (*mlUtil) checkRuntime(source string) (string, error) {
	_, ok := mlRuntimeMap[strings.ToLower(source)]
	if !ok {
		return "", pgerror.Newf(
			pgcode.InvalidParameterValue,
			"runtime '%s' is unsupported",
			source,
		)
	}

	return strings.ToLower(source), nil
}

func (*mlUtil) checkFeatures(featuresDJSON tree.DJSON) (string, []*pb.Feature, error) {
	featuresJSON := featuresDJSON.JSON.String()
	var features []*pb.Feature
	if err := ejson.Unmarshal([]byte(featuresJSON), &features); err != nil {
		return "", nil, pgerror.Newf(
			pgcode.InvalidParameterValue, "features '%s' is unsupported", featuresDJSON,
		)
	}

	if len(features) == 0 {
		return "", nil, pgerror.Newf(
			pgcode.InvalidParameterValue, "features '%s' is unsupported", featuresDJSON,
		)
	}

	for _, f := range features {
		if err := mlUtils.checkDataType(f.Name, f.Type); err != nil {
			return "", nil, pgerror.Newf(pgcode.InvalidParameterValue, "features %v", err)
		}
	}

	return featuresJSON, features, nil
}

func (*mlUtil) checkTargets(targetsDJSON tree.DJSON) (string, []*pb.Target, error) {
	targetsJSON := targetsDJSON.JSON.String()
	var targets []*pb.Target
	if err := ejson.Unmarshal([]byte(targetsJSON), &targets); err != nil {
		return "", nil, pgerror.Newf(
			pgcode.InvalidParameterValue, "targets '%s' is unsupported", targetsDJSON,
		)
	}

	if len(targets) == 0 {
		return "", nil, pgerror.Newf(
			pgcode.InvalidParameterValue, "targets '%s' is unsupported", targetsDJSON,
		)
	}

	for _, f := range targets {
		if err := mlUtils.checkDataType(f.Name, f.Type); err != nil {
			return "", nil, pgerror.Newf(
				pgcode.InvalidParameterValue, "targets %v", err,
			)
		}
	}

	return targetsJSON, targets, nil
}

func (*mlUtil) checkDataType(name, typ string) error {

	if name == "" {
		return errors.Newf("name '%s' is unsupported", name)
	}

	if _, ok := mlTypeMap[typ]; !ok {
		return errors.Newf("type '%s' is unsupported", typ)
	}

	return nil
}

func (*mlUtil) checkMetrics(metricsDJSON tree.DJSON) (string, []*pb.Metrics, error) {
	metricsJSON := metricsDJSON.JSON.String()
	if metricsJSON == "[]" {
		return "", nil, nil
	}

	var metrics []*pb.Metrics
	if err := ejson.Unmarshal([]byte(metricsJSON), &metrics); err != nil {
		return "", nil, pgerror.Newf(
			pgcode.InvalidParameterValue, "metrics '%s' is unsupported", metricsJSON,
		)
	}

	for _, m := range metrics {
		m.Objective = strings.ToLower(m.Objective)
	}

	return metricsJSON, metrics, nil
}

func (*mlUtil) checkJobType(source string) ([]string, error) {
	jobTypeList := strings.Split(source, mlPrivilegeSeparator)

	for i, value := range jobTypeList {
		if value == "*" {
			return []string{}, nil
		}

		if _, ok := pb.JobTypeMap[strings.ToUpper(value)]; !ok {
			return []string{}, pgerror.Newf(
				pgcode.InvalidParameterValue, "job type '%s' is unsupported", value,
			)
		}

		jobTypeList[i] = strings.ToUpper(value)
	}

	return jobTypeList, nil
}

func (*mlUtil) checkStatusType(source string) ([]string, error) {
	jobStatusList := strings.Split(source, mlPrivilegeSeparator)

	for i, value := range jobStatusList {
		if value == "*" {
			return []string{}, nil
		}

		if _, ok := pb.JobStatusMap[strings.ToUpper(value)]; !ok {
			return []string{}, pgerror.Newf(
				pgcode.InvalidParameterValue, "job status '%s' is unsupported", value,
			)
		}

		jobStatusList[i] = strings.ToUpper(value)
	}

	return jobStatusList, nil
}

func (*mlUtil) checkOutputTableAction(source string) (string, error) {
	_, ok := pb.OutputTableAction[strings.ToUpper(source)]
	if !ok {
		return "", pgerror.Newf(
			pgcode.InvalidParameterValue,
			"action_if_exists '%s' is unsupported",
			source,
		)
	}

	return strings.ToUpper(source), nil
}

func (*mlUtil) DatumToString(value tree.Datum) interface{} {
	if value == tree.DNull {
		return nil
	}

	switch value.(type) {
	case *tree.DDate, *tree.DTime, *tree.DTimeTZ, *tree.DTimestamp, *tree.DTimestampTZ, *tree.DUuid, *tree.DIPAddr:
		return strings.Trim(value.String(), "'")
	case *tree.DDecimal:
		toFloat, _ := tree.ParseDFloat(value.String())
		return toFloat
	case *tree.DCollatedString:
		return value.String()
	case *tree.DBitArray:
		return strings.Trim(value.String(), "B'")
	case *tree.DBytes:
		data := tree.MustBeDBytes(value)
		return base64.StdEncoding.EncodeToString([]byte(data.String()))
	case *tree.DArray:
		arr := tree.MustBeDArray(value)
		cols := make([]interface{}, arr.Array.Len())
		for i, data := range arr.Array {
			cols[i] = mlUtils.DatumToString(data)
		}

		return cols
	default:
		return value
	}
}

func (*mlUtil) getStatementTables(stmt *parser.Statement) []*tree.TableName {
	tables := make([]*tree.TableName, 0)

	var walk func(...interface{})

	walk = func(nodes ...interface{}) {
		for _, currentNode := range nodes {
			if currentNode == nil {
				continue
			}
			if _, ok := currentNode.(tree.Datum); ok {
				continue
			}

			switch node := currentNode.(type) {
			case *tree.AliasedTableExpr:
				walk(node.Expr)
			case *tree.AndExpr:
				walk(node.Left, node.Right)
			case *tree.AnnotateTypeExpr:
				walk(node.Expr)
			case *tree.Array:
				walk(node.Exprs)
			case *tree.BinaryExpr:
				walk(node.Left, node.Right)
			case *tree.CaseExpr:
				walk(node.Expr, node.Else)
				for _, w := range node.Whens {
					walk(w.Cond, w.Val)
				}
			case *tree.CastExpr:
				walk(node.Expr)
			case *tree.CoalesceExpr:
				for _, expr := range node.Exprs {
					walk(expr)
				}
			case *tree.ColumnTableDef:
			case *tree.ComparisonExpr:
				walk(node.Left, node.Right)
			case *tree.CreateTable:
				for _, def := range node.Defs {
					walk(def)
				}
				if node.AsSource != nil {
					walk(node.AsSource)
				}
			case *tree.CTE:
				walk(node.Stmt)
			case *tree.DBool:
			case tree.Exprs:
				for _, expr := range node {
					walk(expr)
				}
			case *tree.FamilyTableDef:
			case *tree.FuncExpr:
				if node.WindowDef != nil {
					walk(node.WindowDef)
				}
				walk(node.Exprs, node.Filter)
			case *tree.IndexTableDef:
			case *tree.JoinTableExpr:
				walk(node.Left, node.Right, node.Cond)
			case *tree.NotExpr:
				walk(node.Expr)
			case *tree.NumVal:
			case *tree.OnJoinCond:
				walk(node.Expr)
			case *tree.OrExpr:
				walk(node.Left, node.Right)
			case *tree.ParenExpr:
				walk(node.Expr)
			case *tree.ParenSelect:
				walk(node.Select)
			case *tree.RowsFromExpr:
				for _, expr := range node.Items {
					walk(expr)
				}
			case *tree.Select:
				if node.With != nil {
					walk(node.With)
				}
				walk(node.Select)
			case *tree.SelectClause:
				walk(node.Exprs)
				if node.Where != nil {
					walk(node.Where)
				}
				if node.Having != nil {
					walk(node.Having)
				}
				for _, table := range node.From.Tables {
					walk(table)
				}
			case tree.SelectExpr:
				walk(node.Expr)
			case tree.SelectExprs:
				for _, expr := range node {
					walk(expr)
				}
			case *tree.SetVar:
				for _, expr := range node.Values {
					walk(expr)
				}
			case *tree.StrVal:
			case *tree.Subquery:
				walk(node.Select)
			case *tree.TableName:
				tables = append(tables, node)
			case *tree.Tuple:
				for _, expr := range node.Exprs {
					walk(expr)
				}
			case *tree.UnaryExpr:
				walk(node.Expr)
			case *tree.UniqueConstraintTableDef:
			case *tree.UnionClause:
				walk(node.Left, node.Right)
			case tree.UnqualifiedStar:
			case *tree.UnresolvedName:
			case *tree.ValuesClause:
				for _, row := range node.Rows {
					walk(row)
				}
			case *tree.Where:
				walk(node.Expr)
			case *tree.WindowDef:
				walk(node.Partitions)
				if node.Frame != nil {
					walk(node.Frame)
				}
			case *tree.WindowFrame:
				if node.Bounds.StartBound != nil {
					walk(node.Bounds.StartBound)
				}
				if node.Bounds.EndBound != nil {
					walk(node.Bounds.EndBound)
				}
			case *tree.WindowFrameBound:
				walk(node.OffsetExpr)
			case *tree.Window:
			case *tree.With:
				for _, expr := range node.CTEList {
					walk(expr)
				}
			default:
			}
		}
	}

	walk(stmt.AST)

	return tables
}

func (*mlUtil) IsTable(name string) bool {
	return !strings.Contains(strings.Trim(name, " "), " ")
}
