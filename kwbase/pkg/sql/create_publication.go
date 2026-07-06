//
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
//
// Shanghai Yunxi Technology Co, Ltd makes no representations or warranties about the suitability
// of the software, either express or implied, including but not limited to the implied warranties
// of merchantability, fitness for a particular purpose, or non-infringement. Shanghai Yunxi
// Technology Co, Ltd shall not be liable for any damages suffered by licensee as a result
// of using, modifying or distributing this software or its derivatives.
//

package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/json"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

const (
	optSubTimeout     = "sub_timeout"
	optPublish        = "publish"
	defaultSubTimeout = 2
	defaultPublish    = "insert"
)

// publishOptional stores the optional operation about options in publication
var publishOptional = map[string]interface{}{
	"insert": nil,
	"update": nil,
	"delete": nil,
	"all":    nil,
}
var pubOptionExpectValues = map[string]KVStringOptValidate{
	optPublish:    KVStringOptRequireValue,
	optCheckTag:   KVStringOptRequireValue,
	optBufferSize: KVStringOptRequireValue,
	optSubTimeout: KVStringOptRequireValue,
}

type createPubNode struct {
	n             *tree.CreatePublication
	databaseID    uint64
	tableDescList []*sqlbase.MutableTableDescriptor
	pubOpts       func() (map[string]string, error)
}

// PubMetadata records a list of publication info
type PubMetadata struct {
	id               uint64
	name             tree.Name
	parameters       json.JSON
	createAt         tree.DTimestamp
	createBy         string
	status           string
	runInfo          json.JSON
	jobID            int64
	databaseID       uint64
	lowWaterMark     int64
	cdcWatermarkList []CDCWatermark

	paraInfo cdcpb.PubParameters
}

// Decode decode JSON to struct.
func (p *PubMetadata) Decode() error {
	var err error
	p.paraInfo, err = cdcpb.UnmarshalPubParameters(p.parameters)
	if err != nil {
		return err
	}

	return nil
}

// CreatePublication creates a publication node for exec.
func (p *planner) CreatePublication(
	ctx context.Context, n *tree.CreatePublication,
) (planNode, error) {
	found, err := p.checkPubByName(ctx, n.PubName)
	if err != nil {
		return nil, err
	}
	if found {
		return nil, pgerror.Newf(pgcode.DuplicateObject, "pub %q already exists", n.PubName)
	}
	var databaseID uint64

	if n.Table.TableName != "" {
		// single table
		n.TableNames = append(n.TableNames, n.Table)
	} else if n.Database != "" {
		// single database
		if n.Where != nil {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "where expr is not supported on database")
		}

		dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, string(n.Database), true)
		if err != nil {
			return nil, err
		}
		if dbDesc.EngineType == tree.EngineTypeRelational {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "publication is only used on ts database. but %s is relational database", dbDesc.Name)
		}
		databaseID = uint64(dbDesc.ID)
		schemas, err := p.Tables().getSchemasForDatabase(ctx, p.txn, dbDesc.ID)
		if err != nil {
			return nil, err
		}

		// the names of all objects in the target database
		for _, schema := range schemas {
			toAppend, err := GetObjectNames(
				ctx, p.txn, p, dbDesc, schema, true, /*explicitPrefix*/
			)
			if err != nil {
				return nil, err
			}
			n.TableNames = append(n.TableNames, toAppend...)
		}

	} else {
		// multi table
		if n.Where != nil {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "where expr is not supported on multi table")
		}
	}
	tableDescList := make([]*MutableTableDescriptor, len(n.TableNames))
	checkTableDuplicate := make(map[sqlbase.ID]struct{})

	for i := range n.TableNames {
		tableDescList[i], err = p.ResolveMutableTableDescriptor(
			ctx, &n.TableNames[i], true /*required*/, ResolveRequireTableDesc,
		)
		if err != nil {
			return nil, err
		}

		if !tableDescList[i].IsTSTable() {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "publication is only used on ts table. but %s is relational table", tableDescList[i].GetName())
		}

		tableID := uint64(tableDescList[i].ID)
		isSubTarget, err := p.tableHasCDC(ctx, sqlbase.CDCInstanceType_Subscription, &tableID)
		if err != nil {
			return nil, err
		}

		if isSubTarget {
			return nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"table %s is the subscription target table", n.TableNames[i].FQString())
		}

		if _, ok := checkTableDuplicate[tableDescList[i].ID]; ok {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "duplicate table %s", tableDescList[i].Name)
		}
		checkTableDuplicate[tableDescList[i].ID] = struct{}{}

		if err = p.checkPrivilegeForPubSub(ctx, tableDescList[i], privilege.SELECT); err != nil {
			return nil, err
		}
	}

	pubOpts, err := p.TypeAsStringOpts(n.Options, pubOptionExpectValues)
	if err != nil {
		return nil, err
	}

	return &createPubNode{n: n, tableDescList: tableDescList, pubOpts: pubOpts, databaseID: databaseID}, nil
}

// checkPubByName checks whether the specified publication exists.
func (p *planner) checkPubByName(ctx context.Context, pubName tree.Name) (bool, error) {
	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"check-pub",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT id, name FROM system.kwdb_publications WHERE name = $1`,
		pubName,
	)
	if err != nil {
		return false, err
	}

	// pub does not exist
	if len(row) == 0 {
		return false, nil
	}

	return true, nil
}

// loadPubByName loads metadata of publication from system.kwdb_publications with publication name.
func (p *planner) loadPubByName(ctx context.Context, pubName tree.Name) (*PubMetadata, error) {
	stmt := fmt.Sprintf(`SELECT id,name,parameters,create_at,create_by,database_id
FROM system.kwdb_publications WHERE name = '%s'`, pubName)
	return p.loadPub(ctx, stmt)
}

// loadPubByID loads metadata of publication from system.kwdb_publications with publication ID.
func (p *planner) loadPubByID(ctx context.Context, pubID uint64) (*PubMetadata, error) {
	stmt := fmt.Sprintf(`SELECT id,name,parameters,create_at,create_by,database_id
FROM system.kwdb_publications WHERE id = %d`, pubID)
	return p.loadPub(ctx, stmt)
}

// loadPub loads metadata of publication with the specified statement.
func (p *planner) loadPub(ctx context.Context, stmt string) (*PubMetadata, error) {
	var metadata PubMetadata
	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"load-pub",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		stmt,
	)
	if err != nil {
		return nil, err
	}

	// pub does not exist
	if len(row) == 0 {
		return nil, nil
	}

	metadata.id = uint64(tree.MustBeDInt(row[0]))
	metadata.name = tree.Name(tree.MustBeDString(row[1]))
	metadata.parameters = tree.MustBeDJSON(row[2]).JSON
	metadata.createAt = tree.MustBeDTimestamp(row[3])
	metadata.createBy = string(tree.MustBeDString(row[4]))
	metadata.databaseID = uint64(tree.MustBeDInt(row[5]))

	if err = metadata.Decode(); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// checkPrivilegeForPubSub verifies if the user has `privilege`.
func (p *planner) checkPrivilegeForPubSub(
	ctx context.Context, tableDesc sqlbase.DescriptorProto, priv privilege.Kind,
) error {
	// Verify user has system admin role
	var isAdmin bool
	var err error
	isAdmin, err = p.HasAdminRole(ctx)
	if err != nil {
		return err
	}

	if isAdmin {
		return nil
	}
	// verify table privilege needed.
	if tableDesc != nil {
		if err = p.CheckPrivilege(ctx, tableDesc, priv); err != nil {
			return err
		}
	}
	return nil
}

func (n *createPubNode) startExec(params runParams) (err error) {
	pubTableInfos, _, err := params.p.makeCDCTableInfo(params.ctx, n.tableDescList, n.n.Star, n.n.ColNames, true)
	if err != nil {
		return err
	}

	if n.n.Where != nil {
		whereNeedNormalTag, err := params.p.checkWhereExprForCDC(
			params.ctx, n.n.Table, n.tableDescList[0].TableDescriptor, n.n.Where.Expr)
		if err != nil {
			return err
		}
		if whereNeedNormalTag {
			pubTableInfos[0].NeedNormalTag = true
		}

		pubTableInfos[0].Filter = n.n.Where.Expr.String()
	}

	pubOpts, err := n.pubOpts()
	if err != nil {
		return err
	}
	options, err := makePubOptions(pubOpts, nil)
	if err != nil {
		return err
	}

	for i := range pubTableInfos {
		pubTableInfos[i].NeedNormalTag = pubTableInfos[i].NeedNormalTag && options.CheckTag == optOn
	}

	para := cdcpb.PubParameters{
		TableList:  pubTableInfos,
		PubOptions: options,
	}

	parameters, err := cdcpb.MarshalPubParameters(para)
	if err != nil {
		return err
	}

	metadata := PubMetadata{
		name:       n.n.PubName,
		parameters: parameters,
		databaseID: n.databaseID,
		createBy:   params.p.User(),
		createAt:   tree.DTimestamp{Time: timeutil.Now()},
	}

	if _, err = params.ExecCfg().InternalExecutor.ExecEx(
		params.ctx,
		"write-pub-metadata",
		params.p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`INSERT INTO system.kwdb_publications
(name,parameters,create_by,create_at,database_id)
values ($1,$2,$3,$4,$5)`,
		metadata.name, metadata.parameters, metadata.createBy, metadata.createAt.Time, metadata.databaseID,
	); err != nil {
		return err
	}

	pubSchema, err := params.p.loadPubByName(params.ctx, n.n.PubName)
	if err != nil {
		return err
	}

	for i := range para.TableList {
		if err = params.p.addCDCDescriptorByTableID(
			params.ctx, para.TableList[i].ID, sqlbase.CDCInstanceType_Publication,
			pubSchema.id, []byte(pubSchema.name),
		); err != nil {
			return err
		}

		if err = params.p.addCDCWatermark(params.ctx, CDCWatermark{
			TableID:      para.TableList[i].ID,
			TaskID:       pubSchema.id,
			TaskType:     sqlbase.CDCInstanceType_Publication,
			LowWatermark: cdcpb.InvalidWatermark,
		}); err != nil {
			return err
		}
	}

	params.p.SetAuditTarget(uint32(pubSchema.id), pubSchema.name.String(), nil)

	return err
}

func (n *createPubNode) Next(_ runParams) (bool, error) {
	return false, nil
}

func (n *createPubNode) Values() tree.Datums { return nil }

func (n *createPubNode) Close(context.Context) {}

// MarshalCDCFilter extracts the filter expressions of metrics and tags from the physical plan
// and marshals them to bytes. They will be applied during the data capture phase.
func MarshalCDCFilter(
	ctx context.Context,
	txn *kv.Txn,
	user string,
	execCfg *ExecutorConfig,
	tableInfo *cdcpb.CDCTableInfo,
) ([]byte, [][]byte, error) {
	// make a new local planner
	plan, cleanup := newInternalPlanner("CDC-filter-builder", txn, user, &MemoryMetrics{}, execCfg)
	defer cleanup()

	// The column order in the filter must be consistent with that in the payload
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s",
		tableInfo.Database,
		tableInfo.Table,
		tableInfo.Filter)
	stmt, err := parser.ParseOne(query)
	if err != nil {
		return nil, nil, err
	}

	localPlanner := plan
	localPlanner.stmt = &Statement{Statement: stmt}
	localPlanner.forceFilterInME = true
	localPlanner.optPlanningCtx.init(localPlanner)

	localPlanner.runWithOptions(resolveFlags{skipCache: true}, func() {
		err = localPlanner.makeOptimizerPlan(ctx)
	})
	if err != nil {
		return nil, nil, err
	}

	defer localPlanner.curPlan.close(ctx)
	rec, err := localPlanner.DistSQLPlanner().checkSupportForNode(localPlanner.curPlan.plan)
	isLocal := err != nil || rec == cannotDistribute
	if len(localPlanner.curPlan.subqueryPlans) != 0 {
		return nil, nil, pgerror.New(pgcode.FeatureNotSupported, "cannot include sub-query in the pub filter")
	}

	evalCtx := localPlanner.ExtendedEvalContext()
	planCtx := localPlanner.DistSQLPlanner().NewPlanningCtx(ctx, evalCtx, txn)
	planCtx.isLocal = isLocal
	planCtx.cdcCtx = &CDCContext{}
	planCtx.planner = localPlanner
	planCtx.stmtType = tree.Rows

	physPlan, err := localPlanner.DistSQLPlanner().createPlanForNode(planCtx, localPlanner.curPlan.plan)
	if err != nil {
		return nil, nil, err
	}

	localPlanner.DistSQLPlanner().FinalizePlan(planCtx, &physPlan)

	if len(physPlan.Processors) == 1 {
		if physPlan.Processors[0].Spec.Core.Values != nil {
			return nil, nil, pgerror.Newf(pgcode.FeatureNotSupported, "pub filter %q is invalid", tableInfo.Filter)
		}
	}

	var metricsFilter []byte
	var tagFilter [][]byte

	if planCtx.cdcCtx != nil && planCtx.cdcCtx.metricsFilter.Expr != "" {
		metricsFilter, err = protoutil.Marshal(&planCtx.cdcCtx.metricsFilter)
		if err != nil {
			return nil, nil, err
		}
	}

	if planCtx.cdcCtx != nil && planCtx.cdcCtx.tagFilter != nil {
		for _, tf := range planCtx.cdcCtx.tagFilter {
			filler, err := protoutil.Marshal(&tf)
			if err != nil {
				return nil, nil, err
			}
			tagFilter = append(tagFilter, filler)
		}
	}

	return metricsFilter, tagFilter, nil
}

// makePubOptions checks whether the key and value of options from user are legal.
// Then constructs and updates the options. If the option from user and is legal ,use it. Otherwise,
// use the original options in ALTER PUBLICATION SET OPTIONS.
func makePubOptions(
	pubOpts map[string]string, originOpts *cdcpb.PubOptions,
) (cdcpb.PubOptions, error) {
	var opts cdcpb.PubOptions
	if value, ok := pubOpts[optPublish]; ok {
		lowerValue := strings.ToLower(value)
		operations := strings.Split(lowerValue, ",")
		keys := make(map[string]bool)
		var list []string
		for _, op := range operations {
			op = strings.TrimSpace(op)
			if _, ok = publishOptional[op]; !ok {
				return opts, pgerror.Newf(pgcode.InvalidParameterValue,
					"%s parameter %q is invalid. publish only support insert, update, delete and all",
					optPublish, value)
			}
			if _, exist := keys[op]; exist {
				continue
			}
			if op == cdcpb.EventAll {
				list = []string{cdcpb.EventAll}
				break
			}
			list = append(list, op)
			keys[op] = true
		}
		if len(list) > 0 {
			opts.Publish = strings.Join(list, ",")
		} else {
			return opts, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid for publication", optPublish, value)
		}
	} else {
		if originOpts != nil {
			opts.Publish = originOpts.Publish
		} else {
			opts.Publish = defaultPublish
		}
	}

	if value, ok := pubOpts[optBufferSize]; ok {
		num, err := strconv.Atoi(value)
		if err != nil {
			return opts, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", optBufferSize, value)
		}
		if num < 0 || num > 1024 {
			return opts, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q must between 0 and 1024", optBufferSize, value)
		}
		opts.BufferSize = num
	} else {
		if originOpts != nil {
			opts.BufferSize = originOpts.BufferSize
		} else {
			opts.BufferSize = defaultBufferSize
		}
	}

	if value, ok := pubOpts[optSubTimeout]; ok {
		num, err := strconv.Atoi(value)
		if err != nil {
			return opts, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", optSubTimeout, value)
		}
		if num < 0 || num > 1024 {
			return opts, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q must between 0 and 1024", optSubTimeout, value)
		}
		opts.SubTimeout = num
	} else {
		if originOpts != nil {
			opts.SubTimeout = originOpts.SubTimeout
		} else {
			opts.SubTimeout = defaultSubTimeout
		}
	}

	if value, ok := pubOpts[optCheckTag]; ok {
		lowerValue := strings.ToLower(value)
		switch lowerValue {
		case optOn, optOff:
			opts.CheckTag = lowerValue
		default:
			return opts, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", optCheckTag, value)
		}
	} else {
		if originOpts != nil {
			opts.CheckTag = originOpts.CheckTag
		} else {
			opts.CheckTag = optOn
		}
	}

	return opts, nil
}
