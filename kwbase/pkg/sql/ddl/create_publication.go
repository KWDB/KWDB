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

package ddl

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/metadata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/util/json"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

// publishOptional stores the optional operation about options in publication
var publishOptional = map[string]interface{}{
	"insert": nil,
	"update": nil,
	"delete": nil,
	"all":    nil,
}
var pubOptionExpectValues = map[string]sqlconst.KVStringOptValidate{
	sqlconst.OptPublish:    sqlconst.KVStringOptRequireValue,
	sqlconst.OptCheckTag:   sqlconst.KVStringOptRequireValue,
	sqlconst.OptBufferSize: sqlconst.KVStringOptRequireValue,
	sqlconst.OptSubTimeout: sqlconst.KVStringOptRequireValue,
}

type createPubNode struct {
	n             *tree.CreatePublication
	databaseID    uint64
	tableDescList []*MutableTableDescriptor
	pubOpts       func() (map[string]string, error)
}

// NewCreatePubNode creates a new createPubNode. This func is added only for testing.
// nolint:unexportedreturn
func NewCreatePubNode(
	n *tree.CreatePublication,
	databaseID uint64,
	tableDescList []*MutableTableDescriptor,
	pubOpts func() (map[string]string, error),
) *createPubNode {
	return &createPubNode{
		n:             n,
		databaseID:    databaseID,
		tableDescList: tableDescList,
		pubOpts:       pubOpts,
	}
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
	cdcWatermarkList []metadata.CDCWatermark

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
func CreatePublication(
	ctx context.Context, p *GenericPlanner, n *tree.CreatePublication,
) (sql.PlanNode, error) {
	found, err := checkPubByName(ctx, p, n.PubName)
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
		schemas, err := p.GetSchemasForDatabase(ctx, p.Txn(), dbDesc.ID)
		if err != nil {
			return nil, err
		}

		// the names of all objects in the target database
		for _, schema := range schemas {
			toAppend, err := sql.GetObjectNames(
				ctx, p.Txn(), p, dbDesc, schema, true, /*explicitPrefix*/
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
			ctx, &n.TableNames[i], true /*required*/, sql.ResolveRequireTableDesc,
		)
		if err != nil {
			return nil, err
		}

		if !tableDescList[i].IsTSTable() {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "publication is only used on ts table. but %s is relational table", tableDescList[i].GetName())
		}

		tableID := uint64(tableDescList[i].ID)
		isSubTarget, err := p.TableHasCDC(ctx, sqlbase.CDCInstanceType_Subscription, &tableID)
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

		if err = checkPrivilegeForPubSub(ctx, p, tableDescList[i], privilege.SELECT); err != nil {
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
func checkPubByName(ctx context.Context, p sql.PlanHookState, pubName tree.Name) (bool, error) {
	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"check-pub",
		p.Txn(),
		InternalExecutorSessionDataOverride{User: security.RootUser},
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
func loadPubByName(
	ctx context.Context, p sql.PlanHookState, pubName tree.Name,
) (*PubMetadata, error) {
	stmt := fmt.Sprintf(`SELECT id,name,parameters,create_at,create_by,database_id
FROM system.kwdb_publications WHERE name = '%s'`, pubName)
	return loadPub(ctx, p, stmt)
}

// loadPubByID loads metadata of publication from system.kwdb_publications with publication ID.
func loadPubByID(ctx context.Context, p sql.PlanHookState, pubID uint64) (*PubMetadata, error) {
	stmt := fmt.Sprintf(`SELECT id,name,parameters,create_at,create_by,database_id
FROM system.kwdb_publications WHERE id = %d`, pubID)
	return loadPub(ctx, p, stmt)
}

// loadPub loads metadata of publication with the specified statement.
func loadPub(ctx context.Context, p sql.PlanHookState, stmt string) (*PubMetadata, error) {
	var metadata PubMetadata
	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx,
		"load-pub",
		p.Txn(),
		InternalExecutorSessionDataOverride{User: security.RootUser},
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
func checkPrivilegeForPubSub(
	ctx context.Context, p sql.PlanHookState, tableDesc sqlbase.DescriptorProto, priv privilege.Kind,
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

func (n *createPubNode) StartExec(params RunParams) (err error) {
	pubTableInfos, _, err := sql.MakeCDCTableInfo(
		params.Ctx, params.GetPlanner(), n.tableDescList, n.n.Star, n.n.ColNames, true)
	if err != nil {
		return err
	}

	if n.n.Where != nil {
		whereNeedNormalTag, err := sql.CheckWhereExprForCDC(
			params.Ctx, params.GetPlanner(), n.n.Table, n.tableDescList[0].TableDescriptor, n.n.Where.Expr)
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
		pubTableInfos[i].NeedNormalTag = pubTableInfos[i].NeedNormalTag && options.CheckTag == sqlconst.OptOn
	}

	para := cdcpb.PubParameters{
		TableList:  pubTableInfos,
		PubOptions: options,
	}

	parameters, err := cdcpb.MarshalPubParameters(para)
	if err != nil {
		return err
	}

	md := PubMetadata{
		name:       n.n.PubName,
		parameters: parameters,
		databaseID: n.databaseID,
		createBy:   params.GetPlanner().User(),
		createAt:   tree.DTimestamp{Time: timeutil.Now()},
	}

	if _, err = params.ExecCfg().InternalExecutor.ExecEx(
		params.Ctx,
		"write-pub-metadata",
		params.PlannerTxn(),
		InternalExecutorSessionDataOverride{User: security.RootUser},
		`INSERT INTO system.kwdb_publications
(name,parameters,create_by,create_at,database_id)
values ($1,$2,$3,$4,$5)`,
		md.name, md.parameters, md.createBy, md.createAt.Time, md.databaseID,
	); err != nil {
		return err
	}

	pubSchema, err := loadPubByName(params.Ctx, params.GetPlanner(), n.n.PubName)
	if err != nil {
		return err
	}

	for i := range para.TableList {
		if err = params.GetPlanner().AddCDCDescriptorByTableID(
			params.Ctx, para.TableList[i].ID, sqlbase.CDCInstanceType_Publication,
			pubSchema.id, []byte(pubSchema.name),
		); err != nil {
			return err
		}

		if err = params.GetPlanner().AddCDCWatermark(params.Ctx, metadata.CDCWatermark{
			TableID:      para.TableList[i].ID,
			TaskID:       pubSchema.id,
			TaskType:     sqlbase.CDCInstanceType_Publication,
			LowWatermark: cdcpb.InvalidWatermark,
		}); err != nil {
			return err
		}
	}

	params.GetPlanner().SetAuditTarget(uint32(pubSchema.id), pubSchema.name.String(), nil)

	return err
}

func (n *createPubNode) Next(_ RunParams) (bool, error) {
	return false, nil
}

func (n *createPubNode) Values() tree.Datums { return nil }

func (n *createPubNode) Close(context.Context) {}

// makePubOptions checks whether the key and value of options from user are legal.
// Then constructs and updates the options. If the option from user and is legal ,use it. Otherwise,
// use the original options in ALTER PUBLICATION SET OPTIONS.
func makePubOptions(
	pubOpts map[string]string, originOpts *cdcpb.PubOptions,
) (cdcpb.PubOptions, error) {
	var opts cdcpb.PubOptions
	if value, ok := pubOpts[sqlconst.OptPublish]; ok {
		lowerValue := strings.ToLower(value)
		operations := strings.Split(lowerValue, ",")
		keys := make(map[string]bool)
		var list []string
		for _, op := range operations {
			op = strings.TrimSpace(op)
			if _, ok = publishOptional[op]; !ok {
				return opts, pgerror.Newf(pgcode.InvalidParameterValue,
					"%s parameter %q is invalid. publish only support insert, update, delete and all",
					sqlconst.OptPublish, value)
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
			return opts, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid for publication", sqlconst.OptPublish, value)
		}
	} else {
		if originOpts != nil {
			opts.Publish = originOpts.Publish
		} else {
			opts.Publish = sqlconst.DefaultPublish
		}
	}

	if value, ok := pubOpts[sqlconst.OptBufferSize]; ok {
		num, err := strconv.Atoi(value)
		if err != nil {
			return opts, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", sqlconst.OptBufferSize, value)
		}
		if num < 0 || num > 1024 {
			return opts, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q must between 0 and 1024", sqlconst.OptBufferSize, value)
		}
		opts.BufferSize = num
	} else {
		if originOpts != nil {
			opts.BufferSize = originOpts.BufferSize
		} else {
			opts.BufferSize = sqlconst.DefaultBufferSize
		}
	}

	if value, ok := pubOpts[sqlconst.OptSubTimeout]; ok {
		num, err := strconv.Atoi(value)
		if err != nil {
			return opts, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", sqlconst.OptSubTimeout, value)
		}
		if num < 0 || num > 1024 {
			return opts, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q must between 0 and 1024", sqlconst.OptSubTimeout, value)
		}
		opts.SubTimeout = num
	} else {
		if originOpts != nil {
			opts.SubTimeout = originOpts.SubTimeout
		} else {
			opts.SubTimeout = sqlconst.DefaultSubTimeout
		}
	}

	if value, ok := pubOpts[sqlconst.OptCheckTag]; ok {
		lowerValue := strings.ToLower(value)
		switch lowerValue {
		case sqlconst.OptOn, sqlconst.OptOff:
			opts.CheckTag = lowerValue
		default:
			return opts, pgerror.Newf(pgcode.InvalidParameterValue, "%s parameter %q is invalid", sqlconst.OptCheckTag, value)
		}
	} else {
		if originOpts != nil {
			opts.CheckTag = originOpts.CheckTag
		} else {
			opts.CheckTag = sqlconst.OptOn
		}
	}

	return opts, nil
}
