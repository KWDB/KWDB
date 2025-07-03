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
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type renameDatabaseNode struct {
	dbDesc  *sqlbase.DatabaseDescriptor
	newName string
}

// RenameDatabase renames the database.
// Privileges: superuser, DROP on source database.
//
//	Notes: postgres requires superuser, db owner, or "CREATEDB".
//	       mysql >= 5.1.23 does not allow database renames.
func (p *planner) RenameDatabase(ctx context.Context, n *tree.RenameDatabase) (planNode, error) {
	if n.Name == "" || n.NewName == "" {
		return nil, errEmptyDatabaseName
	}

	if string(n.Name) == p.SessionData().Database && p.SessionData().SafeUpdates {
		return nil, pgerror.DangerousStatementf("RENAME DATABASE on current database")
	}

	if err := p.RequireAdminRole(ctx, "ALTER DATABASE ... RENAME"); err != nil {
		return nil, err
	}

	dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, string(n.Name), true /*required*/)
	if err != nil {
		return nil, err
	}
	if dbDesc.EngineType == tree.EngineTypeTimeseries {
		if sqlbase.ContainsNonAlphaNumSymbol(n.NewName.String()) {
			return nil, sqlbase.NewTSNameInvalidError(n.NewName.String())
		}
		if len(n.NewName) > MaxTSDBNameLength {
			return nil, sqlbase.NewTSNameOutOfLengthError("database", string(n.NewName), MaxTSDBNameLength)
		}
	}
	if err := p.CheckPrivilege(ctx, dbDesc, privilege.DROP); err != nil {
		return nil, err
	}

	if n.Name == n.NewName {
		// Noop.
		return newZeroNode(nil /* columns */), nil
	}

	return &renameDatabaseNode{
		dbDesc:  dbDesc,
		newName: string(n.NewName),
	}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because RENAME DATABASE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *renameDatabaseNode) ReadingOwnWrites() {}

func (n *renameDatabaseNode) startExec(params runParams) error {
	if n.dbDesc.ReplicateFrom != "" {
		return errors.Newf("ALTER REPLICATION DATABASEDB IS NOT ALLOWED")
	}
	log.Infof(params.ctx, "rename database %s start, type: %s, id: %d", n.dbDesc.Name, tree.EngineName(n.dbDesc.EngineType), n.dbDesc.ID)
	p := params.p
	ctx := params.ctx
	dbDesc := n.dbDesc

	// Check if any other tables depend on tables in the database.
	// Because our views and sequence defaults are currently just stored as
	// strings, they (may) explicitly specify the database name.
	// Rather than trying to rewrite them with the changed DB name, we
	// simply disallow such renames for now.
	// See #34416.
	phyAccessor := p.PhysicalSchemaAccessor()
	lookupFlags := p.CommonLookupFlags(true /*required*/)
	// DDL statements bypass the cache.
	lookupFlags.AvoidCached = true
	schemas, err := p.Tables().getSchemasForDatabase(ctx, p.txn, dbDesc.ID)
	if err != nil {
		return err
	}
	for _, schema := range schemas {
		tbNames, err := phyAccessor.GetObjectNames(
			ctx,
			p.txn,
			dbDesc,
			schema,
			tree.DatabaseListFlags{
				CommonLookupFlags: lookupFlags,
				ExplicitPrefix:    true,
			},
		)
		if err != nil {
			return err
		}
		lookupFlags.Required = false
		for i := range tbNames {
			objDesc, err := phyAccessor.GetObjectDesc(ctx, p.txn, p.ExecCfg().Settings,
				&tbNames[i], tree.ObjectLookupFlags{CommonLookupFlags: lookupFlags, IncludeOffline: true})
			if err != nil {
				return err
			}
			if objDesc == nil {
				continue
			}
			tbDesc := objDesc.TableDesc()
			// check table state
			if tbDesc.State == sqlbase.TableDescriptor_OFFLINE {
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"cannot rename a database with OFFLINE tables, ensure %s is"+
						" dropped or made public before renaming database %s",
					tbDesc.GetName(), tree.AsString((*tree.Name)(&dbDesc.Name)))
			}
			for _, dependedOn := range tbDesc.DependedOnBy {
				found, procName, err := params.p.GetProcedureNameByID(ctx, dependedOn.ID)
				if err != nil {
					return err
				}
				if found {
					err = params.p.TryPurgeProcedureCache(ctx, procName, uint32(dependedOn.ID))
					if err != nil {
						return err
					}
					continue
				}
				dependentDesc, err := sqlbase.GetTableDescFromID(ctx, p.txn, dependedOn.ID)
				if err != nil {
					return err
				}

				isAllowed, referencedCol, err := isAllowedDependentDescInRenameDatabase(
					dependedOn,
					tbDesc,
					dependentDesc,
					dbDesc.Name,
				)
				if err != nil {
					return err
				}
				if isAllowed {
					continue
				}

				tbTableName := tree.MakeTableNameWithSchema(
					tree.Name(dbDesc.Name),
					tree.Name(schema),
					tree.Name(tbDesc.Name),
				)
				var dependentDescQualifiedString string
				if dbDesc.ID != dependentDesc.ParentID || tbDesc.GetParentSchemaID() != dependentDesc.GetParentSchemaID() {
					var err error
					dependentDescQualifiedString, err = p.getQualifiedTableName(ctx, dependentDesc)
					if err != nil {
						log.Warningf(
							ctx,
							"unable to retrieve fully-qualified name of %s (id: %d): %v",
							tbTableName.String(),
							dependentDesc.ID,
							err,
						)
						msg := fmt.Sprintf(
							"cannot rename database because a relation depends on relation %q",
							tbTableName.String(),
						)
						return sqlbase.NewDependentObjectError(msg)
					}
				} else {
					dependentDescTableName := tree.MakeTableNameWithSchema(
						tree.Name(dbDesc.Name),
						tree.Name(schema),
						tree.Name(dependentDesc.Name),
					)
					dependentDescQualifiedString = dependentDescTableName.String()
				}
				msg := fmt.Sprintf(
					"cannot rename database because relation %q depends on relation %q",
					dependentDescQualifiedString,
					tbTableName.String(),
				)

				// We can have a more specific error message for sequences.
				if tbDesc.IsSequence() {
					hint := fmt.Sprintf(
						"you can drop the column default %q of %q referencing %q",
						referencedCol,
						tbTableName.String(),
						dependentDescQualifiedString,
					)
					if dependentDesc.GetParentID() == dbDesc.ID {
						hint += fmt.Sprintf(
							" or modify the default to not reference the database name %q",
							dbDesc.Name,
						)
					}
					return sqlbase.NewDependentObjectErrorWithHint(msg, hint)
				}

				// Otherwise, we default to the view error message.
				hint := fmt.Sprintf("you can drop %q instead", dependentDescQualifiedString)
				return sqlbase.NewDependentObjectErrorWithHint(msg, hint)
			}
		}
	}

	if err := p.renameDatabase(ctx, dbDesc, n.newName); err != nil {
		return err
	}

	p.SetAuditTarget(uint32(dbDesc.GetID()), dbDesc.GetName(), nil)
	log.Infof(params.ctx, "rename database %s finished, type: %s, id: %d", n.dbDesc.Name, tree.EngineName(n.dbDesc.EngineType), n.dbDesc.ID)
	return nil
}

// isAllowedDependentDescInRename determines when rename database is allowed with
// a given {tbDesc, dependentDesc} with the relationship dependedOn on a db named dbName.
// Returns a bool representing whether it's allowed, a string indicating the column name
// found to contain the database (if it exists), and an error if any.
// This is a workaround for #45411 until #34416 is resolved.
func isAllowedDependentDescInRenameDatabase(
	dependedOn sqlbase.TableDescriptor_Reference,
	tbDesc *sqlbase.TableDescriptor,
	dependentDesc *sqlbase.TableDescriptor,
	dbName string,
) (bool, string, error) {
	// If it is a sequence, and it does not contain the database name, then we have
	// no reason to block it's deletion.
	if !tbDesc.IsSequence() {
		return false, "", nil
	}

	colIDs := util.MakeFastIntSet()
	for _, colID := range dependedOn.ColumnIDs {
		colIDs.Add(int(colID))
	}

	for _, column := range dependentDesc.Columns {
		if !colIDs.Contains(int(column.ID)) {
			continue
		}
		colIDs.Remove(int(column.ID))

		if column.DefaultExpr == nil {
			return false, "", errors.AssertionFailedf(
				"rename_database: expected column id %d in table id %d to have a default expr",
				dependedOn.ID,
				dependentDesc.ID,
			)
		}
		// Try parse the default expression and find the table name direct reference.
		parsedExpr, err := parser.ParseExpr(*column.DefaultExpr)
		if err != nil {
			return false, "", err
		}
		typedExpr, err := tree.TypeCheck(parsedExpr, nil, &column.Type)
		if err != nil {
			return false, "", err
		}
		seqNames, err := getUsedSequenceNames(typedExpr)
		if err != nil {
			return false, "", err
		}
		for _, seqName := range seqNames {
			parsedSeqName, err := parser.ParseTableName(seqName)
			if err != nil {
				return false, "", err
			}
			// There must be at least two parts for this to work.
			if parsedSeqName.NumParts >= 2 {
				// We only don't allow this if the database name is in there.
				// This is always the last argument.
				if tree.Name(parsedSeqName.Parts[parsedSeqName.NumParts-1]).Normalize() == tree.Name(dbName).Normalize() {
					return false, column.Name, nil
				}
			}
		}
	}
	if colIDs.Len() > 0 {
		return false, "", errors.AssertionFailedf(
			"expected to find column ids %s in table id %d",
			colIDs.String(),
			dependentDesc.ID,
		)
	}
	return true, "", nil
}

func (n *renameDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameDatabaseNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameDatabaseNode) Close(context.Context)        {}
