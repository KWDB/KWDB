// Copyright 2016 The Cockroach Authors.
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
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/sql/vtable"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const (
	informationSchemaName = "information_schema"
	pgCatalogName         = sessiondata.PgCatalogName
	engineTimeseries      = "TIME SERIES"
	engineRelational      = "RELATIONAL"
)

var pgCatalogNameDString = tree.NewDString(pgCatalogName)

// informationSchema lists all the table definitions for
// information_schema.
var informationSchema = virtualSchema{
	name: informationSchemaName,
	allTableNames: buildStringSet(
		// Generated with:
		// select distinct '"'||table_name||'",' from information_schema.tables
		//    where table_schema='information_schema' order by table_name;
		"_pg_foreign_data_wrappers",
		"_pg_foreign_servers",
		"_pg_foreign_table_columns",
		"_pg_foreign_tables",
		"_pg_user_mappings",
		"administrable_role_authorizations",
		"applicable_roles",
		"attributes",
		"character_sets",
		"check_constraint_routine_usage",
		"check_constraints",
		"collation_character_set_applicability",
		"collations",
		"column_domain_usage",
		"column_options",
		"column_privileges",
		"column_udt_usage",
		"columns",
		"constraint_column_usage",
		"constraint_table_usage",
		"data_type_privileges",
		"domain_constraints",
		"domain_udt_usage",
		"domains",
		"element_types",
		"enabled_roles",
		"foreign_data_wrapper_options",
		"foreign_data_wrappers",
		"foreign_server_options",
		"foreign_servers",
		"foreign_table_options",
		"foreign_tables",
		"information_schema_catalog_name",
		"key_column_usage",
		"keywords",
		"parameters",
		"referential_constraints",
		"role_column_grants",
		"role_routine_grants",
		"role_table_grants",
		"role_udt_grants",
		"role_usage_grants",
		"routine_privileges",
		"routines",
		"schemata",
		"sequences",
		"sql_features",
		"sql_implementation_info",
		"sql_languages",
		"sql_packages",
		"sql_parts",
		"sql_sizing",
		"sql_sizing_profiles",
		"table_constraints",
		"table_privileges",
		"tables",
		"transforms",
		"triggered_update_columns",
		"triggers",
		"udt_privileges",
		"usage_privileges",
		"user_defined_types",
		"user_mapping_options",
		"user_mappings",
		"view_column_usage",
		"view_routine_usage",
		"view_table_usage",
		"views",
	),
	tableDefs: map[sqlbase.ID]virtualSchemaDef{
		sqlbase.InformationSchemaAdministrableRoleAuthorizationsID: informationSchemaAdministrableRoleAuthorizations,
		sqlbase.InformationSchemaApplicableRolesID:                 informationSchemaApplicableRoles,
		sqlbase.InformationSchemaCheckConstraints:                  informationSchemaCheckConstraints,
		sqlbase.InformationSchemaColumnPrivilegesID:                informationSchemaColumnPrivileges,
		sqlbase.InformationSchemaColumnsTableID:                    informationSchemaColumnsTable,
		sqlbase.InformationSchemaConstraintColumnUsageTableID:      informationSchemaConstraintColumnUsageTable,
		sqlbase.InformationSchemaEnabledRolesID:                    informationSchemaEnabledRoles,
		sqlbase.InformationSchemaKeyColumnUsageTableID:             informationSchemaKeyColumnUsageTable,
		sqlbase.InformationSchemaKeywordsTableID:                   informationSchemaKeywordsTable,
		sqlbase.InformationSchemaParametersTableID:                 informationSchemaParametersTable,
		sqlbase.InformationSchemaReferentialConstraintsTableID:     informationSchemaReferentialConstraintsTable,
		sqlbase.InformationSchemaRoleColumnGrantsID:                informationSchemaRoleColumnGrants,
		sqlbase.InformationSchemaRoleTableGrantsID:                 informationSchemaRoleTableGrants,
		sqlbase.InformationSchemaRoutineTableID:                    informationSchemaRoutineTable,
		sqlbase.InformationSchemaSchemataTableID:                   informationSchemaSchemataTable,
		sqlbase.InformationSchemaSchemataTablePrivilegesID:         informationSchemaSchemataTablePrivileges,
		sqlbase.InformationSchemaSequencesID:                       informationSchemaSequences,
		sqlbase.InformationSchemaStatisticsTableID:                 informationSchemaStatisticsTable,
		sqlbase.InformationSchemaTableConstraintTableID:            informationSchemaTableConstraintTable,
		sqlbase.InformationSchemaTablePrivilegesID:                 informationSchemaTablePrivileges,
		sqlbase.InformationSchemaTablesTableID:                     informationSchemaTablesTable,
		sqlbase.InformationSchemaViewsTableID:                      informationSchemaViewsTable,
		sqlbase.InformationSchemaViewTableUsageID:                  informationSchemaViewTableUsage,
		sqlbase.InformationSchemaTriggersTableID:                   informationSchemaTriggersTable,
		sqlbase.InformationSchemaUserPrivilegesID:                  informationSchemaUserPrivileges,
		sqlbase.InformationSchemaProcedurePrivilegesID:             informationSchemaProcedurePrivileges,
		sqlbase.InformationSchemaRoleRoutineGrantsID:               informationSchemaRoleRoutineGrants,
	},
	tableValidator:             validateInformationSchemaTable,
	validWithNoDatabaseContext: true,
}

func buildStringSet(ss ...string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, s := range ss {
		m[s] = struct{}{}
	}
	return m
}

var (
	emptyString = tree.NewDString("")
	// information_schema was defined before the BOOLEAN data type was added to
	// the SQL specification. Because of this, boolean values are represented as
	// STRINGs. The BOOLEAN data type should NEVER be used in information_schema
	// tables. Instead, define columns as STRINGs and map bools to STRINGs using
	// yesOrNoDatum.
	yesString = tree.NewDString("YES")
	noString  = tree.NewDString("NO")
)

func yesOrNoDatum(b bool) tree.Datum {
	if b {
		return yesString
	}
	return noString
}

// getIndexComments returns all index comments in the database. An index
// comment is represented by its table ID, index ID, and comment datum.
func getIndexComments(
	ctx context.Context, p *planner,
) (map[sqlbase.ID]map[sqlbase.IndexID]tree.Datum, error) {
	rows, err := p.extendedEvalCtx.ExecCfg.InternalExecutor.Query(
		ctx,
		"select-index-comments",
		p.Txn(),
		`SELECT object_id, sub_id, "comment" FROM system.comments WHERE type = $1`,
		keys.IndexCommentType,
	)
	if err != nil {
		return nil, err
	}

	comments := make(map[sqlbase.ID]map[sqlbase.IndexID]tree.Datum)
	for _, row := range rows {
		tableID := sqlbase.ID(tree.MustBeDInt(row[0]))
		indexID := sqlbase.IndexID(tree.MustBeDInt(row[1]))
		if comments[tableID] == nil {
			comments[tableID] = make(map[sqlbase.IndexID]tree.Datum)
		}
		comments[tableID][indexID] = row[2]
	}
	return comments, nil
}

func dNameOrNull(s string) tree.Datum {
	if s == "" {
		return tree.DNull
	}
	return tree.NewDName(s)
}

func dStringPtrOrEmpty(s *string) tree.Datum {
	if s == nil {
		return emptyString
	}
	return tree.NewDString(*s)
}

func dStringPtrOrNull(s *string) tree.Datum {
	if s == nil {
		return tree.DNull
	}
	return tree.NewDString(*s)
}

func dIntFnOrNull(fn func() (int32, bool)) tree.Datum {
	if n, ok := fn(); ok {
		return tree.NewDInt(tree.DInt(n))
	}
	return tree.DNull
}

func validateInformationSchemaTable(table *sqlbase.TableDescriptor) error {
	// Make sure no tables have boolean columns.
	for i := range table.Columns {
		if table.Columns[i].Type.Family() == types.BoolFamily {
			return errors.Errorf("information_schema tables should never use BOOL columns. "+
				"See the comment about yesOrNoDatum. Found BOOL column in %s.", table.Name)
		}
	}
	return nil
}

var informationSchemaAdministrableRoleAuthorizations = virtualSchemaTable{
	comment: `roles for which the current user has admin option
` + base.DocsURL("information-schema.html#administrable_role_authorizations") + `
https://www.postgresql.org/docs/9.5/infoschema-administrable-role-authorizations.html`,
	schema: vtable.InformationSchemaAdministrableRoleAuthorizations,
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		currentUser := p.SessionData().User
		memberMap, err := p.MemberOfWithAdminOption(ctx, currentUser)
		if err != nil {
			return err
		}

		grantee := tree.NewDString(currentUser)
		for roleName, isAdmin := range memberMap {
			if !isAdmin {
				// We only show memberships with the admin option.
				continue
			}

			if err := addRow(
				grantee,                   // grantee: always the current user
				tree.NewDString(roleName), // role_name
				yesString,                 // is_grantable: always YES
			); err != nil {
				return err
			}
		}

		return nil
	},
}

var informationSchemaApplicableRoles = virtualSchemaTable{
	comment: `roles available to the current user
` + base.DocsURL("information-schema.html#applicable_roles") + `
https://www.postgresql.org/docs/9.5/infoschema-applicable-roles.html`,
	schema: vtable.InformationSchemaApplicableRoles,
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		currentUser := p.SessionData().User
		memberMap, err := p.MemberOfWithAdminOption(ctx, currentUser)
		if err != nil {
			return err
		}

		grantee := tree.NewDString(currentUser)

		for roleName, isAdmin := range memberMap {
			if err := addRow(
				grantee,                   // grantee: always the current user
				tree.NewDString(roleName), // role_name
				yesOrNoDatum(isAdmin),     // is_grantable
			); err != nil {
				return err
			}
		}

		return nil
	},
}

var informationSchemaCheckConstraints = virtualSchemaTable{
	comment: `check constraints
` + base.DocsURL("information-schema.html#check_constraints") + `
https://www.postgresql.org/docs/9.5/infoschema-check-constraints.html`,
	schema: vtable.InformationSchemaCheckConstraints,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDescWithTableLookup(ctx, p, dbContext, hideVirtual /* no constraints in virtual tables */, func(
			db *sqlbase.DatabaseDescriptor,
			scName string,
			table *sqlbase.TableDescriptor,
			tableLookup tableLookupFn,
		) error {
			conInfo, err := table.GetConstraintInfoWithLookup(tableLookup.getTableByID)
			if err != nil {
				return err
			}
			dbNameStr := tree.NewDString(db.Name)
			scNameStr := tree.NewDString(scName)
			for conName, con := range conInfo {
				// Only Check constraints are included.
				if con.Kind != sqlbase.ConstraintTypeCheck {
					continue
				}
				conNameStr := tree.NewDString(conName)
				// Like with pg_catalog.pg_constraint, Postgres wraps the check
				// constraint expression in two pairs of parentheses.
				chkExprStr := tree.NewDString(fmt.Sprintf("((%s))", con.Details))
				if err := addRow(
					dbNameStr,  // constraint_catalog
					scNameStr,  // constraint_schema
					conNameStr, // constraint_name
					chkExprStr, // check_clause
				); err != nil {
					return err
				}
			}

			// Unlike with pg_catalog.pg_constraint, Postgres also includes NOT
			// NULL column constraints in information_schema.check_constraints.
			// Cockroach doesn't track these constraints as check constraints,
			// but we can pull them off of the table's column descriptors.
			colNum := 0
			return forEachColumnInTable(table, func(column *sqlbase.ColumnDescriptor) error {
				colNum++
				// Only visible, non-nullable columns are included.
				if column.Hidden || column.Nullable {
					return nil
				}
				// Generate a unique name for each NOT NULL constraint. Postgres
				// uses the format <namespace_oid>_<table_oid>_<col_idx>_not_null.
				// We might as well do the same.
				conNameStr := tree.NewDString(fmt.Sprintf(
					"%s_%s_%d_not_null", h.NamespaceOid(db, scName), defaultOid(table.ID), colNum,
				))
				chkExprStr := tree.NewDString(fmt.Sprintf(
					"%s IS NOT NULL", column.Name,
				))
				return addRow(
					dbNameStr,  // constraint_catalog
					scNameStr,  // constraint_schema
					conNameStr, // constraint_name
					chkExprStr, // check_clause
				)
			})
		})
	},
}

// hasColumnPrivilege reports whether privileges effectively grant priv at column granularity.
// ALL is stored as an independent bit instead of setting each concrete privilege bit, so the
// column privilege views must treat it as implicitly granting SELECT, INSERT, and UPDATE.
func hasColumnPrivilege(privileges uint32, priv privilege.Kind) bool {
	return privileges&priv.Mask() != 0 || privileges&privilege.ALL.Mask() != 0
}

// canUserSeePrivilegeGrantee reports whether the current user may see a direct
// privilege record for grantee. Callers must apply object visibility separately.
func canUserSeePrivilegeGrantee(
	isAdmin bool, currentUser, grantee string, memberOf map[string]bool,
) bool {
	if isAdmin || grantee == currentUser || grantee == sqlbase.PublicRole {
		return true
	}
	_, isMember := memberOf[grantee]
	return isMember
}

var informationSchemaColumnPrivileges = virtualSchemaTable{
	comment: `column privilege grants (incomplete)
` + base.DocsURL("information-schema.html#column_privileges") + `
https://www.postgresql.org/docs/9.5/infoschema-column-privileges.html`,
	schema: vtable.InformationSchemaColumnPrivileges,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		isAdmin, err := p.HasAdminRole(ctx)
		if err != nil {
			return err
		}

		currentUser := p.SessionData().User
		var memberOf map[string]bool
		if !isAdmin {
			memberOf, err = p.MemberOfWithAdminOption(ctx, currentUser)
			if err != nil {
				return err
			}
		}

		return forEachTableDesc(ctx, p, dbContext, virtualMany, func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
			dbNameStr := tree.NewDString(db.Name)
			scNameStr := tree.NewDString(scName)
			columndata := privilege.List{privilege.SELECT, privilege.INSERT, privilege.UPDATE} // privileges for column level granularity
			for _, u := range table.Privileges.Users {
				if !canUserSeePrivilegeGrantee(isAdmin, currentUser, u.User, memberOf) {
					continue
				}

				for _, priv := range columndata {
					if hasColumnPrivilege(u.Privileges, priv) {
						if err := forEachVisibleColumn(table, func(column *sqlbase.ColumnDescriptor) error {
							return addRow(
								tree.DNull,                     // grantor
								tree.NewDString(u.User),        // grantee
								dbNameStr,                      // table_catalog
								scNameStr,                      // table_schema
								tree.NewDString(table.Name),    // table_name
								tree.NewDString(column.Name),   // column_name
								tree.NewDString(priv.String()), // privilege_type
								tree.DNull,                     // is_grantable
							)
						}); err != nil {
							return err
						}
					}
				}
			}
			return nil
		})
	},
}

var informationSchemaColumnsTable = virtualSchemaTable{
	comment: `table and view columns (incomplete)
` + base.DocsURL("information-schema.html#columns") + `
https://www.postgresql.org/docs/9.5/infoschema-columns.html`,
	schema: vtable.InformationSchemaColumns,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, dbContext, virtualMany, func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
			dbNameStr := tree.NewDString(db.Name)
			scNameStr := tree.NewDString(scName)
			return forEachColumnInTable(table, func(column *sqlbase.ColumnDescriptor) error {
				collationCatalog := tree.DNull
				collationSchema := tree.DNull
				collationName := tree.DNull
				if locale := column.Type.Locale(); locale != "" {
					collationCatalog = dbNameStr
					collationSchema = pgCatalogNameDString
					collationName = tree.NewDString(locale)
				}

				// hide the primary tag of the template table
				if table.TableType == tree.TemplateTable && column.TsCol.ColumnType == sqlbase.ColumnType_TYPE_PTAG {
					return nil
				}

				isTag := column.TsCol.ColumnType != sqlbase.ColumnType_TYPE_DATA
				return addRow(
					dbNameStr,                                            // table_catalog
					scNameStr,                                            // table_schema
					tree.NewDString(table.Name),                          // table_name
					tree.NewDString(column.Name),                         // column_name
					tree.NewDInt(tree.DInt(column.ID)),                   // ordinal_position
					dStringPtrOrNull(column.DefaultExpr),                 // column_default
					yesOrNoDatum(column.Nullable),                        // is_nullable
					tree.NewDString(column.Type.InformationSchemaName()), // data_type
					characterMaximumLength(&column.Type),                 // character_maximum_length
					characterOctetLength(&column.Type),                   // character_octet_length
					numericPrecision(&column.Type),                       // numeric_precision
					numericPrecisionRadix(&column.Type),                  // numeric_precision_radix
					numericScale(&column.Type),                           // numeric_scale
					datetimePrecision(&column.Type),                      // datetime_precision
					tree.DNull,                                           // interval_type
					tree.DNull,                                           // interval_precision
					tree.DNull,                                           // character_set_catalog
					tree.DNull,                                           // character_set_schema
					tree.DNull,                                           // character_set_name
					collationCatalog,                                     // collation_catalog
					collationSchema,                                      // collation_schema
					collationName,                                        // collation_name
					tree.DNull,                                           // domain_catalog
					tree.DNull,                                           // domain_schema
					tree.DNull,                                           // domain_name
					dbNameStr,                                            // udt_catalog
					pgCatalogNameDString,                                 // udt_schema
					tree.NewDString(column.Type.PGName()),                // udt_name
					tree.DNull,                                           // scope_catalog
					tree.DNull,                                           // scope_schema
					tree.DNull,                                           // scope_name
					tree.DNull,                                           // maximum_cardinality
					tree.DNull,                                           // dtd_identifier
					tree.DNull,                                           // is_self_referencing
					tree.DNull,                                           // is_identity
					tree.DNull,                                           // identity_generation
					tree.DNull,                                           // identity_start
					tree.DNull,                                           // identity_increment
					tree.DNull,                                           // identity_maximum
					tree.DNull,                                           // identity_minimum
					tree.DNull,                                           // identity_cycle
					yesOrNoDatum(column.IsComputed()),                    // is_generated
					dStringPtrOrEmpty(column.ComputeExpr),                // generation_expression
					yesOrNoDatum(table.IsTable() &&
						!table.IsVirtualTable() &&
						!column.IsComputed(),
					), // is_updatable
					yesOrNoDatum(column.Hidden),                             // is_hidden
					tree.NewDString(column.Type.SQLString()),                // kwdb_sql_type
					tree.NewDInt(tree.DInt(column.Type.InternalType.Width)), // storage_length
					yesOrNoDatum(isTag),                                     // is_tag
					noString,                                                // is_system_time_period_start
					noString,                                                // is_system_time_period_end
					tree.DNull,                                              // system_time_period_timestamp_generation
					tree.NewDString(column.Type.SQLString()),                // declared_data_type
					numericPrecision(&column.Type),                          // declared_numeric_precision
					numericScale(&column.Type),                              // declared_numeric_scale
				)
			})
		})
	},
}

var informationSchemaEnabledRoles = virtualSchemaTable{
	comment: `roles for the current user
` + base.DocsURL("information-schema.html#enabled_roles") + `
https://www.postgresql.org/docs/9.5/infoschema-enabled-roles.html`,
	schema: `
CREATE TABLE information_schema.enabled_roles (
	ROLE_NAME STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		currentUser := p.SessionData().User
		memberMap, err := p.MemberOfWithAdminOption(ctx, currentUser)
		if err != nil {
			return err
		}

		// The current user is always listed.
		if err := addRow(
			tree.NewDString(currentUser), // role_name: the current user
		); err != nil {
			return err
		}

		for roleName := range memberMap {
			if err := addRow(
				tree.NewDString(roleName), // role_name
			); err != nil {
				return err
			}
		}

		return nil
	},
}

// characterMaximumLength returns the declared maximum length of
// characters if the type is a character or bit string data
// type. Returns false if the data type is not a character or bit
// string, or if the string's length is not bounded.
func characterMaximumLength(colType *types.T) tree.Datum {
	return dIntFnOrNull(func() (int32, bool) {
		switch colType.Family() {
		case types.StringFamily, types.CollatedStringFamily, types.BitFamily:
			if colType.Width() > 0 {
				return colType.Width(), true
			}
		}
		return 0, false
	})
}

// characterOctetLength returns the maximum possible length in
// octets of a datum if the T is a character string. Returns
// false if the data type is not a character string, or if the
// string's length is not bounded.
func characterOctetLength(colType *types.T) tree.Datum {
	return dIntFnOrNull(func() (int32, bool) {
		switch colType.Family() {
		case types.StringFamily, types.CollatedStringFamily:
			if colType.Width() > 0 {
				return colType.Width() * utf8.UTFMax, true
			}
		}
		return 0, false
	})
}

// numericPrecision returns the declared or implicit precision of numeric
// data types. Returns false if the data type is not numeric, or if the precision
// of the numeric type is not bounded.
func numericPrecision(colType *types.T) tree.Datum {
	return dIntFnOrNull(func() (int32, bool) {
		switch colType.Family() {
		case types.IntFamily:
			return colType.Width(), true
		case types.FloatFamily:
			if colType.Width() == 32 {
				return 24, true
			}
			return 53, true
		case types.DecimalFamily:
			if colType.Precision() > 0 {
				return colType.Precision(), true
			}
		}
		return 0, false
	})
}

// numericPrecisionRadix returns the implicit precision radix of
// numeric data types. Returns false if the data type is not numeric.
func numericPrecisionRadix(colType *types.T) tree.Datum {
	return dIntFnOrNull(func() (int32, bool) {
		switch colType.Family() {
		case types.IntFamily:
			return 2, true
		case types.FloatFamily:
			return 2, true
		case types.DecimalFamily:
			return 10, true
		}
		return 0, false
	})
}

// NumericScale returns the declared or implicit precision of exact numeric
// data types. Returns false if the data type is not an exact numeric, or if the
// scale of the exact numeric type is not bounded.
func numericScale(colType *types.T) tree.Datum {
	return dIntFnOrNull(func() (int32, bool) {
		switch colType.Family() {
		case types.IntFamily:
			return 0, true
		case types.DecimalFamily:
			if colType.Precision() > 0 {
				return colType.Width(), true
			}
		}
		return 0, false
	})
}

func datetimePrecision(colType *types.T) tree.Datum {
	// We currently do not support a datetime precision.
	return tree.DNull
}

var informationSchemaKeywordsTable = virtualSchemaTable{
	comment: `SQL keywords`,
	schema:  vtable.InformationSchemaKeywords,
	populate: func(_ context.Context, _ *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		for _, keyword := range lex.KeywordNames {
			reserved := tree.DInt(1)
			if lex.KeywordsCategories[keyword] == "U" {
				reserved = 0
			}
			if err := addRow(
				tree.NewDString(keyword), // word
				tree.NewDInt(reserved),   // reserved
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var informationSchemaConstraintColumnUsageTable = virtualSchemaTable{
	comment: `columns usage by constraints
https://www.postgresql.org/docs/9.5/infoschema-constraint-column-usage.html`,
	schema: `
CREATE TABLE information_schema.constraint_column_usage (
	TABLE_CATALOG      STRING NOT NULL,
	TABLE_SCHEMA       STRING NOT NULL,
	TABLE_NAME         STRING NOT NULL,
	COLUMN_NAME        STRING NOT NULL,
	CONSTRAINT_CATALOG STRING NOT NULL,
	CONSTRAINT_SCHEMA  STRING NOT NULL,
	CONSTRAINT_NAME    STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDescWithTableLookup(ctx, p, dbContext, hideVirtual /* no constraints in virtual tables */, func(
			db *sqlbase.DatabaseDescriptor,
			scName string,
			table *sqlbase.TableDescriptor,
			tableLookup tableLookupFn,
		) error {
			conInfo, err := table.GetConstraintInfoWithLookup(tableLookup.getTableByID)
			if err != nil {
				return err
			}
			scNameStr := tree.NewDString(scName)
			dbNameStr := tree.NewDString(db.Name)

			for conName, con := range conInfo {
				conTable := table
				conCols := con.Columns
				conNameStr := tree.NewDString(conName)
				if con.Kind == sqlbase.ConstraintTypeFK {
					// For foreign key constraint, constraint_column_usage
					// identifies the table/columns that the foreign key
					// references.
					conTable = con.ReferencedTable
					conCols, err = conTable.NamesForColumnIDs(con.FK.ReferencedColumnIDs)
					if err != nil {
						return err
					}
				}
				tableNameStr := tree.NewDString(conTable.Name)
				for _, col := range conCols {
					if err := addRow(
						dbNameStr,            // table_catalog
						scNameStr,            // table_schema
						tableNameStr,         // table_name
						tree.NewDString(col), // column_name
						dbNameStr,            // constraint_catalog
						scNameStr,            // constraint_schema
						conNameStr,           // constraint_name
					); err != nil {
						return err
					}
				}
			}
			return nil
		})
	},
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/key-column-usage-table.html
var informationSchemaKeyColumnUsageTable = virtualSchemaTable{
	comment: `column usage by indexes and key constraints
` + base.DocsURL("information-schema.html#key_column_usage") + `
https://www.postgresql.org/docs/9.5/infoschema-key-column-usage.html`,
	schema: `
CREATE TABLE information_schema.key_column_usage (
	CONSTRAINT_CATALOG STRING NOT NULL,
	CONSTRAINT_SCHEMA  STRING NOT NULL,
	CONSTRAINT_NAME    STRING NOT NULL,
	TABLE_CATALOG      STRING NOT NULL,
	TABLE_SCHEMA       STRING NOT NULL,
	TABLE_NAME         STRING NOT NULL,
	COLUMN_NAME        STRING NOT NULL,
	ORDINAL_POSITION   INT8 NOT NULL,
	POSITION_IN_UNIQUE_CONSTRAINT INT8
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDescWithTableLookup(ctx, p, dbContext, hideVirtual /* no constraints in virtual tables */, func(
			db *sqlbase.DatabaseDescriptor,
			scName string,
			table *sqlbase.TableDescriptor,
			tableLookup tableLookupFn,
		) error {
			conInfo, err := table.GetConstraintInfoWithLookup(tableLookup.getTableByID)
			if err != nil {
				return err
			}
			dbNameStr := tree.NewDString(db.Name)
			scNameStr := tree.NewDString(scName)
			tbNameStr := tree.NewDString(table.Name)
			for conName, con := range conInfo {
				// Only Primary Key, Foreign Key, and Unique constraints are included.
				switch con.Kind {
				case sqlbase.ConstraintTypePK:
				case sqlbase.ConstraintTypeFK:
				case sqlbase.ConstraintTypeUnique:
				default:
					continue
				}

				cstNameStr := tree.NewDString(conName)

				for pos, col := range con.Columns {
					ordinalPos := tree.NewDInt(tree.DInt(pos + 1))
					uniquePos := tree.DNull
					if con.Kind == sqlbase.ConstraintTypeFK {
						uniquePos = ordinalPos
					}
					if err := addRow(
						dbNameStr,            // constraint_catalog
						scNameStr,            // constraint_schema
						cstNameStr,           // constraint_name
						dbNameStr,            // table_catalog
						scNameStr,            // table_schema
						tbNameStr,            // table_name
						tree.NewDString(col), // column_name
						ordinalPos,           // ordinal_position, 1-indexed
						uniquePos,            // position_in_unique_constraint
					); err != nil {
						return err
					}
				}
			}
			return nil
		})
	},
}

// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-parameters.html
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/parameters-table.html
var informationSchemaParametersTable = virtualSchemaTable{
	comment: `user-defined function and procedure parameters
https://www.postgresql.org/docs/9.5/infoschema-parameters.html`,
	schema: `
CREATE TABLE information_schema.parameters (
	SPECIFIC_CATALOG STRING,
	SPECIFIC_SCHEMA STRING,
	SPECIFIC_NAME STRING,
	ORDINAL_POSITION INT8,
	PARAMETER_MODE STRING,
	IS_RESULT STRING,
	AS_LOCATOR STRING,
	PARAMETER_NAME STRING,
	FROM_SQL_SPECIFIC_CATALOG STRING,
	FROM_SQL_SPECIFIC_SCHEMA STRING,
	FROM_SQL_SPECIFIC_NAME STRING,
	TO_SQL_SPECIFIC_CATALOG STRING,
	TO_SQL_SPECIFIC_SCHEMA STRING,
	TO_SQL_SPECIFIC_NAME STRING,
	DATA_TYPE STRING,
	CHARACTER_MAXIMUM_LENGTH INT8,
	CHARACTER_OCTET_LENGTH INT8,
	CHARACTER_SET_CATALOG STRING,
	CHARACTER_SET_SCHEMA STRING,
	CHARACTER_SET_NAME STRING,
	COLLATION_CATALOG STRING,
	COLLATION_SCHEMA STRING,
	COLLATION_NAME STRING,
	NUMERIC_PRECISION INT8,
	NUMERIC_PRECISION_RADIX INT8,
	NUMERIC_SCALE INT8,
	DATETIME_PRECISION INT8,
	INTERVAL_TYPE STRING,
	INTERVAL_PRECISION INT8,
	UDT_CATALOG STRING,
	UDT_SCHEMA STRING,
	UDT_NAME STRING,
	SCOPE_CATALOG STRING,
	SCOPE_SCHEMA STRING,
	SCOPE_NAME STRING,
	MAXIMUM_CARDINALITY INT8,
	DTD_IDENTIFIER STRING,
	DECLARED_DATA_TYPE STRING,
	DECLARED_NUMERIC_PRECISION INT8,
	DECLARED_NUMERIC_SCALE INT8,
	PARAMETER_DEFAULT STRING,
	TABLE_SEMANTICS STRING,
	IS_PRUNABLE STRING,
	HAS_PASS_THROUGH_COLUMNS STRING,
	ROUTINE_TYPE STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachRoutine(ctx, p, dbContext, func(routine informationSchemaRoutine) error {
			var parameters []informationSchemaParameter
			switch routine.routineType {
			case informationSchemaRoutineTypeProcedure:
				// Procedure descriptors retain parameter names and IN/OUT directions.
				for i := range routine.procedure.Parameters {
					parameter := &routine.procedure.Parameters[i]
					mode := ""
					switch parameter.Direction {
					case sqlbase.ProcParam_InDirection:
						mode = "IN"
					case sqlbase.ProcParam_OutDirection:
						mode = "OUT"
					default:
						return errors.AssertionFailedf(
							"procedure %q parameter %d has unknown direction %d",
							routine.name, i+1, parameter.Direction,
						)
					}
					parameterName := parameter.Name
					parameters = append(parameters, informationSchemaParameter{
						catalog: routine.catalog, schema: routine.schema, specificName: routine.name,
						ordinal: i + 1, mode: mode, name: &parameterName, typ: &parameter.Type,
						routineType: routine.routineType,
					})
				}
			case informationSchemaRoutineTypeFunction:
				// Lua UDFs store encoded parameter types and widths, but no names or directions.
				if len(routine.function.TypesLength) < len(routine.function.ArgumentTypes) {
					return errors.AssertionFailedf(
						"function %q has %d argument types but only %d type lengths",
						routine.name, len(routine.function.ArgumentTypes), len(routine.function.TypesLength),
					)
				}
				for i, encodedType := range routine.function.ArgumentTypes {
					parameterType, err := decodeFunctionParameterType(encodedType, routine.function.TypesLength[i])
					if err != nil {
						return errors.Wrapf(err, "function %q parameter %d", routine.name, i+1)
					}
					parameters = append(parameters, informationSchemaParameter{
						catalog: routine.catalog, specificName: routine.name, ordinal: i + 1,
						mode: "IN", typ: parameterType, routineType: routine.routineType,
					})
				}
				if len(routine.function.ReturnType) == 0 {
					return errors.AssertionFailedf("function %q has no return type", routine.name)
				}
				if len(routine.function.TypesLength) <= len(routine.function.ArgumentTypes) {
					return errors.AssertionFailedf(
						"function %q has no return type length", routine.name,
					)
				}
				returnType, err := decodeFunctionParameterType(
					routine.function.ReturnType[0],
					routine.function.TypesLength[len(routine.function.ArgumentTypes)],
				)
				if err != nil {
					return errors.Wrapf(err, "function %q return type", routine.name)
				}
				// The function result is represented as an unnamed ordinal-zero row.
				parameters = append(parameters, informationSchemaParameter{
					catalog: routine.catalog, specificName: routine.name,
					isResult: true, typ: returnType, routineType: routine.routineType,
				})
			}

			// Normalize both descriptor types before producing the common 45-column row.
			for _, param := range parameters {
				catalog := tree.NewDString(param.catalog)
				schema := tree.DNull
				if param.schema != nil {
					schema = tree.NewDString(*param.schema)
				}
				parameterName := tree.DNull
				if param.name != nil {
					parameterName = tree.NewDString(*param.name)
				}
				parameterMode := tree.DNull
				if !param.isResult {
					parameterMode = tree.NewDString(param.mode)
				}
				isResult := noString
				if param.isResult {
					isResult = yesString
				}

				collationCatalog := tree.DNull
				collationSchema := tree.DNull
				collationName := tree.DNull
				if locale := param.typ.Locale(); locale != "" {
					collationCatalog = catalog
					collationSchema = pgCatalogNameDString
					collationName = tree.NewDString(locale)
				}

				if err := addRow(
					catalog,                                // specific_catalog
					schema,                                 // specific_schema
					tree.NewDString(param.specificName),    // specific_name
					tree.NewDInt(tree.DInt(param.ordinal)), // ordinal_position
					parameterMode,                          // parameter_mode
					isResult,                               // is_result
					noString,                               // as_locator
					parameterName,                          // parameter_name
					tree.DNull,                             // from_sql_specific_catalog
					tree.DNull,                             // from_sql_specific_schema
					tree.DNull,                             // from_sql_specific_name
					tree.DNull,                             // to_sql_specific_catalog
					tree.DNull,                             // to_sql_specific_schema
					tree.DNull,                             // to_sql_specific_name
					tree.NewDString(param.typ.InformationSchemaName()), // data_type
					characterMaximumLength(param.typ),                  // character_maximum_length
					characterOctetLength(param.typ),                    // character_octet_length
					tree.DNull,                                         // character_set_catalog
					tree.DNull,                                         // character_set_schema
					tree.DNull,                                         // character_set_name
					collationCatalog,                                   // collation_catalog
					collationSchema,                                    // collation_schema
					collationName,                                      // collation_name
					numericPrecision(param.typ),                        // numeric_precision
					numericPrecisionRadix(param.typ),                   // numeric_precision_radix
					numericScale(param.typ),                            // numeric_scale
					datetimePrecision(param.typ),                       // datetime_precision
					tree.DNull,                                         // interval_type
					tree.DNull,                                         // interval_precision
					catalog,                                            // udt_catalog
					pgCatalogNameDString,                               // udt_schema
					tree.NewDString(param.typ.PGName()),                // udt_name
					tree.DNull,                                         // scope_catalog
					tree.DNull,                                         // scope_schema
					tree.DNull,                                         // scope_name
					tree.DNull,                                         // maximum_cardinality
					tree.NewDString(param.typ.SQLString()),             // dtd_identifier
					tree.NewDString(param.typ.SQLString()),             // declared_data_type
					numericPrecision(param.typ),                        // declared_numeric_precision
					numericScale(param.typ),                            // declared_numeric_scale
					tree.DNull,                                         // parameter_default
					tree.DNull,                                         // table_semantics
					tree.DNull,                                         // is_prunable
					tree.DNull,                                         // has_pass_through_columns
					tree.NewDString(param.routineType),                 // routine_type
				); err != nil {
					return err
				}
			}
			return nil
		})
	},
}

type informationSchemaParameter struct {
	catalog      string
	schema       *string
	specificName string
	ordinal      int
	mode         string
	name         *string
	typ          *types.T
	isResult     bool
	routineType  string
}

const (
	informationSchemaRoutineTypeProcedure = "PROCEDURE"
	informationSchemaRoutineTypeFunction  = "FUNCTION"
)

type informationSchemaRoutine struct {
	catalog     string
	schema      *string
	name        string
	routineType string
	definer     string // The creator recorded by system.user_defined_routine; exposed as DEFINER.
	created     *tree.DTimestampTZ
	lastAltered *tree.DTimestampTZ
	function    *sqlbase.FunctionDescriptor
	procedure   *sqlbase.ProcedureDescriptor
}

// forEachRoutine iterates user-defined routines visible to the current user.
// It reads the system table, decodes descriptors, resolves catalog/schema names,
// and applies visibility filtering; the callback performs view-specific conversion.
// Procedures are limited to a non-nil database context, while Lua UDFs remain global.
func forEachRoutine(
	ctx context.Context,
	p *planner,
	dbContext *DatabaseDescriptor,
	fn func(informationSchemaRoutine) error,
) error {
	isAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}

	var memberOf map[string]bool
	if !isAdmin {
		// Procedure privileges can be inherited through roles; administrators need no expansion.
		memberOf, err = p.MemberOfWithAdminOption(ctx, p.SessionData().User)
		if err != nil {
			return err
		}
	}

	// Functions and procedures share this table; routine_type determines the descriptor type.
	query := `SELECT name, db_id, schema_id, descriptor, routine_type, create_time, modification_time, creator FROM system.user_defined_routine`
	rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
		ctx, "read-routine", p.txn, query,
	)
	if err != nil {
		return err
	}

	// Reuse catalog and schema name resolutions for routines in the same namespace.
	databaseNames := make(map[sqlbase.ID]string)
	schemaNames := make(map[sqlbase.ID]string)
	for _, row := range rows {
		routineName := string(tree.MustBeDString(row[0]))
		dbID := sqlbase.ID(tree.MustBeDInt(row[1]))
		schemaID := sqlbase.ID(tree.MustBeDInt(row[2]))
		if row[3] == tree.DNull {
			return errors.AssertionFailedf("routine %q has no descriptor", routineName)
		}
		descriptor := []byte(tree.MustBeDBytes(row[3]))
		routineType := sqlbase.RoutineType(tree.MustBeDInt(row[4]))
		created := tree.MustBeDTimestamp(row[5])
		lastAltered := tree.MustBeDTimestamp(row[6])
		createdTZ := tree.MakeDTimestampTZ(created.Time, time.Microsecond)
		lastAlteredTZ := tree.MakeDTimestampTZ(lastAltered.Time, time.Microsecond)
		// user_defined_routine stores the routine owner as creator. The routines
		// view exposes this existing metadata through its standard DEFINER column.
		definer := string(tree.MustBeDString(row[7]))

		switch routineType {
		case sqlbase.Procedure:
			if dbContext != nil && dbID != dbContext.ID {
				continue
			}

			var proc sqlbase.ProcedureDescriptor
			if err := protoutil.Unmarshal(descriptor, &proc); err != nil {
				return errors.AssertionFailedf("failed to decode procedure %q: %v", routineName, err)
			}
			if proc.Privileges == nil {
				return errors.AssertionFailedf("procedure %q has no privilege descriptor", routineName)
			}

			// A procedure is visible through a direct, PUBLIC, or inherited-role privilege.
			visible := isAdmin || proc.Privileges.AnyPrivilege(p.SessionData().User) ||
				proc.Privileges.AnyPrivilege(sqlbase.PublicRole)
			if !visible {
				for role := range memberOf {
					if proc.Privileges.AnyPrivilege(role) {
						visible = true
						break
					}
				}
			}
			if !visible {
				continue
			}

			databaseName, ok := databaseNames[dbID]
			if !ok {
				if dbContext != nil {
					databaseName = dbContext.Name
				} else {
					db, err := getDatabaseDescByID(ctx, p.txn, dbID)
					if err != nil {
						return err
					}
					databaseName = db.Name
				}
				databaseNames[dbID] = databaseName
			}

			schemaName, ok := schemaNames[schemaID]
			if !ok {
				schema, err := getSchemaDescByID(ctx, p.txn, schemaID)
				if err != nil {
					return err
				}
				schemaName = schema.Name
				schemaNames[schemaID] = schemaName
			}

			if err := fn(informationSchemaRoutine{
				catalog: databaseName, schema: &schemaName, name: proc.Name,
				routineType: informationSchemaRoutineTypeProcedure,
				definer:     definer, created: createdTZ, lastAltered: lastAlteredTZ, procedure: &proc,
			}); err != nil {
				return err
			}

		case sqlbase.Function:
			var function sqlbase.FunctionDescriptor
			if err := protoutil.Unmarshal(descriptor, &function); err != nil {
				return errors.AssertionFailedf("failed to decode function %q: %v", routineName, err)
			}
			// Lua UDFs have no reliable db_id/schema_id; DbName supplies the metadata catalog.
			if function.DbName == "" {
				return errors.AssertionFailedf("function %q has no database name", routineName)
			}
			// FunctionDescriptor has no privilege descriptor, so Lua UDFs remain visible to all users.
			if err := fn(informationSchemaRoutine{
				catalog: function.DbName, name: function.Name,
				routineType: informationSchemaRoutineTypeFunction,
				definer:     definer, created: createdTZ, lastAltered: lastAlteredTZ, function: &function,
			}); err != nil {
				return err
			}

		default:
			return errors.AssertionFailedf(
				"routine %q has unknown routine type %d", routineName, routineType,
			)
		}
	}

	return nil
}

// decodeFunctionParameterType restores a SQL type from a Lua UDF descriptor's encoded type and width.
func decodeFunctionParameterType(encodedType uint32, encodedWidth uint32) (*types.T, error) {
	const maxInt32 = uint32(1<<31 - 1)
	if encodedWidth > maxInt32 {
		return nil, errors.AssertionFailedf("function parameter width %d exceeds INT32", encodedWidth)
	}
	width := int32(encodedWidth)

	switch sqlbase.DataType(encodedType) {
	case sqlbase.DataType_TIMESTAMP:
		return types.Timestamp, nil
	case sqlbase.DataType_BIGINT:
		return types.Int, nil
	case sqlbase.DataType_SMALLINT:
		return types.Int2, nil
	case sqlbase.DataType_INT:
		return types.Int4, nil
	case sqlbase.DataType_FLOAT:
		return types.Float4, nil
	case sqlbase.DataType_DOUBLE:
		return types.Float, nil
	case sqlbase.DataType_BOOL:
		return types.Bool, nil
	case sqlbase.DataType_CHAR:
		return types.MakeChar(width), nil
	case sqlbase.DataType_NCHAR:
		return types.MakeNChar(width), nil
	case sqlbase.DataType_VARCHAR:
		return types.MakeVarChar(width, 0), nil
	case sqlbase.DataType_NVARCHAR:
		return types.MakeNVarChar(width), nil
	case sqlbase.DataType_TIMESTAMPTZ:
		return types.TimestampTZ, nil
	default:
		return nil, pgerror.Newf(
			pgcode.FdwInvalidDataType, "unknown udf type identifier: %d", encodedType,
		)
	}
}

var (
	matchOptionFull    = tree.NewDString("FULL")
	matchOptionPartial = tree.NewDString("PARTIAL")
	matchOptionNone    = tree.NewDString("NONE")

	matchOptionMap = map[sqlbase.ForeignKeyReference_Match]tree.Datum{
		sqlbase.ForeignKeyReference_SIMPLE:  matchOptionNone,
		sqlbase.ForeignKeyReference_FULL:    matchOptionFull,
		sqlbase.ForeignKeyReference_PARTIAL: matchOptionPartial,
	}

	refConstraintRuleNoAction   = tree.NewDString("NO ACTION")
	refConstraintRuleRestrict   = tree.NewDString("RESTRICT")
	refConstraintRuleSetNull    = tree.NewDString("SET NULL")
	refConstraintRuleSetDefault = tree.NewDString("SET DEFAULT")
	refConstraintRuleCascade    = tree.NewDString("CASCADE")
)

func dStringForFKAction(action sqlbase.ForeignKeyReference_Action) tree.Datum {
	switch action {
	case sqlbase.ForeignKeyReference_NO_ACTION:
		return refConstraintRuleNoAction
	case sqlbase.ForeignKeyReference_RESTRICT:
		return refConstraintRuleRestrict
	case sqlbase.ForeignKeyReference_SET_NULL:
		return refConstraintRuleSetNull
	case sqlbase.ForeignKeyReference_SET_DEFAULT:
		return refConstraintRuleSetDefault
	case sqlbase.ForeignKeyReference_CASCADE:
		return refConstraintRuleCascade
	}
	panic(errors.Errorf("unexpected ForeignKeyReference_Action: %v", action))
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/referential-constraints-table.html
var informationSchemaReferentialConstraintsTable = virtualSchemaTable{
	comment: `foreign key constraints
` + base.DocsURL("information-schema.html#referential_constraints") + `
https://www.postgresql.org/docs/9.5/infoschema-referential-constraints.html`,
	schema: `
CREATE TABLE information_schema.referential_constraints (
	CONSTRAINT_CATALOG        STRING NOT NULL,
	CONSTRAINT_SCHEMA         STRING NOT NULL,
	CONSTRAINT_NAME           STRING NOT NULL,
	UNIQUE_CONSTRAINT_CATALOG STRING NOT NULL,
	UNIQUE_CONSTRAINT_SCHEMA  STRING NOT NULL,
	UNIQUE_CONSTRAINT_NAME    STRING,
	MATCH_OPTION              STRING NOT NULL,
	UPDATE_RULE               STRING NOT NULL,
	DELETE_RULE               STRING NOT NULL,
	TABLE_NAME                STRING NOT NULL,
	REFERENCED_TABLE_NAME     STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Referenced tables can belong to another database, so resolve all schema names.
		schemaNames, err := getSchemaNames(ctx, p, nil /* all databases */)
		if err != nil {
			return err
		}
		return forEachTableDescWithTableLookup(ctx, p, dbContext, hideVirtual /* no constraints in virtual tables */, func(
			db *sqlbase.DatabaseDescriptor,
			scName string,
			table *sqlbase.TableDescriptor,
			tableLookup tableLookupFn,
		) error {
			dbNameStr := tree.NewDString(db.Name)
			scNameStr := tree.NewDString(scName)
			tbNameStr := tree.NewDString(table.Name)
			for i := range table.OutboundFKs {
				fk := &table.OutboundFKs[i]
				refTable, err := tableLookup.getTableByID(fk.ReferencedTableID)
				if err != nil {
					return err
				}
				refDB, err := tableLookup.getDatabaseByID(refTable.GetParentID())
				if err != nil {
					return err
				}
				refSchemaName, ok := schemaNames[refTable.GetParentSchemaID()]
				if !ok {
					return errors.AssertionFailedf("schema id %d not found", refTable.GetParentSchemaID())
				}
				var matchType = tree.DNull
				if r, ok := matchOptionMap[fk.Match]; ok {
					matchType = r
				}
				referencedIdx, err := sqlbase.FindFKReferencedIndex(refTable, fk.ReferencedColumnIDs)
				if err != nil {
					return err
				}
				if err := addRow(
					dbNameStr,                           // constraint_catalog
					scNameStr,                           // constraint_schema
					tree.NewDString(fk.Name),            // constraint_name
					tree.NewDString(refDB.Name),         // unique_constraint_catalog
					tree.NewDString(refSchemaName),      // unique_constraint_schema
					tree.NewDString(referencedIdx.Name), // unique_constraint_name
					matchType,                           // match_option
					dStringForFKAction(fk.OnUpdate),     // update_rule
					dStringForFKAction(fk.OnDelete),     // delete_rule
					tbNameStr,                           // table_name
					tree.NewDString(refTable.Name),      // referenced_table_name
				); err != nil {
					return err
				}
			}
			return nil
		})
	},
}

// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-role-table-grants.html
// MySQL:    https://dev.mysql.com/doc/refman/8.0/en/information-schema-role-table-grants-table.html
var informationSchemaRoleTableGrants = virtualSchemaTable{
	comment: `privileges granted on tables or views to roles available to the current user (incomplete; grantor information unavailable)
` + base.DocsURL("information-schema.html#role_table_grants") + `
https://www.postgresql.org/docs/9.5/infoschema-role-table-grants.html`,
	schema: `
CREATE TABLE information_schema.role_table_grants (
	GRANTOR        STRING,
	GRANTEE        STRING NOT NULL,
	TABLE_CATALOG  STRING NOT NULL,
	TABLE_SCHEMA   STRING NOT NULL,
	TABLE_NAME     STRING NOT NULL,
	PRIVILEGE_TYPE STRING NOT NULL,
	IS_GRANTABLE   STRING,
	WITH_HIERARCHY STRING
)`,
	populate: func(
		ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error,
	) error {
		roleGrantees, err := getVisibleRoleGrantees(ctx, p)
		if err != nil {
			return err
		}

		return forEachTableDesc(ctx, p, dbContext, virtualMany,
			func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
				dbNameStr := tree.NewDString(db.Name)
				scNameStr := tree.NewDString(scName)
				tbNameStr := tree.NewDString(table.Name)

				for _, u := range table.Privileges.Users {
					if _, ok := roleGrantees[u.User]; !ok {
						continue
					}

					// Preserve Show()'s sorted output while reading the same raw
					// privilege entries as role_column_grants. The ALL bit remains ALL.
					privileges := privilege.ListFromBitField(u.Privileges).SortedNames()
					for _, priv := range privileges {
						if err := addRow(
							tree.DNull,              // grantor
							tree.NewDString(u.User), // grantee
							dbNameStr,               // table_catalog
							scNameStr,               // table_schema
							tbNameStr,               // table_name
							tree.NewDString(priv),   // privilege_type
							tree.DNull,              // is_grantable
							noString,                // with_hierarchy
						); err != nil {
							return err
						}
					}
				}
				return nil
			})
	},
}

// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-role-column-grants.html
// MySQL:    https://dev.mysql.com/doc/refman/8.0/en/information-schema-role-column-grants-table.html
var informationSchemaRoleColumnGrants = virtualSchemaTable{
	comment: `column privilege grants to roles available to the current user (incomplete; grantor information unavailable)
` + base.DocsURL("information-schema.html#role_column_grants") + `
https://www.postgresql.org/docs/9.5/infoschema-role-column-grants.html`,
	schema: `
CREATE TABLE information_schema.role_column_grants (
	GRANTOR        STRING,
	GRANTEE        STRING NOT NULL,
	TABLE_CATALOG  STRING NOT NULL,
	TABLE_SCHEMA   STRING NOT NULL,
	TABLE_NAME     STRING NOT NULL,
	COLUMN_NAME    STRING NOT NULL,
	PRIVILEGE_TYPE STRING NOT NULL,
	IS_GRANTABLE   STRING
)`,
	populate: func(
		ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error,
	) error {
		roleGrantees, err := getVisibleRoleGrantees(ctx, p)
		if err != nil {
			return err
		}

		return forEachTableDesc(ctx, p, dbContext, virtualMany,
			func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
				dbNameStr := tree.NewDString(db.Name)
				scNameStr := tree.NewDString(scName)
				tbNameStr := tree.NewDString(table.Name)
				columnPrivileges := privilege.List{privilege.SELECT, privilege.INSERT, privilege.UPDATE}

				for _, u := range table.Privileges.Users {
					if _, ok := roleGrantees[u.User]; !ok {
						continue
					}

					for _, priv := range columnPrivileges {
						if !hasColumnPrivilege(u.Privileges, priv) {
							continue
						}

						if err := forEachVisibleColumn(table, func(column *sqlbase.ColumnDescriptor) error {
							return addRow(
								tree.DNull,                     // grantor
								tree.NewDString(u.User),        // grantee
								dbNameStr,                      // table_catalog
								scNameStr,                      // table_schema
								tbNameStr,                      // table_name
								tree.NewDString(column.Name),   // column_name
								tree.NewDString(priv.String()), // privilege_type
								tree.DNull,                     // is_grantable
							)
						}); err != nil {
							return err
						}
					}
				}
				return nil
			})
	},
}

// MySQL:    https://dev.mysql.com/doc/mysql-infoschema-excerpt/5.7/en/routines-table.html
var informationSchemaRoutineTable = virtualSchemaTable{
	comment: `user-defined functions and procedures
https://www.postgresql.org/docs/9.5/infoschema-routines.html`,
	schema: `
CREATE TABLE information_schema.routines (
	SPECIFIC_CATALOG STRING,
	SPECIFIC_SCHEMA STRING,
	SPECIFIC_NAME STRING,
	ROUTINE_CATALOG STRING,
	ROUTINE_SCHEMA STRING,
	ROUTINE_NAME STRING,
	ROUTINE_TYPE STRING,
	MODULE_CATALOG STRING,
	MODULE_SCHEMA STRING,
	MODULE_NAME STRING,
	UDT_CATALOG STRING,
	UDT_SCHEMA STRING,
	UDT_NAME STRING,
	DATA_TYPE STRING,
	CHARACTER_MAXIMUM_LENGTH INT8,
	CHARACTER_OCTET_LENGTH INT8,
	CHARACTER_SET_CATALOG STRING,
	CHARACTER_SET_SCHEMA STRING,
	CHARACTER_SET_NAME STRING,
	COLLATION_CATALOG STRING,
	COLLATION_SCHEMA STRING,
	COLLATION_NAME STRING,
	NUMERIC_PRECISION INT8,
	NUMERIC_PRECISION_RADIX INT8,
	NUMERIC_SCALE INT8,
	DATETIME_PRECISION INT8,
	INTERVAL_TYPE STRING,
	INTERVAL_PRECISION INT8,
	TYPE_UDT_CATALOG STRING,
	TYPE_UDT_SCHEMA STRING,
	TYPE_UDT_NAME STRING,
	SCOPE_CATALOG STRING,
	SCOPE_SCHEMA STRING,
	SCOPE_NAME STRING,
	MAXIMUM_CARDINALITY INT8,
	DTD_IDENTIFIER STRING,
	ROUTINE_BODY STRING,
	ROUTINE_DEFINITION STRING,
	EXTERNAL_NAME STRING,
	EXTERNAL_LANGUAGE STRING,
	PARAMETER_STYLE STRING,
	IS_DETERMINISTIC STRING,
	SQL_DATA_ACCESS STRING,
	IS_NULL_CALL STRING,
	SQL_PATH STRING,
	SCHEMA_LEVEL_ROUTINE STRING,
	MAX_DYNAMIC_RESULT_SETS INT8,
	IS_USER_DEFINED_CAST STRING,
	IS_IMPLICITLY_INVOCABLE STRING,
	SECURITY_TYPE STRING,
	TO_SQL_SPECIFIC_CATALOG STRING,
	TO_SQL_SPECIFIC_SCHEMA STRING,
	TO_SQL_SPECIFIC_NAME STRING,
	AS_LOCATOR STRING,
	CREATED  TIMESTAMPTZ,
	LAST_ALTERED TIMESTAMPTZ,
	NEW_SAVEPOINT_LEVEL  STRING,
	IS_UDT_DEPENDENT STRING,
	RESULT_CAST_FROM_DATA_TYPE STRING,
	RESULT_CAST_AS_LOCATOR STRING,
	RESULT_CAST_CHAR_MAX_LENGTH INT8,
	RESULT_CAST_CHAR_OCTET_LENGTH INT8,
	RESULT_CAST_CHAR_SET_CATALOG STRING,
	RESULT_CAST_CHAR_SET_SCHEMA STRING,
	RESULT_CAST_CHARACTER_SET_NAME STRING,
	RESULT_CAST_COLLATION_CATALOG STRING,
	RESULT_CAST_COLLATION_SCHEMA STRING,
	RESULT_CAST_COLLATION_NAME STRING,
	RESULT_CAST_NUMERIC_PRECISION INT8,
	RESULT_CAST_NUMERIC_RADIX INT8,
	RESULT_CAST_NUMERIC_SCALE INT8,
	RESULT_CAST_DATETIME_PRECISION INT8,
	RESULT_CAST_INTERVAL_TYPE STRING,
	RESULT_CAST_INTERVAL_PRECISION INT8,
	RESULT_CAST_TYPE_UDT_CATALOG STRING,
	RESULT_CAST_TYPE_UDT_SCHEMA  STRING,
	RESULT_CAST_TYPE_UDT_NAME STRING,
	RESULT_CAST_SCOPE_CATALOG STRING,
	RESULT_CAST_SCOPE_SCHEMA STRING,
	RESULT_CAST_SCOPE_NAME STRING,
	RESULT_CAST_MAX_CARDINALITY INT8,
	RESULT_CAST_DTD_IDENTIFIER STRING,
	DECLARED_DATA_TYPE STRING,
	DECLARED_NUMERIC_PRECISION INT8,
	DECLARED_NUMERIC_SCALE INT8,
	RESULT_CAST_FROM_DECLARED_DATA_TYPE STRING,
	RESULT_CAST_DECLARED_NUMERIC_PRECISION INT8,
	RESULT_CAST_DECLARED_NUMERIC_SCALE INT8,
	RETURNS_ONLY_PASS_THROUGH STRING,
	DESCRIBE_PROCEDURE_SPECIFIC_CATALOG STRING,
	DESCRIBE_PROCEDURE_SPECIFIC_SCHEMA STRING,
	DESCRIBE_PROCEDURE_SPECIFIC_NAME STRING,
	START_PROCEDURE_SPECIFIC_CATALOG STRING,
	START_PROCEDURE_SPECIFIC_SCHEMA STRING,
	START_PROCEDURE_SPECIFIC_NAME STRING,
	FULFILL_PROCEDURE_SPECIFIC_CATALOG STRING,
	FULFILL_PROCEDURE_SPECIFIC_SCHEMA STRING,
	FULFILL_PROCEDURE_SPECIFIC_NAME STRING,
	FINISH_PROCEDURE_SPECIFIC_CATALOG STRING,
	FINISH_PROCEDURE_SPECIFIC_SCHEMA STRING,
	FINISH_PROCEDURE_SPECIFIC_NAME STRING,
	ROUTINE_COMMENT STRING,
	DEFINER STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachRoutine(ctx, p, dbContext, func(routine informationSchemaRoutine) error {
			// Start with the routine identity and common metadata. Type-specific
			// result metadata is filled below, while unsupported standard fields stay NULL.
			catalog := tree.NewDString(routine.catalog)
			var schema tree.Datum = tree.DNull
			if routine.schema != nil {
				schema = tree.NewDString(*routine.schema)
			}

			var udtCatalog tree.Datum = tree.DNull
			var udtSchema tree.Datum = tree.DNull
			var udtName tree.Datum = tree.DNull
			var dataType tree.Datum = tree.DNull
			var characterMaxLength tree.Datum = tree.DNull
			var characterOctetLengthDatum tree.Datum = tree.DNull
			var collationCatalog tree.Datum = tree.DNull
			var collationSchema tree.Datum = tree.DNull
			var collationName tree.Datum = tree.DNull
			var numericPrecisionDatum tree.Datum = tree.DNull
			var numericPrecisionRadixDatum tree.Datum = tree.DNull
			var numericScaleDatum tree.Datum = tree.DNull
			var datetimePrecisionDatum tree.Datum = tree.DNull
			var typeUDTCatalog tree.Datum = tree.DNull
			var typeUDTSchema tree.Datum = tree.DNull
			var typeUDTName tree.Datum = tree.DNull
			var dtdIdentifier tree.Datum = tree.DNull
			var declaredDataType tree.Datum = tree.DNull
			var declaredNumericPrecision tree.Datum = tree.DNull
			var declaredNumericScale tree.Datum = tree.DNull
			var routineBody tree.Datum
			var routineDefinition tree.Datum
			var externalLanguage tree.Datum

			// Functions expose their return type and Lua body; procedures expose
			// only the metadata available from their procedure descriptor.
			switch routine.routineType {
			case informationSchemaRoutineTypeFunction:
				if routine.function == nil {
					return errors.AssertionFailedf("function %q has no descriptor", routine.name)
				}
				if len(routine.function.ReturnType) == 0 {
					return errors.AssertionFailedf("function %q has no return type", routine.name)
				}
				if len(routine.function.TypesLength) <= len(routine.function.ArgumentTypes) {
					return errors.AssertionFailedf(
						"function %q has no return type length", routine.name,
					)
				}
				returnType, err := decodeFunctionParameterType(
					routine.function.ReturnType[0],
					routine.function.TypesLength[len(routine.function.ArgumentTypes)],
				)
				if err != nil {
					return errors.Wrapf(err, "function %q return type", routine.name)
				}

				udtCatalog = catalog
				udtSchema = pgCatalogNameDString
				udtName = tree.NewDString(returnType.PGName())
				dataType = tree.NewDString(returnType.InformationSchemaName())
				characterMaxLength = characterMaximumLength(returnType)
				characterOctetLengthDatum = characterOctetLength(returnType)
				numericPrecisionDatum = numericPrecision(returnType)
				numericPrecisionRadixDatum = numericPrecisionRadix(returnType)
				numericScaleDatum = numericScale(returnType)
				datetimePrecisionDatum = datetimePrecision(returnType)
				typeUDTCatalog = catalog
				typeUDTSchema = pgCatalogNameDString
				typeUDTName = tree.NewDString(returnType.PGName())
				dtdIdentifier = tree.NewDString(returnType.SQLString())
				declaredDataType = tree.NewDString(returnType.SQLString())
				declaredNumericPrecision = numericPrecision(returnType)
				declaredNumericScale = numericScale(returnType)
				if locale := returnType.Locale(); locale != "" {
					collationCatalog = catalog
					collationSchema = pgCatalogNameDString
					collationName = tree.NewDString(locale)
				}
				// Lua UDF bodies are executed by the Lua runtime rather than as SQL.
				routineBody = tree.NewDString("EXTERNAL")
				routineDefinition = tree.NewDString(routine.function.FunctionBody)
				externalLanguage = tree.NewDString(strings.ToUpper(routine.function.Language))

			case informationSchemaRoutineTypeProcedure:
				if routine.procedure == nil {
					return errors.AssertionFailedf("procedure %q has no descriptor", routine.name)
				}
				// Procedure bodies are stored and executed as SQL procedure text.
				routineBody = tree.NewDString("SQL")
				routineDefinition = tree.NewDString(routine.procedure.ProcBody)
				externalLanguage = tree.NewDString(strings.ToUpper(routine.procedure.Language))

			default:
				return errors.AssertionFailedf(
					"routine %q has unknown routine type %q", routine.name, routine.routineType,
				)
			}

			// Keep this argument order identical to the CREATE TABLE definition above.
			// The row combines shared routine metadata with the type-specific values
			// prepared above; fields without a reliable KWDB source remain NULL.
			return addRow(
				catalog,                              // specific_catalog
				schema,                               // specific_schema
				tree.NewDString(routine.name),        // specific_name
				catalog,                              // routine_catalog
				schema,                               // routine_schema
				tree.NewDString(routine.name),        // routine_name
				tree.NewDString(routine.routineType), // routine_type
				tree.DNull,                           // module_catalog
				tree.DNull,                           // module_schema
				tree.DNull,                           // module_name
				udtCatalog,                           // udt_catalog
				udtSchema,                            // udt_schema
				udtName,                              // udt_name
				dataType,                             // data_type
				characterMaxLength,                   // character_maximum_length
				characterOctetLengthDatum,            // character_octet_length
				tree.DNull,                           // character_set_catalog
				tree.DNull,                           // character_set_schema
				tree.DNull,                           // character_set_name
				collationCatalog,                     // collation_catalog
				collationSchema,                      // collation_schema
				collationName,                        // collation_name
				numericPrecisionDatum,                // numeric_precision
				numericPrecisionRadixDatum,           // numeric_precision_radix
				numericScaleDatum,                    // numeric_scale
				datetimePrecisionDatum,               // datetime_precision
				tree.DNull,                           // interval_type
				tree.DNull,                           // interval_precision
				typeUDTCatalog,                       // type_udt_catalog
				typeUDTSchema,                        // type_udt_schema
				typeUDTName,                          // type_udt_name
				tree.DNull,                           // scope_catalog
				tree.DNull,                           // scope_schema
				tree.DNull,                           // scope_name
				tree.DNull,                           // maximum_cardinality
				dtdIdentifier,                        // dtd_identifier
				routineBody,                          // routine_body
				routineDefinition,                    // routine_definition
				tree.DNull,                           // external_name
				externalLanguage,                     // external_language
				tree.DNull,                           // parameter_style
				tree.DNull,                           // is_deterministic
				tree.DNull,                           // sql_data_access
				tree.DNull,                           // is_null_call
				tree.DNull,                           // sql_path
				tree.DNull,                           // schema_level_routine
				tree.DNull,                           // max_dynamic_result_sets
				tree.DNull,                           // is_user_defined_cast
				tree.DNull,                           // is_implicitly_invocable
				tree.DNull,                           // security_type
				tree.DNull,                           // to_sql_specific_catalog
				tree.DNull,                           // to_sql_specific_schema
				tree.DNull,                           // to_sql_specific_name
				tree.DNull,                           // as_locator
				routine.created,                      // created
				routine.lastAltered,                  // last_altered
				tree.DNull,                           // new_savepoint_level
				tree.DNull,                           // is_udt_dependent
				tree.DNull,                           // result_cast_from_data_type
				tree.DNull,                           // result_cast_as_locator
				tree.DNull,                           // result_cast_char_max_length
				tree.DNull,                           // result_cast_char_octet_length
				tree.DNull,                           // result_cast_char_set_catalog
				tree.DNull,                           // result_cast_char_set_schema
				tree.DNull,                           // result_cast_character_set_name
				tree.DNull,                           // result_cast_collation_catalog
				tree.DNull,                           // result_cast_collation_schema
				tree.DNull,                           // result_cast_collation_name
				tree.DNull,                           // result_cast_numeric_precision
				tree.DNull,                           // result_cast_numeric_radix
				tree.DNull,                           // result_cast_numeric_scale
				tree.DNull,                           // result_cast_datetime_precision
				tree.DNull,                           // result_cast_interval_type
				tree.DNull,                           // result_cast_interval_precision
				tree.DNull,                           // result_cast_type_udt_catalog
				tree.DNull,                           // result_cast_type_udt_schema
				tree.DNull,                           // result_cast_type_udt_name
				tree.DNull,                           // result_cast_scope_catalog
				tree.DNull,                           // result_cast_scope_schema
				tree.DNull,                           // result_cast_scope_name
				tree.DNull,                           // result_cast_max_cardinality
				tree.DNull,                           // result_cast_dtd_identifier
				declaredDataType,                     // declared_data_type
				declaredNumericPrecision,             // declared_numeric_precision
				declaredNumericScale,                 // declared_numeric_scale
				tree.DNull,                           // result_cast_from_declared_data_type
				tree.DNull,                           // result_cast_declared_numeric_precision
				tree.DNull,                           // result_cast_declared_numeric_scale
				tree.DNull,                           // returns_only_pass_through
				tree.DNull,                           // describe_procedure_specific_catalog
				tree.DNull,                           // describe_procedure_specific_schema
				tree.DNull,                           // describe_procedure_specific_name
				tree.DNull,                           // start_procedure_specific_catalog
				tree.DNull,                           // start_procedure_specific_schema
				tree.DNull,                           // start_procedure_specific_name
				tree.DNull,                           // fulfill_procedure_specific_catalog
				tree.DNull,                           // fulfill_procedure_specific_schema
				tree.DNull,                           // fulfill_procedure_specific_name
				tree.DNull,                           // finish_procedure_specific_catalog
				tree.DNull,                           // finish_procedure_specific_schema
				tree.DNull,                           // finish_procedure_specific_name
				tree.DNull,                           // routine_comment
				tree.NewDString(routine.definer),     // definer, the routine creator/owner metadata
			)
		})
	},
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/schemata-table.html
var informationSchemaSchemataTable = virtualSchemaTable{
	comment: `database schemas (may contain schemata without permission)
` + base.DocsURL("information-schema.html#schemata") + `
https://www.postgresql.org/docs/9.5/infoschema-schemata.html`,
	schema: vtable.InformationSchemaSchemata,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, dbContext, false, /* requiresPrivileges */
			func(db *sqlbase.DatabaseDescriptor) error {
				return forEachSchemaName(ctx, p, db, false, func(sc sqlbase.ResolvedSchema) error {
					engineType := engineRelational
					if db.EngineType == tree.EngineTypeTimeseries {
						engineType = engineTimeseries
					}
					lifetime := db.TsDb.Lifetime
					partitionInterval := db.TsDb.PartitionInterval
					return addRow(
						tree.NewDString(db.Name), // catalog_name
						tree.NewDString(sc.Name), // schema_name
						tree.NewDInt(tree.DInt(db.ID)),
						tree.NewDInt(tree.DInt(sc.ID)),
						tree.DNull, // default_character_set_name
						tree.DNull, // sql_path
						tree.NewDString(engineType),
						tree.NewDInt(tree.DInt(lifetime)),
						tree.NewDInt(tree.DInt(partitionInterval)),
						tree.DNull, // schema_owner
						tree.DNull, // default_character_set_catalog
						tree.DNull, // default_character_set_schema
					)
				})
			})
	},
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/schema-privileges-table.html
var informationSchemaSchemataTablePrivileges = virtualSchemaTable{
	comment: `schema privileges for the current user, public, or available roles; administrators can view all grantees (incomplete; grant option information unavailable)
` + base.DocsURL("information-schema.html#schema_privileges"),
	schema: `
CREATE TABLE information_schema.schema_privileges (
	GRANTEE         STRING NOT NULL,
	TABLE_CATALOG   STRING NOT NULL,
	TABLE_SCHEMA    STRING NOT NULL,
	PRIVILEGE_TYPE  STRING NOT NULL,
	IS_GRANTABLE    STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		isAdmin, err := p.HasAdminRole(ctx)
		if err != nil {
			return err
		}
		currentUser := p.SessionData().User
		var memberOf map[string]bool
		if !isAdmin {
			memberOf, err = p.MemberOfWithAdminOption(ctx, currentUser)
			if err != nil {
				return err
			}
		}

		// Database enumeration must include users who only have privileges on a child schema.
		return forEachDatabaseDesc(ctx, p, dbContext, false, /* requiresPrivileges */
			func(db *sqlbase.DatabaseDescriptor) error {
				return forEachSchemaName(ctx, p, db, true, func(sc sqlbase.ResolvedSchema) error {
					var privs []sqlbase.UserPrivilegeString
					if sc.Kind == sqlbase.SchemaUserDefined {
						if sc.Desc == nil || sc.Desc.GetPrivileges() == nil {
							return nil
						}
						// User defined schemas have their own privileges.
						privs = sc.Desc.GetPrivileges().Show()
					} else {
						// Other schemas inherit from the parent database.
						privs = db.Privileges.Show()
					}
					dbNameStr := tree.NewDString(db.Name)
					scNameStr := tree.NewDString(sc.Name)

					for _, u := range privs {
						if !canUserSeePrivilegeGrantee(isAdmin, currentUser, u.User, memberOf) {
							continue
						}
						userNameStr := tree.NewDString(u.User)
						for _, priv := range u.Privileges {
							if err := addRow(
								userNameStr,           // grantee
								dbNameStr,             // table_catalog
								scNameStr,             // table_schema
								tree.NewDString(priv), // privilege_type
								tree.DNull,            // is_grantable
							); err != nil {
								return err
							}
						}
					}
					return nil
				})
			})
	},
}

var (
	indexDirectionNA   = tree.NewDString("N/A")
	indexDirectionAsc  = tree.NewDString(sqlbase.IndexDescriptor_ASC.String())
	indexDirectionDesc = tree.NewDString(sqlbase.IndexDescriptor_DESC.String())
)

func dStringForIndexDirection(dir sqlbase.IndexDescriptor_Direction) tree.Datum {
	switch dir {
	case sqlbase.IndexDescriptor_ASC:
		return indexDirectionAsc
	case sqlbase.IndexDescriptor_DESC:
		return indexDirectionDesc
	}
	panic("unreachable")
}

var informationSchemaSequences = virtualSchemaTable{
	comment: `sequences
` + base.DocsURL("information-schema.html#sequences") + `
https://www.postgresql.org/docs/9.5/infoschema-sequences.html`,
	schema: `
CREATE TABLE information_schema.sequences (
    SEQUENCE_CATALOG         STRING NOT NULL,
    SEQUENCE_SCHEMA          STRING NOT NULL,
    SEQUENCE_NAME            STRING NOT NULL,
    DATA_TYPE                STRING NOT NULL,
    NUMERIC_PRECISION        INT8 NOT NULL,
    NUMERIC_PRECISION_RADIX  INT8 NOT NULL,
    NUMERIC_SCALE            INT8 NOT NULL,
    START_VALUE              STRING NOT NULL,
    MINIMUM_VALUE            STRING NOT NULL,
    MAXIMUM_VALUE            STRING NOT NULL,
    INCREMENT                STRING NOT NULL,
    CYCLE_OPTION             STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, dbContext, hideVirtual, /* no sequences in virtual schemas */
			func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
				if !table.IsSequence() {
					return nil
				}
				return addRow(
					tree.NewDString(db.GetName()),    // catalog
					tree.NewDString(scName),          // schema
					tree.NewDString(table.GetName()), // name
					tree.NewDString("bigint"),        // type
					tree.NewDInt(64),                 // numeric precision
					tree.NewDInt(2),                  // numeric precision radix
					tree.NewDInt(0),                  // numeric scale
					tree.NewDString(strconv.FormatInt(table.SequenceOpts.Start, 10)),     // start value
					tree.NewDString(strconv.FormatInt(table.SequenceOpts.MinValue, 10)),  // min value
					tree.NewDString(strconv.FormatInt(table.SequenceOpts.MaxValue, 10)),  // max value
					tree.NewDString(strconv.FormatInt(table.SequenceOpts.Increment, 10)), // increment
					noString, // cycle
				)
			})
	},
}

// Postgres: missing
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/statistics-table.html
var informationSchemaStatisticsTable = virtualSchemaTable{
	comment: `index metadata and statistics (incomplete)
` + base.DocsURL("information-schema.html#statistics"),
	schema: `
CREATE TABLE information_schema.statistics (
	TABLE_CATALOG STRING NOT NULL,
	TABLE_SCHEMA  STRING NOT NULL,
	TABLE_NAME    STRING NOT NULL,
	NON_UNIQUE    INT8 NOT NULL,
	INDEX_SCHEMA  STRING NOT NULL,
	INDEX_NAME    STRING NOT NULL,
	SEQ_IN_INDEX  INT8 NOT NULL,
	COLUMN_NAME   STRING NOT NULL,
	"COLLATION"   STRING,
	CARDINALITY   INT8,
	DIRECTION     STRING NOT NULL,
	STORING       STRING NOT NULL,
	IMPLICIT      STRING NOT NULL,
	SUB_PART      INT8,
	PACKED        STRING,
	NULLABLE      STRING NOT NULL,
	INDEX_TYPE    STRING NOT NULL,
	"COMMENT"    STRING,
	INDEX_COMMENT STRING,
	IS_VISIBLE    STRING,
	EXPRESSION    STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		indexComments, err := getIndexComments(ctx, p)
		if err != nil {
			return err
		}

		return forEachTableDesc(ctx, p, dbContext, hideVirtual, /* virtual tables have no indexes */
			func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
				dbNameStr := tree.NewDString(db.GetName())
				scNameStr := tree.NewDString(scName)
				tbNameStr := tree.NewDString(table.GetName())

				appendRow := func(index *sqlbase.IndexDescriptor, colName string, sequence int,
					direction tree.Datum, isStored, isImplicit bool,
				) error {
					column, err := table.FindActiveColumnByName(colName)
					if err != nil {
						return err
					}
					nonUnique := tree.NewDInt(0)
					if !index.Unique {
						nonUnique = tree.NewDInt(1)
					}
					indexComment := tree.DNull
					if commentsByIndex := indexComments[table.ID]; commentsByIndex != nil {
						if comment, ok := commentsByIndex[index.ID]; ok {
							indexComment = comment
						}
					}
					return addRow(
						dbNameStr,                            // table_catalog
						scNameStr,                            // table_schema
						tbNameStr,                            // table_name
						nonUnique,                            // non_unique
						scNameStr,                            // index_schema
						tree.NewDString(index.Name),          // index_name
						tree.NewDInt(tree.DInt(sequence)),    // seq_in_index
						tree.NewDString(colName),             // column_name
						tree.DNull,                           // collation
						tree.DNull,                           // cardinality
						direction,                            // direction
						yesOrNoDatum(isStored),               // storing
						yesOrNoDatum(isImplicit),             // implicit
						tree.DNull,                           // sub_part
						tree.DNull,                           // packed
						yesOrNoDatum(column.Nullable),        // nullable
						tree.NewDString(index.Type.String()), // index_type
						tree.DNull,                           // comment
						indexComment,                         // index_comment
						tree.DNull,                           // is_visible
						tree.DNull,                           // expression
					)
				}

				return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
					// Columns in the primary key that aren't in index.ColumnNames or
					// index.StoreColumnNames are implicit columns in the index.
					var implicitCols map[string]struct{}
					var hasImplicitCols bool
					if index.HasOldStoredColumns() {
						// Old STORING format: implicit columns are extra columns minus stored
						// columns.
						hasImplicitCols = len(index.ExtraColumnIDs) > len(index.StoreColumnNames)
					} else {
						// New STORING format: implicit columns are extra columns.
						hasImplicitCols = len(index.ExtraColumnIDs) > 0
					}
					if hasImplicitCols {
						implicitCols = make(map[string]struct{})
						for _, col := range table.PrimaryIndex.ColumnNames {
							implicitCols[col] = struct{}{}
						}
					}

					sequence := 1
					for i, col := range index.ColumnNames {
						// We add a row for each column of index.
						dir := dStringForIndexDirection(index.ColumnDirections[i])
						if err := appendRow(index, col, sequence, dir, false, false); err != nil {
							return err
						}
						sequence++
						delete(implicitCols, col)
					}
					for _, col := range index.StoreColumnNames {
						// We add a row for each stored column of index.
						if err := appendRow(index, col, sequence,
							indexDirectionNA, true, false); err != nil {
							return err
						}
						sequence++
						delete(implicitCols, col)
					}
					if len(implicitCols) > 0 {
						// In order to have the implicit columns reported in a
						// deterministic order, we will add all of them in the
						// same order as they are mentioned in the primary key.
						//
						// Note that simply iterating over implicitCols map
						// produces non-deterministic output.
						for _, col := range table.GetPrimaryIndex().ColumnNames {
							if _, isImplicit := implicitCols[col]; isImplicit {
								// We add a row for each implicit column of index.
								if err := appendRow(index, col, sequence,
									indexDirectionAsc, false, true); err != nil {
									return err
								}
								sequence++
							}
						}
					}
					return nil
				})
			})
	},
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/table-constraints-table.html
var informationSchemaTableConstraintTable = virtualSchemaTable{
	comment: `table constraints
` + base.DocsURL("information-schema.html#table_constraints") + `
https://www.postgresql.org/docs/9.5/infoschema-table-constraints.html`,
	schema: `
CREATE TABLE information_schema.table_constraints (
	CONSTRAINT_CATALOG STRING NOT NULL,
	CONSTRAINT_SCHEMA  STRING NOT NULL,
	CONSTRAINT_NAME    STRING NOT NULL,
	TABLE_CATALOG      STRING NOT NULL,
	TABLE_SCHEMA       STRING NOT NULL,
	TABLE_NAME         STRING NOT NULL,
	CONSTRAINT_TYPE    STRING NOT NULL,
	IS_DEFERRABLE      STRING NOT NULL,
	INITIALLY_DEFERRED STRING NOT NULL,
	ENFORCED           STRING NOT NULL,
	NULLS_DISTINCT     STRING
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		h := makeOidHasher()
		return forEachTableDescWithTableLookup(ctx, p, dbContext, hideVirtual, /* virtual tables have no constraints */
			func(
				db *sqlbase.DatabaseDescriptor,
				scName string,
				table *sqlbase.TableDescriptor,
				tableLookup tableLookupFn,
			) error {
				conInfo, err := table.GetConstraintInfoWithLookup(tableLookup.getTableByID)
				if err != nil {
					return err
				}

				dbNameStr := tree.NewDString(db.Name)
				scNameStr := tree.NewDString(scName)
				tbNameStr := tree.NewDString(table.Name)

				for conName, c := range conInfo {
					var nullsDistinct tree.Datum = tree.DNull
					if c.Kind == sqlbase.ConstraintTypeUnique {
						nullsDistinct = yesString
					}
					if err := addRow(
						dbNameStr,                       // constraint_catalog
						scNameStr,                       // constraint_schema
						tree.NewDString(conName),        // constraint_name
						dbNameStr,                       // table_catalog
						scNameStr,                       // table_schema
						tbNameStr,                       // table_name
						tree.NewDString(string(c.Kind)), // constraint_type
						yesOrNoDatum(false),             // is_deferrable
						yesOrNoDatum(false),             // initially_deferred
						yesString,                       // enforced
						nullsDistinct,                   // nulls_distinct
					); err != nil {
						return err
					}
				}

				// Unlike with pg_catalog.pg_constraint, Postgres also includes NOT
				// NULL column constraints in information_schema.check_constraints.
				// Cockroach doesn't track these constraints as check constraints,
				// but we can pull them off of the table's column descriptors.
				colNum := 0
				return forEachColumnInTable(table, func(col *sqlbase.ColumnDescriptor) error {
					colNum++
					// NOT NULL column constraints are implemented as a CHECK in postgres.
					conNameStr := tree.NewDString(fmt.Sprintf(
						"%s_%s_%d_not_null", h.NamespaceOid(db, scName), defaultOid(table.ID), colNum,
					))
					if !col.Nullable {
						if err := addRow(
							dbNameStr,                // constraint_catalog
							scNameStr,                // constraint_schema
							conNameStr,               // constraint_name
							dbNameStr,                // table_catalog
							scNameStr,                // table_schema
							tbNameStr,                // table_name
							tree.NewDString("CHECK"), // constraint_type
							yesOrNoDatum(false),      // is_deferrable
							yesOrNoDatum(false),      // initially_deferred
							yesString,                // enforced
							tree.DNull,               // nulls_distinct
						); err != nil {
							return err
						}
					}
					return nil
				})
			})
	},
}

// Postgres: not provided
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/user-privileges-table.html
// TODO(knz): this introspection facility is of dubious utility.
// KWDB does not support MySQL-style global grants such as GRANT ... ON *.*.
// Until a persistent global privilege source exists, expose only the
// compatibility rows for the current root user and admin role membership.
var informationSchemaUserPrivileges = virtualSchemaTable{
	comment: `global privilege compatibility rows for root and admin role membership; non-admin users see no rows`,
	schema: `
CREATE TABLE information_schema.user_privileges (
	GRANTEE        STRING NOT NULL,
	TABLE_CATALOG  STRING NOT NULL,
	PRIVILEGE_TYPE STRING NOT NULL,
	IS_GRANTABLE   STRING
	)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		grantees := make(map[string]struct{}, 2)
		if p.User() == security.RootUser {
			grantees[security.RootUser] = struct{}{}
		}
		if p.User() == sqlbase.AdminRole {
			grantees[sqlbase.AdminRole] = struct{}{}
		}
		roles, err := p.MemberOfWithAdminOption(ctx, p.User())
		if err != nil {
			return err
		}
		if _, ok := roles[sqlbase.AdminRole]; ok {
			grantees[sqlbase.AdminRole] = struct{}{}
		}

		return forEachDatabaseDesc(ctx, p, dbContext, true, /* requiresPrivileges */
			func(dbDesc *DatabaseDescriptor) error {
				dbNameStr := tree.NewDString(dbDesc.Name)
				for _, u := range []string{security.RootUser, sqlbase.AdminRole} {
					if _, ok := grantees[u]; !ok {
						continue
					}
					grantee := tree.NewDString(u)
					for _, p := range privilege.List(privilege.ByValue[:]).SortedNames() {
						if err := addRow(
							grantee,            // grantee
							dbNameStr,          // table_catalog
							tree.NewDString(p), // privilege_type
							tree.DNull,         // is_grantable
						); err != nil {
							return err
						}
					}
				}
				return nil
			})
	},
}

// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/table-privileges-table.html
var informationSchemaTablePrivileges = virtualSchemaTable{
	comment: `privileges on visible tables or views for the current user, public, or available roles; administrators can view all grantees (incomplete; grantor and grant option information unavailable)
` + base.DocsURL("information-schema.html#table_privileges") + `
https://www.postgresql.org/docs/9.5/infoschema-table-privileges.html`,
	schema: `
CREATE TABLE information_schema.table_privileges (
	GRANTOR        STRING,
	GRANTEE        STRING NOT NULL,
	TABLE_CATALOG  STRING NOT NULL,
	TABLE_SCHEMA   STRING NOT NULL,
	TABLE_NAME     STRING NOT NULL,
	PRIVILEGE_TYPE STRING NOT NULL,
	IS_GRANTABLE   STRING,
	WITH_HIERARCHY STRING NOT NULL
)`,
	populate: func(
		ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error,
	) error {
		isAdmin, err := p.HasAdminRole(ctx)
		if err != nil {
			return err
		}

		currentUser := p.SessionData().User
		var memberOf map[string]bool
		if !isAdmin {
			memberOf, err = p.MemberOfWithAdminOption(ctx, currentUser)
			if err != nil {
				return err
			}
		}

		return forEachTableDesc(ctx, p, dbContext, virtualMany,
			func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
				dbNameStr := tree.NewDString(db.Name)
				scNameStr := tree.NewDString(scName)
				tbNameStr := tree.NewDString(table.Name)

				for _, u := range table.Privileges.Show() {
					if !canUserSeePrivilegeGrantee(isAdmin, currentUser, u.User, memberOf) {
						continue
					}

					for _, priv := range u.Privileges {
						if err := addRow(
							tree.DNull,              // grantor
							tree.NewDString(u.User), // grantee
							dbNameStr,               // table_catalog
							scNameStr,               // table_schema
							tbNameStr,               // table_name
							tree.NewDString(priv),   // privilege_type
							tree.DNull,              // is_grantable
							noString,                // with_hierarchy
						); err != nil {
							return err
						}
					}
				}
				return nil
			})
	},
}

var (
	tableTypeSystemView       = tree.NewDString("SYSTEM VIEW")
	tableTypeBaseTable        = tree.NewDString("BASE TABLE")
	tableTypeView             = tree.NewDString("VIEW")
	tableTypeTemporary        = tree.NewDString("LOCAL TEMPORARY")
	tableTypeNormalTimeSeries = tree.NewDString("TIME SERIES TABLE")
	tableTypeMaterializedView = tree.NewDString("MATERIALIZED VIEW")
	tableTypeSparse           = tree.NewDString("SPARSE TIME SERIES TABLE")
)

var (
	rowFormatRow     = tree.NewDString("ROW")
	rowFormatColumn  = tree.NewDString("COLUMN")
	tableCommentView = tree.NewDString("VIEW")
)

// getTableComments returns user-defined table comments keyed by table ID.
func getTableComments(ctx context.Context, p *planner) (map[sqlbase.ID]tree.Datum, error) {
	rows, err := p.extendedEvalCtx.ExecCfg.InternalExecutor.Query(
		ctx,
		"select-table-comments",
		nil,
		`SELECT object_id, "comment"
		   FROM system.comments
		  WHERE type = $1 AND sub_id = 0`,
		keys.TableCommentType,
	)
	if err != nil {
		return nil, err
	}

	comments := make(map[sqlbase.ID]tree.Datum, len(rows))
	for _, row := range rows {
		comments[sqlbase.ID(tree.MustBeDInt(row[0]))] = row[1]
	}
	return comments, nil
}

var informationSchemaTablesTable = virtualSchemaTable{
	comment: `tables and views
` + base.DocsURL("information-schema.html#tables"),
	schema: vtable.InformationSchemaTables,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		tableComments, err := getTableComments(ctx, p)
		if err != nil {
			return err
		}

		return forEachTableDesc(ctx, p, dbContext, virtualMany,
			func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
				if table.IsSequence() {
					return nil
				}
				tableType := tableTypeBaseTable
				insertable := yesString
				if table.IsVirtualTable() {
					tableType = tableTypeSystemView
					insertable = noString
				} else if table.IsView() {
					tableType = tableTypeView
					if table.MaterializedView() {
						tableType = tableTypeMaterializedView
					}
					insertable = noString
				} else if table.Temporary {
					tableType = tableTypeTemporary
				}
				dbNameStr := tree.NewDString(db.Name)
				scNameStr := tree.NewDString(scName)
				tbNameStr := tree.NewDString(table.Name)

				h := makeOidHasher()
				namespaceOid := h.NamespaceOid(db, scName)
				switch table.TableType {
				case tree.RelationalTable:
				// do nothing
				case tree.SparseTable:
					tableType = tableTypeSparse
				case tree.TimeseriesTable:
					tableType = tableTypeNormalTimeSeries
				default:
					return pgerror.Newf(pgcode.WrongObjectType, "invalid table type of %v", table.TableType)
				}

				engine := tree.DNull
				rowFormat := tree.DNull
				updateTime := tree.DNull
				tableComment := tree.DNull
				if table.IsView() {
					// MySQL compatibility requires VIEW as the default comment for views.
					tableComment = tableCommentView
				}
				if !table.IsVirtualTable() && !table.IsView() {
					engine = tree.NewDString(tree.EngineName(db.EngineType))
					switch table.TableType {
					case tree.RelationalTable:
						rowFormat = rowFormatRow
					case tree.TimeseriesTable, tree.SparseTable:
						rowFormat = rowFormatColumn
					}
					if !table.ModificationTime.IsEmpty() {
						updateTime = tree.TimestampToInexactDTimestamp(table.ModificationTime)
					}
					if comment, ok := tableComments[table.ID]; ok {
						tableComment = comment
					}
				} else if !table.IsVirtualTable() && !table.ModificationTime.IsEmpty() {
					updateTime = tree.TimestampToInexactDTimestamp(table.ModificationTime)
				}

				if err := addRow(
					dbNameStr,                              // table_catalog
					scNameStr,                              // table_schema
					tbNameStr,                              // table_name
					tableType,                              // table_type
					insertable,                             // is_insertable_into
					tree.NewDInt(tree.DInt(table.Version)), // version
					namespaceOid,                           // namespace_oid
					tree.DNull,                             // self_referencing_column_name
					tree.DNull,                             // reference_generation
					tree.DNull,                             // user_defined_type_catalog
					tree.DNull,                             // user_defined_type_schema
					tree.DNull,                             // user_defined_type_name
					noString,                               // is_typed
					tree.DNull,                             // commit_action
					engine,                                 // engine
					rowFormat,                              // row_format
					updateTime,                             // update_time
					tableComment,                           // table_comment
				); err != nil {
					return err
				}
				return nil
			})
	},
}

// Postgres: https://www.postgresql.org/docs/9.6/static/infoschema-views.html
// MySQL:    https://dev.mysql.com/doc/refman/5.7/en/views-table.html
var informationSchemaViewsTable = virtualSchemaTable{
	comment: `views (incomplete)
` + base.DocsURL("information-schema.html#views") + `
https://www.postgresql.org/docs/9.5/infoschema-views.html`,
	schema: `
CREATE TABLE information_schema.views (
    TABLE_CATALOG              STRING NOT NULL,
    TABLE_SCHEMA               STRING NOT NULL,
    TABLE_NAME                 STRING NOT NULL,
    VIEW_DEFINITION            STRING NOT NULL,
    CHECK_OPTION               STRING,
    IS_UPDATABLE               STRING NOT NULL,
    INSERTABLE_INTO            STRING NOT NULL,
    IS_TRIGGER_UPDATABLE       STRING NOT NULL,
    IS_TRIGGER_DELETABLE       STRING NOT NULL,
    IS_TRIGGER_INSERTABLE_INTO STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDesc(ctx, p, dbContext, hideVirtual, /* virtual schemas have no views */
			func(db *sqlbase.DatabaseDescriptor, scName string, table *sqlbase.TableDescriptor) error {
				if !table.IsView() {
					return nil
				}
				// Note that the view query printed will not include any column aliases
				// specified outside the initial view query into the definition returned,
				// unlike Postgres. For example, for the view created via
				//  `CREATE VIEW (a) AS SELECT b FROM foo`
				// we'll only print `SELECT b FROM foo` as the view definition here,
				// while Postgres would more accurately print `SELECT b AS a FROM foo`.
				// TODO(a-robinson): Insert column aliases into view query once we
				// have a semantic query representation to work with (#10083).
				return addRow(
					tree.NewDString(db.Name),         // table_catalog
					tree.NewDString(scName),          // table_schema
					tree.NewDString(table.Name),      // table_name
					tree.NewDString(table.ViewQuery), // view_definition
					tree.DNull,                       // check_option
					noString,                         // is_updatable
					noString,                         // insertable_into
					noString,                         // is_trigger_updatable
					noString,                         // is_trigger_deletable
					noString,                         // is_trigger_insertable_into
				)
			})
	},
}

// MySQL: https://dev.mysql.com/doc/refman/8.4/en/information-schema-view-table-usage-table.html
var informationSchemaViewTableUsage = virtualSchemaTable{
	comment: `tables and views directly referenced by views`,
	schema: `
CREATE TABLE information_schema.view_table_usage (
	VIEW_CATALOG  STRING NOT NULL,
	VIEW_SCHEMA   STRING NOT NULL,
	VIEW_NAME     STRING NOT NULL,
	TABLE_CATALOG STRING NOT NULL,
	TABLE_SCHEMA  STRING NOT NULL,
	TABLE_NAME    STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// A descriptor ID pair uniquely identifies one direct view dependency.
		seen := make(map[[2]sqlbase.ID]struct{})
		schemaNames := make(map[sqlbase.ID]string)
		return forEachTableDescWithTableLookup(ctx, p, dbContext, hideVirtual, func(
			db *sqlbase.DatabaseDescriptor,
			viewSchemaName string,
			view *sqlbase.TableDescriptor,
			tableLookup tableLookupFn,
		) error {
			if !view.IsView() {
				return nil
			}
			schemaNames[view.GetParentSchemaID()] = viewSchemaName

			for _, dependencyID := range view.DependsOn {
				// System virtual tables are not persisted in tableLookup and are
				// outside VIEW_TABLE_USAGE's physical relation scope.
				if sqlbase.IsVirtualTable(dependencyID) {
					continue
				}
				dependency, err := tableLookup.getTableByID(dependencyID)
				if err != nil {
					return err
				}
				// VIEW_TABLE_USAGE includes only direct relation dependencies, not sequences or routines.
				if !dependency.IsTable() && !dependency.IsView() {
					continue
				}
				// The traversal already verifies access to the view; also require access to its dependency.
				if !canUserSeeTable(ctx, p, dependency, false /* allowAdding */) {
					continue
				}

				dependencyDB, err := tableLookup.getDatabaseByID(dependency.ParentID)
				if err != nil {
					return err
				}
				dependencySchemaID := dependency.GetParentSchemaID()
				dependencySchemaName, ok := schemaNames[dependencySchemaID]
				if !ok {
					dependencySchema, err := getSchemaDescByID(ctx, p.txn, dependencySchemaID)
					if err != nil {
						return err
					}
					dependencySchemaName = dependencySchema.Name
					schemaNames[dependencySchemaID] = dependencySchemaName
				}

				key := [2]sqlbase.ID{view.ID, dependency.ID}
				if _, ok := seen[key]; ok {
					continue
				}
				if err := addRow(
					tree.NewDString(db.Name),              // view_catalog
					tree.NewDString(viewSchemaName),       // view_schema
					tree.NewDString(view.Name),            // view_name
					tree.NewDString(dependencyDB.Name),    // table_catalog
					tree.NewDString(dependencySchemaName), // table_schema
					tree.NewDString(dependency.Name),      // table_name
				); err != nil {
					return err
				}
				seen[key] = struct{}{}
			}
			return nil
		})
	},
}

var informationSchemaTriggersTable = virtualSchemaTable{
	comment: `triggers on tables visible to the current user`,
	schema: `
CREATE TABLE information_schema.triggers (
	TRIGGER_CATALOG             STRING NOT NULL,
	TRIGGER_SCHEMA              STRING NOT NULL,
	TRIGGER_NAME                STRING NOT NULL,
	EVENT_MANIPULATION          STRING NOT NULL,
	EVENT_OBJECT_CATALOG        STRING NOT NULL,
	EVENT_OBJECT_SCHEMA         STRING NOT NULL,
	EVENT_OBJECT_TABLE          STRING NOT NULL,
	ACTION_ORDER                INT8 NOT NULL,
	ACTION_CONDITION            STRING,
	ACTION_STATEMENT            STRING,
	ACTION_ORIENTATION          STRING NOT NULL,
	ACTION_TIMING               STRING NOT NULL,
	ACTION_REFERENCE_OLD_TABLE  STRING,
	ACTION_REFERENCE_NEW_TABLE  STRING,
	ACTION_REFERENCE_OLD_ROW    STRING,
	ACTION_REFERENCE_NEW_ROW    STRING,
	CREATED                     TIMESTAMPTZ
)`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// forEachTableDesc filters tables through CheckAnyPrivilege, matching
		// the visibility rule used by SHOW CREATE TRIGGER.
		return forEachTableDesc(ctx, p, dbContext, hideVirtual, func(
			db *sqlbase.DatabaseDescriptor, schemaName string, table *sqlbase.TableDescriptor,
		) error {
			for i := range table.Triggers {
				trigger := &table.Triggers[i]
				orientation := "STATEMENT"
				if trigger.ForEachRow {
					orientation = "ROW"
				}
				if err := addRow(
					tree.NewDString(db.Name),                      // trigger_catalog
					tree.NewDString(schemaName),                   // trigger_schema
					tree.NewDString(trigger.Name),                 // trigger_name
					tree.NewDString(trigger.Event.String()),       // event_manipulation
					tree.NewDString(db.Name),                      // event_object_catalog
					tree.NewDString(schemaName),                   // event_object_schema
					tree.NewDString(table.Name),                   // event_object_table
					tree.NewDInt(tree.DInt(trigger.TriggerOrder)), // action_order
					tree.DNull,                                   // action_condition
					tree.NewDString(trigger.TriggerBody),         // action_statement
					tree.NewDString(orientation),                 // action_orientation
					tree.NewDString(trigger.ActionTime.String()), // action_timing
					tree.DNull, // action_reference_old_table
					tree.DNull, // action_reference_new_table
					tree.DNull, // action_reference_old_row
					tree.DNull, // action_reference_new_row
					tree.DNull, // created
				); err != nil {
					return err
				}
			}
			return nil
		})
	},
}

// forEachSchemaName iterates over the physical and virtual schemas.
func forEachSchemaName(
	ctx context.Context,
	p *planner,
	db *sqlbase.DatabaseDescriptor,
	requiresPrivileges bool,
	fn func(sqlbase.ResolvedSchema) error,
) error {
	schemaNames, err := getSchemaNames(ctx, p, db)
	if err != nil {
		return err
	}
	vtableEntries := p.getVirtualTabler().getEntries()
	schemas := make([]sqlbase.ResolvedSchema, 0, len(schemaNames)+len(vtableEntries))
	var userDefinedSchemaIDs []sqlbase.ID
	for id, name := range schemaNames {
		switch {
		case strings.HasPrefix(name, sessiondata.PgTempSchemaName):
			schemas = append(schemas, sqlbase.ResolvedSchema{
				Name: name,
				ID:   id,
				Kind: sqlbase.SchemaTemporary,
			})
		case name == tree.PublicSchema:
			schemas = append(schemas, sqlbase.ResolvedSchema{
				Name: name,
				ID:   id,
				Kind: sqlbase.SchemaPublic,
			})
		default:
			// The default case is a user defined schema. Collect the ID to get the
			// descriptor later.
			userDefinedSchemaIDs = append(userDefinedSchemaIDs, id)
		}
	}

	userDefinedSchemas, err := getSchemaDescriptorsFromIDs(ctx, p.txn, userDefinedSchemaIDs)
	if err != nil {
		return err
	}
	for i := range userDefinedSchemas {
		desc := userDefinedSchemas[i]
		schemas = append(schemas, sqlbase.ResolvedSchema{
			Name: desc.GetName(),
			ID:   desc.GetID(),
			Kind: sqlbase.SchemaUserDefined,
			Desc: desc,
		})
	}

	for _, schema := range vtableEntries {
		schemas = append(schemas, sqlbase.ResolvedSchema{
			Name: schema.desc.Name,
			Kind: sqlbase.SchemaVirtual,
		})
	}

	sort.Slice(schemas, func(i int, j int) bool {
		return schemas[i].Name < schemas[j].Name
	})

	for _, sc := range schemas {
		if !requiresPrivileges || canUserSeeSchema(ctx, p, sc) {
			if err := fn(sc); err != nil {
				return err
			}
		}
	}
	return nil
}

// forEachDatabaseDesc calls a function for the given DatabaseDescriptor, or if
// it is nil, retrieves all database descriptors and iterates through them in
// lexicographical order with respect to their name. If privileges are required,
// the function is only called if the user has privileges on the database.
func forEachDatabaseDesc(
	ctx context.Context,
	p *planner,
	dbContext *DatabaseDescriptor,
	requiresPrivileges bool,
	fn func(*sqlbase.DatabaseDescriptor) error,
) error {
	var dbDescs []*sqlbase.DatabaseDescriptor
	if dbContext == nil {
		allDbDescs, err := p.Tables().getAllDatabaseDescriptors(ctx, p.txn)
		if err != nil {
			return err
		}
		dbDescs = allDbDescs
	} else {
		// We can't just use dbContext here because we need to fetch the descriptor
		// with privileges from kv.
		fetchedDbDesc, err := getDatabaseDescriptorsFromIDs(ctx, p.txn, []sqlbase.ID{dbContext.ID})
		if err != nil {
			return err
		}
		dbDescs = fetchedDbDesc
	}

	// Ignore databases that the user cannot see.
	for _, dbDesc := range dbDescs {
		if !requiresPrivileges || canUserSeeDatabase(ctx, p, dbDesc) {
			if err := fn(dbDesc); err != nil {
				return err
			}
		}
	}

	return nil
}

// forEachTableDesc retrieves all table descriptors from the current
// database and all system databases and iterates through them. For
// each table, the function will call fn with its respective database
// and table descriptor.
//
// The dbContext argument specifies in which database context we are
// requesting the descriptors. In context nil all descriptors are
// visible, in non-empty contexts only the descriptors of that
// database are visible.
//
// The virtualOpts argument specifies how virtual tables are made
// visible.
func forEachTableDesc(
	ctx context.Context,
	p *planner,
	dbContext *DatabaseDescriptor,
	virtualOpts virtualOpts,
	fn func(*sqlbase.DatabaseDescriptor, string, *sqlbase.TableDescriptor) error,
) error {
	return forEachTableDescWithTableLookup(ctx, p, dbContext, virtualOpts, func(
		db *sqlbase.DatabaseDescriptor,
		scName string,
		table *sqlbase.TableDescriptor,
		_ tableLookupFn,
	) error {
		return fn(db, scName, table)
	})
}

type virtualOpts int

const (
	// virtualMany iterates over virtual schemas in every catalog/database.
	virtualMany virtualOpts = iota
	// virtualOnce iterates over virtual schemas once, in the nil database.
	virtualOnce
	// hideVirtual completely hides virtual schemas during iteration.
	hideVirtual
)

// forEachTableDescAll does the same as forEachTableDesc but also
// includes newly added non-public descriptors.
func forEachTableDescAll(
	ctx context.Context,
	p *planner,
	dbContext *DatabaseDescriptor,
	virtualOpts virtualOpts,
	fn func(*sqlbase.DatabaseDescriptor, string, *sqlbase.TableDescriptor) error,
) error {
	return forEachTableDescAllWithTableLookup(ctx,
		p, dbContext, virtualOpts,
		func(
			db *sqlbase.DatabaseDescriptor,
			scName string,
			table *sqlbase.TableDescriptor,
			_ tableLookupFn,
		) error {
			return fn(db, scName, table)
		})
}

// forEachTableDescAllWithTableLookup is like forEachTableDescAll, but it also
// provides a tableLookupFn like forEachTableDescWithTableLookup.
func forEachTableDescAllWithTableLookup(
	ctx context.Context,
	p *planner,
	dbContext *DatabaseDescriptor,
	virtualOpts virtualOpts,
	fn func(*sqlbase.DatabaseDescriptor, string, *sqlbase.TableDescriptor, tableLookupFn) error,
) error {
	return forEachTableDescWithTableLookupInternal(ctx,
		p, dbContext, virtualOpts, true /* allowAdding */, fn)
}

// forEachTableDescWithTableLookup acts like forEachTableDesc, except it also provides a
// tableLookupFn when calling fn to allow callers to lookup fetched table descriptors
// on demand. This is important for callers dealing with objects like foreign keys, where
// the metadata for each object must be augmented by looking at the referenced table.
//
// The dbContext argument specifies in which database context we are
// requesting the descriptors.  In context "" all descriptors are
// visible, in non-empty contexts only the descriptors of that
// database are visible.
func forEachTableDescWithTableLookup(
	ctx context.Context,
	p *planner,
	dbContext *DatabaseDescriptor,
	virtualOpts virtualOpts,
	fn func(*sqlbase.DatabaseDescriptor, string, *sqlbase.TableDescriptor, tableLookupFn) error,
) error {
	return forEachTableDescWithTableLookupInternal(ctx, p, dbContext, virtualOpts, false /* allowAdding */, fn)
}

func getSchemaNames(
	ctx context.Context, p *planner, dbContext *DatabaseDescriptor,
) (map[sqlbase.ID]string, error) {
	if dbContext != nil {
		return p.Tables().getSchemasForDatabase(ctx, p.txn, dbContext.ID)
	}
	ret := make(map[sqlbase.ID]string)
	dbs, err := p.Tables().getAllDatabaseDescriptors(ctx, p.txn)
	if err != nil {
		return nil, err
	}
	for _, db := range dbs {
		schemas, err := p.Tables().getSchemasForDatabase(ctx, p.txn, db.ID)
		if err != nil {
			return nil, err
		}
		for id, name := range schemas {
			ret[id] = name
		}
	}
	return ret, nil
}

// forEachTableDescWithTableLookupInternal is the logic that supports
// forEachTableDescWithTableLookup.
//
// The allowAdding argument if true includes newly added tables that
// are not yet public.
func forEachTableDescWithTableLookupInternal(
	ctx context.Context,
	p *planner,
	dbContext *DatabaseDescriptor,
	virtualOpts virtualOpts,
	allowAdding bool,
	fn func(*DatabaseDescriptor, string, *TableDescriptor, tableLookupFn) error,
) error {
	descs, err := p.Tables().getAllDescriptors(ctx, p.txn)
	if err != nil {
		return err
	}
	lCtx := newInternalLookupCtx(descs, dbContext)

	if virtualOpts == virtualMany || virtualOpts == virtualOnce {
		// Virtual descriptors first.
		vt := p.getVirtualTabler()
		vEntries := vt.getEntries()
		vSchemaNames := vt.getSchemaNames()
		iterate := func(dbDesc *DatabaseDescriptor) error {
			for _, virtSchemaName := range vSchemaNames {
				e := vEntries[virtSchemaName]
				for _, tName := range e.orderedDefNames {
					te := e.defs[tName]
					if err := fn(dbDesc, virtSchemaName, te.desc, lCtx); err != nil {
						return err
					}
				}
			}
			return nil
		}

		switch virtualOpts {
		case virtualOnce:
			if err := iterate(nil); err != nil {
				return err
			}
		case virtualMany:
			for _, dbID := range lCtx.dbIDs {
				dbDesc := lCtx.dbDescs[dbID]
				if err := iterate(dbDesc); err != nil {
					return err
				}
			}
		}
	}

	// Generate all schema names, and keep a mapping.
	schemaNames, err := getSchemaNames(ctx, p, dbContext)
	if err != nil {
		return err
	}

	// Physical descriptors next.
	for _, tbID := range lCtx.tbIDs {
		table := lCtx.tbDescs[tbID]
		dbDesc, parentExists := lCtx.dbDescs[table.GetParentID()]
		if table.Dropped() || !canUserSeeTable(ctx, p, table, allowAdding) || !parentExists {
			continue
		}
		scName, ok := schemaNames[table.GetParentSchemaID()]
		if !ok {
			// if we can not find schema, check if table exist,
			// descriptor may be drooped after get all descriptors from cache.
			tab, err := sqlbase.GetTableDescFromID(ctx, p.txn, table.ID)
			if err != nil {
				if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
					log.Errorf(ctx, "The table(name:%s,id:%d,state:%s,version:%d) retrieved from the cache "+
						"cannot be found in the system table.", table.Name, table.ID, table.State.String(), table.Version)
					continue
				} else {
					return err
				}
			}
			if tab.Dropped() {
				log.Errorf(ctx, "The table(name:%s,id:%d,state:%s,version:%d) retrieved from the cache is being dropped"+
					" when retrieved from the system table.", table.Name, table.ID, table.State.String(), table.Version)
				continue
			}
			return errors.AssertionFailedf("schema id %d not found", table.GetParentSchemaID())
		}
		if err := fn(dbDesc, scName, table, lCtx); err != nil {
			return err
		}
	}
	return nil
}

func forEachIndexInTable(
	table *sqlbase.TableDescriptor, fn func(*sqlbase.IndexDescriptor) error,
) error {
	if table.IsPhysicalTable() {
		if err := fn(&table.PrimaryIndex); err != nil {
			return err
		}
	}
	for i := range table.Indexes {
		if err := fn(&table.Indexes[i]); err != nil {
			return err
		}
	}
	return nil
}

func forEachColumnInTable(
	table *sqlbase.TableDescriptor, fn func(*sqlbase.ColumnDescriptor) error,
) error {
	// Table descriptors already hold columns in-order.
	for i := range table.Columns {
		if err := fn(&table.Columns[i]); err != nil {
			return err
		}
	}
	return nil
}

// forEachVisibleColumn visits non-hidden columns in descriptor order.
func forEachVisibleColumn(
	table *sqlbase.TableDescriptor, fn func(*sqlbase.ColumnDescriptor) error,
) error {
	return forEachColumnInTable(table, func(column *sqlbase.ColumnDescriptor) error {
		if column.Hidden {
			return nil
		}
		return fn(column)
	})
}

func forEachColumnInIndex(
	table *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	fn func(*sqlbase.ColumnDescriptor) error,
) error {
	colMap := make(map[sqlbase.ColumnID]*sqlbase.ColumnDescriptor, len(table.Columns))
	for i := range table.Columns {
		id := table.Columns[i].ID
		colMap[id] = &table.Columns[i]
	}
	for _, columnID := range index.ColumnIDs {
		column := colMap[columnID]
		if err := fn(column); err != nil {
			return err
		}
	}
	return nil
}

func forEachRole(
	ctx context.Context, p *planner, fn func(username string, isRole bool, noLogin bool) error,
) error {
	query := `
SELECT
	username,
	"isRole",
	EXISTS(
		SELECT
			option
		FROM
			system.role_options AS r
		WHERE
			r.username = u.username AND option = 'NOLOGIN'
	)
		AS nologin
FROM
	system.users AS u;
`
	rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
		ctx, "read-roles", p.txn, query,
	)

	if err != nil {
		return err
	}

	for _, row := range rows {
		username := tree.MustBeDString(row[0])
		isRole, ok := row[1].(*tree.DBool)
		if !ok {
			return errors.Errorf("isRole should be a boolean value, found %s instead", row[1].ResolvedType())
		}
		noLogin, ok := row[2].(*tree.DBool)
		if !ok {
			return errors.Errorf("noLogin should be a boolean value, found %s instead", row[1].ResolvedType())
		}
		if err := fn(string(username), bool(*isRole), bool(*noLogin)); err != nil {
			return err
		}
	}

	return nil
}

// getVisibleRoleGrantees returns role names visible in role grant views. Admins
// can inspect every role grant, while other users can inspect only their roles.
func getVisibleRoleGrantees(ctx context.Context, p *planner) (map[string]struct{}, error) {
	roleGrantees := make(map[string]struct{})
	if err := forEachRole(ctx, p, func(username string, isRole bool, noLogin bool) error {
		if isRole {
			roleGrantees[username] = struct{}{}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	// PUBLIC is an implicit role and is not stored in system.users.
	roleGrantees[sqlbase.PublicRole] = struct{}{}

	allGrantees, err := p.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}
	if allGrantees {
		return roleGrantees, nil
	}

	memberOf, err := p.MemberOfWithAdminOption(ctx, p.SessionData().User)
	if err != nil {
		return nil, err
	}
	for grantee := range roleGrantees {
		if grantee == sqlbase.PublicRole {
			continue
		}
		// Map membership identifies inherited roles; the boolean value only
		// indicates whether the user has ADMIN OPTION for that role.
		if _, ok := memberOf[grantee]; !ok {
			delete(roleGrantees, grantee)
		}
	}
	return roleGrantees, nil
}

func forEachRoleMembership(
	ctx context.Context, p *planner, fn func(role, member string, isAdmin bool) error,
) error {
	query := `SELECT "role", "member", "isAdmin" FROM system.role_members`
	rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
		ctx, "read-members", p.txn, query,
	)
	if err != nil {
		return err
	}

	for _, row := range rows {
		roleName := tree.MustBeDString(row[0])
		memberName := tree.MustBeDString(row[1])
		isAdmin := row[2].(*tree.DBool)

		if err := fn(string(roleName), string(memberName), bool(*isAdmin)); err != nil {
			return err
		}
	}
	return nil
}

func canUserSeeDatabase(ctx context.Context, p *planner, desc sqlbase.DescriptorProto) bool {
	return p.CheckAnyPrivilege(ctx, desc) == nil
}

func canUserSeeSchema(ctx context.Context, p *planner, sc sqlbase.ResolvedSchema) bool {
	if sc.Kind == sqlbase.SchemaUserDefined {
		return p.CheckAnyPrivilege(ctx, sc.Desc) == nil
	}
	return true
}

func canUserSeeTable(
	ctx context.Context, p *planner, table *sqlbase.TableDescriptor, allowAdding bool,
) bool {
	return isTableVisible(table, allowAdding) && p.CheckAnyPrivilege(ctx, table) == nil
}

func isTableVisible(table *TableDescriptor, allowAdding bool) bool {
	return table.State == sqlbase.TableDescriptor_PUBLIC ||
		(allowAdding && table.State == sqlbase.TableDescriptor_ADD) ||
		table.State == sqlbase.TableDescriptor_ALTER
}

func getAllProcedures(ctx context.Context, p *planner) ([]sqlbase.ProcedureDescriptor, error) {
	query := `SELECT descriptor FROM system.user_defined_routine WHERE routine_type = 1`
	rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
		ctx, "read-procedures", p.txn, query,
	)
	if err != nil {
		return nil, err
	}
	var procs []sqlbase.ProcedureDescriptor
	for _, row := range rows {
		var desc sqlbase.ProcedureDescriptor
		val := tree.MustBeDBytes(row[0])
		if err := protoutil.Unmarshal([]byte(val), &desc); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to parse value for key %q", val)
		}
		procs = append(procs, desc)
	}
	return procs, nil
}

var informationSchemaProcedurePrivileges = virtualSchemaTable{
	comment: `privileges granted on procedure (incomplete; may contain excess users or roles)`,
	schema: `
CREATE TABLE information_schema.procedure_privileges (
	GRANTEE            STRING NOT NULL,
	PROCEDURE_CATALOG  STRING NOT NULL,
	PROCEDURE_SCHEMA   STRING NOT NULL,
	PROCEDURE_NAME     STRING NOT NULL,
	PRIVILEGE_TYPE     STRING NOT NULL
)`,
	populate: populateProcedurePrivileges,
}

var informationSchemaRoleRoutineGrants = virtualSchemaTable{
	comment: `privileges granted on procedures and Lua user-defined functions`,
	schema: `
CREATE TABLE information_schema.role_routine_grants (
	GRANTOR          STRING NULL,
	GRANTEE          STRING NOT NULL,
	SPECIFIC_CATALOG STRING NULL,
	SPECIFIC_SCHEMA  STRING NULL,
	SPECIFIC_NAME    STRING NULL,
	ROUTINE_CATALOG  STRING NULL,
	ROUTINE_SCHEMA   STRING NULL,
	ROUTINE_NAME     STRING NULL,
	PRIVILEGE_TYPE   STRING NOT NULL,
	IS_GRANTABLE     STRING NULL
)`,
	populate: populateRoleRoutineGrants,
}

type luaUDFRoutine struct {
	name   string
	dbName string
}

func getAllLuaUDFs(ctx context.Context, p *planner) ([]luaUDFRoutine, error) {
	query := `SELECT name, descriptor FROM system.user_defined_routine WHERE routine_type = 2`
	rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
		ctx, "read-lua-udfs", p.txn, query,
	)
	if err != nil {
		return nil, err
	}
	udfs := make([]luaUDFRoutine, 0, len(rows))
	for _, row := range rows {
		var desc sqlbase.FunctionDescriptor
		val := tree.MustBeDBytes(row[1])
		if err := protoutil.Unmarshal([]byte(val), &desc); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to parse Lua UDF descriptor for %q", val)
		}
		if desc.Language != "LUA" {
			continue
		}
		udfs = append(udfs, luaUDFRoutine{
			name:   string(tree.MustBeDString(row[0])),
			dbName: desc.DbName,
		})
	}
	return udfs, nil
}

func populateRoleRoutineGrants(
	ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error,
) error {
	roleGrantees, err := getVisibleRoleGrantees(ctx, p)
	if err != nil {
		return err
	}

	// Stored procedures use their persisted privilege descriptors. The view
	// exposes only EXECUTE grants, matching the routine privilege semantics.
	procs, err := getAllProcedures(ctx, p)
	if err != nil {
		return err
	}
	for _, proc := range procs {
		db, err := getDatabaseDescByID(ctx, p.txn, proc.DbID)
		if err != nil {
			return err
		}
		sc, err := getSchemaDescByID(ctx, p.txn, proc.SchemaID)
		if err != nil {
			return err
		}
		for _, u := range proc.Privileges.Show() {
			if _, ok := roleGrantees[u.User]; !ok {
				continue
			}
			for _, priv := range u.Privileges {
				// ALL is stored as its own privilege bit, but the ISO routine
				// privilege exposed by this view is always EXECUTE.
				if priv == privilege.EXECUTE.String() || priv == privilege.ALL.String() {
					if err := addRow(
						tree.DNull,                                  // grantor
						tree.NewDString(u.User),                     // grantee
						tree.NewDString(db.Name),                    // specific_catalog
						tree.NewDString(sc.Name),                    // specific_schema
						tree.NewDString(proc.Name),                  // specific_name
						tree.NewDString(db.Name),                    // routine_catalog
						tree.NewDString(sc.Name),                    // routine_schema
						tree.NewDString(proc.Name),                  // routine_name
						tree.NewDString(privilege.EXECUTE.String()), // privilege_type
						tree.DNull,                                  // is_grantable
					); err != nil {
						return err
					}
				}
			}
		}
	}

	// Lua UDFs are public in KWDB: expose EXECUTE for every visible role.
	udfs, err := getAllLuaUDFs(ctx, p)
	if err != nil {
		return err
	}
	for _, udf := range udfs {
		for grantee := range roleGrantees {
			if err := addRow(
				tree.DNull,                                  // grantor
				tree.NewDString(grantee),                    // grantee
				tree.NewDString(udf.dbName),                 // specific_catalog
				tree.DNull,                                  // specific_schema
				tree.NewDString(udf.name),                   // specific_name
				tree.NewDString(udf.dbName),                 // routine_catalog
				tree.DNull,                                  // routine_schema
				tree.NewDString(udf.name),                   // routine_name
				tree.NewDString(privilege.EXECUTE.String()), // privilege_type
				tree.DNull,                                  // is_grantable
			); err != nil {
				return err
			}
		}
	}
	return nil
}

// populateProcedurePrivileges is used to populate both procedure_privileges and role_procedure_grants.
func populateProcedurePrivileges(
	ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error,
) error {
	procs, err := getAllProcedures(ctx, p)
	if err != nil {
		return err
	}
	for _, proc := range procs {
		db, err := getDatabaseDescByID(ctx, p.txn, proc.DbID)
		if err != nil {
			return err
		}
		sc, err := getSchemaDescByID(ctx, p.txn, proc.SchemaID)
		if err != nil {
			return err
		}
		dbNameStr := tree.NewDString(db.Name)
		scNameStr := tree.NewDString(sc.Name)
		tbNameStr := tree.NewDString(proc.Name)

		for _, u := range proc.Privileges.Show() {
			for _, priv := range u.Privileges {
				if err := addRow(
					tree.NewDString(u.User), // grantee
					dbNameStr,               // procedure_catalog
					scNameStr,               // procedure_schema
					tbNameStr,               // procedure_name
					tree.NewDString(priv),   // privilege_type
				); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
