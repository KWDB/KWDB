// Copyright 2012, Google Inc. All rights reserved.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
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

// This code was derived from https://github.com/youtube/vitess.

package parser

import (
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Statement is the result of parsing a single statement. It contains the AST
// node along with other information.
type Statement struct {
	// AST is the root of the AST tree for the parsed statement.
	AST tree.Statement

	// SQL is the original SQL from which the statement was parsed. Note that this
	// is not appropriate for use in logging, as it may contain passwords and
	// other sensitive data.
	SQL string

	// NumPlaceholders indicates the number of arguments to the statement (which
	// are referenced through placeholders). This corresponds to the highest
	// argument position (i.e. the x in "$x") that appears in the query.
	//
	// Note: where there are "gaps" in the placeholder positions, this number is
	// based on the highest position encountered. For example, for `SELECT $3`,
	// NumPlaceholders is 3. These cases are malformed and will result in a
	// type-check error.
	NumPlaceholders int

	// NumAnnotations indicates the number of annotations in the tree. It is equal
	// to the maximum annotation index.
	NumAnnotations tree.AnnotationIdx

	StmtPrepparam []string

	Insertdirectstmt Insertdirectstmt
}

// Insertdirectstmt used to record insert_direct related information
type Insertdirectstmt struct {
	InsertValues []string
	ValuesType   []TokenType
	InsertFast   bool
	RowsAffected int64
	ErrorInfo    error
	UseDeepRule  bool
	// DedupRule mode, 2 is reject mode, and 3 is discard mode
	DedupRule          int64
	DedupRows          int64
	PreparePlaceholder int
	IgnoreBatcherror   bool
	BatchFailed        int
	BatchFailedColumn  int
}

// Statements is a list of parsed statements.
type Statements []Statement

// String returns the AST formatted as a string.
func (stmts Statements) String() string {
	return stmts.StringWithFlags(tree.FmtSimple)
}

// StringWithFlags returns the AST formatted as a string (with the given flags).
func (stmts Statements) StringWithFlags(flags tree.FmtFlags) string {
	ctx := tree.NewFmtCtx(flags)
	for i, s := range stmts {
		if i > 0 {
			ctx.WriteString("; ")
		}
		ctx.FormatNode(s.AST)
	}
	return ctx.CloseAndGetString()
}

// Parser wraps a scanner, parser and other utilities present in the parser
// package.
type Parser struct {
	scanner        scanner
	lexer          lexer
	parserImpl     sqlParserImpl
	tokBuf         [10]sqlSymType
	stmtBuf        [1]Statement
	Dudgetstable   func(dbName *string, tableName string) bool
	IsShortcircuit bool
	TsSupportBatch bool
	Customize      bool
}

// INT8 is the historical interpretation of INT. This should be left
// alone in the future, since there are many sql fragments stored
// in various descriptors.  Any user input that was created after
// INT := INT4 will simply use INT4 in any resulting code.
var defaultNakedIntType = types.Int

// tsdirectcap provides a larger capacity to avoid performance consumption
// caused by frequent expansion during sequential insertion.
var tsdirectcap = 1000

// SetPrepareMode is used to set whether insert_direct
// prepare is enabled or not
func (p *Parser) SetPrepareMode(b bool) {
	p.scanner.shortinsert.PrepareMode = b
}

// Parse parses the sql and returns a list of statements.
func (p *Parser) Parse(sql string) (Statements, error) {
	return p.parseWithDepth(1, sql, defaultNakedIntType)
}

func addAutoLimit(stmt tree.Statement, values *settings.Values) tree.Statement {
	switch s := stmt.(type) {
	case *tree.Explain:
		s.Statement = addAutoLimit(s.Statement, values)
	case *tree.Prepare:
		s.Statement = addAutoLimit(s.Statement, values)
	case *tree.Select:
		if s.Limit == nil {
			s.Limit = &tree.Limit{Count: tree.NewDInt(tree.DInt(opt.AutoLimitQuantity.Get(values))), IsAutoLimit: true}
		}
	}
	return stmt
}

// ParseWithInt parses a sql statement string and returns a list of
// Statements. The INT token will result in the specified TInt type.
//
//	sql: input statement;
//	nakedIntType: it's a restriction on the ConnectionHandler type;
//	Statements: AST Trees
//	error: error
func (p *Parser) ParseWithInt(
	sql string, nakedIntType *types.T, values *settings.Values,
) (Statements, error) {
	stmts, err := p.parseWithDepth(1, sql, nakedIntType)
	if err != nil {
		return stmts, err
	}
	if values != nil && opt.AutoLimitQuantity.Get(values) > 0 {
		for i, stmt := range stmts {
			stmts[i].AST = addAutoLimit(stmt.AST, values)
		}
	}
	return stmts, nil
}

func (p *Parser) parseOneWithDepth(depth int, sql string) (Statement, error) {
	stmts, err := p.parseWithDepth(1, sql, defaultNakedIntType)
	if err != nil {
		return Statement{}, err
	}
	if len(stmts) != 1 {
		return Statement{}, errors.AssertionFailedf("expected 1 statement, but found %d", len(stmts))
	}
	return stmts[0], nil
}

/* scanOneStmt
 * @Description：scan one statement for syntax check and lex check;
 * @In tokens: split the SQL by word to list;
 * @In done: whether scan complete;
 */
func (p *Parser) scanOneStmt() (sql string, tokens []sqlSymType, done bool) {
	var lval sqlSymType
	tokens = p.tokBuf[:0]

	// Scan the first token.
	for {
		// loop scan each word
		p.scanner.scan(&lval)
		if lval.id == 0 {
			return "", nil, true
		}
		if lval.id == INSERT {
			p.scanner.shortinsert.isInsert = true
			break
		}
		if lval.id != ';' {
			break
		}
	}

	startPos := lval.pos
	// We make the resulting token positions match the returned string.
	lval.pos = 0
	tokens = append(tokens, lval)
	var dot int
	decided := false
	for {
		if lval.id == ERROR {
			return p.scanner.in[startPos:], tokens, true
		}
		posBeforeScan := p.scanner.pos
		p.scanner.scan(&lval)
		if lval.id == 0 || lval.id == ';' {
			return p.scanner.in[startPos:posBeforeScan], tokens, (lval.id == 0)
		}
		lval.pos -= startPos

		if p.scanner.shortinsert.isInsert && p.IsShortcircuit {
			if lval.id == '.' {
				dot++
			}
			if len(tokens) == 9 || p.scanner.shortinsert.isValues {
				if len(tokens) > 5 && tokens[5].str == "tag" || len(tokens) < 3 || tokens[1].id != INTO && tokens[3].id != INTO {
					p.scanner.shortinsert.isInsert = false
					tokens = append(tokens, lval)
					continue
				}
				db, tb := extractDBAndTable(tokens, dot, &p.scanner.shortinsert.NoSchema)
				p.scanner.shortinsert.db, p.scanner.shortinsert.tb = db, tb
			}
			if lval.id == VALUES {
				p.scanner.shortinsert.RowCount = 0
				p.scanner.shortinsert.isValues = true
			}
			if lval.id == SELECT {
				tokens = append(tokens, lval)
				p.IsShortcircuit = false
				continue
			}

			if p.scanner.shortinsert.isValues && p.scanner.shortinsert.isTsTable {
				continue
			}
			// "insert into database.table": len(tokens) == 4
			if len(p.scanner.shortinsert.tb) != 0 {
				if !decided && p.Dudgetstable != nil && p.Dudgetstable(&p.scanner.shortinsert.db, p.scanner.shortinsert.tb) {
					p.scanner.shortinsert.isTsTable = true
					if p.scanner.shortinsert.InsertValues == nil {
						p.scanner.shortinsert.InsertValues = make([]string, 0, tsdirectcap)
						p.scanner.shortinsert.ValuesType = make([]TokenType, 0, tsdirectcap)
					}
				}
				decided = true
			}
		}
		tokens = append(tokens, lval)
	}
}

// Function for tsinsert_direct database and table names extraction
func extractDBAndTable(tokens []sqlSymType, dot int, NoSchema *bool) (db, tb string) {
	*NoSchema = tokens[1].str == "without" && tokens[2].str == "schema"
	boolAsInt := 0
	if *NoSchema {
		boolAsInt = 1
	}
	switch dot {
	case 0:
		// Case 1: insert into table values; insert without schema into table values
		tb = tokens[2+2*boolAsInt].str
	case 1:
		// Case 2: insert into database.table values; insert without schema into database.table values
		// Case 3: insert into schema.table values; insert without schema into schema.table values; The actualDBName is provided in the Dudgetstable
		db, tb = tokens[2+2*boolAsInt].str, tokens[4+2*boolAsInt].str
	case 2:
		// Case 4: insert into database.schema.table values; insert without schema into database.schema.table values
		db, tb = tokens[2+2*boolAsInt].str, tokens[6+2*boolAsInt].str
	}
	return db, tb
}

// MakeDirectSTmt constructs the stmt structure required for tsinsert_direct
func makeDirectStmt(p *Parser, NoScheNameList tree.NoSchemaNameList, sql string) Statement {
	if p.scanner.shortinsert.NoSchema {
		NoScheNameList = make(tree.NoSchemaNameList, 0, len(p.scanner.shortinsert.InsertValues))
		for i := 0; i < len(p.scanner.shortinsert.Columnsname); i += 3 {
			noScheName := tree.NoSchemaName{
				Name:    p.scanner.shortinsert.Columnsname[i],
				StrType: string(p.scanner.shortinsert.Columnsname[i+1]),
				IsTag:   p.scanner.shortinsert.Columnsname[i+2] == "tag",
			}
			if typelen, ok := p.scanner.shortinsert.ColumnsLengthMap[noScheName.Name]; ok {
				noScheName.TypeLen = typelen
			}
			NoScheNameList = append(NoScheNameList, noScheName)
		}
	}
	stmt := Statement{
		AST: &tree.Insert{
			Table:           tree.NewTableName(tree.Name(p.scanner.shortinsert.db), tree.Name(p.scanner.shortinsert.tb)),
			Returning:       &tree.NoReturningClause{},
			IsNoSchema:      p.scanner.shortinsert.NoSchema,
			NoSchemaColumns: NoScheNameList,
			Columns:         p.scanner.shortinsert.Columnsname},
		SQL: sql,
		Insertdirectstmt: Insertdirectstmt{
			InsertValues:       p.scanner.shortinsert.InsertValues,
			ValuesType:         p.scanner.shortinsert.ValuesType,
			RowsAffected:       int64(p.scanner.shortinsert.RowCount),
			PreparePlaceholder: p.scanner.shortinsert.Prepareplaceholder,
		},
		NumPlaceholders: p.scanner.shortinsert.Prepareplaceholder,
	}
	return stmt
}

/* parseWithDepth
 * @Description：construct SQL to AST Trees;
 * @In depth: construct depth;
 * @In sql: input SQL;
 * @In nakedIntType: it's a restriction on the ConnectionHandler type;
 * @In pTag: primary tag column
 * @In dataColNum: number of data columns
 * @Return 1: AST Trees
 * @Return 2: error
 */
func (p *Parser) parseWithDepth(depth int, sql string, nakedIntType *types.T) (Statements, error) {
	stmts := Statements(p.stmtBuf[:0])
	p.scanner.init(sql)
	defer p.scanner.cleanup()
	for {
		p.scanner.shortinsert.isValues, p.scanner.shortinsert.isInsert = false, false
		p.scanner.shortinsert.Customize = false
		// loop parse sql
		sql, tokens, done := p.scanOneStmt()
		if p.scanner.shortinsert.isInsert && p.scanner.shortinsert.isTsTable {
			p.Customize = p.scanner.shortinsert.Customize
			var NoScheNameList tree.NoSchemaNameList
			stmt := makeDirectStmt(p, NoScheNameList, sql)
			if sql != "" {
				stmts = append(stmts, stmt)
			}
			if done {
				break
			}
			continue
		}
		stmt, err := p.parse(depth+1, sql, tokens, nakedIntType)
		if err != nil {
			return nil, err
		}
		if stmt.AST != nil {
			stmts = append(stmts, stmt)
		}
		if done {
			break
		}
	}
	return stmts, nil
}

/* parse
 * @Description：parse parses a statement from the given scanned tokens;
 * @In tokens: split the SQL by word to list;
 * @Return 1: AST Tree;
 * @Return 3: error
 */
func (p *Parser) parse(
	depth int, sql string, tokens []sqlSymType, nakedIntType *types.T,
) (Statement, error) {
	p.lexer.init(sql, tokens, nakedIntType)
	defer p.lexer.cleanup()
	if p.parserImpl.Parse(&p.lexer) != 0 {
		if p.lexer.lastError == nil {
			// This should never happen -- there should be an error object
			// every time Parse() returns nonzero. We're just playing safe
			// here.
			p.lexer.Error("syntax error")
		}
		err := p.lexer.lastError

		// Compatibility with 19.1 telemetry: prefix the telemetry keys
		// with the "syntax." prefix.
		// TODO(knz): move the auto-prefixing of feature names to a
		// higher level in the call stack.
		tkeys := errors.GetTelemetryKeys(err)
		if len(tkeys) > 0 {
			for i := range tkeys {
				tkeys[i] = "syntax." + tkeys[i]
			}
			err = errors.WithTelemetry(err, tkeys...)
		}

		return Statement{}, err
	}
	return Statement{
		AST:             p.lexer.stmt,
		SQL:             sql,
		NumPlaceholders: p.lexer.numPlaceholders,
		NumAnnotations:  p.lexer.numAnnotations,
	}, nil
}

// unaryNegation constructs an AST node for a negation. This attempts
// to preserve constant NumVals and embed the negative sign inside
// them instead of wrapping in an UnaryExpr. This in turn ensures
// that negative numbers get considered as a single constant
// for the purpose of formatting and scrubbing.
func unaryNegation(e tree.Expr) tree.Expr {
	if cst, ok := e.(*tree.NumVal); ok {
		cst.Negate()
		return cst
	}

	// Common case.
	return &tree.UnaryExpr{Operator: tree.UnaryMinus, Expr: e}
}

// Parse parses a sql statement string and returns a list of Statements.
func Parse(sql string) (Statements, error) {
	var p Parser
	return p.parseWithDepth(1, sql, defaultNakedIntType)
}

// ParseOne parses a sql statement string, ensuring that it contains only a
// single statement, and returns that Statement. ParseOne will always
// interpret the INT and SERIAL types as 64-bit types, since this is
// used in various internal-execution paths where we might receive
// bits of SQL from other nodes. In general, we expect that all
// user-generated SQL has been run through the ParseWithInt() function.
func ParseOne(sql string) (Statement, error) {
	var p Parser
	return p.parseOneWithDepth(1, sql)
}

// ParseQualifiedTableName parses a SQL string of the form
// `[ database_name . ] [ schema_name . ] table_name`.
func ParseQualifiedTableName(sql string) (*tree.TableName, error) {
	name, err := ParseTableName(sql)
	if err != nil {
		return nil, err
	}
	tn := name.ToTableName()
	return &tn, nil
}

// ParseTableName parses a table name.
func ParseTableName(sql string) (*tree.UnresolvedObjectName, error) {
	// We wrap the name we want to parse into a dummy statement since our parser
	// can only parse full statements.
	stmt, err := ParseOne(fmt.Sprintf("ALTER TABLE %s RENAME TO x", sql))
	if err != nil {
		return nil, err
	}
	rename, ok := stmt.AST.(*tree.RenameTable)
	if !ok {
		return nil, errors.AssertionFailedf("expected an ALTER TABLE statement, but found %T", stmt)
	}
	return rename.Name, nil
}

// parseExprs parses one or more sql expressions.
func parseExprs(exprs []string) (tree.Exprs, error) {
	stmt, err := ParseOne(fmt.Sprintf("SET ROW (%s)", strings.Join(exprs, ",")))
	if err != nil {
		return nil, err
	}
	set, ok := stmt.AST.(*tree.SetVar)
	if !ok {
		return nil, errors.AssertionFailedf("expected a SET statement, but found %T", stmt)
	}
	return set.Values, nil
}

// ParseExprs is a short-hand for parseExprs(sql)
func ParseExprs(sql []string) (tree.Exprs, error) {
	if len(sql) == 0 {
		return tree.Exprs{}, nil
	}
	return parseExprs(sql)
}

// ParseExpr is a short-hand for parseExprs([]string{sql})
func ParseExpr(sql string) (tree.Expr, error) {
	exprs, err := parseExprs([]string{sql})
	if err != nil {
		return nil, err
	}
	if len(exprs) != 1 {
		return nil, errors.AssertionFailedf("expected 1 expression, found %d", len(exprs))
	}
	return exprs[0], nil
}

// ParseType parses a column type.
func ParseType(sql string) (*types.T, error) {
	expr, err := ParseExpr(fmt.Sprintf("1::%s", sql))
	if err != nil {
		return nil, err
	}

	cast, ok := expr.(*tree.CastExpr)
	if !ok {
		return nil, errors.AssertionFailedf("expected a tree.CastExpr, but found %T", expr)
	}

	return cast.Type, nil
}

var errBitLengthNotPositive = pgerror.WithCandidateCode(
	errors.New("length for type bit must be at least 1"), pgcode.InvalidParameterValue)

// newBitType creates a new BIT type with the given bit width.
func newBitType(width int32, varying bool) (*types.T, error) {
	if width < 1 {
		return nil, errBitLengthNotPositive
	}
	if varying {
		return types.MakeVarBit(width), nil
	}
	return types.MakeBit(width), nil
}

func newBytesType(width int32, typeEngine uint32) (*types.T, error) {
	if width < 1 {
		return nil, errBitLengthNotPositive
	}
	return types.MakeBytes(width, typeEngine), nil
}

func newVarBytesType(width int32, typeEngine uint32) (*types.T, error) {
	if width < 1 {
		return nil, errBitLengthNotPositive
	}
	return types.MakeVarBytes(width, typeEngine), nil
}

var errFloatPrecAtLeast1 = pgerror.WithCandidateCode(
	errors.New("precision for type float must be at least 1 bit"), pgcode.InvalidParameterValue)
var errFloatPrecMax54 = pgerror.WithCandidateCode(
	errors.New("precision for type float must be less than 54 bits"), pgcode.InvalidParameterValue)

// newFloat creates a type for FLOAT with the given precision.
func newFloat(prec int64) (*types.T, error) {
	if prec < 1 {
		return nil, errFloatPrecAtLeast1
	}
	if prec <= 24 {
		return types.Float4, nil
	}
	if prec <= 54 {
		return types.Float, nil
	}
	return nil, errFloatPrecMax54
}

// newDecimal creates a type for DECIMAL with the given precision and scale.
func newDecimal(prec, scale int32) (*types.T, error) {
	if scale > prec {
		err := pgerror.WithCandidateCode(
			errors.Newf("scale (%d) must be between 0 and precision (%d)", scale, prec),
			pgcode.InvalidParameterValue)
		return nil, err
	}
	return types.MakeDecimal(prec, scale), nil
}

// ArrayOf creates a type alias for an array of the given element type and fixed
// bounds.
func arrayOf(colType *types.T, bounds []int32) (*types.T, error) {
	if err := types.CheckArrayElementType(colType); err != nil {
		return nil, err
	}

	// Currently bounds are ignored.
	return types.MakeArray(colType), nil
}

// The SERIAL types are pseudo-types that are only used during parsing. After
// that, they should behave identically to INT columns. They are declared
// as INT types, but using different instances than types.Int, types.Int2, etc.
// so that they can be compared by pointer to differentiate them from the
// singleton INT types. While the usual requirement is that == is never used to
// compare types, this is one case where it's allowed.
var (
	serial2Type = *types.Int2
	serial4Type = *types.Int4
	serial8Type = *types.Int
)

func isSerialType(typ *types.T) bool {
	// This is a special case where == is used to compare types, since the SERIAL
	// types are pseudo-types.
	return typ == &serial2Type || typ == &serial4Type || typ == &serial8Type
}

// GetIsTsTable return isTsTable
func (p *Parser) GetIsTsTable() bool {
	return p.scanner.shortinsert.isTsTable
}
