// Copyright 2018 The Cockroach Authors.
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

package main

import (
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optgen/lang"
)

// metadata generates and stores the mapping from Optgen language expressions to
// the Go types that the code generators use to implement those expressions. In
// addition, when generating strongly-typed Go code, it is often necessary to
// insert casts, struct referencing (&), and pointer dereferences (*). Since the
// Optgen code generators do not have access to regular Go metadata for these
// types, aspects of it must be replicated here.
type metadata struct {
	compiled  *lang.CompiledExpr
	types     map[string]*typeDef
	exprTypes map[lang.Expr]*typeDef
}

// typeDef describes the name and characteristics of each Go type used by the
// code generators. This is used by the Optgen code generators to generate
// correct strongly-typed code.
//
// Depending on the context, values of a particular Optgen type are represented
// as different Go types. Here are the three contexts:
//
//	field type: Go type used for a struct field having the Optgen type
//	param type: Go type used when a value of the Optgen type is passed as a
//	            strongly-typed parameter (i.e. of same type as itself)
//	dynamic param type: Go type used when a value of the Optgen type is passed
//	                    as a dynamic type (i.e. interface{} or opt.Expr)
//
// Here are some examples:
//
//	              Field Type         Param Type         Dynamic Param Type
//	ScalarExpr    opt.ScalarExpr     opt.ScalarExpr     opt.ScalarExpr
//	FiltersExpr   memo.FiltersExpr   memo.FiltersExpr   *memo.FiltersExpr
//	ConstExpr     *memo.ConstExpr    *memo.ConstExpr    *memo.ConstExpr
//	ScanPrivate   memo.ScanPrivate   *memo.ScanPrivate  *memo.ScanPrivate
//	ColSet        opt.ColSet         opt.ColSet         *opt.ColSet
//
// The reason for these different representations is to avoid extra Go object
// allocations. In particular, when a non-pointer field is cast directly to an
// interface like opt.Expr, the Go runtime will allocate a copy of the field on
// the heap. Taking the address of the field avoids that allocation.
type typeDef struct {
	// name is the correctly qualified Go name of the type, as it should appear
	// in the current package. For example, if the current package is "memo":
	//
	//   RelExpr
	//   opt.Expr
	//   FiltersExpr
	//   ScanLimit
	//   tree.Subquery
	//   types.T
	//
	name string

	// fullName is the fully qualified Go name of the type, irrespective of the
	// current package. For example, if the current package is "memo":
	//
	//   memo.RelExpr
	//   opt.Expr
	//   memo.FiltersExpr
	//   memo.ScanLimit
	//   tree.Subquery
	//   types.T
	//
	fullName string

	// friendlyName is a human-friendly name for the type that is a simple
	// identifier unqualified by a package name. It is used in .opt files to refer
	// to types. It's also used to generate methods based on the type, like Intern
	// methods, where special characters are not allowed. For example:
	//
	//   RelExpr
	//   Expr
	//   FiltersExpr
	//   ScanLimit
	//   Subquery
	//   DatumType
	//
	friendlyName string

	// isExpr is true if the type is either one of the expression-related
	// interfaces (opt.Expr, memo.RelExpr, opt.ScalarExpr), or one of the auto-
	// generated expression types (memo.ScanExpr, memo.AndExpr, etc.).
	isExpr bool

	// isPointer is true if the type is a Go pointer or interface type (types.T,
	// memo.RelExpr, *tree.Subquery, etc.).
	isPointer bool

	// isInterface is true if the type is a Go interface (opt.ScalarExpr,
	// memo.RelExpr, etc.). If isInterface is true, then isPointer is
	// automatically set to true as well.
	isInterface bool

	// usePointerIntern is true if the type should be treated as a pointer during
	// interning, meaning that an instance will be hashed by address rather than
	// by value. This reduces the benefits of interning, but is fine for types
	// that are difficult to hash and/or where there is little or no possibility
	// of sharing anyway. See interner.go for more details.
	usePointerIntern bool

	// passByVal is true if the type should be passed by value to custom functions,
	// as well as stored by value in structs and variables.
	passByVal bool

	// isGenerated is true if the type's definition was auto-generated by Optgen,
	// vs. manually defined as part of the map in the newMetadata method.
	isGenerated bool

	// listItemType links to the type of items in the list, if this type is a
	// a list type (e.g. memo.FiltersExpr). If this is not a list type, then
	// listItemType is nil.
	listItemType *typeDef
}

// isListType is true if this type is represented as a Go slice. For example:
//
//	type FiltersExpr []FiltersItem
func (t *typeDef) isListType() bool {
	return t.listItemType != nil
}

// asField returns the Go type used when this Optgen type is used as a field in
// a struct. For example:
//
//	type SomeExpr struct {
//	  expr    opt.ScalarExpr
//	  filters FiltersExpr
//	  var     *VariablExpr
//	}
func (t *typeDef) asField() string {
	// If the type is a pointer (but not an interface), then prefix with "*".
	if t.isPointer && !t.isInterface {
		return fmt.Sprintf("*%s", t.name)
	}
	return t.name
}

// asParam returns the Go type used to pass this Optgen type around as a
// parameter. For example:
//
//	func SomeFunc(expr opt.ScalarExpr, filters FiltersExpr)
//	func SomeFunc(scanPrivate *ScanPrivate)
func (t *typeDef) asParam() string {
	// Non-interface pointers and by-ref structs need to be prefixed with "*".
	if (t.isPointer && !t.isInterface) || !t.passByVal {
		return fmt.Sprintf("*%s", t.name)
	}
	return t.name
}

// newMetadata creates a new instance of metadata from the compiled expression.
// The pkg parameter is used to correctly qualify type names. For example, if
// pkg is "memo", then:
//
//	memo.RelExpr     => RelExpr
//	opt.ScalarExpr   => opt.ScalarExpr
//	memo.ScanPrivate => ScanPrivate
func newMetadata(compiled *lang.CompiledExpr, pkg string) *metadata {
	md := &metadata{
		compiled:  compiled,
		types:     make(map[string]*typeDef),
		exprTypes: make(map[lang.Expr]*typeDef),
	}

	// Add all types used in Optgen defines here.
	md.types = map[string]*typeDef{

		"PredicateStats":    {fullName: "opt.PredicateStats", isPointer: true},
		"RelExpr":           {fullName: "memo.RelExpr", isExpr: true, isInterface: true},
		"Expr":              {fullName: "opt.Expr", isExpr: true, isInterface: true},
		"Exprs":             {fullName: "opt.Exprs", isExpr: true, isInterface: true},
		"ScalarExpr":        {fullName: "opt.ScalarExpr", isExpr: true, isInterface: true},
		"Operator":          {fullName: "opt.Operator", passByVal: true},
		"ColumnID":          {fullName: "opt.ColumnID", passByVal: true},
		"ColSet":            {fullName: "opt.ColSet", passByVal: true},
		"ColList":           {fullName: "opt.ColList", passByVal: true},
		"TableID":           {fullName: "opt.TableID", passByVal: true},
		"SchemaID":          {fullName: "opt.SchemaID", passByVal: true},
		"SequenceID":        {fullName: "opt.SequenceID", passByVal: true},
		"UniqueID":          {fullName: "opt.UniqueID", passByVal: true},
		"WithID":            {fullName: "opt.WithID", passByVal: true},
		"Ordering":          {fullName: "opt.Ordering", passByVal: true},
		"OrderingChoice":    {fullName: "physical.OrderingChoice", passByVal: true},
		"TupleOrdinal":      {fullName: "memo.TupleOrdinal", passByVal: true},
		"ScanLimit":         {fullName: "memo.ScanLimit", passByVal: true},
		"ScanFlags":         {fullName: "memo.ScanFlags", passByVal: true},
		"JoinFlags":         {fullName: "memo.JoinFlags", passByVal: true},
		"JoinHintInfo":      {fullName: "memo.JoinHintInfo", passByVal: true},
		"WindowFrame":       {fullName: "memo.WindowFrame", passByVal: true},
		"ExplainOptions":    {fullName: "tree.ExplainOptions", passByVal: true},
		"StatementType":     {fullName: "tree.StatementType", passByVal: true},
		"ShowTraceType":     {fullName: "tree.ShowTraceType", passByVal: true},
		"bool":              {fullName: "bool", passByVal: true},
		"int":               {fullName: "int", passByVal: true},
		"string":            {fullName: "string", passByVal: true},
		"Type":              {fullName: "types.T", isPointer: true},
		"Datum":             {fullName: "tree.Datum", isInterface: true},
		"TypedExpr":         {fullName: "tree.TypedExpr", isInterface: true},
		"Statement":         {fullName: "tree.Statement", isInterface: true},
		"Subquery":          {fullName: "tree.Subquery", isPointer: true, usePointerIntern: true},
		"CreateTable":       {fullName: "tree.CreateTable", isPointer: true, usePointerIntern: true},
		"CreateProcedure":   {fullName: "tree.CreateProcedure", isPointer: true, usePointerIntern: true},
		"CallProcedure":     {fullName: "tree.CallProcedure", isPointer: true, usePointerIntern: true},
		"Constraint":        {fullName: "constraint.Constraint", isPointer: true, usePointerIntern: true},
		"FuncProps":         {fullName: "tree.FunctionProperties", isPointer: true, usePointerIntern: true},
		"FuncOverload":      {fullName: "tree.Overload", isPointer: true, usePointerIntern: true},
		"PhysProps":         {fullName: "physical.Required", isPointer: true},
		"Presentation":      {fullName: "physical.Presentation", passByVal: true},
		"RelProps":          {fullName: "props.Relational"},
		"RelPropsPtr":       {fullName: "props.Relational", isPointer: true, usePointerIntern: true},
		"ScalarProps":       {fullName: "props.Scalar"},
		"FuncDepSet":        {fullName: "props.FuncDepSet"},
		"OpaqueMetadata":    {fullName: "opt.OpaqueMetadata", isInterface: true},
		"JobCommand":        {fullName: "tree.JobCommand", passByVal: true},
		"IndexOrdinal":      {fullName: "cat.IndexOrdinal", passByVal: true},
		"ViewDeps":          {fullName: "opt.ViewDeps", passByVal: true},
		"LockingItem":       {fullName: "tree.LockingItem", isPointer: true},
		"AggregateFuncs":    {fullName: "opt.AggFuncNames", passByVal: true},
		"RowsValue":         {fullName: "opt.RowsValue", passByVal: true},
		"UpdateValue":       {fullName: "opt.UpdateValue", passByVal: true},
		"TsSpan":            {fullName: "opt.TsSpan", passByVal: true},
		"TsSpans":           {fullName: "opt.TsSpans", passByVal: true},
		"ColsMap":           {fullName: "opt.ColsMap", passByVal: true},
		"PrimaryTags":       {fullName: "memo.PrimaryTags", passByVal: true},
		"ScanAggArray":      {fullName: "memo.ScanAggArray", passByVal: true},
		"PTagValues":        {fullName: "memo.PTagValues", passByVal: true},
		"TagIndexValues":    {fullName: "memo.TagIndexValues", passByVal: true},
		"TSHintType":        {fullName: "keys.ScanMethodHintType", passByVal: true},
		"ColIdxs":           {fullName: "opt.ColIdxs", passByVal: true},
		"StatisticIndex":    {fullName: "opt.StatisticIndex", passByVal: true},
		"TSOrderedScanType": {fullName: "opt.OrderedTableType", passByVal: true},
		"TSGroupOptType":    {fullName: "opt.GroupOptType", passByVal: true},
		"TagIndexInfo":      {fullName: "memo.TagIndexInfo", passByVal: true},
		"ProcComms":         {fullName: "memo.ProcComms", passByVal: true},
		"VarNames":          {fullName: "opt.VarNames", passByVal: true},
	}

	// Add types of generated op and private structs.
	for _, define := range compiled.Defines {
		typ := &typeDef{isGenerated: true}

		var friendlyName string
		if define.Tags.Contains("List") {
			friendlyName = fmt.Sprintf("%sExpr", define.Name)
			typ.isExpr = true
			typ.passByVal = true
		} else if define.Tags.Contains("ListItem") {
			friendlyName = string(define.Name)
			typ.isExpr = true
		} else if define.Tags.Contains("Private") {
			friendlyName = string(define.Name)
		} else {
			friendlyName = fmt.Sprintf("%sExpr", define.Name)
			typ.isExpr = true
			typ.isPointer = true
			typ.usePointerIntern = true
		}
		typ.fullName = fmt.Sprintf("memo.%s", friendlyName)

		md.types[friendlyName] = typ
		md.exprTypes[define] = typ
	}

	// 1. Associate each DefineField with its type.
	// 2. Link list types to the types of their list items. A list item type has
	//    the same name as its list parent + the "Item" prefix.
	for _, define := range compiled.Defines {
		// Associate each DefineField with its type.
		for _, field := range define.Fields {
			md.exprTypes[field] = md.lookupType(string(field.Type))
		}

		if define.Tags.Contains("List") {
			listTyp := md.typeOf(define)
			if itemTyp, ok := md.types[fmt.Sprintf("%sItem", define.Name)]; ok {
				listTyp.listItemType = itemTyp
			} else {
				listTyp.listItemType = md.lookupType("ScalarExpr")
			}
		}
	}

	// Now walk each type and fill in any remaining fields.
	for friendlyName, typ := range md.types {
		// If type is an interface, then it's also considered a pointer.
		if typ.isInterface {
			typ.isPointer = true
		}

		// If type is a pointer/interface, then it should always be passed byref.
		if typ.isPointer {
			typ.passByVal = true
		}

		typ.friendlyName = friendlyName

		// Remove package prefix from types in the same package.
		if strings.HasPrefix(typ.fullName, pkg+".") {
			typ.name = typ.fullName[len(pkg)+1:]
		} else {
			typ.name = typ.fullName
		}
	}

	return md
}

// typeOf returns a type definition for a *lang.Define or *lang.DefineField
// Optgen expression.
func (m *metadata) typeOf(e lang.Expr) *typeDef {
	return m.exprTypes[e]
}

// lookupType returns the type definition with the given friendly name (e.g.
// RelExpr, DatumType, ScanExpr, etc.).
func (m *metadata) lookupType(friendlyName string) *typeDef {
	res, ok := m.types[friendlyName]
	if !ok {
		panic(fmt.Sprintf("%s is not registered as a valid type in metadata.go", friendlyName))
	}
	return res
}

// fieldName maps the Optgen field name to the corresponding Go field name. In
// particular, fields named "_" are mapped to the name of a Go embedded field,
// which is equal to the field's type name:
//
//	define Scan {
//	  _ ScanPrivate
//	}
//
// gets compiled into:
//
//	  type ScanExpr struct {
//		   ScanPrivate
//	    ...
//	  }
//
// Note that the field's type name is always a simple alphanumeric identifier
// with no package specified (that's only specified in the fullName field of the
// typeDef).
func (m *metadata) fieldName(field *lang.DefineFieldExpr) string {
	if field.Name == "_" {
		return string(field.Type)
	}
	return string(field.Name)
}

// childFields returns the set of fields for an operator define expression that
// are considered children of that operator. Private (non-expression) and
// unexported fields are omitted from the result. For example, for the Project
// operator:
//
//	 define Project {
//	   Input            RelExpr
//	   Projections      ProjectionsExpr
//	   Passthrough      ColSet
//	   internalFuncDeps FuncDepSet
//	}
//
// The Input and Projections fields are children, but the Passthrough and
// the internalFuncDeps fields will not be returned.
func (m *metadata) childFields(define *lang.DefineExpr) lang.DefineFieldsExpr {
	// Skip until non-expression field is found.
	n := 0
	for _, field := range define.Fields {
		typ := m.typeOf(field)
		if !typ.isExpr {
			break
		}
		n++
	}
	return define.Fields[:n]
}

// privateField returns the private field for an operator define expression, if
// one exists. For example, for the Project operator:
//
//	 define Project {
//	   Input       RelExpr
//	   Projections ProjectionsExpr
//	   Passthrough ColSet
//	}
//
// The Passthrough field is the private field. If no private field exists for
// the operator, then privateField returns nil.
func (m *metadata) privateField(define *lang.DefineExpr) *lang.DefineFieldExpr {
	// Skip until non-expression field is found.
	n := 0
	for _, field := range define.Fields {
		typ := m.typeOf(field)
		if !typ.isExpr {
			if !isEmbeddedField(field) && !isExportedField(field) {
				// This is an unexported field; because we force the private to appear
				// before unexported fields, there must be no private.
				return nil
			}
			return define.Fields[n]
		}
		n++
	}
	return nil
}

// childAndPrivateFields returns the set of fields for an operator excluding
// unexported fields (i.e. the children and the private field).
func (m *metadata) childAndPrivateFields(define *lang.DefineExpr) lang.DefineFieldsExpr {
	// Skip until non-expression field is found.
	n := 0
	for _, field := range define.Fields {
		typ := m.typeOf(field)
		if !typ.isExpr && !isEmbeddedField(field) && !isExportedField(field) {
			break
		}
		n++
	}
	return define.Fields[:n]
}

// hasUnexportedFields returns true if the operator has unexported fields.
func (m *metadata) hasUnexportedFields(define *lang.DefineExpr) bool {
	for _, field := range define.Fields {
		typ := m.typeOf(field)
		if !typ.isExpr && !isEmbeddedField(field) && !isExportedField(field) {
			return true
		}
	}
	return false
}

// fieldLoadPrefix returns "&" if the address of a field of the given type
// should be taken when loading that field from an instance in order to pass it
// elsewhere as a parameter (like to a function). For example:
//
//	f.ConstructUnion(union.Left, union.Right, &union.SetPrivate)
//
// The Left and Right fields are passed by value, but the SetPrivate is passed
// by reference.
func fieldLoadPrefix(typ *typeDef) string {
	if !typ.passByVal {
		return "&"
	}
	return ""
}

// fieldStorePrefix is the inverse of fieldLoadPrefix, used when a value being
// used as a parameter is stored into a field:
//
//	union.Left = left
//	union.Right = right
//	union.SetPrivate = *setPrivate
//
// Since SetPrivate values are passed by reference, they must be dereferenced
// before copying them to a target field.
func fieldStorePrefix(typ *typeDef) string {
	if !typ.passByVal {
		return "*"
	}
	return ""
}

// dynamicFieldLoadPrefix returns "&" if the address of a field of the given
// type should be taken when loading that field from an instance in order to
// pass it as a dynamic parameter (like interface{} or opt.Expr). For example:
//
//	f.ConstructDynamic(
//	  project.Input,
//	  &project.Projections,
//	  &project.Passthrough,
//	)
//
// Note that normally the Projections and Passthrough fields would be passed by
// value, but here their addresses are passed in order to avoid Go allocating an
// object when passing as interface{}.
func dynamicFieldLoadPrefix(typ *typeDef) string {
	if !typ.isPointer {
		return "&"
	}
	return ""
}

// castFromDynamicParam is used when a dynamic parameter (i.e. typed as
// interface{} or opt.Expr) is cast to a parameter of the given type (i.e. typed
// as typ.asParam()). Dynamic parameters are used as the return value of methods
// like Expr.Child and for arguments to ConstructDynamic. Fields having a type
// normally passed by value (like FilterExpr or ColSet) are passed by reference
// in order to avoid an extra allocation when passing as an interface. For
// example:
//
//	var val interface{}
//	project.Input = val.(RelExpr)
//	project.Filers = *val.(*FiltersExpr)
//	project.Projections = *val.(*opt.ColSet)
func castFromDynamicParam(param string, typ *typeDef) string {
	if typ.isInterface {
		// Interfaces are passed as interfaces for both param and dynamic param
		// types.
		return fmt.Sprintf("%s.(%s)", param, typ.name)
	} else if typ.isPointer {
		// Pointers are passed as pointers for both param and dynamic param types.
		return fmt.Sprintf("%s.(*%s)", param, typ.name)
	} else if typ.passByVal {
		// Values passed by value as a param type are passed by reference when
		// passed as a dynamic param type.
		return fmt.Sprintf("*%s.(*%s)", param, typ.name)
	}

	// All other values are passed by reference whether passed as a param or
	// dynamic param type.
	return fmt.Sprintf("%s.(*%s)", param, typ.name)
}
