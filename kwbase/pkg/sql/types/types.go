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

package types

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// T is an instance of a SQL scalar, array, or tuple type. It describes the
// domain of possible values which a column can return, or to which an
// expression can evaluate. The type system does not differentiate between
// nullable and non-nullable types. It is up to the caller to store that
// information separately if it is needed. Here are some example types:
//
//	INT4                     - any 32-bit integer
//	DECIMAL(10, 3)           - any base-10 value with at most 10 digits, with
//	                           up to 3 to right of decimal point
//	FLOAT[]                  - array of 64-bit IEEE 754 floating-point values
//	TUPLE[TIME, VARCHAR(20)] - any pair of values where first value is a time
//	                           of day and the second value is a string having
//	                           up to 20 characters
//
// Fundamentally, a type consists of the following attributes, each of which has
// a corresponding accessor method. Some of these attributes are only defined
// for a subset of types. See the method comments for more details.
//
//	Family        - equivalence group of the type (enumeration)
//	Oid           - Postgres Object ID that describes the type (enumeration)
//	Precision     - maximum accuracy of the type (numeric)
//	Width         - maximum size or scale of the type (numeric)
//	Locale        - location which governs sorting, formatting, etc. (string)
//	ArrayContents - array element type (T)
//	TupleContents - slice of types of each tuple field ([]T)
//	TupleLabels   - slice of labels of each tuple field ([]string)
//
// Some types are not currently allowed as the type of a column (e.g. nested
// arrays). Other usages of the types package may have similar restrictions.
// Each such caller is responsible for enforcing their own restrictions; it's
// not the concern of the types package.
//
// Implementation-wise, types.T wraps a protobuf-generated InternalType struct.
// The generated protobuf code defines the struct fields, marshals/unmarshals
// them, formats a string representation, etc. Meanwhile, the wrapper types.T
// struct overrides the Marshal/Unmarshal methods in order to map to/from older
// persisted InternalType representations. For example, older versions of
// InternalType (previously called ColumnType) used a VisibleType field to
// represent INT2, whereas newer versions use Width/Oid. Unmarshal upgrades from
// this old format to the new, and Marshal downgrades, thus preserving backwards
// compatibility.
//
// Simple (unary) scalars types
// ----------------------------
//
// | SQL type          | Family         | Oid           | Precision | Width |
// |-------------------|----------------|---------------|-----------|-------|
// | NULL (unknown)    | UNKNOWN        | T_unknown     | 0         | 0     |
// | BOOL              | BOOL           | T_bool        | 0         | 0     |
// | DATE              | DATE           | T_date        | 0         | 0     |
// | TIMESTAMP         | TIMESTAMP      | T_timestamp   | 0         | 0     |
// | INTERVAL          | INTERVAL       | T_interval    | 0         | 0     |
// | TIMESTAMPTZ       | TIMESTAMPTZ    | T_timestamptz | 0         | 0     |
// | OID               | OID            | T_oid         | 0         | 0     |
// | UUID              | UUID           | T_uuid        | 0         | 0     |
// | INET              | INET           | T_inet        | 0         | 0     |
// | TIME              | TIME           | T_time        | 0         | 0     |
// | TIMETZ            | TIMETZ         | T_timetz      | 0         | 0     |
// | JSON              | JSONB          | T_jsonb       | 0         | 0     |
// | JSONB             | JSONB          | T_jsonb       | 0         | 0     |
// |                   |                |               |           |       |
// | BYTES             | BYTES          | T_bytea       | 0         | 0     |
// |                   |                |               |           |       |
// | STRING            | STRING         | T_text        | 0         | 0     |
// | GEOMETRY          | GEOMETRY       | T_geometry    | 0         | 254   |
// | STRING(N)         | STRING         | T_text        | 0         | N     |
// | VARCHAR           | STRING         | T_varchar     | 0         | 0     |
// | VARCHAR(N)        | STRING         | T_varchar     | 0         | N     |
// | CHAR              | STRING         | T_bpchar      | 0         | 1     |
// | CHAR(N)           | STRING         | T_bpchar      | 0         | N     |
// | NCHAR(N)          | STRING         | T_nchar       | 0         | N     |
// | "char"            | STRING         | T_char        | 0         | 0     |
// | NAME              | STRING         | T_name        | 0         | 0     |
// |                   |                |               |           |       |
// | STRING COLLATE en | COLLATEDSTRING | T_text        | 0         | 0     |
// | STRING(N) COL...  | COLLATEDSTRING | T_text        | 0         | N     |
// | VARCHAR COL...    | COLLATEDSTRING | T_varchar     | 0         | N     |
// | VARCHAR(N) COL... | COLLATEDSTRING | T_varchar     | 0         | N     |
// | CHAR COL...       | COLLATEDSTRING | T_bpchar      | 0         | 1     |
// | CHAR(N) COL...    | COLLATEDSTRING | T_bpchar      | 0         | N     |
// | "char" COL...     | COLLATEDSTRING | T_char        | 0         | 0     |
// |                   |                |               |           |       |
// | DECIMAL           | DECIMAL        | T_decimal     | 0         | 0     |
// | DECIMAL(N)        | DECIMAL        | T_decimal     | N         | 0     |
// | DECIMAL(N,M)      | DECIMAL        | T_decimal     | N         | M     |
// |                   |                |               |           |       |
// | FLOAT8            | FLOAT          | T_float8      | 0         | 0     |
// | FLOAT4            | FLOAT          | T_float4      | 0         | 0     |
// |                   |                |               |           |       |
// | BIT               | BIT            | T_bit         | 0         | 1     |
// | BIT(N)            | BIT            | T_bit         | 0         | N     |
// | VARBIT            | BIT            | T_varbit      | 0         | 0     |
// | VARBIT(N)         | BIT            | T_varbit      | 0         | N     |
// |                   |                |               |           |       |
// | INT,INTEGER       | INT            | T_int8        | 0         | 64    |
// | INT2,SMALLINT     | INT            | T_int2        | 0         | 16    |
// | INT4              | INT            | T_int4        | 0         | 32    |
// | INT8,INT64,BIGINT | INT            | T_int8        | 0         | 64    |
// | INT1,TINYINT      | INT            | T_int1        | 0         | 8     |
//
// Tuple types
// -----------
//
// These cannot (yet) be used in tables but are used in DistSQL flow
// processors for queries that have tuple-typed intermediate results.
//
// | Field           | Description                                             |
// |-----------------|---------------------------------------------------------|
// | Family          | TupleFamily                                             |
// | Oid             | T_record                                                |
// | TupleContents   | Contains tuple field types (can be recursively defined) |
// | TupleLabels     | Contains labels for each tuple field                    |
//
// Array types
// -----------
//
// | Field           | Description                                             |
// |-----------------|---------------------------------------------------------|
// | Family          | ArrayFamily                                             |
// | Oid             | T__XXX (double underscores), where XXX is the Oid name  |
// |                 | of a scalar type                                        |
// | ArrayContents   | Type of array elements (scalar, array, or tuple)        |
//
// There are two special ARRAY types:
//
// | SQL type          | Family         | Oid           | ArrayContents |
// |-------------------|----------------|---------------|---------------|
// | INT2VECTOR        | ARRAY          | T_int2vector  | Int           |
// | OIDVECTOR         | ARRAY          | T_oidvector   | Oid           |
//
// When these types are themselves made into arrays, the Oids become T__int2vector and
// T__oidvector, respectively.
type T struct {
	// InternalType should never be directly referenced outside this package. The
	// only reason it is exported is because gogoproto panics when printing the
	// string representation of an unexported field. This is a problem when this
	// struct is embedded in a larger struct (like a ColumnDescriptor).
	InternalType InternalType
}

// Convenience list of pre-constructed types. Caller code can use any of these
// types, or use the MakeXXX methods to construct a custom type that is not
// listed here (e.g. if a custom width is needed).
var (
	// Unknown is the type of an expression that statically evaluates to NULL.
	// This type should never be returned for an expression that does not *always*
	// evaluate to NULL.
	Unknown = &T{InternalType: InternalType{
		Family: UnknownFamily, Oid: oid.T_unknown, Locale: &emptyLocale}}

	// Bool is the type of a boolean true/false value.
	Bool = &T{InternalType: InternalType{
		Family: BoolFamily, Oid: oid.T_bool, Locale: &emptyLocale}}

	Char = &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_bpchar, Locale: &emptyLocale}}

	// Char10 and Char20 are used to create the ColumnDescriptor
	Char10 = &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_bpchar, Width: 10, Locale: &emptyLocale}}

	Char20 = &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_bpchar, Width: 20, Locale: &emptyLocale}}

	NChar = &T{InternalType: InternalType{
		Family: StringFamily, Oid: T_nchar, Locale: &emptyLocale}}

	// VarBit is the type of an ordered list of bits (0 or 1 valued), with no
	// specified limit on the count of bits.
	VarBit = &T{InternalType: InternalType{
		Family: BitFamily, Oid: oid.T_varbit, Locale: &emptyLocale}}

	// Int is the type of a 64-bit signed integer. This is the canonical type
	// for IntFamily.
	Int = &T{InternalType: InternalType{
		Family: IntFamily, Width: 64, Oid: oid.T_int8, Locale: &emptyLocale}}

	// Int4 is the type of a 32-bit signed integer.
	Int4 = &T{InternalType: InternalType{
		Family: IntFamily, Width: 32, Oid: oid.T_int4, Locale: &emptyLocale}}

	// Int2 is the type of a 16-bit signed integer.
	Int2 = &T{InternalType: InternalType{
		Family: IntFamily, Width: 16, Oid: oid.T_int2, Locale: &emptyLocale}}

	// Int1 is the type of a 8-bit signed integer.
	Int1 = &T{InternalType: InternalType{
		Family: IntFamily, Width: 8, Oid: T_int1, Locale: &emptyLocale}}

	// Float is the type of a 64-bit base-2 floating-point number (IEEE 754).
	// This is the canonical type for FloatFamily.
	Float = &T{InternalType: InternalType{
		Family: FloatFamily, Width: 64, Oid: oid.T_float8, Locale: &emptyLocale}}

	// Float4 is the type of a 32-bit base-2 floating-point number (IEEE 754).
	Float4 = &T{InternalType: InternalType{
		Family: FloatFamily, Width: 32, Oid: oid.T_float4, Locale: &emptyLocale}}

	// Decimal is the type of a base-10 floating-point number, with no specified
	// limit on precision (number of digits) or scale (digits to right of decimal
	// point).
	Decimal = &T{InternalType: InternalType{
		Family: DecimalFamily, Oid: oid.T_numeric, Locale: &emptyLocale}}

	// String is the type of a Unicode string, with no specified limit on the
	// count of characters. This is the canonical type for StringFamily. It is
	// reported as STRING in SHOW CREATE but "text" in introspection for
	// compatibility with PostgreSQL.
	String = &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_text, Locale: &emptyLocale}}

	// Geometry is equivalent to String, but has a differing OID (T_geometry),
	// which makes it show up differently when displayed. It is reported as
	// Geometry type for now.
	Geometry = &T{InternalType: InternalType{
		Family: StringFamily, Oid: T_geometry, Locale: &emptyLocale}}

	// VarChar is equivalent to String, but has a differing OID (T_varchar),
	// which makes it show up differently when displayed. It is reported as
	// VARCHAR in SHOW CREATE and "character varying" in introspection for
	// compatibility with PostgreSQL.
	VarChar = &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_varchar, Locale: &emptyLocale}}

	NVarChar = &T{InternalType: InternalType{
		Family: StringFamily, Oid: T_nvarchar, Locale: &emptyLocale}}

	// Name is a type-alias for String with a different OID (T_name). It is
	// reported as NAME in SHOW CREATE and "name" in introspection for
	// compatibility with PostgreSQL.
	Name = &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_name, Locale: &emptyLocale}}

	// Clob is a type-alias for String with a different OID (T_clob). It is
	// reported as CLOB in SHOW CREATE.
	Clob = &T{InternalType: InternalType{
		Family: StringFamily, Oid: T_clob, Locale: &emptyLocale}}

	// Blob is a type-alias for Bytes with a different OID (T_blob). It is
	// reported as BLOB in SHOW CREATE.
	Blob = &T{InternalType: InternalType{
		Family: BytesFamily, Oid: T_blob, Locale: &emptyLocale}}

	// Bytes is the type of a list of raw byte values.
	Bytes = &T{InternalType: InternalType{
		Family: BytesFamily, Oid: oid.T_bytea, Locale: &emptyLocale}}

	// VarBytes is the type of a list of raw byte values.
	VarBytes = &T{InternalType: InternalType{
		Family: BytesFamily, Oid: T_varbytea, Locale: &emptyLocale}}

	// Date is the type of a value specifying year, month, day (with no time
	// component). There is no timezone associated with it. For example:
	//
	//   YYYY-MM-DD
	//
	Date = &T{InternalType: InternalType{
		Family: DateFamily, Oid: oid.T_date, Locale: &emptyLocale}}

	// Time is the type of a value specifying hour, minute, second (with no date
	// component). By default, it has microsecond precision. There is no timezone
	// associated with it. For example:
	//
	//   HH:MM:SS.ssssss
	//
	Time = &T{InternalType: InternalType{
		Family:             TimeFamily,
		Precision:          0,
		TimePrecisionIsSet: false,
		Oid:                oid.T_time,
		Locale:             &emptyLocale,
	}}

	// TimeTZ is the type specifying hour, minute, second and timezone with
	// no date component. By default, it has microsecond precision.
	// For example:
	//
	//   HH:MM:SS.ssssss+-ZZ:ZZ
	TimeTZ = &T{InternalType: InternalType{
		Family:             TimeTZFamily,
		Precision:          0,
		TimePrecisionIsSet: false,
		Oid:                oid.T_timetz,
		Locale:             &emptyLocale,
	}}

	// Timestamp is the type of a value specifying year, month, day, hour, minute,
	// and second, but with no associated timezone. By default, it has microsecond
	// precision. For example:
	//
	//   YYYY-MM-DD HH:MM:SS.ssssss
	//
	Timestamp = &T{InternalType: InternalType{
		Family:             TimestampFamily,
		Precision:          0,
		TimePrecisionIsSet: false,
		Oid:                oid.T_timestamp,
		Locale:             &emptyLocale,
	}}

	// TimestampTZ is the type of a value specifying year, month, day, hour,
	// minute, and second, as well as an associated timezone. By default, it has
	// microsecond precision. For example:
	//
	//   YYYY-MM-DD HH:MM:SS.ssssss+-ZZ:ZZ
	//
	TimestampTZ = &T{InternalType: InternalType{
		Family:             TimestampTZFamily,
		Precision:          0,
		TimePrecisionIsSet: false,
		Oid:                oid.T_timestamptz,
		Locale:             &emptyLocale,
	}}

	// Interval is the type of a value describing a duration of time. By default,
	// it has microsecond precision.
	Interval = &T{InternalType: InternalType{
		Family:                IntervalFamily,
		Precision:             0,
		TimePrecisionIsSet:    false,
		Oid:                   oid.T_interval,
		Locale:                &emptyLocale,
		IntervalDurationField: &IntervalDurationField{},
	}}

	// Jsonb is the type of a JavaScript Object Notation (JSON) value that is
	// stored in a decomposed binary format (hence the "b" in jsonb).
	Jsonb = &T{InternalType: InternalType{
		Family: JsonFamily, Oid: oid.T_jsonb, Locale: &emptyLocale}}

	// Uuid is the type of a universally unique identifier (UUID), which is a
	// 128-bit quantity that is very unlikely to ever be generated again, and so
	// can be relied on to be distinct from all other UUID values.
	Uuid = &T{InternalType: InternalType{
		Family: UuidFamily, Oid: oid.T_uuid, Locale: &emptyLocale}}

	// INet is the type of an IPv4 or IPv6 network address. For example:
	//
	//   192.168.100.128/25
	//   FE80:CD00:0:CDE:1257:0:211E:729C
	//
	INet = &T{InternalType: InternalType{
		Family: INetFamily, Oid: oid.T_inet, Locale: &emptyLocale}}

	// Scalar contains all types that meet this criteria:
	//
	//   1. Scalar type (no ArrayFamily or TupleFamily types).
	//   2. Non-ambiguous type (no UnknownFamily or AnyFamily types).
	//   3. Canonical type for one of the type families.
	//
	Scalar = []*T{
		Bool,
		Int,
		Float,
		Decimal,
		Date,
		Timestamp,
		Interval,
		String,
		Bytes,
		TimestampTZ,
		Oid,
		Uuid,
		INet,
		Time,
		TimeTZ,
		Jsonb,
		VarBit,
	}

	// Any is a special type used only during static analysis as a wildcard type
	// that matches any other type, including scalar, array, and tuple types.
	// Execution-time values should never have this type. As an example of its
	// use, many SQL builtin functions allow an input value to be of any type,
	// and so use this type in their static definitions.
	Any = &T{InternalType: InternalType{
		Family: AnyFamily, Oid: oid.T_anyelement, Locale: &emptyLocale}}

	// AnyArray is a special type used only during static analysis as a wildcard
	// type that matches an array having elements of any (uniform) type (including
	// nested array types). Execution-time values should never have this type.
	AnyArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Any, Oid: oid.T_anyarray, Locale: &emptyLocale}}

	// AnyTuple is a special type used only during static analysis as a wildcard
	// type that matches a tuple with any number of fields of any type (including
	// tuple types). Execution-time values should never have this type.
	AnyTuple = &T{InternalType: InternalType{
		Family: TupleFamily, TupleContents: []T{*Any}, Oid: oid.T_record, Locale: &emptyLocale}}

	// AnyCollatedString is a special type used only during static analysis as a
	// wildcard type that matches a collated string with any locale. Execution-
	// time values should never have this type.
	AnyCollatedString = &T{InternalType: InternalType{
		Family: CollatedStringFamily, Oid: oid.T_text, Locale: &emptyLocale}}

	// EmptyTuple is the tuple type with no fields. Note that this is different
	// than AnyTuple, which is a wildcard type.
	EmptyTuple = &T{InternalType: InternalType{
		Family: TupleFamily, Oid: oid.T_record, Locale: &emptyLocale}}

	// StringArray is the type of an array value having String-typed elements.
	StringArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: String, Oid: oid.T__text, Locale: &emptyLocale}}

	// Int1Array is the type of an array value having Int1-typed elements.
	Int1Array = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Int, Oid: T__int1, Locale: &emptyLocale}}

	// Int2Array is the type of an array value having Int2-typed elements.
	Int2Array = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Int, Oid: oid.T__int2, Locale: &emptyLocale}}

	// Int4Array is the type of an array value having Int4-typed elements.
	Int4Array = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Int, Oid: oid.T__int4, Locale: &emptyLocale}}

	// IntArray is the type of an array value having Int-typed elements.
	IntArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Int, Oid: oid.T__int8, Locale: &emptyLocale}}

	// DecimalArray is the type of an array value having Decimal-typed elements.
	DecimalArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Decimal, Oid: oid.T__numeric, Locale: &emptyLocale}}

	// Int2Vector is a type-alias for an array of Int2 values with a different
	// OID (T_int2vector instead of T__int2). It is a special VECTOR type used
	// by Postgres in system tables. Int2vectors are 0-indexed, unlike normal arrays.
	Int2Vector = &T{InternalType: InternalType{
		Family: ArrayFamily, Oid: oid.T_int2vector, ArrayContents: Int2, Locale: &emptyLocale}}
)

// Unexported wrapper types.
var (
	// typeBit is the SQL BIT type. It is not exported to avoid confusion with
	// the VarBit type, and confusion over whether its default Width is
	// unspecified or is 1. More commonly used instead is the VarBit type.
	typeBit = &T{InternalType: InternalType{
		Family: BitFamily, Oid: oid.T_bit, Locale: &emptyLocale}}

	// typeBpChar is the "standard SQL" string type of fixed length, where "bp"
	// stands for "blank padded". It is not exported to avoid confusion with
	// typeQChar, as well as confusion over its default width.
	//
	// It is reported as CHAR in SHOW CREATE and "character" in introspection for
	// compatibility with PostgreSQL.
	//
	// Its default maximum with is 1. It always has a maximum width.
	typeBpChar = &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_bpchar, Locale: &emptyLocale}}

	// typeQChar is a special PostgreSQL-only type supported for compatibility.
	// It behaves like VARCHAR, its maximum width cannot be modified, and has a
	// peculiar name in the syntax and introspection. It is not exported to avoid
	// confusion with typeBpChar, as well as confusion over its default width.
	//
	// It is reported as "char" (with double quotes included) in SHOW CREATE and
	// "char" in introspection for compatibility with PostgreSQL.
	typeQChar = &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_char, Locale: &emptyLocale}}
)

const (
	// Deprecated after 19.1, since it's now represented using the Oid field.
	name Family = 11

	// Deprecated after 19.1, since it's now represented using the Oid field.
	int2vector Family = 200

	// Deprecated after 19.1, since it's now represented using the Oid field.
	oidvector Family = 201

	visibleNONE = 0

	// Deprecated after 2.1, since it's no longer used.
	visibleINTEGER = 1

	// Deprecated after 2.1, since it's now represented using the Width field.
	visibleSMALLINT = 2

	// Deprecated after 2.1, since it's now represented using the Width field.
	visibleBIGINT = 3

	// Deprecated after 2.0, since the original BIT representation was buggy.
	visibleBIT = 4

	// Deprecated after 19.1, since it's now represented using the Width field.
	visibleREAL = 5

	// Deprecated after 2.1, since it's now represented using the Width field.
	visibleDOUBLE = 6

	// Deprecated after 19.1, since it's now represented using the Oid field.
	visibleVARCHAR = 7

	// Deprecated after 19.1, since it's now represented using the Oid field.
	visibleCHAR = 8

	// Deprecated after 19.1, since it's now represented using the Oid field.
	visibleQCHAR = 9

	// Deprecated after 19.1, since it's now represented using the Oid field.
	visibleVARBIT = 10

	visibleTINYINT = 11

	visibleNCHAR = 12

	visibleNVARCHAR = 13

	visibleBYTES = 14

	visibleVARBYTES = 15

	visibleGEOMETRY = 16

	visibleCLOB = 17

	visibleBLOB = 18

	// OID returned for the unknown[] array type. PG has no OID for this case.
	unknownArrayOid = 0
)

const (
	// DefaultTimePrecision is the default precision to return for time families
	// if time is not set.
	DefaultTimePrecision = 6
)

var (
	emptyLocale = ""
)

// Engine stands which engine support this type
type Engine uint32

// engine type can support the type.
// RELATIONAL stands this type can be supported by relation engine
// TIMESERIES stands this type can be supported by ts engine
const (
	_ Engine = iota
	RELATIONAL
	TIMESERIES
)

// Mask returns the bitmask for a given engine.
func (e Engine) Mask() uint32 {
	return 1 << e
}

// EngineList is the list of engine
type EngineList []Engine

// ToBitField returns the bitfield representation of
// a list of engines.
func (el EngineList) ToBitField() uint32 {
	var ret uint32
	for _, e := range el {
		ret |= e.Mask()
	}
	return ret
}

// MakeScalar constructs a new instance of a scalar type (i.e. not array or
// tuple types) using the provided fields.
func MakeScalar(
	family Family, o oid.Oid, precision, width int32, locale string, typeEngine uint32,
) *T {
	t := OidToType[o]
	if family != t.Family() {
		if family != CollatedStringFamily || StringFamily != t.Family() {
			panic(errors.AssertionFailedf(
				"oid %s does not match %s", oid.TypeName[o], family))
		}
	}
	if family == ArrayFamily || family == TupleFamily {
		panic(errors.AssertionFailedf("cannot make non-scalar type %s", family))
	}
	if family != CollatedStringFamily && locale != "" {
		panic(errors.AssertionFailedf("non-collation type cannot have locale %s", locale))
	}

	timePrecisionIsSet := false
	var intervalDurationField *IntervalDurationField
	switch family {
	case IntervalFamily:
		intervalDurationField = &IntervalDurationField{}
		if precision < 0 || precision > 6 {
			panic(errors.AssertionFailedf("precision must be between 0 and 6 inclusive"))
		}
		timePrecisionIsSet = true
	case TimestampFamily, TimestampTZFamily, TimeFamily, TimeTZFamily:
		if precision < 0 || precision > 6 {
			panic(errors.AssertionFailedf("precision must be between 0 and 6 inclusive"))
		}
		timePrecisionIsSet = true
	case DecimalFamily:
		if precision < 0 {
			panic(errors.AssertionFailedf("negative precision is not allowed"))
		}
	default:
		if precision != 0 {
			panic(errors.AssertionFailedf("type %s cannot have precision", family))
		}
	}

	if width < 0 {
		panic(errors.AssertionFailedf("negative width is not allowed"))
	}
	switch family {
	case IntFamily:
		switch width {
		case 16, 32, 64:
		default:
			panic(errors.AssertionFailedf("invalid width %d for IntFamily type", width))
		}
	case FloatFamily:
		switch width {
		case 32, 64:
		default:
			panic(errors.AssertionFailedf("invalid width %d for FloatFamily type", width))
		}
	case DecimalFamily:
		if width > precision {
			panic(errors.AssertionFailedf(
				"decimal scale %d cannot be larger than precision %d", width, precision))
		}
	case StringFamily, BytesFamily, CollatedStringFamily, BitFamily:
		// These types can have any width.
	default:
		if width != 0 {
			panic(errors.AssertionFailedf("type %s cannot have width", family))
		}
	}

	return &T{InternalType: InternalType{
		Family:                family,
		Oid:                   o,
		Precision:             precision,
		TimePrecisionIsSet:    timePrecisionIsSet,
		Width:                 width,
		Locale:                &locale,
		IntervalDurationField: intervalDurationField,
		TypeEngine:            typeEngine,
	}}
}

// MakeBytes constructs a new instance of the BYTES type (oid = T_bytea) having
// the given max # Bytes (0 = unspecified number).
func MakeBytes(width int32, typeEngine uint32) *T {
	if width < 0 {
		panic(errors.AssertionFailedf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		Family: BytesFamily, Oid: oid.T_bytea, Width: width, Locale: &emptyLocale, TypeEngine: typeEngine}}
}

// MakeVarBytes constructs a new instance of the VARBYTES type (oid = T_varbytea) having
// the given max # Bytes (0 = unspecified number).
func MakeVarBytes(width int32, typeEngine uint32) *T {
	if width == 0 {
		return VarBytes
	}
	if width < 0 {
		panic(errors.AssertionFailedf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		Family: BytesFamily, Oid: T_varbytea, Width: width, Locale: &emptyLocale, TypeEngine: typeEngine}}
}

// MakeBit constructs a new instance of the BIT type (oid = T_bit) having the
// given max # bits (0 = unspecified number).
func MakeBit(width int32) *T {
	if width == 0 {
		return typeBit
	}
	if width < 0 {
		panic(errors.AssertionFailedf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		Family: BitFamily, Oid: oid.T_bit, Width: width, Locale: &emptyLocale}}
}

// MakeVarBit constructs a new instance of the BIT type (oid = T_varbit) having
// the given max # bits (0 = unspecified number).
func MakeVarBit(width int32) *T {
	if width == 0 {
		return VarBit
	}
	if width < 0 {
		panic(errors.AssertionFailedf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		Family: BitFamily, Width: width, Oid: oid.T_varbit, Locale: &emptyLocale}}
}

// MakeString constructs a new instance of the STRING type (oid = T_text) having
// the given max # characters (0 = unspecified number).
func MakeString(width int32) *T {
	if width == 0 {
		return String
	}
	if width < 0 {
		panic(errors.AssertionFailedf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_text, Width: width, Locale: &emptyLocale}}
}

// MakeGeometry constructs a new instance of the GEOMETRY type (oid = T_geometry) having
// the given max # characters (0 = unspecified number).
func MakeGeometry(width int32) *T {
	if width == 0 {
		return Geometry
	}
	if width < 0 {
		panic(errors.AssertionFailedf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		Family: StringFamily, Oid: T_geometry, Width: width, Locale: &emptyLocale}}
}

// MakeVarChar constructs a new instance of the VARCHAR type (oid = T_varchar)
// having the given max # characters (0 = unspecified number).
func MakeVarChar(width int32, typeEngine uint32) *T {
	if width < 0 {
		panic(errors.AssertionFailedf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_varchar, Width: width, Locale: &emptyLocale, TypeEngine: typeEngine}}
}

// MakeNVarChar constructs a new instance of the NVARCHAR type (oid = T_nvarchar)
// having the given max # characters (0 = unspecified number).
func MakeNVarChar(width int32) *T {
	if width == 0 {
		return NVarChar
	}
	if width < 0 {
		panic(errors.AssertionFailedf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		Family: StringFamily, Oid: T_nvarchar, Width: width, Locale: &emptyLocale}}
}

// MakeChar constructs a new instance of the CHAR type (oid = T_bpchar) having
// the given max # characters (0 = unspecified number).
func MakeChar(width int32) *T {
	if width == 0 {
		return typeBpChar
	}
	if width < 0 {
		panic(errors.AssertionFailedf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_bpchar, Width: width, Locale: &emptyLocale}}
}

// MakeNChar constructs a new instance of the CHAR type (oid = T_nchar) having
// the given max # characters (0 = unspecified number).
func MakeNChar(width int32) *T {
	if width == 0 {
		return NChar
	}
	if width < 0 {
		panic(errors.AssertionFailedf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		Family: StringFamily, Oid: T_nchar, Width: width, Locale: &emptyLocale}}
}

// MakeQChar constructs a new instance of the "char" type (oid = T_char) having
// the given max # characters (0 = unspecified number).
func MakeQChar(width int32) *T {
	if width == 0 {
		return typeQChar
	}
	return &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_char, Width: width, Locale: &emptyLocale}}
}

// MakeCollatedString constructs a new instance of a CollatedStringFamily type
// that is collated according to the given locale. The new type is based upon
// the given string type, having the same oid and width values. For example:
//
//	STRING      => STRING COLLATE EN
//	VARCHAR(20) => VARCHAR(20) COLLATE EN
func MakeCollatedString(strType *T, locale string) *T {
	switch strType.Oid() {
	case oid.T_text, oid.T_varchar, oid.T_bpchar, oid.T_char, oid.T_name, T_nchar, T_nvarchar, T_clob:
		return &T{InternalType: InternalType{
			Family: CollatedStringFamily, Oid: strType.Oid(), Width: strType.Width(), Locale: &locale}}
	}
	panic(errors.AssertionFailedf("cannot apply collation to non-string type: %s", strType))
}

// MakeDecimal constructs a new instance of a DECIMAL type (oid = T_numeric)
// that has at most "precision" # of decimal digits (0 = unspecified number of
// digits) and at most "scale" # of decimal digits after the decimal point
// (0 = unspecified number of digits). scale must be <= precision.
func MakeDecimal(precision, scale int32) *T {
	if precision == 0 && scale == 0 {
		return Decimal
	}
	if precision < 0 {
		panic(errors.AssertionFailedf("precision %d cannot be negative", precision))
	}
	if scale < 0 {
		panic(errors.AssertionFailedf("scale %d cannot be negative", scale))
	}
	if scale > precision {
		panic(errors.AssertionFailedf(
			"scale %d cannot be larger than precision %d", scale, precision))
	}
	return &T{InternalType: InternalType{
		Family:    DecimalFamily,
		Oid:       oid.T_numeric,
		Precision: precision,
		Width:     scale,
		Locale:    &emptyLocale,
	}}
}

// MakeTime constructs a new instance of a TIME type (oid = T_time) that has at
// most the given number of fractional second digits.
//
// To use the default precision, use the `Time` variable.
func MakeTime(precision int32) *T {
	return &T{InternalType: InternalType{
		Family:             TimeFamily,
		Oid:                oid.T_time,
		Precision:          precision,
		TimePrecisionIsSet: true,
		Locale:             &emptyLocale,
	}}
}

// MakeTimeTZ constructs a new instance of a TIMETZ type (oid = T_timetz) that
// has at most the given number of fractional second digits.
//
// To use the default precision, use the `TimeTZ` variable.
func MakeTimeTZ(precision int32) *T {
	return &T{InternalType: InternalType{
		Family:             TimeTZFamily,
		Oid:                oid.T_timetz,
		Precision:          precision,
		TimePrecisionIsSet: true,
		Locale:             &emptyLocale,
	}}
}

var (
	// DefaultIntervalTypeMetadata returns a duration field that is unset,
	// using INTERVAL or INTERVAL ( iconst32 ) syntax instead of INTERVAL
	// with a qualifier afterwards.
	DefaultIntervalTypeMetadata = IntervalTypeMetadata{}
)

// IntervalTypeMetadata is metadata pertinent for intervals.
type IntervalTypeMetadata struct {
	// DurationField represents the duration field definition.
	DurationField IntervalDurationField
	// Precision is the precision to use - note this matches InternalType rules.
	Precision int32
	// PrecisionIsSet indicates whether Precision is explicitly set.
	PrecisionIsSet bool
}

// IsMinuteToSecond returns whether the IntervalDurationField represents
// the MINUTE TO SECOND interval qualifier.
func (m *IntervalDurationField) IsMinuteToSecond() bool {
	return m.FromDurationType == IntervalDurationType_MINUTE &&
		m.DurationType == IntervalDurationType_SECOND
}

// IsDayToHour returns whether the IntervalDurationField represents
// the DAY TO HOUR interval qualifier.
func (m *IntervalDurationField) IsDayToHour() bool {
	return m.FromDurationType == IntervalDurationType_DAY &&
		m.DurationType == IntervalDurationType_HOUR
}

// IntervalTypeMetadata returns the IntervalTypeMetadata for interval types.
func (t *T) IntervalTypeMetadata() (IntervalTypeMetadata, error) {
	if t.Family() != IntervalFamily {
		return IntervalTypeMetadata{}, errors.Newf("cannot call IntervalTypeMetadata on non-intervals")
	}
	return IntervalTypeMetadata{
		DurationField:  *t.InternalType.IntervalDurationField,
		Precision:      t.InternalType.Precision,
		PrecisionIsSet: t.InternalType.TimePrecisionIsSet,
	}, nil
}

// MakeInterval constructs a new instance of a
// INTERVAL type (oid = T_interval) with a duration field.
//
// To use the default precision and field, use the `Interval` variable.
func MakeInterval(itm IntervalTypeMetadata) *T {
	switch itm.DurationField.DurationType {
	case IntervalDurationType_SECOND, IntervalDurationType_UNSET:
	default:
		if itm.PrecisionIsSet {
			panic(errors.Errorf("cannot set precision for duration type %s", itm.DurationField.DurationType))
		}
	}
	if itm.Precision > 0 && !itm.PrecisionIsSet {
		panic(errors.Errorf("precision must be set if Precision > 0"))
	}

	return &T{InternalType: InternalType{
		Family:                IntervalFamily,
		Oid:                   oid.T_interval,
		Locale:                &emptyLocale,
		Precision:             itm.Precision,
		TimePrecisionIsSet:    itm.PrecisionIsSet,
		IntervalDurationField: &itm.DurationField,
	}}
}

// MakeTimestamp constructs a new instance of a TIMESTAMP type that has at most
// the given number of fractional second digits.
//
// To use the default precision, use the `Timestamp` variable.
func MakeTimestamp(precision int32) *T {
	return &T{InternalType: InternalType{
		Family:             TimestampFamily,
		Oid:                oid.T_timestamp,
		Precision:          precision,
		TimePrecisionIsSet: true,
		Locale:             &emptyLocale,
	}}
}

// MakeTimestampTZ constructs a new instance of a TIMESTAMPTZ type that has at
// most the given number of fractional second digits.
//
// To use the default precision, use the `TimestampTZ` variable.
func MakeTimestampTZ(precision int32) *T {
	return &T{InternalType: InternalType{
		Family:             TimestampTZFamily,
		Oid:                oid.T_timestamptz,
		Precision:          precision,
		TimePrecisionIsSet: true,
		Locale:             &emptyLocale,
	}}
}

// MakeArray constructs a new instance of an ArrayFamily type with the given
// element type (which may itself be an ArrayFamily type).
func MakeArray(typ *T) *T {
	return &T{InternalType: InternalType{
		Family:        ArrayFamily,
		Oid:           calcArrayOid(typ),
		ArrayContents: typ,
		Locale:        &emptyLocale,
	}}
}

// MakeTuple constructs a new instance of a TupleFamily type with the given
// field types (some/all of which may be other TupleFamily types).
//
// Warning: the contents slice is used directly; the caller should not modify it
// after calling this function.
func MakeTuple(contents []T) *T {
	return &T{InternalType: InternalType{
		Family: TupleFamily, Oid: oid.T_record, TupleContents: contents, Locale: &emptyLocale,
	}}
}

// MakeLabeledTuple constructs a new instance of a TupleFamily type with the
// given field types and labels.
func MakeLabeledTuple(contents []T, labels []string) *T {
	if len(contents) != len(labels) && labels != nil {
		panic(errors.AssertionFailedf(
			"tuple contents and labels must be of same length: %v, %v", contents, labels))
	}
	return &T{InternalType: InternalType{
		Family:        TupleFamily,
		Oid:           oid.T_record,
		TupleContents: contents,
		TupleLabels:   labels,
		Locale:        &emptyLocale,
	}}
}

// Family specifies a group of types that are compatible with one another. Types
// in the same family can be compared, assigned, etc., but may differ from one
// another in width, precision, locale, and other attributes. For example, it is
// always an error to insert an INT value into a FLOAT column, because they are
// not in the same family. However, values of different types within the same
// family are "insert-compatible" with one another. Insertion may still result
// in an error because of width overflow or other constraints, but it can at
// least be attempted.
//
// Families are convenient for performing type switches on types, because in
// most cases it is the type family that matters, not the specific type. For
// example, when KWDB encodes values, it maintains routines for each family,
// since types in the same family encode in very similar ways.
//
// Most type families have an associated "canonical type" that is the default
// representative of that family, and which is a superset of all other types in
// that family. Values with other types (in the same family) can always be
// trivially converted to the canonical type with no loss of information. For
// example, the canonical type for IntFamily is Int, which is a 64-bit integer.
// Both 32-bit and 16-bit integers can be trivially converted to it.
//
// Execution operators and functions are permissive in terms of input (allow any
// type within a given family), and typically return only values having
// canonical types as output. For example, the IntFamily Plus operator allows
// values having any IntFamily type as input. But then it will always convert
// those values to 64-bit integers, and return a final 64-bit integer value
// (types.Int). Doing this vastly reduces the required number of operator
// overloads.
func (t *T) Family() Family {
	return t.InternalType.Family
}

// Oid returns the type's Postgres Object ID. The OID identifies the type more
// specifically than the type family, and is used by the Postgres wire protocol
// various Postgres catalog tables, functions like pg_typeof, etc. Maintaining
// the OID is required for Postgres-compatibility.
func (t *T) Oid() oid.Oid {
	return t.InternalType.Oid
}

// Locale identifies a specific geographical, political, or cultural region that
// impacts various character-based operations such as sorting, pattern matching,
// and builtin functions like lower and upper. It is only defined for the
// types in the CollatedStringFamily, and is the empty string for all other
// types.
func (t *T) Locale() string {
	return *t.InternalType.Locale
}

// TypeEngine return which engine can be supported by this type.
func (t *T) TypeEngine() uint32 { return t.InternalType.TypeEngine }

// Width is the size or scale of the type, such as number of bits or characters.
//
//	INT           : # of bits (64, 32, 16)
//	FLOAT         : # of bits (64, 32)
//	DECIMAL       : max # of digits after decimal point (must be <= Precision)
//	STRING        : max # of characters
//	COLLATEDSTRING: max # of characters
//	BIT           : max # of bits
//
// Width is always 0 for other types.
func (t *T) Width() int32 {
	return t.InternalType.Width
}

// Precision is the accuracy of the data type.
//
//	DECIMAL    : max # digits (must be >= Width/Scale)
//	INTERVAL   : max # fractional second digits
//	TIME       : max # fractional second digits
//	TIMETZ     : max # fractional second digits
//	TIMESTAMP  : max # fractional second digits
//	TIMESTAMPTZ: max # fractional second digits
//
// Precision for time-related families has special rules for 0 -- see
// `precision_is_set` on the `InternalType` proto.
//
// Precision is always 0 for other types.
func (t *T) Precision() int32 {
	switch t.InternalType.Family {
	case IntervalFamily, TimestampFamily, TimestampTZFamily, TimeFamily, TimeTZFamily:
		if t.InternalType.Precision == 0 && !t.InternalType.TimePrecisionIsSet {
			return DefaultTimePrecision
		}
	}
	return t.InternalType.Precision
}

// TypeModifier returns the type modifier of the type. This corresponds to the
// pg_attribute.atttypmod column. atttypmod records type-specific data supplied
// at table creation time (for example, the maximum length of a varchar column).
// The value will be -1 for types that do not need atttypmod.
func (t *T) TypeModifier() int32 {
	typeModifier := int32(-1)
	if width := t.Width(); width != 0 {
		switch t.Family() {
		case StringFamily, BytesFamily:
			// Postgres adds 4 to the attypmod for bounded string types, the
			// var header size.
			typeModifier = width + 4
		case BitFamily:
			typeModifier = width
		case DecimalFamily:
			// attTypMod is calculated by putting the precision in the upper
			// bits and the scale in the lower bits of a 32-bit int, and adding
			// 4 (the var header size). We mock this for clients' sake. See
			// numeric.c.
			typeModifier = ((t.Precision() << 16) | width) + 4
		}
	}
	return typeModifier
}

// Scale is an alias method for Width, used for clarity for types in
// DecimalFamily.
func (t *T) Scale() int32 {
	return t.InternalType.Width
}

// ArrayContents returns the type of array elements. This is nil for types that
// are not in the ArrayFamily.
func (t *T) ArrayContents() *T {
	return t.InternalType.ArrayContents
}

// TupleContents returns a slice containing the type of each tuple field. This
// is nil for non-TupleFamily types.
func (t *T) TupleContents() []T {
	return t.InternalType.TupleContents
}

// TupleLabels returns a slice containing the labels of each tuple field. This
// is nil for types not in the TupleFamily, or if the tuple type does not
// specify labels.
func (t *T) TupleLabels() []string {
	return t.InternalType.TupleLabels
}

// Name returns a single word description of the type that describes it
// succinctly, but without all the details, such as width, locale, etc. The name
// is sometimes the same as the name returned by SQLStandardName, but is more
// KWDB friendly.
//
// TODO(andyk): Should these be changed to be the same as SQLStandardName?
func (t *T) Name() string {
	switch t.Family() {
	case AnyFamily:
		return "anyelement"
	case ArrayFamily:
		switch t.Oid() {
		case oid.T_oidvector:
			return "oidvector"
		case oid.T_int2vector:
			return "int2vector"
		}
		return t.ArrayContents().Name() + "[]"
	case BitFamily:
		if t.Oid() == oid.T_varbit {
			return "varbit"
		}
		return "bit"
	case BoolFamily:
		return "bool"
	case BytesFamily:
		switch t.Oid() {
		case oid.T_bytea:
			return "bytes"
		case T_varbytea:
			return "varbytes"
		case T_blob:
			return "blob"
		}
		panic(errors.AssertionFailedf("unexpected OID: %d", t.Oid()))
	case DateFamily:
		return "date"
	case DecimalFamily:
		return "decimal"
	case FloatFamily:
		switch t.Width() {
		case 64:
			return "float"
		case 32:
			return "float4"
		default:
			panic(errors.AssertionFailedf("programming error: unknown float width: %d", t.Width()))
		}
	case INetFamily:
		return "inet"
	case IntFamily:
		switch t.Width() {
		case 64:
			return "int"
		case 32:
			return "int4"
		case 16:
			return "int2"
		case 8:
			return "int1"
		default:
			panic(errors.AssertionFailedf("programming error: unknown int width: %d", t.Width()))
		}
	case IntervalFamily:
		return "interval"
	case JsonFamily:
		return "jsonb"
	case OidFamily:
		return t.SQLStandardName()
	case StringFamily, CollatedStringFamily:
		switch t.Oid() {
		case oid.T_text:
			return "string"
		case oid.T_bpchar:
			return "char"
		case T_nchar:
			return "nchar"
		case oid.T_char:
			// Yes, that's the name. The ways of PostgreSQL are inscrutable.
			return `"char"`
		case oid.T_varchar:
			return "varchar"
		case T_geometry:
			return "geometry"
		case T_nvarchar:
			return "nvarchar"
		case oid.T_name:
			return "name"
		case T_clob:
			return "clob"
		}
		panic(errors.AssertionFailedf("unexpected OID: %d", t.Oid()))
	case TimeFamily:
		return "time"
	case TimestampFamily:
		return "timestamp"
	case TimestampTZFamily:
		return "timestamptz"
	case TimeTZFamily:
		return "timetz"
	case TupleFamily:
		// Tuple types are currently anonymous, with no name.
		return ""
	case UnknownFamily:
		return "unknown"
	case UuidFamily:
		return "uuid"
	default:
		panic(errors.AssertionFailedf("unexpected Family: %s", t.Family()))
	}
}

// PGName returns the Postgres name for the type. This is sometimes different
// than the native KWDB name for it (i.e. the Name function). It is used when
// compatibility with PG is important. Examples of differences:
//
//	Name()       PGName()
//	--------------------------
//	char         bpchar
//	"char"       char
//	bytes        bytea
//	int4[]       _int4
func (t *T) PGName() string {
	name, ok := TypeName(t.Oid())
	if ok {
		return strings.ToLower(name)
	}

	// Postgres does not have an UNKNOWN[] type. However, KWDB does, so
	// manufacture a name for it.
	if t.Family() != ArrayFamily || t.ArrayContents().Family() != UnknownFamily {
		panic(errors.AssertionFailedf("unknown PG name for oid %d", t.Oid()))
	}
	return "_unknown"
}

// TypeName checks the name for a given type by first looking up oid.TypeName
// before falling back to looking at the oid extension ExtensionTypeName.
func TypeName(o oid.Oid) (string, bool) {
	name, ok := oid.TypeName[o]
	if ok {
		return name, ok
	}
	name, ok = ExtensionTypeName[o]
	return name, ok
}

// IsTypeEngineSet return true if this type support engine.
func (t *T) IsTypeEngineSet(typeEngine Engine) bool {
	return t.InternalType.TypeEngine&typeEngine.Mask() != 0
}

// SQLStandardName returns the type's name as it is specified in the SQL
// standard (or by Postgres for any non-standard types). This can be looked up
// for any type in Postgres using a query similar to this:
//
//	SELECT format_type(pg_typeof(1::int)::regtype, NULL)
func (t *T) SQLStandardName() string {
	return t.SQLStandardNameWithTypmod(false, 0)
}

var telemetryNameReplaceRegex = regexp.MustCompile("[^a-zA-Z0-9]")

// TelemetryName returns a name that is friendly for telemetry.
func (t *T) TelemetryName() string {
	return strings.ToLower(telemetryNameReplaceRegex.ReplaceAllString(t.SQLString(), "_"))
}

// SQLStandardNameWithTypmod is like SQLStandardName but it also accepts a
// typmod argument, and a boolean which indicates whether or not a typmod was
// even specified. The expected results of this function should be, in Postgres:
//
//	SELECT format_type('thetype'::regype, typmod)
//
// Generally, what this does with a non-0 typmod is append the scale, precision
// or length of a datatype to the name of the datatype. For example, a
// varchar(20) would appear as character varying(20) when provided the typmod
// value for varchar(20), which happens to be 24.
//
// This function is full of special cases. See backend/utils/adt/format_type.c
// in Postgres.
func (t *T) SQLStandardNameWithTypmod(haveTypmod bool, typmod int) string {
	var buf strings.Builder
	switch t.Family() {
	case AnyFamily:
		return "anyelement"
	case ArrayFamily:
		switch t.Oid() {
		case oid.T_oidvector:
			return "oidvector"
		case oid.T_int2vector:
			return "int2vector"
		}
		return t.ArrayContents().SQLStandardName() + "[]"
	case BitFamily:
		if t.Oid() == oid.T_varbit {
			buf.WriteString("bit varying")
		} else {
			buf.WriteString("bit")
		}
		if !haveTypmod || typmod <= 0 {
			return buf.String()
		}
		buf.WriteString(fmt.Sprintf("(%d)", typmod))
		return buf.String()
	case BoolFamily:
		return "boolean"
	case BytesFamily:
		switch t.Oid() {
		case oid.T_bytea:
			return "bytea"
		case T_varbytea:
			return "varbytea"
		case T_blob:
			return "blob"
		default:
			panic(errors.AssertionFailedf("unexpected Oid: %v", errors.Safe(t.Oid())))
		}
	case DateFamily:
		return "date"
	case DecimalFamily:
		if !haveTypmod || typmod <= 0 {
			return "numeric"
		}
		// The typmod of a numeric has the precision in the upper bits and the
		// scale in the lower bits of a 32-bit int, after subtracting 4 (the var
		// header size). See numeric.c.
		typmod -= 4
		return fmt.Sprintf(
			"numeric(%d,%d)",
			(typmod>>16)&0xffff,
			typmod&0xffff,
		)

	case FloatFamily:
		switch t.Width() {
		case 32:
			return "real"
		case 64:
			return "double precision"
		default:
			panic(errors.AssertionFailedf("programming error: unknown float width: %d", t.Width()))
		}
	case INetFamily:
		return "inet"
	case IntFamily:
		switch t.Width() {
		case 8:
			return "tinyint"
		case 16:
			return "smallint"
		case 32:
			// PG shows "integer" for int4.
			return "integer"
		case 64:
			return "bigint"
		default:
			panic(errors.AssertionFailedf("programming error: unknown int width: %d", t.Width()))
		}
	case IntervalFamily:
		// TODO(jordan): intervals can have typmods, but we don't support them in the same way.
		// Masking is used to extract the precision (src/include/utils/timestamp.h), whereas
		// we store it as `IntervalDurationField`.
		return "interval"
	case JsonFamily:
		// Only binary JSON is currently supported.
		return "jsonb"
	case OidFamily:
		switch t.Oid() {
		case oid.T_oid:
			return "oid"
		case oid.T_regclass:
			return "regclass"
		case oid.T_regnamespace:
			return "regnamespace"
		case oid.T_regproc:
			return "regproc"
		case oid.T_regprocedure:
			return "regprocedure"
		case oid.T_regtype:
			return "regtype"
		default:
			panic(errors.AssertionFailedf("unexpected Oid: %v", errors.Safe(t.Oid())))
		}
	case StringFamily, CollatedStringFamily:
		switch t.Oid() {
		case oid.T_text:
			buf.WriteString("text")
		case oid.T_varchar:
			buf.WriteString("character varying")
		case T_nvarchar:
			buf.WriteString("character varying")
		case oid.T_bpchar:
			if haveTypmod && typmod < 0 {
				// Special case. Run `select format_type('bpchar'::regtype, -1);` in pg.
				return "bpchar"
			}
			buf.WriteString("character")
		case T_nchar:
			if haveTypmod && typmod < 0 {
				return "nchar"
			}
			buf.WriteString("character")
		case oid.T_char:
			// Type modifiers not allowed for "char".
			return `"char"`
		case oid.T_name:
			// Type modifiers not allowed for name.
			return "name"
		case T_geometry:
			buf.WriteString("geometry")
		case T_clob:
			buf.WriteString("clob")
		default:
			panic(errors.AssertionFailedf("unexpected OID: %d", t.Oid()))
		}
		if !haveTypmod {
			return buf.String()
		}

		// Typmod gets subtracted by 4 for all non-text string-like types to produce
		// the length.
		if t.Oid() != oid.T_text {
			typmod -= 4
		}
		if typmod <= 0 {
			// In this case, we don't print any modifier.
			return buf.String()
		}
		buf.WriteString(fmt.Sprintf("(%d)", typmod))
		return buf.String()

	case TimeFamily:
		if !haveTypmod || typmod < 0 {
			return "time without time zone"
		}
		return fmt.Sprintf("time(%d) without time zone", typmod)
	case TimeTZFamily:
		if !haveTypmod || typmod < 0 {
			return "time with time zone"
		}
		return fmt.Sprintf("time(%d) with time zone", typmod)
	case TimestampFamily:
		if !haveTypmod || typmod < 0 {
			return "timestamp without time zone"
		}
		return fmt.Sprintf("timestamp(%d) without time zone", typmod)
	case TimestampTZFamily:
		if !haveTypmod || typmod < 0 {
			return "timestamp with time zone"
		}
		return fmt.Sprintf("timestamp(%d) with time zone", typmod)
	case TupleFamily:
		return "record"
	case UnknownFamily:
		return "unknown"
	case UuidFamily:
		return "uuid"
	default:
		panic(errors.AssertionFailedf("unexpected Family: %v", errors.Safe(t.Family())))
	}
}

// InformationSchemaName returns the string suitable to populate the data_type
// column of information_schema.columns.
//
// This is different from SQLString() in that it must report SQL standard names
// that are compatible with PostgreSQL client expectations.
func (t *T) InformationSchemaName() string {
	// This is the same as SQLStandardName, except for the case of arrays.
	if t.Family() == ArrayFamily {
		return "ARRAY"
	}
	return t.SQLStandardName()
}

// SQLString returns the CockroachDB native SQL string that can be used to
// reproduce the type via parsing the string as a type. It is used in error
// messages and also to produce the output of SHOW CREATE.
func (t *T) SQLString() string {
	switch t.Family() {
	case BytesFamily:
		var typName string
		switch t.Oid() {
		case oid.T_bytea:
			typName = "BYTES"
		case T_varbytea:
			typName = "VARBYTES"
		case T_blob:
			typName = "BLOB"
		}
		if t.Width() > 0 {
			typName = fmt.Sprintf("%s(%d)", typName, t.Width())
		}
		return typName
	case BitFamily:
		o := t.Oid()
		typName := "BIT"
		if o == oid.T_varbit {
			typName = "VARBIT"
		}
		// BIT(1) pretty-prints as just BIT.
		if (o != oid.T_varbit && t.Width() > 1) ||
			(o == oid.T_varbit && t.Width() > 0) {
			typName = fmt.Sprintf("%s(%d)", typName, t.Width())
		}
		return typName
	case IntFamily:
		switch t.Width() {
		case 8:
			return "INT1"
		case 16:
			return "INT2"
		case 32:
			return "INT4"
		case 64:
			return "INT8"
		default:
			panic(errors.AssertionFailedf("programming error: unknown int width: %d", t.Width()))
		}
	case StringFamily:
		return t.stringTypeSQL()
	case CollatedStringFamily:
		return t.collatedStringTypeSQL(false /* isArray */)
	case FloatFamily:
		const realName = "FLOAT4"
		const doubleName = "FLOAT8"
		if t.Width() == 32 {
			return realName
		}
		return doubleName
	case DecimalFamily:
		if t.Precision() > 0 {
			if t.Width() > 0 {
				return fmt.Sprintf("DECIMAL(%d,%d)", t.Precision(), t.Scale())
			}
			return fmt.Sprintf("DECIMAL(%d)", t.Precision())
		}
	case JsonFamily:
		// Only binary JSON is currently supported.
		return "JSONB"
	case TimestampFamily, TimestampTZFamily, TimeFamily, TimeTZFamily:
		if t.InternalType.Precision > 0 || t.InternalType.TimePrecisionIsSet {
			return fmt.Sprintf("%s(%d)", strings.ToUpper(t.Name()), t.Precision())
		}
	case IntervalFamily:
		switch t.InternalType.IntervalDurationField.DurationType {
		case IntervalDurationType_UNSET:
			if t.InternalType.Precision > 0 || t.InternalType.TimePrecisionIsSet {
				return fmt.Sprintf("%s(%d)", strings.ToUpper(t.Name()), t.Precision())
			}
		default:
			fromStr := ""
			if t.InternalType.IntervalDurationField.FromDurationType != IntervalDurationType_UNSET {
				fromStr = fmt.Sprintf("%s TO ", t.InternalType.IntervalDurationField.FromDurationType.String())
			}
			precisionStr := ""
			if t.InternalType.Precision > 0 || t.InternalType.TimePrecisionIsSet {
				precisionStr = fmt.Sprintf("(%d)", t.Precision())
			}
			return fmt.Sprintf(
				"%s %s%s%s",
				strings.ToUpper(t.Name()),
				fromStr,
				t.InternalType.IntervalDurationField.DurationType.String(),
				precisionStr,
			)
		}
	case OidFamily:
		if name, ok := oid.TypeName[t.Oid()]; ok {
			return name
		}
	case ArrayFamily:
		switch t.Oid() {
		case oid.T_oidvector:
			return "OIDVECTOR"
		case oid.T_int2vector:
			return "INT2VECTOR"
		}
		if t.ArrayContents().Family() == CollatedStringFamily {
			return t.ArrayContents().collatedStringTypeSQL(true /* isArray */)
		}
		return t.ArrayContents().SQLString() + "[]"
	}
	return strings.ToUpper(t.Name())
}

// Equivalent returns true if this type is "equivalent" to the given type.
// Equivalent types are compatible with one another: they can be compared,
// assigned, and unioned. Equivalent types must always have the same type family
// for the root type and any descendant types (i.e. in case of array or tuple
// types). Types in the CollatedStringFamily must have the same locale. But
// other attributes of equivalent types, such as width, precision, and oid, can
// be different.
//
// Wildcard types (e.g. Any, AnyArray, AnyTuple, etc) have special equivalence
// behavior. AnyFamily types match any other type, including other AnyFamily
// types. And a wildcard collation (empty string) matches any other collation.
func (t *T) Equivalent(other *T) bool {
	if t.Family() == AnyFamily || other.Family() == AnyFamily {
		return true
	}
	if t.Family() != other.Family() {
		return false
	}

	switch t.Family() {
	case CollatedStringFamily:
		if t.Locale() != "" && other.Locale() != "" && t.Locale() != other.Locale() {
			return false
		}

	case TupleFamily:
		// If either tuple is the wildcard tuple, it's equivalent to any other
		// tuple type. This allows overloads to specify that they take an arbitrary
		// tuple type.
		if IsWildcardTupleType(t) || IsWildcardTupleType(other) {
			return true
		}
		if len(t.TupleContents()) != len(other.TupleContents()) {
			return false
		}
		for i := range t.TupleContents() {
			if !t.TupleContents()[i].Equivalent(&other.TupleContents()[i]) {
				return false
			}
		}

	case ArrayFamily:
		if !t.ArrayContents().Equivalent(other.ArrayContents()) {
			return false
		}
	}

	return true
}

// Identical returns true if every field in this ColumnType is exactly the same
// as every corresponding field in the given ColumnType. Identical performs a
// deep comparison, traversing any Tuple or Array contents.
//
// NOTE: Consider whether the desired semantics really require identical types,
// or if Equivalent is the right method to call instead.
func (t *T) Identical(other *T) bool {
	return t.InternalType.Identical(&other.InternalType)
}

// Equal is for use in generated protocol buffer code only.
func (t *T) Equal(other T) bool {
	return t.Identical(&other)
}

// Size returns the size, in bytes, of this type once it has been marshaled to
// a byte buffer. This is typically called to determine the size of the buffer
// that needs to be allocated before calling Marshal.
//
// Marshal is part of the protoutil.Message interface.
func (t *T) Size() (n int) {
	// Need to first downgrade the type before delegating to InternalType,
	// because Marshal will downgrade.
	temp := *t
	err := temp.downgradeType()
	if err != nil {
		panic(errors.AssertionFailedf("error during Size call: %v", err))
	}
	return temp.InternalType.Size()
}

// ProtoMessage is the protobuf marker method. It is part of the
// protoutil.Message interface.
func (t *T) ProtoMessage() {}

// Reset clears the type instance. It is part of the protoutil.Message
// interface.
func (t *T) Reset() {
	*t = T{}
}

// Identical is the internal implementation for T.Identical. See that comment
// for details.
func (t *InternalType) Identical(other *InternalType) bool {
	if t.Family != other.Family {
		return false
	}
	if t.Width != other.Width {
		return false
	}
	if t.Precision != other.Precision {
		return false
	}
	if t.TimePrecisionIsSet != other.TimePrecisionIsSet {
		return false
	}
	if t.IntervalDurationField != nil && other.IntervalDurationField != nil {
		if *t.IntervalDurationField != *other.IntervalDurationField {
			return false
		}
	} else if t.IntervalDurationField != nil {
		return false
	} else if other.IntervalDurationField != nil {
		return false
	}
	if t.Locale != nil && other.Locale != nil {
		if *t.Locale != *other.Locale {
			return false
		}
	} else if t.Locale != nil {
		return false
	} else if other.Locale != nil {
		return false
	}
	if t.ArrayContents != nil && other.ArrayContents != nil {
		if !t.ArrayContents.Identical(other.ArrayContents) {
			return false
		}
	} else if t.ArrayContents != nil {
		return false
	} else if other.ArrayContents != nil {
		return false
	}
	if len(t.TupleContents) != len(other.TupleContents) {
		return false
	}
	for i := range t.TupleContents {
		if !t.TupleContents[i].Identical(&other.TupleContents[i]) {
			return false
		}
	}
	if len(t.TupleLabels) != len(other.TupleLabels) {
		return false
	}
	for i := range t.TupleLabels {
		if t.TupleLabels[i] != other.TupleLabels[i] {
			return false
		}
	}
	return t.Oid == other.Oid
}

// Unmarshal deserializes a type from the given byte representation using gogo
// protobuf serialization rules. It is backwards-compatible with formats used
// by older versions of KWDB.
//
//	var t T
//	err := protoutil.Unmarshal(data, &t)
//
// Unmarshal is part of the protoutil.Message interface.
func (t *T) Unmarshal(data []byte) error {
	// Unmarshal the internal type, and then perform an upgrade step to convert
	// to the latest format.
	err := protoutil.Unmarshal(data, &t.InternalType)
	if err != nil {
		return err
	}
	return t.upgradeType()
}

// upgradeType assumes its input was just unmarshaled from bytes that may have
// been serialized by any previous version of KWDB. It upgrades the object
// according to the requirements of the latest version by remapping fields and
// setting required values. This is necessary to preserve backwards-
// compatibility with older formats (e.g. restoring database from old backup).
func (t *T) upgradeType() error {
	switch t.Family() {
	case IntFamily:
		// Check VisibleType field that was populated in previous versions.
		switch t.InternalType.VisibleType {
		case visibleTINYINT:
			t.InternalType.Width = 8
			t.InternalType.Oid = T_int1
		case visibleSMALLINT:
			t.InternalType.Width = 16
			t.InternalType.Oid = oid.T_int2
		case visibleINTEGER:
			t.InternalType.Width = 32
			t.InternalType.Oid = oid.T_int4
		case visibleBIGINT:
			t.InternalType.Width = 64
			t.InternalType.Oid = oid.T_int8
		case visibleBIT, visibleNONE:
			// Pre-2.1 BIT was using IntFamily with arbitrary widths. Clamp them
			// to fixed/known widths. See #34161.
			switch t.Width() {
			case 8:
				t.InternalType.Oid = T_int1
			case 16:
				t.InternalType.Oid = oid.T_int2
			case 32:
				t.InternalType.Oid = oid.T_int4
			default:
				// Assume INT8 if width is 0 or not valid.
				t.InternalType.Oid = oid.T_int8
				t.InternalType.Width = 64
			}
		default:
			return errors.AssertionFailedf("unexpected visible type: %d", t.InternalType.VisibleType)
		}

	case FloatFamily:
		// Map visible REAL type to 32-bit width.
		switch t.InternalType.VisibleType {
		case visibleREAL:
			t.InternalType.Oid = oid.T_float4
			t.InternalType.Width = 32
		case visibleDOUBLE:
			t.InternalType.Oid = oid.T_float8
			t.InternalType.Width = 64
		case visibleNONE:
			switch t.Width() {
			case 32:
				t.InternalType.Oid = oid.T_float4
			case 64:
				t.InternalType.Oid = oid.T_float8
			default:
				// Pre-2.1 (before Width) there were 3 cases:
				// - VisibleType = DOUBLE PRECISION, Width = 0 -> now clearly FLOAT8
				// - VisibleType = NONE, Width = 0 -> now clearly FLOAT8
				// - VisibleType = NONE, Precision > 0 -> we need to derive the width.
				if t.Precision() >= 1 && t.Precision() <= 24 {
					t.InternalType.Oid = oid.T_float4
					t.InternalType.Width = 32
				} else {
					t.InternalType.Oid = oid.T_float8
					t.InternalType.Width = 64
				}
			}
		default:
			return errors.AssertionFailedf("unexpected visible type: %d", t.InternalType.VisibleType)
		}

		// Precision should always be set to 0 going forward.
		t.InternalType.Precision = 0

	case TimestampFamily, TimestampTZFamily, TimeFamily, TimeTZFamily:
		// Some bad/experimental versions of master had precision stored as `-1`.
		// This represented a default - so upgrade this to 0 with TimePrecisionIsSet = false.
		if t.InternalType.Precision == -1 {
			t.InternalType.Precision = 0
			t.InternalType.TimePrecisionIsSet = false
		}
		// Going forwards after 19.2, we want `TimePrecisionIsSet` to be explicitly set
		// if Precision is > 0.
		if t.InternalType.Precision > 0 {
			t.InternalType.TimePrecisionIsSet = true
		}
	case IntervalFamily:
		// Fill in the IntervalDurationField here.
		if t.InternalType.IntervalDurationField == nil {
			t.InternalType.IntervalDurationField = &IntervalDurationField{}
		}
		// Going forwards after 19.2, we want `TimePrecisionIsSet` to be explicitly set
		// if Precision is > 0.
		if t.InternalType.Precision > 0 {
			t.InternalType.TimePrecisionIsSet = true
		}
	case StringFamily, CollatedStringFamily:
		// Map string-related visible types to corresponding Oid values.
		switch t.InternalType.VisibleType {
		case visibleVARCHAR:
			t.InternalType.Oid = oid.T_varchar
		case visibleNVARCHAR:
			t.InternalType.Oid = T_nvarchar
		case visibleGEOMETRY:
			t.InternalType.Oid = T_geometry
		case visibleCHAR:
			t.InternalType.Oid = oid.T_bpchar
		case visibleNCHAR:
			t.InternalType.Oid = T_nchar
		case visibleQCHAR:
			t.InternalType.Oid = oid.T_char
		case visibleNONE:
			t.InternalType.Oid = oid.T_text
		case visibleCLOB:
			t.InternalType.Oid = T_clob
		default:
			return errors.AssertionFailedf("unexpected visible type: %d", t.InternalType.VisibleType)
		}
		if t.InternalType.Family == StringFamily {
			if t.InternalType.Locale != nil && len(*t.InternalType.Locale) != 0 {
				return errors.AssertionFailedf(
					"STRING type should not have locale: %s", *t.InternalType.Locale)
			}
		}

	case BitFamily:
		// Map visible VARBIT type to T_varbit OID value.
		switch t.InternalType.VisibleType {
		case visibleVARBIT:
			t.InternalType.Oid = oid.T_varbit
		case visibleNONE:
			t.InternalType.Oid = oid.T_bit
		default:
			return errors.AssertionFailedf("unexpected visible type: %d", t.InternalType.VisibleType)
		}

	case ArrayFamily:
		if t.ArrayContents() == nil {
			// This array type was serialized by a previous version of KWDB,
			// so construct the array contents from scratch.
			arrayContents := *t
			arrayContents.InternalType.Family = *t.InternalType.ArrayElemType
			arrayContents.InternalType.ArrayDimensions = nil
			arrayContents.InternalType.ArrayElemType = nil
			if err := arrayContents.upgradeType(); err != nil {
				return err
			}
			t.InternalType.ArrayContents = &arrayContents
			t.InternalType.Oid = calcArrayOid(t.ArrayContents())
		}

		// Marshaling/unmarshaling nested arrays is not yet supported.
		if t.ArrayContents().Family() == ArrayFamily {
			return errors.AssertionFailedf("nested array should never be unmarshaled")
		}

		// Zero out fields that may have been used to store information about
		// the array element type, or which are no longer in use.
		t.InternalType.Width = 0
		t.InternalType.Precision = 0
		t.InternalType.Locale = nil
		t.InternalType.VisibleType = 0
		t.InternalType.ArrayElemType = nil
		t.InternalType.ArrayDimensions = nil

	case int2vector:
		t.InternalType.Family = ArrayFamily
		t.InternalType.Width = 0
		t.InternalType.Oid = oid.T_int2vector
		t.InternalType.ArrayContents = Int2

	case oidvector:
		t.InternalType.Family = ArrayFamily
		t.InternalType.Oid = oid.T_oidvector
		t.InternalType.ArrayContents = Oid

	case name:
		if t.InternalType.Locale != nil {
			t.InternalType.Family = CollatedStringFamily
		} else {
			t.InternalType.Family = StringFamily
		}
		t.InternalType.Oid = oid.T_name
		if t.Width() != 0 {
			return errors.AssertionFailedf("name type cannot have non-zero width: %d", t.Width())
		}
	}

	if t.InternalType.Oid == 0 {
		t.InternalType.Oid = familyToOid[t.Family()]
	}

	// Clear the deprecated visible types, since they are now handled by the
	// Width or Oid fields.
	t.InternalType.VisibleType = 0

	// If locale is not set, always set it to the empty string, in order to avoid
	// bothersome deref errors when the Locale method is called.
	if t.InternalType.Locale == nil {
		t.InternalType.Locale = &emptyLocale
	}

	return nil
}

// Marshal serializes a type into a byte representation using gogo protobuf
// serialization rules. It returns the resulting bytes as a slice. The bytes
// are serialized in a format that is backwards-compatible with the previous
// version of KWDB so that clusters can run in mixed version mode during
// upgrade.
//
//	bytes, err := protoutil.Marshal(&typ)
func (t *T) Marshal() (data []byte, err error) {
	// First downgrade to a struct that will be serialized in a backwards-
	// compatible bytes format.
	temp := *t
	if err := temp.downgradeType(); err != nil {
		return nil, err
	}
	return protoutil.Marshal(&temp.InternalType)
}

// MarshalTo behaves like Marshal, except that it deserializes to an existing
// byte slice and returns the number of bytes written to it. The slice must
// already have sufficient capacity. Callers can use the Size method to
// determine how much capacity needs to be allocated.
//
// Marshal is part of the protoutil.Message interface.
func (t *T) MarshalTo(data []byte) (int, error) {
	temp := *t
	if err := temp.downgradeType(); err != nil {
		return 0, err
	}
	return temp.InternalType.MarshalTo(data)
}

// of the latest KWDB version. It updates the fields so that they will be
// marshaled into a format that is compatible with the previous version of
// KWDB. This is necessary to preserve backwards-compatibility in mixed-version
// scenarios, such as during upgrade.
func (t *T) downgradeType() error {
	// Set Family and VisibleType for 19.1 backwards-compatibility.
	switch t.Family() {
	case BitFamily:
		if t.Oid() == oid.T_varbit {
			t.InternalType.VisibleType = visibleVARBIT
		}

	case FloatFamily:
		switch t.Width() {
		case 32:
			t.InternalType.VisibleType = visibleREAL
		}

	case BytesFamily:
		switch t.Oid() {
		case oid.T_bytea:
			t.InternalType.VisibleType = visibleBYTES
		case T_varbytea:
			t.InternalType.VisibleType = visibleVARBYTES
		case T_blob:
			t.InternalType.VisibleType = visibleBLOB
		default:
			return errors.AssertionFailedf("unexpected Oid: %d", t.Oid())
		}

	case StringFamily, CollatedStringFamily:
		switch t.Oid() {
		case oid.T_text:
			// Nothing to do.
		case oid.T_varchar:
			t.InternalType.VisibleType = visibleVARCHAR
		case T_nvarchar:
			t.InternalType.VisibleType = visibleNVARCHAR
		case T_geometry:
			t.InternalType.VisibleType = visibleGEOMETRY
		case oid.T_bpchar:
			t.InternalType.VisibleType = visibleCHAR
		case T_nchar:
			t.InternalType.VisibleType = visibleNCHAR
		case T_clob:
			t.InternalType.VisibleType = visibleCLOB
		case oid.T_char:
			t.InternalType.VisibleType = visibleQCHAR
		case oid.T_name:
			t.InternalType.Family = name
		default:
			return errors.AssertionFailedf("unexpected Oid: %d", t.Oid())
		}

	case ArrayFamily:
		// Marshaling/unmarshaling nested arrays is not yet supported.
		if t.ArrayContents().Family() == ArrayFamily {
			return errors.AssertionFailedf("nested array should never be marshaled")
		}

		// Downgrade to array representation used before 19.2, in which the array
		// type fields specified the width, locale, etc. of the element type.
		temp := *t.InternalType.ArrayContents
		if err := temp.downgradeType(); err != nil {
			return err
		}
		t.InternalType.Width = temp.InternalType.Width
		t.InternalType.Precision = temp.InternalType.Precision
		t.InternalType.Locale = temp.InternalType.Locale
		t.InternalType.VisibleType = temp.InternalType.VisibleType
		t.InternalType.ArrayElemType = &t.InternalType.ArrayContents.InternalType.Family

		switch t.Oid() {
		case oid.T_int2vector:
			t.InternalType.Family = int2vector
		case oid.T_oidvector:
			t.InternalType.Family = oidvector
		}
	}

	// Map empty locale to nil.
	if t.InternalType.Locale != nil && len(*t.InternalType.Locale) == 0 {
		t.InternalType.Locale = nil
	}

	return nil
}

// String returns the name of the type, similar to the Name method. However, it
// expands CollatedStringFamily, ArrayFamily, and TupleFamily types to be more
// descriptive.
//
// TODO(andyk): It'd be nice to have this return SqlString() method output,
// since that is more descriptive.
func (t *T) String() string {
	switch t.Family() {
	case CollatedStringFamily:
		if t.Locale() == "" {
			// Used in telemetry.
			return fmt.Sprintf("collated%s{*}", t.Name())
		}
		return fmt.Sprintf("collated%s{%s}", t.Name(), t.Locale())

	case ArrayFamily:
		switch t.Oid() {
		case oid.T_oidvector, oid.T_int2vector:
			return t.Name()
		}
		return t.ArrayContents().String() + "[]"

	case TupleFamily:
		var buf bytes.Buffer
		buf.WriteString("tuple")
		if len(t.TupleContents()) != 0 && !IsWildcardTupleType(t) {
			buf.WriteByte('{')
			for i, typ := range t.TupleContents() {
				if i != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(typ.String())
				if t.TupleLabels() != nil {
					buf.WriteString(" AS ")
					buf.WriteString(t.InternalType.TupleLabels[i])
				}
			}
			buf.WriteByte('}')
		}
		return buf.String()
	case IntervalFamily, TimestampFamily, TimestampTZFamily, TimeFamily, TimeTZFamily:
		if t.InternalType.Precision > 0 || t.InternalType.TimePrecisionIsSet {
			return fmt.Sprintf("%s(%d)", t.Name(), t.Precision())
		}
	}
	return t.Name()
}

// DebugString returns a detailed dump of the type protobuf struct, suitable for
// debugging scenarios.
func (t *T) DebugString() string {
	return t.InternalType.String()
}

// IsAmbiguous returns true if this type is in UnknownFamily or AnyFamily.
// Instances of ambiguous types can be NULL or be in one of several different
// type families. This is important for parameterized types to determine whether
// they are fully concrete or not.
func (t *T) IsAmbiguous() bool {
	switch t.Family() {
	case UnknownFamily, AnyFamily:
		return true
	case CollatedStringFamily:
		return t.Locale() == ""
	case TupleFamily:
		if len(t.TupleContents()) == 0 {
			return true
		}
		for i := range t.TupleContents() {
			if t.TupleContents()[i].IsAmbiguous() {
				return true
			}
		}
		return false
	case ArrayFamily:
		return t.ArrayContents().IsAmbiguous()
	}
	return false
}

// IsStringType returns true iff the given type is String or a collated string
// type.
func IsStringType(t *T) bool {
	switch t.Family() {
	case StringFamily, CollatedStringFamily:
		return true
	default:
		return false
	}
}

// IsValidArrayElementType returns true if the given type can be used as the
// element type of an ArrayFamily-typed column. If the valid return is false,
// the issue number should be included in the error report to inform the user.
func IsValidArrayElementType(t *T) (valid bool, issueNum int) {
	switch t.InternalType.Oid {
	case T_nvarchar, T_nchar, T_int1, T_varbytea, T_geometry, T_clob, T_blob:
		return false, 0
	}
	switch t.Family() {
	case JsonFamily:
		return false, 23468
	default:
		return true, 0
	}
}

// CheckArrayElementType ensures that the given type can be used as the element
// type of an ArrayFamily-typed column. If not, it returns an error.
func CheckArrayElementType(t *T) error {
	if ok, issueNum := IsValidArrayElementType(t); !ok {
		return unimplemented.NewWithIssueDetailf(issueNum, t.String(),
			"arrays of %s not allowed", t)
	}
	return nil
}

// IsDateTimeType returns true if the given type is a date or time-related type.
func IsDateTimeType(t *T) bool {
	switch t.Family() {
	case DateFamily:
		return true
	case TimeFamily:
		return true
	case TimeTZFamily:
		return true
	case TimestampFamily:
		return true
	case TimestampTZFamily:
		return true
	case IntervalFamily:
		return true
	default:
		return false
	}
}

// IsAdditiveType returns true if the given type supports addition and
// subtraction.
func IsAdditiveType(t *T) bool {
	switch t.Family() {
	case IntFamily:
		return true
	case FloatFamily:
		return true
	case DecimalFamily:
		return true
	default:
		return IsDateTimeType(t)
	}
}

// IsWildcardTupleType returns true if this is the wildcard AnyTuple type. The
// wildcard type matches a tuple type having any number of fields (including 0).
func IsWildcardTupleType(t *T) bool {
	return len(t.TupleContents()) == 1 && t.TupleContents()[0].Family() == AnyFamily
}

// collatedStringTypeSQL returns the string representation of a COLLATEDSTRING
// or []COLLATEDSTRING type. This is tricky in the case of an array of collated
// string, since brackets must precede the COLLATE identifier:
//
//	STRING COLLATE EN
//	VARCHAR(20)[] COLLATE DE
func (t *T) collatedStringTypeSQL(isArray bool) string {
	var buf bytes.Buffer
	buf.WriteString(t.stringTypeSQL())
	if isArray {
		buf.WriteString("[] COLLATE ")
	} else {
		buf.WriteString(" COLLATE ")
	}
	lex.EncodeLocaleName(&buf, t.Locale())
	return buf.String()
}

// stringTypeSQL returns the visible type name plus any width specifier for the
// STRING/COLLATEDSTRING type.
func (t *T) stringTypeSQL() string {
	typName := "STRING"
	switch t.Oid() {
	case oid.T_varchar:
		typName = "VARCHAR"
	case T_nvarchar:
		typName = "NVARCHAR"
	case T_geometry:
		return "GEOMETRY"
	case oid.T_bpchar:
		typName = "CHAR"
	case T_nchar:
		typName = "NCHAR"
	case oid.T_char:
		// Yes, that's the name. The ways of PostgreSQL are inscrutable.
		typName = `"char"`
	case oid.T_name:
		typName = "NAME"
	case T_clob:
		typName = "CLOB"
	}

	// In general, if there is a specified width we want to print it next to the
	// type. However, in the specific case of CHAR and "char", the default is 1
	// and the width should be omitted in that case.
	if t.Width() > 0 {
		o := t.Oid()
		if t.Width() != 1 || (o != oid.T_bpchar && o != oid.T_char && o != T_nchar) {
			typName = fmt.Sprintf("%s(%d)", typName, t.Width())
		}
	}

	return typName
}

var typNameLiterals map[string]*T

func init() {
	typNameLiterals = make(map[string]*T)
	for o, t := range OidToType {
		name := strings.ToLower(oid.TypeName[o])
		if _, ok := typNameLiterals[name]; !ok {
			typNameLiterals[name] = t
		}
	}
}

// TypeForNonKeywordTypeName returns the column type for the string name of a
// type, if one exists. The third return value indicates:
//
//	 0 if no error or the type is not known in postgres.
//	 -1 if the type is known in postgres.
//	>0 for a github issue number.
func TypeForNonKeywordTypeName(name string) (*T, bool, int) {
	t, ok := typNameLiterals[name]
	if ok {
		return t, ok, 0
	}
	return nil, false, postgresPredefinedTypeIssues[name]
}

// SupportTimeCalc return if type can push down addtime or subtime.
func (t *T) SupportTimeCalc() bool {
	return t.Oid() == oid.T_timestamp || t.Oid() == oid.T_timestamptz || t.Oid() == oid.T_interval
}

// DetailedName returns the detailed name of the type, similar to the Name method.
// However, it expands CollatedStringFamily, ArrayFamily, and TupleFamily types
// to be more descriptive.
func (t *T) DetailedName() string {
	switch t.Family() {
	case CollatedStringFamily:
		if t.Locale() == "" {
			return fmt.Sprintf("COLLATED%s{*}", strings.ToUpper(t.Name()))
		}
		return fmt.Sprintf("COLLATED%s{%s}", strings.ToUpper(t.Name()), t.Locale())

	case ArrayFamily:
		switch t.Oid() {
		case oid.T_oidvector, oid.T_int2vector:
			return t.SQLString()
		}
		return t.ArrayContents().DetailedName() + "[]"

	case TupleFamily:
		var buf bytes.Buffer
		buf.WriteString("tuple")
		if len(t.TupleContents()) != 0 && !IsWildcardTupleType(t) {
			buf.WriteByte('{')
			for i, typ := range t.TupleContents() {
				if i != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(typ.DetailedName())
				if t.TupleLabels() != nil {
					buf.WriteString(" AS ")
					buf.WriteString(t.InternalType.TupleLabels[i])
				}
			}
			buf.WriteByte('}')
		}
		return buf.String()
	}
	return t.SQLString()
}

// The following map must include all types predefined in PostgreSQL
// that are also not yet defined in CockroachDB and link them to
// github issues. It is also possible, but not necessary, to include
// PostgreSQL types that are already implemented in CockroachDB.
var postgresPredefinedTypeIssues = map[string]int{
	"box":           21286,
	"cidr":          18846,
	"circle":        21286,
	"line":          21286,
	"lseg":          21286,
	"macaddr":       -1,
	"macaddr8":      -1,
	"money":         -1,
	"path":          21286,
	"pg_lsn":        -1,
	"point":         21286,
	"polygon":       21286,
	"tsquery":       7821,
	"tsvector":      7821,
	"txid_snapshot": -1,
	"xml":           -1,
}

// OIDs in this block are extensions of postgres, thus having no official OID.
const (
	T_int1      = oid.Oid(91000)
	T__int1     = oid.Oid(91001)
	T_nchar     = oid.Oid(91002)
	T__nchar    = oid.Oid(91003)
	T_nvarchar  = oid.Oid(91004)
	T__nvarchar = oid.Oid(91005)
	T_varbytea  = oid.Oid(91006)
	T__varbytea = oid.Oid(91007)
	T_geometry  = oid.Oid(91008)
	T__geometry = oid.Oid(91009)
	T_clob      = oid.Oid(91010)
	T__clob     = oid.Oid(91011)
	T_blob      = oid.Oid(91012)
	T__blob     = oid.Oid(91013)
)

// ExtensionTypeName returns a mapping from extension oids
// to their type name.
var ExtensionTypeName = map[oid.Oid]string{
	T_int1:      "INT1",
	T__int1:     "_INT1",
	T_nchar:     "NCHAR",
	T__nchar:    "_NCHAR",
	T_nvarchar:  "NVARCHAR",
	T__nvarchar: "_NVARCHAR",
	T_varbytea:  "VARBYTES",
	T__varbytea: "_VARBYTES",
	T_geometry:  "GEOMETRY",
	T__geometry: "_GEOMETRY",
	T_clob:      "CLOB",
	T__clob:     "_CLOB",
	T_blob:      "BLOB",
	T__blob:     "_BLOB",
}

// CsvOptionCharset is used for IMPORT/EXPORT
var CsvOptionCharset = map[string]struct{}{
	"UTF-8":   {},
	"GBK":     {},
	"GB18030": {},
	"BIG5":    {},
}
