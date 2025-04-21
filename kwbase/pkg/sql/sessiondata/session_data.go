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

package sessiondata

import (
	"fmt"
	"net"
	"strings"
	"time"
)

// SessionData contains session parameters. They are all user-configurable.
// A SQL Session changes fields in SessionData through sql.sessionDataMutator.
type SessionData struct {
	// ApplicationName is the name of the application running the
	// current session. This can be used for logging and per-application
	// statistics.
	ApplicationName string
	// AvoidBuffering is used to avoid result sets being buffered.
	// See DisableBuffering() for details.
	AvoidBuffering bool
	// Database indicates the "current" database for the purpose of
	// resolving names. See searchAndQualifyDatabase() for details.
	Database string
	// DefaultReadOnly indicates the default read-only status of newly created
	// transactions.
	DefaultReadOnly bool
	// DistSQLMode indicates whether to run queries using the distributed
	// execution engine.
	DistSQLMode DistSQLExecMode
	// OptimizerFKs indicates whether we should use the new paths to plan foreign
	// key checks in the optimizer.
	OptimizerFKs bool
	// SerialNormalizationMode indicates how to handle the SERIAL pseudo-type.
	SerialNormalizationMode SerialNormalizationMode
	// SearchPath is a list of namespaces to search builtins in.
	SearchPath SearchPath
	// StmtTimeout is the duration a query is permitted to run before it is
	// canceled by the session. If set to 0, there is no timeout.
	StmtTimeout time.Duration
	// IdleInSessionTimeout is the duration a session is permitted to idle before
	// the session is canceled. If set to 0, there is no timeout.
	IdleInSessionTimeout time.Duration
	// User is the name of the user logged into the session.
	User string
	// Roles is the roles of the user logged into the session.
	Roles []string
	// SafeUpdates causes errors when the client
	// sends syntax that may have unwanted side effects.
	SafeUpdates bool
	// RemoteAddr is used to generate logging events.
	RemoteAddr net.Addr
	// ZigzagJoinEnabled indicates whether the optimizer should try and plan a
	// zigzag join.
	ZigzagJoinEnabled bool
	// HashScanMode indicates which mode the tagreader should use for multi-model analysis.
	HashScanMode int
	// MultiModelEnabled indicates whether the optimizer should do multi-model analysis and
	// consider the optimal plan for a multi-model query.
	MultiModelEnabled bool
	// ReorderJoinsLimit indicates the number of joins at which the optimizer should
	// stop attempting to reorder.
	ReorderJoinsLimit int
	// MultiModelReorderJoinsLimit indicates the number of joins at which the optimizer should
	// stop attempting to reorder in multi-model processing.
	MultiModelReorderJoinsLimit int
	// RequireExplicitPrimaryKeys indicates whether CREATE TABLE statements should
	// error out if no primary key is provided.
	RequireExplicitPrimaryKeys bool
	// SequenceState gives access to the SQL sequences that have been manipulated
	// by the session.
	SequenceState *SequenceState
	// DataConversion gives access to the data conversion configuration.
	DataConversion DataConversionConfig
	// VectorizeMode indicates which kinds of queries to use vectorized execution
	// engine for.
	VectorizeMode VectorizeExecMode
	// VectorizeRowCountThreshold indicates the row count above which the
	// vectorized execution engine will be used if possible.
	VectorizeRowCountThreshold uint64
	// ForceSavepointRestart overrides the default SAVEPOINT behavior
	// for compatibility with certain ORMs. When this flag is set,
	// the savepoint name will no longer be compared against the magic
	// identifier `kwbase_restart` in order use a restartable
	// transaction.
	ForceSavepointRestart bool
	// DefaultIntSize specifies the size in bits or bytes (preferred)
	// of how a "naked" INT type should be parsed.
	DefaultIntSize int
	// ResultsBufferSize specifies the size at which the pgwire results buffer
	// will self-flush.
	ResultsBufferSize int64
	// AllowPrepareAsOptPlan must be set to allow use of
	//   PREPARE name AS OPT PLAN '...'
	AllowPrepareAsOptPlan bool
	// SaveTablesPrefix indicates that a table should be created with the
	// given prefix for the output of each subexpression in a query. If
	// SaveTablesPrefix is empty, no tables are created.
	SaveTablesPrefix string
	// TempTablesEnabled indicates whether temporary tables can be created or not.
	TempTablesEnabled bool
	// HashShardedIndexesEnabled indicates whether hash sharded indexes can be created.
	HashShardedIndexesEnabled bool
	// ImplicitSelectForUpdate is true if FOR UPDATE locking may be used during
	// the row-fetch phase of mutation statements.
	ImplicitSelectForUpdate bool
	// InsertFastPath is true if the fast path for insert (with VALUES input) may
	// be used.
	InsertFastPath bool
	// Instance indicates the "current" instance for the purpose of resolving names.
	Instance string
	// Tenant indicates the tenant user logged.
	Tenant string
	// Portal indicates the portal user logged.
	Portal               string
	ClusterID            []byte
	ClientIP             string
	CommandLimit         int64
	OutFormats           bool
	TsInsertShortcircuit bool
	// tsinsert_direct special handling error
	TsSupportBatch bool
	// Support GB18030 and GBK
	ClientEncoding           string
	DefaultTxnIsolationLevel int64

	// MaxPushLimitNumber if number of limit shall not exceed it, limit can push to tsScan
	MaxPushLimitNumber int64

	// CanPushSorter if set true, can push sorter to ts engine
	CanPushSorter bool

	// InsideOutRowRatio use for control output rows of groupby in inside out case.
	InsideOutRowRatio float64

	// NeedControlIndideOut is set when statistical information is inaccurate and then
	// need adjust output rows of groupby in inside out case.
	NeedControlIndideOut bool

	// UserDefinedVars stores variables defined by user
	UserDefinedVars map[string]interface{}
}

// DataConversionConfig contains the parameters that influence
// the conversion between SQL data types and strings/byte arrays.
type DataConversionConfig struct {
	// Location indicates the current time zone.
	Location *time.Location

	// BytesEncodeFormat indicates how to encode byte arrays when converting
	// to string.
	BytesEncodeFormat BytesEncodeFormat

	// ExtraFloatDigits indicates the number of digits beyond the
	// standard number to use for float conversions.
	// This must be set to a value between -15 and 3, inclusive.
	ExtraFloatDigits int
	// Support GB18030 and GBK
	ClientEncoding string
}

// GetFloatPrec computes a precision suitable for a call to
// strconv.FormatFloat() or for use with '%.*g' in a printf-like
// function.
func (c *DataConversionConfig) GetFloatPrec() int {
	// The user-settable parameter ExtraFloatDigits indicates the number
	// of digits to be used to format the float value. PostgreSQL
	// combines this with %g.
	// The formula is <type>_DIG + extra_float_digits,
	// where <type> is either FLT (float4) or DBL (float8).

	// Also the value "3" in PostgreSQL is special and meant to mean
	// "all the precision needed to reproduce the float exactly". The Go
	// formatter uses the special value -1 for this and activates a
	// separate path in the formatter. We compare >= 3 here
	// just in case the value is not gated properly in the implementation
	// of SET.
	if c.ExtraFloatDigits >= 3 {
		return -1
	}

	// CockroachDB only implements float8 at this time and Go does not
	// expose DBL_DIG, so we use the standard literal constant for
	// 64bit floats.
	const StdDoubleDigits = 15

	nDigits := StdDoubleDigits + c.ExtraFloatDigits
	if nDigits < 1 {
		// Ensure the value is clamped at 1: printf %g does not allow
		// values lower than 1. PostgreSQL does this too.
		nDigits = 1
	}
	return nDigits
}

// Equals returns true if the two DataConversionConfigs are identical.
func (c *DataConversionConfig) Equals(other *DataConversionConfig) bool {
	if c.BytesEncodeFormat != other.BytesEncodeFormat ||
		c.ExtraFloatDigits != other.ExtraFloatDigits {
		return false
	}
	if c.Location != other.Location && c.Location.String() != other.Location.String() {
		return false
	}
	return true
}

// BytesEncodeFormat controls which format to use for BYTES->STRING
// conversions.
type BytesEncodeFormat int

const (
	// BytesEncodeHex uses the hex format: e'abc\n'::BYTES::STRING -> '\x61626312'.
	// This is the default, for compatibility with PostgreSQL.
	BytesEncodeHex BytesEncodeFormat = iota
	// BytesEncodeEscape uses the escaped format: e'abc\n'::BYTES::STRING -> 'abc\012'.
	BytesEncodeEscape
	// BytesEncodeBase64 uses base64 encoding.
	BytesEncodeBase64
	// BytesEncodeString uses string encoding.
	BytesEncodeString
)

func (f BytesEncodeFormat) String() string {
	switch f {
	case BytesEncodeHex:
		return "hex"
	case BytesEncodeEscape:
		return "escape"
	case BytesEncodeBase64:
		return "base64"
	default:
		return fmt.Sprintf("invalid (%d)", f)
	}
}

// BytesEncodeFormatFromString converts a string into a BytesEncodeFormat.
func BytesEncodeFormatFromString(val string) (_ BytesEncodeFormat, ok bool) {
	switch strings.ToUpper(val) {
	case "HEX":
		return BytesEncodeHex, true
	case "ESCAPE":
		return BytesEncodeEscape, true
	case "BASE64":
		return BytesEncodeBase64, true
	default:
		return -1, false
	}
}

// DistSQLExecMode controls if and when the Executor distributes queries.
// Since 2.1, we run everything through the DistSQL infrastructure,
// and these settings control whether to use a distributed plan, or use a plan
// that only involves local DistSQL processors.
type DistSQLExecMode int64

const (
	// DistSQLOff means that we never distribute queries.
	DistSQLOff DistSQLExecMode = iota
	// DistSQLAuto means that we automatically decide on a case-by-case basis if
	// we distribute queries.
	DistSQLAuto
	// DistSQLOn means that we distribute queries that are supported.
	DistSQLOn
	// DistSQLAlways means that we only distribute; unsupported queries fail.
	DistSQLAlways
)

func (m DistSQLExecMode) String() string {
	switch m {
	case DistSQLOff:
		return "off"
	case DistSQLAuto:
		return "auto"
	case DistSQLOn:
		return "on"
	case DistSQLAlways:
		return "always"
	default:
		return fmt.Sprintf("invalid (%d)", m)
	}
}

// DistSQLExecModeFromString converts a string into a DistSQLExecMode
func DistSQLExecModeFromString(val string) (_ DistSQLExecMode, ok bool) {
	switch strings.ToUpper(val) {
	case "OFF":
		return DistSQLOff, true
	case "AUTO":
		return DistSQLAuto, true
	case "ON":
		return DistSQLOn, true
	case "ALWAYS":
		return DistSQLAlways, true
	default:
		return 0, false
	}
}

// VectorizeExecMode controls if an when the Executor executes queries using the
// columnar execution engine.
// WARNING: When adding a VectorizeExecMode, note that nodes at previous
// versions might interpret the integer value differently. To avoid this, only
// append to the list or bump the minimum required distsql version (maybe also
// take advantage of that to reorder the list as you see fit).
type VectorizeExecMode int64

const (
	// VectorizeOff means that columnar execution is disabled.
	VectorizeOff VectorizeExecMode = iota
	// VectorizeAuto means that that any supported queries that use only
	// streaming operators (i.e. those that do not require any buffering) will
	// be run using the columnar execution. If any part of a query is not
	// supported by the vectorized execution engine, the whole query will fall
	// back to row execution.
	VectorizeAuto
	// VectorizeOn means that any supported queries will be run using the
	// columnar execution.
	VectorizeOn
	// VectorizeExperimentalAlways means that we attempt to vectorize all
	// queries; unsupported queries will fail. Mostly used for testing.
	VectorizeExperimentalAlways
)

func (m VectorizeExecMode) String() string {
	switch m {
	case VectorizeOff:
		return "off"
	case VectorizeAuto:
		return "auto"
	case VectorizeOn:
		return "on"
	case VectorizeExperimentalAlways:
		return "experimental_always"
	default:
		return fmt.Sprintf("invalid (%d)", m)
	}
}

// VectorizeExecModeFromString converts a string into a VectorizeExecMode. False
// is returned if the conversion was unsuccessful.
func VectorizeExecModeFromString(val string) (VectorizeExecMode, bool) {
	var m VectorizeExecMode
	switch strings.ToUpper(val) {
	case "OFF":
		m = VectorizeOff
	case "AUTO":
		m = VectorizeAuto
	case "ON":
		m = VectorizeOn
	case "EXPERIMENTAL_ALWAYS":
		m = VectorizeExperimentalAlways
	default:
		return 0, false
	}
	return m, true
}

// SerialNormalizationMode controls if and when the Executor uses DistSQL.
type SerialNormalizationMode int64

const (
	// SerialUsesRowID means use INT NOT NULL DEFAULT unique_rowid().
	SerialUsesRowID SerialNormalizationMode = iota
	// SerialUsesVirtualSequences means create a virtual sequence and
	// use INT NOT NULL DEFAULT nextval(...).
	SerialUsesVirtualSequences
	// SerialUsesSQLSequences means create a regular SQL sequence and
	// use INT NOT NULL DEFAULT nextval(...).
	SerialUsesSQLSequences
)

func (m SerialNormalizationMode) String() string {
	switch m {
	case SerialUsesRowID:
		return "rowid"
	case SerialUsesVirtualSequences:
		return "virtual_sequence"
	case SerialUsesSQLSequences:
		return "sql_sequence"
	default:
		return fmt.Sprintf("invalid (%d)", m)
	}
}

// SerialNormalizationModeFromString converts a string into a SerialNormalizationMode
func SerialNormalizationModeFromString(val string) (_ SerialNormalizationMode, ok bool) {
	switch strings.ToUpper(val) {
	case "ROWID":
		return SerialUsesRowID, true
	case "VIRTUAL_SEQUENCE":
		return SerialUsesVirtualSequences, true
	case "SQL_SEQUENCE":
		return SerialUsesSQLSequences, true
	default:
		return 0, false
	}
}
