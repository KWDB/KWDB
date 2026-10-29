// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package mysqlwirebase

// MySQL reports failures as a numeric error code plus a 5-character SQLSTATE.
// Clients and, more importantly, application frameworks branch on both: an
// ORM deciding whether to retry a transaction or surface a constraint
// violation reads the code, not the message. KWDB's own errors carry
// PostgreSQL error codes, so the mysqlwire package has to translate; these
// constants are the vocabulary it translates into.
//
// SQLSTATE values are the ANSI ones MySQL uses, so most map across from
// PostgreSQL unchanged. The numeric codes do not, and are listed here with the
// names from MySQL's include/mysqld_error.h.

// SQLSTATE values used by this package.
const (
	// SQLStateGeneralError is MySQL's catch-all, used whenever a KWDB error
	// carries no PostgreSQL code that maps to something more specific.
	SQLStateGeneralError = "HY000"
	// SQLStateAccessDenied covers authentication and authorization failures.
	SQLStateAccessDenied = "28000"
	// SQLStateSyntaxError covers parse and semantic analysis failures.
	SQLStateSyntaxError = "42000"
	// SQLStateUnknownDatabase is reported when a named database is absent.
	SQLStateUnknownDatabase = "42000"
	// SQLStateConnectionFailure marks a connection that is being torn down.
	SQLStateConnectionFailure = "08S01"
	// SQLStateFeatureDisabled marks a protocol feature KWDB declines to serve.
	SQLStateFeatureDisabled = "0A000"
)

// MySQL error codes returned by this package.
const (
	// ErrUnknownError is MySQL's generic code, paired with SQLStateGeneralError.
	ErrUnknownError uint16 = 1105
	// ErrAccessDenied is ER_ACCESS_DENIED_ERROR, the response to a failed
	// password check.
	ErrAccessDenied uint16 = 1045
	// ErrDBAccessDenied is ER_DBACCESS_DENIED_ERROR.
	ErrDBAccessDenied uint16 = 1044
	// ErrSpecificAccessDenied is ER_SPECIFIC_ACCESS_DENIED_ERROR, reported when
	// an authenticated user lacks the privilege for a statement. This is
	// distinct from ErrAccessDenied, which is about who the caller is rather
	// than what they may do.
	ErrSpecificAccessDenied uint16 = 1227
	// ErrBadDB is ER_BAD_DB_ERROR, reported when COM_INIT_DB names a database
	// that does not exist.
	ErrBadDB uint16 = 1049
	// ErrParse is ER_PARSE_ERROR.
	ErrParse uint16 = 1064
	// ErrNotSupportedYet is ER_NOT_SUPPORTED_YET.
	ErrNotSupportedYet uint16 = 1235
	// ErrUnknownCommand is ER_UNKNOWN_COM_ERROR, the response to a command byte
	// KWDB does not implement.
	ErrUnknownCommand uint16 = 1047
	// ErrServerShutdown is ER_SERVER_SHUTDOWN, sent to clients when the node is
	// draining.
	ErrServerShutdown uint16 = 1053
	// ErrHandshake is ER_HANDSHAKE_ERROR, reported when the connection phase
	// itself fails rather than the credentials.
	ErrHandshake uint16 = 1043
	// ErrNetPacketTooLarge is ER_NET_PACKET_TOO_LARGE.
	ErrNetPacketTooLarge uint16 = 1153
	// ErrAborting is ER_ABORTING_CONNECTION.
	ErrAborting uint16 = 1152
	// ErrUnknownStmtHandler is ER_UNKNOWN_STMT_HANDLER, reported when a
	// COM_STMT_* command names a statement handle that was never issued or has
	// already been closed.
	ErrUnknownStmtHandler uint16 = 1243
	// ErrMalformedPacket is ER_MALFORMED_PACKET, reported when a command's
	// payload cannot be parsed.
	ErrMalformedPacket uint16 = 1835
)

// MySQL error codes for statement failures. Frameworks branch on these to tell
// a constraint violation from a retryable conflict from a schema mistake, so
// the classes below are the ones worth translating precisely; anything else
// reaches the client as ErrUnknownError with its SQLSTATE intact.
const (
	// ErrDupEntry is ER_DUP_ENTRY, a unique constraint violation. ORMs match on
	// this code to turn an insert into an update.
	ErrDupEntry uint16 = 1062
	// ErrBadNull is ER_BAD_NULL_ERROR, a NOT NULL violation.
	ErrBadNull uint16 = 1048
	// ErrNoReferencedRow is ER_NO_REFERENCED_ROW: a child row has no parent.
	ErrNoReferencedRow uint16 = 1216
	// ErrRowIsReferenced is ER_ROW_IS_REFERENCED: a parent row still has
	// children.
	ErrRowIsReferenced uint16 = 1217
	// ErrNoSuchTable is ER_NO_SUCH_TABLE.
	ErrNoSuchTable uint16 = 1146
	// ErrBadField is ER_BAD_FIELD_ERROR, an unknown column.
	ErrBadField uint16 = 1054
	// ErrTableExists is ER_TABLE_EXISTS_ERROR.
	ErrTableExists uint16 = 1050
	// ErrDBCreateExists is ER_DB_CREATE_EXISTS.
	ErrDBCreateExists uint16 = 1007
	// ErrDupFieldName is ER_DUP_FIELDNAME.
	ErrDupFieldName uint16 = 1060
	// ErrSPDoesNotExist is ER_SP_DOES_NOT_EXIST, the closest MySQL code for a
	// call to a function that does not exist.
	ErrSPDoesNotExist uint16 = 1305
	// ErrDataTooLong is ER_DATA_TOO_LONG.
	ErrDataTooLong uint16 = 1406
	// ErrWarnDataOutOfRange is ER_WARN_DATA_OUT_OF_RANGE.
	ErrWarnDataOutOfRange uint16 = 1264
	// ErrTruncatedWrongValue is ER_TRUNCATED_WRONG_VALUE, reported when a value
	// cannot be parsed as the target type.
	ErrTruncatedWrongValue uint16 = 1292
	// ErrDivisionByZero is ER_DIVISION_BY_ZERO.
	ErrDivisionByZero uint16 = 1365
	// ErrLockDeadlock is ER_LOCK_DEADLOCK. Clients retry the transaction on
	// this code, which is what KWDB's serialization failures ask for too.
	ErrLockDeadlock uint16 = 1213
	// ErrLockWaitTimeout is ER_LOCK_WAIT_TIMEOUT.
	ErrLockWaitTimeout uint16 = 1205
	// ErrQueryInterrupted is ER_QUERY_INTERRUPTED, reported for a canceled
	// statement.
	ErrQueryInterrupted uint16 = 1317
	// ErrConCountError is ER_CON_COUNT_ERROR, too many connections.
	ErrConCountError uint16 = 1040
	// ErrOutOfResources is ER_OUT_OF_RESOURCES.
	ErrOutOfResources uint16 = 1041
	// ErrUnknownStmtName is ER_UNKNOWN_STMT_HANDLER reused for a SQL-level
	// prepared statement name the server does not know.
	ErrUnknownStmtName = ErrUnknownStmtHandler
)

// Error is a MySQL-shaped error: the code and SQLSTATE a client will see.
type Error struct {
	Code     uint16
	SQLState string
	Message  string
}

// Error implements the error interface.
func (e *Error) Error() string { return e.Message }

// NewError builds an Error.
func NewError(code uint16, sqlState, message string) *Error {
	return &Error{Code: code, SQLState: sqlState, Message: message}
}

// NewAccessDeniedError builds the error a client sees when its credentials are
// rejected.
//
// The message deliberately mirrors MySQL's wording, including the fact that it
// does not distinguish "no such user" from "wrong password": telling the two
// apart would let an unauthenticated caller enumerate accounts.
func NewAccessDeniedError(user, host string, usedPassword bool) *Error {
	yesNo := "NO"
	if usedPassword {
		yesNo = "YES"
	}
	return &Error{
		Code:     ErrAccessDenied,
		SQLState: SQLStateAccessDenied,
		Message: "Access denied for user '" + user + "'@'" + host +
			"' (using password: " + yesNo + ")",
	}
}
