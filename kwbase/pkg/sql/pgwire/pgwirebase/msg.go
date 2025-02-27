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

package pgwirebase

import "math"

//ClientMessageType represents a client pgwire message.
//go:generate stringer -type=ClientMessageType
type ClientMessageType byte

//ServerMessageType represents a server pgwire message.
//go:generate stringer -type=ServerMessageType
type ServerMessageType byte

// http://www.postgresql.org/docs/9.4/static/protocol-message-formats.html
const (
	ClientMsgBind        ClientMessageType = 'B'
	ClientMsgClose       ClientMessageType = 'C'
	ClientMsgCopyData    ClientMessageType = 'd'
	ClientMsgCopyDone    ClientMessageType = 'c'
	ClientMsgCopyFail    ClientMessageType = 'f'
	ClientMsgDescribe    ClientMessageType = 'D'
	ClientMsgExecute     ClientMessageType = 'E'
	ClientMsgFlush       ClientMessageType = 'H'
	ClientMsgParse       ClientMessageType = 'P'
	ClientMsgPassword    ClientMessageType = 'p'
	ClientMsgSimpleQuery ClientMessageType = 'Q'
	ClientMsgSync        ClientMessageType = 'S'
	ClientMsgTerminate   ClientMessageType = 'X'
	ClientMsgMetadata    ClientMessageType = 'm'

	ServerMsgAuth                 ServerMessageType = 'R'
	ServerMsgBindComplete         ServerMessageType = '2'
	ServerMsgCommandComplete      ServerMessageType = 'C'
	ServerMsgCloseComplete        ServerMessageType = '3'
	ServerMsgCopyInResponse       ServerMessageType = 'G'
	ServerMsgDataRow              ServerMessageType = 'D'
	ServerMsgEmptyQuery           ServerMessageType = 'I'
	ServerMsgErrorResponse        ServerMessageType = 'E'
	ServerMsgNoticeResponse       ServerMessageType = 'N'
	ServerMsgNoData               ServerMessageType = 'n'
	ServerMsgParameterDescription ServerMessageType = 't'
	ServerMsgParameterStatus      ServerMessageType = 'S'
	ServerMsgParseComplete        ServerMessageType = '1'
	ServerMsgPortalSuspended      ServerMessageType = 's'
	ServerMsgReady                ServerMessageType = 'Z'
	ServerMsgRowDescription       ServerMessageType = 'T'
	ServerMsgAgentParameter       ServerMessageType = 'e'
	ServerMsgMetadata             ServerMessageType = 'm'
	ServerMsgConnection           ServerMessageType = 'c'
)

// ServerErrFieldType represents the error fields.
//go:generate stringer -type=ServerErrFieldType
type ServerErrFieldType byte

// http://www.postgresql.org/docs/current/static/protocol-error-fields.html
const (
	ServerErrFieldSeverity    ServerErrFieldType = 'S'
	ServerErrFieldSQLState    ServerErrFieldType = 'C'
	ServerErrFieldMsgPrimary  ServerErrFieldType = 'M'
	ServerErrFileldDetail     ServerErrFieldType = 'D'
	ServerErrFileldHint       ServerErrFieldType = 'H'
	ServerErrFieldSrcFile     ServerErrFieldType = 'F'
	ServerErrFieldSrcLine     ServerErrFieldType = 'L'
	ServerErrFieldSrcFunction ServerErrFieldType = 'R'
)

// PrepareType represents a subtype for prepare messages.
//go:generate stringer -type=PrepareType
type PrepareType byte

const (
	// PrepareStatement represents a prepared statement.
	PrepareStatement PrepareType = 'S'
	// PreparePortal represents a portal.
	PreparePortal PrepareType = 'P'
)

// MaxPreparedStatementArgs is the maximum number of arguments a prepared
// statement can have when prepared via the Postgres wire protocol. This is not
// documented by Postgres, but is a consequence of the fact that a 16-bit
// integer in the wire format is used to indicate the number of values to bind
// during prepared statement execution.
const MaxPreparedStatementArgs = math.MaxUint16
