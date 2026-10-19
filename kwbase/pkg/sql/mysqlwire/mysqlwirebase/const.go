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

// Package mysqlwirebase contains the wire-format primitives of the MySQL
// client/server protocol: packet framing, the connection phase, the
// authentication plugins, and text-protocol result sets.
//
// The package deliberately depends on the Go standard library only. The
// protocol is intricate and easy to get subtly wrong, so keeping it free of
// KWDB dependencies lets it be exercised by unit tests that need neither a
// running server nor the cgo-linked storage engine. Everything that needs to
// know about KWDB (sessions, SQL execution, users) lives in the parent
// mysqlwire package.
//
// Wire-format references are given as section names from the MySQL Internals
// Manual, "Client/Server Protocol".
package mysqlwirebase

// ProtocolVersion is the version byte leading the initial handshake packet.
// 10 is the only version modern clients understand.
const ProtocolVersion = 10

// Capability flags negotiated between client and server during the connection
// phase. See "Capability Flags".
const (
	ClientLongPassword               uint32 = 0x00000001
	ClientFoundRows                  uint32 = 0x00000002
	ClientLongFlag                   uint32 = 0x00000004
	ClientConnectWithDB              uint32 = 0x00000008
	ClientNoSchema                   uint32 = 0x00000010
	ClientCompress                   uint32 = 0x00000020
	ClientODBC                       uint32 = 0x00000040
	ClientLocalFiles                 uint32 = 0x00000080
	ClientIgnoreSpace                uint32 = 0x00000100
	ClientProtocol41                 uint32 = 0x00000200
	ClientInteractive                uint32 = 0x00000400
	ClientSSL                        uint32 = 0x00000800
	ClientIgnoreSigpipe              uint32 = 0x00001000
	ClientTransactions               uint32 = 0x00002000
	ClientReserved                   uint32 = 0x00004000
	ClientSecureConnection           uint32 = 0x00008000
	ClientMultiStatements            uint32 = 0x00010000
	ClientMultiResults               uint32 = 0x00020000
	ClientPSMultiResults             uint32 = 0x00040000
	ClientPluginAuth                 uint32 = 0x00080000
	ClientConnectAttrs               uint32 = 0x00100000
	ClientPluginAuthLenEncClientData uint32 = 0x00200000
	ClientCanHandleExpiredPasswords  uint32 = 0x00400000
	ClientSessionTrack               uint32 = 0x00800000
	ClientDeprecateEOF               uint32 = 0x01000000
	ClientOptionalResultsetMetadata  uint32 = 0x02000000
	ClientZstdCompressionAlgorithm   uint32 = 0x04000000
	ClientQueryAttributes            uint32 = 0x08000000
	ClientMultiFactorAuthentication  uint32 = 0x10000000
	ClientCapabilityExtension        uint32 = 0x20000000
	ClientSSLVerifyServerCert        uint32 = 0x40000000
	ClientRememberOptions            uint32 = 0x80000000
)

// ServerCapabilities is the capability set KWDB advertises.
//
// Notable omissions: CLIENT_COMPRESS (packet compression is not implemented,
// and clients only use it when the server offers it) and CLIENT_LOCAL_FILES
// (LOAD DATA LOCAL INFILE lets the server ask the client for arbitrary local
// files, which is a client-side attack surface we have no reason to open).
//
// CLIENT_DEPRECATE_EOF is advertised so that modern clients accept OK packets
// in place of EOF packets; the encoders below honor whichever the client
// finally negotiates.
const ServerCapabilities = ClientLongPassword |
	ClientFoundRows |
	ClientLongFlag |
	ClientConnectWithDB |
	ClientProtocol41 |
	ClientTransactions |
	ClientSecureConnection |
	ClientMultiStatements |
	ClientMultiResults |
	ClientPSMultiResults |
	ClientPluginAuth |
	ClientConnectAttrs |
	ClientPluginAuthLenEncClientData |
	ClientSessionTrack |
	ClientDeprecateEOF

// Status flags reported in OK and EOF packets. See "Status Flags".
const (
	ServerStatusInTrans            uint16 = 0x0001
	ServerStatusAutocommit         uint16 = 0x0002
	ServerMoreResultsExists        uint16 = 0x0008
	ServerStatusNoGoodIndexUsed    uint16 = 0x0010
	ServerStatusNoIndexUsed        uint16 = 0x0020
	ServerStatusCursorExists       uint16 = 0x0040
	ServerStatusLastRowSent        uint16 = 0x0080
	ServerStatusDBDropped          uint16 = 0x0100
	ServerStatusNoBackslashEscapes uint16 = 0x0200
	ServerStatusMetadataChanged    uint16 = 0x0400
	ServerQueryWasSlow             uint16 = 0x0800
	ServerPSOutParams              uint16 = 0x1000
	ServerStatusInTransReadonly    uint16 = 0x2000
	ServerSessionStateChanged      uint16 = 0x4000
)

// Command is the first byte of a packet in the command phase.
type Command byte

// Commands KWDB recognizes. See "Text Protocol" and "Prepared Statements".
const (
	ComSleep            Command = 0x00
	ComQuit             Command = 0x01
	ComInitDB           Command = 0x02
	ComQuery            Command = 0x03
	ComFieldList        Command = 0x04
	ComCreateDB         Command = 0x05
	ComDropDB           Command = 0x06
	ComRefresh          Command = 0x07
	ComShutdown         Command = 0x08
	ComStatistics       Command = 0x09
	ComProcessInfo      Command = 0x0a
	ComConnect          Command = 0x0b
	ComProcessKill      Command = 0x0c
	ComDebug            Command = 0x0d
	ComPing             Command = 0x0e
	ComTime             Command = 0x0f
	ComDelayedInsert    Command = 0x10
	ComChangeUser       Command = 0x11
	ComBinlogDump       Command = 0x12
	ComTableDump        Command = 0x13
	ComConnectOut       Command = 0x14
	ComRegisterSlave    Command = 0x15
	ComStmtPrepare      Command = 0x16
	ComStmtExecute      Command = 0x17
	ComStmtSendLongData Command = 0x18
	ComStmtClose        Command = 0x19
	ComStmtReset        Command = 0x1a
	ComSetOption        Command = 0x1b
	ComStmtFetch        Command = 0x1c
	ComDaemon           Command = 0x1d
	ComBinlogDumpGTID   Command = 0x1e
	ComResetConnection  Command = 0x1f
)

// String implements fmt.Stringer, so that command bytes render readably in
// logs and error messages.
func (c Command) String() string {
	switch c {
	case ComQuit:
		return "COM_QUIT"
	case ComInitDB:
		return "COM_INIT_DB"
	case ComQuery:
		return "COM_QUERY"
	case ComFieldList:
		return "COM_FIELD_LIST"
	case ComStatistics:
		return "COM_STATISTICS"
	case ComProcessInfo:
		return "COM_PROCESS_INFO"
	case ComPing:
		return "COM_PING"
	case ComChangeUser:
		return "COM_CHANGE_USER"
	case ComStmtPrepare:
		return "COM_STMT_PREPARE"
	case ComStmtExecute:
		return "COM_STMT_EXECUTE"
	case ComStmtSendLongData:
		return "COM_STMT_SEND_LONG_DATA"
	case ComStmtClose:
		return "COM_STMT_CLOSE"
	case ComStmtReset:
		return "COM_STMT_RESET"
	case ComSetOption:
		return "COM_SET_OPTION"
	case ComStmtFetch:
		return "COM_STMT_FETCH"
	case ComResetConnection:
		return "COM_RESET_CONNECTION"
	default:
		return "COM_UNKNOWN"
	}
}

// Leading bytes distinguishing generic packet kinds.
const (
	// OKPacketHeader leads an OK packet.
	OKPacketHeader byte = 0x00
	// EOFPacketHeader leads an EOF packet, and also leads an OK packet when
	// CLIENT_DEPRECATE_EOF has been negotiated.
	EOFPacketHeader byte = 0xfe
	// ErrPacketHeader leads an ERR packet.
	ErrPacketHeader byte = 0xff
	// AuthMoreDataHeader leads an AuthMoreData packet sent during the
	// authentication exchange.
	AuthMoreDataHeader byte = 0x01
	// AuthSwitchRequestHeader leads an AuthSwitchRequest packet.
	AuthSwitchRequestHeader byte = 0xfe
	// NullColumnHeader stands in for a SQL NULL value in a text-protocol row.
	NullColumnHeader byte = 0xfb
)

// Column types, as encoded in ColumnDefinition41. See "Column Type".
const (
	TypeDecimal    byte = 0x00
	TypeTiny       byte = 0x01
	TypeShort      byte = 0x02
	TypeLong       byte = 0x03
	TypeFloat      byte = 0x04
	TypeDouble     byte = 0x05
	TypeNull       byte = 0x06
	TypeTimestamp  byte = 0x07
	TypeLongLong   byte = 0x08
	TypeInt24      byte = 0x09
	TypeDate       byte = 0x0a
	TypeTime       byte = 0x0b
	TypeDatetime   byte = 0x0c
	TypeYear       byte = 0x0d
	TypeNewDate    byte = 0x0e
	TypeVarchar    byte = 0x0f
	TypeBit        byte = 0x10
	TypeJSON       byte = 0xf5
	TypeNewDecimal byte = 0xf6
	TypeEnum       byte = 0xf7
	TypeSet        byte = 0xf8
	TypeTinyBlob   byte = 0xf9
	TypeMediumBlob byte = 0xfa
	TypeLongBlob   byte = 0xfb
	TypeBlob       byte = 0xfc
	TypeVarString  byte = 0xfd
	TypeString     byte = 0xfe
	TypeGeometry   byte = 0xff
)

// Column definition flags. See "Column Definition Flags".
const (
	ColumnFlagNotNull     uint16 = 0x0001
	ColumnFlagPrimaryKey  uint16 = 0x0002
	ColumnFlagUniqueKey   uint16 = 0x0004
	ColumnFlagMultiKey    uint16 = 0x0008
	ColumnFlagBlob        uint16 = 0x0010
	ColumnFlagUnsigned    uint16 = 0x0020
	ColumnFlagZerofill    uint16 = 0x0040
	ColumnFlagBinary      uint16 = 0x0080
	ColumnFlagEnum        uint16 = 0x0100
	ColumnFlagAutoIncr    uint16 = 0x0200
	ColumnFlagTimestamp   uint16 = 0x0400
	ColumnFlagSet         uint16 = 0x0800
	ColumnFlagNoDefault   uint16 = 0x1000
	ColumnFlagOnUpdateNow uint16 = 0x2000
	ColumnFlagNum         uint16 = 0x8000
)

// Character set / collation ids used in the handshake and in column
// definitions.
const (
	// CollationUTF8MB4GeneralCI is utf8mb4_general_ci (id 45). KWDB stores and
	// returns UTF-8, so every string column is reported with this collation.
	CollationUTF8MB4GeneralCI uint16 = 45
	// CollationBinary is the "binary" collation (id 63), used for BLOB-like
	// columns so that clients do not try to reinterpret the bytes.
	CollationBinary uint16 = 63
)

// Auth plugin names. These are the values exchanged in the handshake, in
// AuthSwitchRequest packets, and in the client's handshake response.
const (
	// AuthNativePassword is mysql_native_password: a SHA1-based
	// challenge-response that requires the server to hold SHA1(SHA1(password)).
	AuthNativePassword = "mysql_native_password"
	// AuthCachingSHA2Password is caching_sha2_password, MySQL 8.0's default. It
	// has a SHA256-based fast path requiring SHA256(SHA256(password)), and a
	// "full authentication" fallback that transports the cleartext password
	// over TLS or RSA.
	AuthCachingSHA2Password = "caching_sha2_password"
	// AuthSHA256Password is sha256_password, which always transports the
	// cleartext password over TLS or RSA.
	AuthSHA256Password = "sha256_password"
)

// Sentinel bytes exchanged during the caching_sha2_password and
// sha256_password conversations.
const (
	// CachingSHA2FastAuthSuccess tells the client its fast-path scramble
	// matched; an OK packet follows.
	CachingSHA2FastAuthSuccess byte = 0x03
	// CachingSHA2PerformFullAuth tells the client the server could not verify
	// the fast-path scramble and needs the cleartext password.
	CachingSHA2PerformFullAuth byte = 0x04
	// CachingSHA2RequestPublicKey is sent by the client to ask for the server's
	// RSA public key during caching_sha2_password full authentication.
	CachingSHA2RequestPublicKey byte = 0x02
	// SHA256RequestPublicKey is the equivalent request byte for
	// sha256_password.
	SHA256RequestPublicKey byte = 0x01
)

// ScrambleLen is the length of the nonce the server generates for the
// authentication exchange. The wire format reserves 21 bytes for it (8 in
// part 1, 13 in part 2), but the last byte of part 2 is a NUL terminator, so
// only 20 bytes are significant.
const ScrambleLen = 20

// MaxPayloadLen is the largest payload a single MySQL packet can carry. A
// payload of exactly this size signals that the logical packet continues in
// the next physical packet.
const MaxPayloadLen = 1<<24 - 1
