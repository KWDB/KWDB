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

package server

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

// RestfulResponseCodeSuccess indicates success
const RestfulResponseCodeSuccess = 0

// RestfulResponseCodeFail indicates fail
const RestfulResponseCodeFail = -1

// DDlIncluded use for ddl.
var DDlIncluded = []string{
	"create",
	"drop",
	"delete",
	"use",
	"alter",
	"update",
	"grant",
	"revoke"}

// queryKeyWords to be filtered out when query
var queryKeyWords = []string{
	"insert",
	"set",
	"drop",
	"alter",
	"delete",
	"update",
}

var transTypeToLength = map[string]int64{
	"BOOL":        1,
	"INT2":        2,
	"INT4":        4,
	"INT8":        8,
	"INT":         8,
	"_INT8":       8,
	"FLOAT4":      4,
	"FLOAT8":      8,
	"FLOAT":       8,
	"TIMESTAMP":   8,
	"TIMESTAMPTZ": 8,
	"INTERVAL":    8,
	"BIT":         8,
	"VARBIT":      8,
	"DATE":        8,
	"TIME":        8,
	"JSONB":       8,
	"INET":        8,
	"UUID":        8,
	"GEOMETRY":    9223372036854775807,
	"STRING":      9223372036854775807,
	"STRING[]":    9223372036854775807,
	"TEXT":        9223372036854775807,
	"CHAR":        9223372036854775807,
	"NCHAR":       9223372036854775807,
	"VARCHAR":     9223372036854775807,
	"NVARCHAR":    9223372036854775807,
	"BYTES":       9223372036854775807,
	"VARBYTES":    9223372036854775807,
	"_TEXT":       9223372036854775807,
	"NAME":        9223372036854775807}

var isBool = map[string]bool{
	"true":  true,
	"t":     true,
	"T":     true,
	"True":  true,
	"TRUE":  true,
	"false": false,
	"f":     false,
	"F":     false,
	"False": false,
	"FALSE": false,
}

var (
	// Regular expression matching for numbers
	isFloat        = regexp.MustCompile(`^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$`)
	isNumericWithU = regexp.MustCompile(`^[-+]?[0-9]+u$`)
	isNumericWithI = regexp.MustCompile(`^[-+]?[0-9]+i$`)
	// error indicating that the table does not exist
	reRelationNotExist, _ = regexp.Compile(`relation .* does not exist$`)
	// error indicating that waiting for success
	reWaitForSuccess, _ = regexp.Compile(`.* Please wait for success$`)
	// indicate errors that already exist in the table
	reRelationAlreadyExists, _ = regexp.Compile(`relation .* already exists$`)
	// txn retriable error
	retriableErr, _ = regexp.Compile(`TransactionRetryWithProtoRefreshError: .*`)
)

const (
	//insert_type_str = "INSERT INTO"
	insertTypeStrLowercase = "insert into"
	//ddl_exclude_str = "SHOW CREATE"
	ddlExcludeStrLowercase = "show create"
	//without schema
	insertWithoutSchema = "insert without schema into"
	// maxtimes of retry
	maxRetries = 10
	// varchar length
	varcharlen = 254
)

// MetricData is used for opentsdb json
type MetricData struct {
	Metric    string                 `json:"metric"`
	Timestamp *int64                 `json:"timestamp"`
	Value     *float64               `json:"value"`
	Tags      map[string]interface{} `json:"tags"`
}

type colMetaInfo struct {
	Name   string
	Type   string
	Length int64
}

// RestfulUser provides login user
type RestfulUser struct {
	UserName  string
	LoginTime int64
}

// restfulServer provides a RESTful HTTP API to administration of the kwbase cluster
type restfulServer struct {
	server        *Server
	connPool      *restfulConnPool
	authorization string
	ifByLogin     bool
}

// restful connection, store connectonHandler and session info
type restfulConn struct {
	h             sql.ConnectionHandler
	res           chan []sql.RestfulRes
	sessionID     string
	username      string
	lastLoginTime int64
	isAdmin       bool
	isStrTimezone bool
}

// restful connection pool, store connection cache and lifetime info
type restfulConnPool struct {
	connCache   map[string]*restfulConn
	maxLifeTime int64
}

// SQLRestfulTimeOut maximum overdue time
var SQLRestfulTimeOut = settings.RegisterPublicIntSetting(
	"server.rest.timeout",
	"time out for restful api(in minutes)",
	60,
)

// SQLRestfulTimeZone information of timezone
var SQLRestfulTimeZone = settings.RegisterValidatedIntSetting(
	"server.restful_service.default_request_timezone",
	"set time zone for restful api",
	0,
	func(v int64) error {
		if v < -12 || v > 14 {
			return pgerror.Newf(pgcode.InvalidParameterValue,
				"server.restful_service.default_request_timezone must be set between -12 and 14")
		}
		return nil
	},
)

// loginResponseSuccess is use for return login success
type loginResponseSuccess struct {
	Code  int    `json:"code"`
	Token string `json:"token"`
}

type baseResponse struct {
	Code int     `json:"code"`
	Desc string  `json:"desc"`
	Time float64 `json:"time"`
}

type ddlResponse struct {
	*baseResponse
}

type insertResponse struct {
	*baseResponse
	Notice string `json:"notice"`
	Rows   int64  `json:"rows"`
}

type teleInsertResponse struct {
	*baseResponse
	Rows int64 `json:"rows"`
}

type queryResponse struct {
	*baseResponse
	ColumnMeta []colMetaInfo `json:"column_meta"`
	Data       [][]string    `json:"data"`
	Rows       int           `json:"rows"`
}

// loginResponseFail returns login fail
type showAllSuccess struct {
	Code  int           `json:"code"`
	Conns []sessionInfo `json:"conns"`
}

// sessionInfo shows seesion infos
type sessionInfo struct {
	Connid         string
	Username       string
	Token          string
	MaxLifeTime    int64
	LastLoginTime  string
	ExpirationTime string
}

type resultToken struct {
	Code int    `json:"code"`
	Desc string `json:"desc"`
}

func (col colMetaInfo) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`["%s", "%s", %d]`, col.Name, col.Type, col.Length)), nil
}

func (inStr insertResponse) MarshalJSON() ([]byte, error) {
	results := strings.Split(inStr.Desc, ",")
	resultStr := "["
	for _, result := range results {
		resultStr = resultStr + fmt.Sprintf(`"%s",`, result)
	}
	// erase the redundant symbols.
	resultStr = strings.TrimRight(resultStr, ",")
	resultStr = resultStr + "]"
	inStr.Desc = resultStr

	if "" == inStr.Notice {
		inStr.Notice = fmt.Sprintf(`null`)
	} else {
		notices := strings.Split(inStr.Notice, ",")
		noticeStr := "["
		for _, nResult := range notices {
			noticeStr = noticeStr + fmt.Sprintf(`%s,`, nResult)
		}
		// erase the redundant symbols.
		noticeStr = strings.TrimRight(noticeStr, ",")
		noticeStr = noticeStr + "]"
		inStr.Notice = noticeStr
	}

	str := fmt.Sprintf(`{"code":%d,"desc":%s,"rows":%d,"notice":%s,"time":%f}`,
		inStr.Code,
		inStr.Desc,
		inStr.Rows,
		inStr.Notice,
		inStr.Time)

	return []byte(str), nil
}

// splitStringQuotes handles special characters that exist for string splitting
func splitStringQuotes(data string, separator rune) []string {
	var result []string
	var start int
	inQuotes := false
	start = 0

	for i, char := range data {
		switch char {
		case '"':
			inQuotes = !inQuotes
		case separator:
			if inQuotes {
				// do not process separator inside quotes
				continue
			}
			// outside of quotes, extract substring and add to result
			if start < i {
				result = append(result, data[start:i])
			}
			// update starting position
			start = i + 1
		}
	}

	// add the last field
	if start < len(data) {
		result = append(result, data[start:])
	}
	return result
}

func (ddlStr ddlResponse) MarshalJSON() ([]byte, error) {
	results := strings.Split(ddlStr.Desc, ",")
	resultStr := "["
	for _, result := range results {
		resultStr = resultStr + fmt.Sprintf(`"%s",`, result)
	}
	// erase the redundant symbols.
	resultStr = strings.TrimRight(resultStr, ",")
	resultStr = resultStr + "]"
	ddlStr.Desc = resultStr
	return []byte(fmt.Sprintf(`{"code":%d,"desc":%s,"time":%f}`,
		ddlStr.Code,
		ddlStr.Desc,
		ddlStr.Time)), nil
}

// newRestfulServer allocates and returns a new REST server for Restful APIs
func newRestfulServer(s *Server) *restfulServer {
	connPool := restfulConnPool{
		connCache:   make(map[string]*restfulConn),
		maxLifeTime: SQLRestfulTimeOut.Get(&s.cfg.Settings.SV) * 60,
	}
	server := &restfulServer{server: s, connPool: &connPool}
	return server
}

func ifContainsType(target []string, src string) bool {
	for _, t := range target {
		re := regexp.MustCompile(`\b` + t + `\b`)
		if re.MatchString(src) {
			return true
		}
	}
	return false
}

// handleLogin handles authentication when login
func (s *restfulServer) handleLogin(w http.ResponseWriter, r *http.Request) {
	desc := "success"
	// the same rule of td.
	code := RestfulResponseCodeSuccess
	token := ""
	var err error
	// Extract the authentication info from request header
	ctx := r.Context()

	// db is illegal.
	// get dbname by context.
	paraDbName := r.FormValue("db")
	// get dbname by path.
	if paraDbName != "" {
		desc := "wrong db parameter for login."
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}

	// check if it is GET.
	if r.Method != "GET" {
		desc := "support only GET method."
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}
	// Read the request body
	_, err = ioutil.ReadAll(r.Body)
	if err != nil {
		desc := "body err:" + err.Error()
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}
	var username, password string
	// case 1: -H "Authorization:Basic d3k5OTk6MTIzNDU2Nzg5"
	usr, pass, err := s.getUserWIthPass(r)
	if err != nil {
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, err.Error())
		return
	}
	username, password = usr, pass

	// Call the verifySession/verifyPassword function from authentication.go
	valid, expired, err := s.server.authentication.verifyPassword(ctx, username, password)
	if err != nil {
		desc := "auth err:" + err.Error()
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}

	if expired {
		desc := "the password for user has expired."
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}

	if !valid {
		desc = "the provided username and password did not match any credentials on the server."
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}
	tNow := timeutil.Now().Unix()
	token, err = s.generateKey(username, tNow)
	if err != nil {
		desc = "Failed to encode struct" + err.Error()
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}

	if _, ok := s.connPool.connCache[token]; !ok {
		connHandler, res, err := s.server.PGServer().SQLServer.NewConnectionHandler(ctx, username)
		if err != nil {
			desc = "new connection handler error: " + err.Error()
			s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
			return
		}

		conn, err := s.newRestfulConn(ctx, connHandler, res, username, tNow)
		if err != nil {
			desc = "new connection error: " + err.Error()
			s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
			return
		}
		s.connPool.connCache[token] = conn

		log.Infof(ctx, "session %s has established", conn.sessionID)
	}

	responseSuccess := loginResponseSuccess{code, token}
	s.sendJSONResponse(ctx, w, code, responseSuccess, desc)

	// critical: must clean the basic field.
	s.ifByLogin = false
	s.authorization = ""
}

func (s *restfulServer) newRestfulConn(
	ctx context.Context,
	h sql.ConnectionHandler,
	res chan []sql.RestfulRes,
	userName string,
	tNow int64,
) (*restfulConn, error) {
	sessionID, err := generateSessionID()
	if err != nil {
		return nil, err
	}
	role, err := s.isAdminRole(ctx, userName)
	if err != nil {
		return nil, err
	}
	conn := restfulConn{
		h:             h,
		res:           res,
		sessionID:     sessionID,
		username:      userName,
		lastLoginTime: tNow,
		isAdmin:       role,
	}
	return &conn, nil
}

// checkUser checks user information
func (s *restfulServer) checkUser(ctx context.Context, username string, password string) error {
	// Call the verifySession/verifyPassword function from authentication.go
	valid, expired, err := s.server.authentication.verifyPassword(ctx, username, password)
	if err != nil {
		desc := "auth err:" + err.Error()
		return fmt.Errorf(desc)
	}

	if expired {
		desc := "the password for user has expired."
		return fmt.Errorf(desc)
	}

	if !valid {
		desc := "the provided username and password did not match any credentials on the server."
		return fmt.Errorf(desc)
	}
	return nil
}

func (s *restfulServer) getSQLFromReqBody(r *http.Request) (string, error) {
	body, err := ioutil.ReadAll(r.Body)

	if err != nil {
		return "", err
	}
	sql := string(body)
	return sql, nil
}

// handleDDL handles DDL SQL interface
func (s *restfulServer) handleDDL(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()
	//reserved := s.server.PGServer().GetConnMonitor().MakeBoundAccount()

	code := RestfulResponseCodeSuccess
	desc := ""

	if err := s.checkFormat(ctx, w, r, "POST"); err != nil {
		return
	}

	restDDL, err := s.checkInput(ctx, w, r)
	if err != nil {
		return
	}
	connCache, err := s.checkConn(ctx, w, r)
	if err != nil {
		return
	}
	// Calculate the execution time if needed
	var executionTime float64
	// split the stmts.
	ddlStmts := parseSQL(restDDL)
	for _, stmt := range ddlStmts {
		if stmt == "" {
			continue
		}
		connCache.h.NewStmt()
		// ddl includes
		includeDDlflag := ifContainsType(DDlIncluded, strings.ToLower(stmt))
		excludeDDlCount := strings.Count(strings.ToLower(stmt), ddlExcludeStrLowercase)
		if !includeDDlflag || 0 < excludeDDlCount {
			desc += "wrong statement for ddl interface and please check,"
			code = RestfulResponseCodeFail
			continue
		}
		sqlStmt, err := parser.ParseOne(stmt)
		if err != nil {
			errStr := strings.ReplaceAll(err.Error(), `"`, `\"`)
			desc += "pq: " + errStr + ","
			code = RestfulResponseCodeFail
			continue
		}
		err = connCache.h.PushStmt(ctx, sql.ExecStmt{Statement: sqlStmt})
		if err != nil {
			desc = makeResErr(desc, err)
			code = RestfulResponseCodeFail
			continue
		}
		ddlStartTime := timeutil.Now()
		res, err := connCache.h.ExecRestfulStmt(ctx)
		if err != nil {
			desc = makeResErr(desc, err)
			code = RestfulResponseCodeFail
			continue
		}
		duration := timeutil.Now().Sub(ddlStartTime)
		executionTime = executionTime + float64(duration)/float64(time.Second)
		if res.Err != nil {
			errStr := strings.ReplaceAll(res.Err.Error(), `"`, `\"`)
			desc += "pq: " + errStr + ","
			code = RestfulResponseCodeFail
		} else {
			desc += "success" + ","
		}
	}
	ddldesc := parseDesc(desc)

	// Create the response struct
	response := &ddlResponse{
		baseResponse: &baseResponse{
			Code: code,
			Desc: ddldesc,
			Time: executionTime,
		},
	}
	s.sendJSONResponse(ctx, w, RestfulResponseCodeSuccess, response, ddldesc)
	refreshRequestTime(connCache)
}

// makeResErr make error desc for return
func makeResErr(desc string, err error) string {
	errStr := strings.ReplaceAll(err.Error(), `"`, `\"`)
	desc = desc + errStr + ","
	return desc
}

// handleInsert handles insert interface
func (s *restfulServer) handleInsert(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()
	code := RestfulResponseCodeSuccess
	desc := ""

	if err := s.checkFormat(ctx, w, r, "POST"); err != nil {
		return
	}

	restInsert, err := s.checkInput(ctx, w, r)
	if err != nil {
		return
	}
	connCache, err := s.checkConn(ctx, w, r)
	if err != nil {
		return
	}
	// Calculate the execution time if needed
	var executionTime float64
	var rowsAffected int64
	notice := ""
	// split the stmts.
	insertStmts := parseSQL(restInsert)
	for _, insSQL := range insertStmts {
		if insSQL == "" {
			continue
		}
		insertflag := strings.HasPrefix(strings.ToLower(insSQL), insertTypeStrLowercase)
		if !insertflag {
			desc += "can not find insert statement and please check,"
			code = RestfulResponseCodeFail
			continue
		}
		connCache.h.NewStmt()
		stmt, err := parser.ParseOne(insSQL)
		if err != nil {
			errStr := strings.ReplaceAll(err.Error(), `"`, `\"`)
			desc += "pq: " + errStr + ","
			code = RestfulResponseCodeFail
			continue
		}
		err = connCache.h.PushStmt(ctx, sql.ExecStmt{Statement: stmt})
		if err != nil {
			desc = makeResErr(desc, err)
			code = RestfulResponseCodeFail
		}
		InsertStartTime := timeutil.Now()
		res, err := connCache.h.ExecRestfulStmt(ctx)
		if err != nil {
			desc = makeResErr(desc, err)
			code = RestfulResponseCodeFail
			continue
		}
		duration := timeutil.Now().Sub(InsertStartTime)
		executionTime = executionTime + float64(duration)/float64(time.Second)
		if res.Err != nil {
			errStr := strings.ReplaceAll(res.Err.Error(), `"`, `\"`)
			desc += "pq: " + errStr + ","
			code = RestfulResponseCodeFail
		} else {
			rowsAffected = rowsAffected + int64(res.RowsAffected)
			desc += "success" + ","
		}
	}
	insertdesc := parseDesc(desc)

	// Create the response struct
	response := insertResponse{
		baseResponse: &baseResponse{
			Code: code,
			Desc: insertdesc,
			Time: executionTime,
		},
		Notice: notice,
		Rows:   rowsAffected,
	}
	s.sendJSONResponse(ctx, w, RestfulResponseCodeSuccess, response, insertdesc)
	refreshRequestTime(connCache)
}

// handleQuery handles query interface
func (s *restfulServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()
	code := RestfulResponseCodeSuccess
	desc := ""

	if err := s.checkFormat(ctx, w, r, "POST"); err != nil {
		return
	}

	restQuery, err := s.checkInput(ctx, w, r)
	if err != nil {
		return
	}

	connCache, err := s.checkConn(ctx, w, r)
	if err != nil {
		return
	}

	// Execute the query
	restDatas := [][]string{}
	var columnMeta = []colMetaInfo{}
	var executionTime float64

	queryStmtCount := strings.Count(restQuery, ";")
	createFlag := strings.HasPrefix(strings.ToLower(restQuery), "create")
	showCreateFlag := strings.HasPrefix(strings.ToLower(restQuery), ddlExcludeStrLowercase)

	if queryStmtCount > 1 {
		desc = "only support single statement for each query interface, please check."
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}
	if createFlag == true && !showCreateFlag {
		desc = "do not support create statement for query interface, please check."
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}

	if ifKey, keyword := startsWithKeywords(restQuery); ifKey {
		desc = "do not support " + keyword + " statement for query interface, please check."
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}

	stmts, err := parser.Parse(restQuery)
	if err != nil {
		errStr := strings.ReplaceAll(err.Error(), `"`, `\"`)
		desc = desc + errStr
		code = RestfulResponseCodeFail
		// Create the response struct
		response := &ddlResponse{
			baseResponse: &baseResponse{
				Code: code,
				Desc: desc,
				Time: executionTime,
			},
		}
		s.sendJSONResponse(ctx, w, RestfulResponseCodeSuccess, response, desc)
		return
	}
	var rows int
	for _, stmt := range stmts {
		connCache.h.NewStmt()
		err = connCache.h.PushStmt(ctx, sql.ExecStmt{Statement: stmt})
		if err != nil {
			desc = makeResErr(desc, err)
			code = RestfulResponseCodeFail
		}
		// Calculate the execution time if needed
		QueryStartTime := timeutil.Now()
		re, err := connCache.h.ExecRestfulStmt(ctx)
		if err != nil {
			desc = makeResErr(desc, err)
			code = RestfulResponseCodeFail
			continue
		}
		duration := timeutil.Now().Sub(QueryStartTime)
		executionTime = executionTime + float64(duration)/float64(time.Second)
		if re.Err != nil {
			desc += "pq: " + re.Err.Error()
			code = RestfulResponseCodeFail
		} else {
			// Get column meta.
			cols := re.Cols
			nameIdx := make(map[int]string, len(cols))
			for i, colMeta := range cols {
				var info colMetaInfo
				colType := strings.ToUpper(colMeta.Typ.Name())
				colName := strings.ReplaceAll(colMeta.Name, `"`, `\"`)
				colLength := int64(colMeta.Typ.Width())
				if colType == "NAME" {
					nameIdx[i] = colName
				}
				if _, ok := transTypeToLength[colType]; ok {
					if (colType == "VARCHAR" || colType == "NVARCHAR" || colType == "BYTES" ||
						colType == "VARBYTES" || colType == "STRING") && colLength != 0 {
						columnMeta = append(columnMeta, colMetaInfo{colName, colType, colLength})
					} else if (colType == "CHAR" || colType == "NCHAR") && colLength != 1 {
						columnMeta = append(columnMeta, colMetaInfo{colName, colType, colLength})
					} else {
						info = transColType(colName, colType)
						columnMeta = append(columnMeta, info)
					}
				} else {
					columnMeta = append(columnMeta, colMetaInfo{colName, colType, colLength})
				}
			}
			// get row data.
			for _, datums := range re.Rows {
				restData := makeQueryRes(datums, connCache.h.GetTimeZone(), connCache.isStrTimezone, nameIdx)
				restDatas = append(restDatas, restData)
			}
			rows = len(re.Rows)
			desc = desc + "success"
		}
	}

	for row := range restDatas {
		for col := range restDatas[row] {
			// replace "\n"
			restDatas[row][col] = strings.ReplaceAll(restDatas[row][col], "\n", "")
			// replace "\t"
			restDatas[row][col] = strings.ReplaceAll(restDatas[row][col], "\t", " ")
		}
	}
	// Create the response struct
	response := queryResponse{
		baseResponse: &baseResponse{
			Code: code,
			Desc: desc,
			Time: executionTime,
		},
		Rows:       rows,
		ColumnMeta: columnMeta,
		Data:       restDatas,
	}

	s.sendJSONResponse(ctx, w, code, response, desc)
	refreshRequestTime(connCache)
}

// makeQueryRes change query result to make it same as original to make test access
func makeQueryRes(
	datums tree.Datums, location *time.Location, isStrTimezone bool, nameIdx map[int]string,
) []string {
	var restData []string
	for i, datum := range datums {
		if ts, ok := datum.(*tree.DTimestampTZ); ok {
			ts.Time = ts.Time.In(location)
		}
		str := sqlbase.DatumToString(datum)
		switch datum.(type) {
		case *tree.DInterval, *tree.DTimestampTZ, *tree.DUuid, *tree.DJSON, *tree.DIPAddr, *tree.DTime, *tree.DDate:
			if len(str) > 0 && str[0] == '\'' {
				str = str[1:]
			}
			if len(str) > 0 && str[len(str)-1] == '\'' {
				str = str[:len(str)-1]
			}
		}
		if _, ok := nameIdx[i]; ok {
			if len(str) > 0 && str[0] == '\'' {
				str = str[1:]
			}
			if len(str) > 0 && str[len(str)-1] == '\'' {
				str = str[:len(str)-1]
			}
		}
		switch val := datum.(type) {
		case *tree.DTimestamp:
			strs := strings.Split(val.Time.Format(time.RFC822), " ")
			zoneStr := strs[len(strs)-1]
			timeStr := strings.Split(str, "+")
			if len(timeStr) == 2 {
				tz := strings.ReplaceAll(timeStr[1], ":", "")
				if isStrTimezone {
					str = timeStr[0] + " +" + tz + " " + zoneStr
				} else {
					str = timeStr[0] + " +" + tz + " +" + tz
				}
			} else {
				idx := strings.LastIndex(str, "-")
				tz := strings.ReplaceAll(str[idx:], ":", "")
				if isStrTimezone {
					str = str[:idx] + " " + tz + " " + zoneStr
				} else {
					str = str[:idx] + " " + tz + " " + tz
				}
			}
		case *tree.DTimestampTZ:
			strs := strings.Split(val.Time.Format(time.RFC822), " ")
			zoneStr := strs[len(strs)-1]
			timeStr := strings.Split(str, "+")
			if len(timeStr) == 2 {
				tz := strings.ReplaceAll(timeStr[1], ":", "")
				if isStrTimezone {
					str = timeStr[0] + " +" + tz + " " + zoneStr
				} else {
					str = timeStr[0] + " +" + tz + " +" + tz
				}
			} else {
				idx := strings.LastIndex(str, "-")
				tz := strings.ReplaceAll(str[idx:], ":", "")
				if isStrTimezone {
					str = str[:idx] + " " + tz + " " + zoneStr
				} else {
					str = str[:idx] + " " + tz + " " + tz
				}
			}
		case *tree.DDate:
			str = str + " 00:00:00 +0000 +0000"
		case *tree.DTime:
			str = "0000-01-01 " + str + " +0000 UTC"
		case *tree.DCollatedString:
			str = val.Contents
		case *tree.DFloat:
			if len(str) > 2 && str[len(str)-2:] == ".0" {
				str = str[:len(str)-2]
			}
		case *tree.DBitArray:
			if len(str) > 1 && str[0] == 'B' && str[1] == '\'' {
				str = str[2:]
			}
			if len(str) > 1 && str[len(str)-1] == '\'' {
				str = str[:len(str)-1]
			}
		case *tree.DBytes:
			str = fmt.Sprintf("\\x%x", str)
		case *tree.DArray:
			str = "{"
			if len(val.Array) > 0 {
				for _, arr := range val.Array {
					str += sqlbase.DatumToString(arr)
					str += ","
				}
				str = str[:len(str)-1] + "}"
			} else {
				str += "}"
			}
		}
		restData = append(restData, str)
	}
	return restData
}

func transColType(colName, colType string) colMetaInfo {
	switch colType {
	case "INT2", "INT4", "INT8", "FLOAT4", "CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "BYTES", "VARBYTES":
	case "INT":
		colType = "INT8"
	case "FLOAT":
		colType = "FLOAT8"
	case "STRING":
		colType = "TEXT"
	case "STRING[]":
		colType = "_TEXT"
	}
	length := transTypeToLength[colType]
	colMeta := colMetaInfo{
		Name:   colName,
		Type:   colType,
		Length: length,
	}
	return colMeta
}

// handleTelegraf handle telegraf interface
func (s *restfulServer) handleTelegraf(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()
	code := RestfulResponseCodeSuccess
	desc := ""

	if err := s.checkFormat(ctx, w, r, "POST"); err != nil {
		return
	}

	restTelegraph, err := s.checkInput(ctx, w, r)
	if err != nil {
		return
	}

	connCache, err := s.checkConn(ctx, w, r)
	if err != nil {
		return
	}

	// Calculate the execution time if needed
	var executionTime float64
	var rowsAffected int64
	rowsAffected = 0
	// the program will get a batch of data at once, so it needs to handle it first
	statements := strings.Split(strings.ReplaceAll(restTelegraph, "\r\n", "\n"), "\n")
	for _, stmt := range statements {
		connCache.h.NewStmt()
		insertTelegraphStmt := makeInsertStmt(stmt)

		insertflag := strings.HasPrefix(strings.ToLower(insertTelegraphStmt), insertTypeStrLowercase)
		if insertTelegraphStmt == "" {
			desc = desc + "wrong telegraf insert statement and please check;"
			code = RestfulResponseCodeFail
			continue
		}
		if !insertflag {
			desc = desc + "can not find insert statement and please check;"
			code = RestfulResponseCodeFail
			continue
		}

		teleStmtCount := strings.Count(insertTelegraphStmt, ";")
		if teleStmtCount > 1 {
			desc = desc + "only support single statement for each telegraf interface and please check;"
			code = RestfulResponseCodeFail
			continue
		}

		execStmt, err := parser.ParseOne(insertTelegraphStmt)
		if err != nil {
			desc += "pq: " + err.Error()
			code = RestfulResponseCodeFail
			continue
		}
		err = connCache.h.PushStmt(ctx, sql.ExecStmt{Statement: execStmt})
		if err != nil {
			desc = makeResErr(desc, err)
			code = RestfulResponseCodeFail
		}
		TeleInsertStartTime := timeutil.Now()
		re, err := connCache.h.ExecRestfulStmt(ctx)
		if err != nil {
			desc = makeResErr(desc, err)
			code = RestfulResponseCodeFail
			continue
		}

		duration := timeutil.Now().Sub(TeleInsertStartTime)
		executionTime = executionTime + float64(duration)/float64(time.Second)
		if re.Err != nil {
			desc += "pq: " + re.Err.Error() + ";"
			code = RestfulResponseCodeFail
		} else {
			curRowsAffected := re.RowsAffected
			desc = desc + "success;"
			rowsAffected += int64(curRowsAffected)
		}
	}

	// Create the response struct
	response := teleInsertResponse{
		baseResponse: &baseResponse{
			Code: code,
			Desc: desc,
			Time: executionTime,
		},
		Rows: rowsAffected,
	}
	s.sendJSONResponse(ctx, w, RestfulResponseCodeSuccess, response, desc)
	refreshRequestTime(connCache)
}

// checkConn checks connection of users
func (s *restfulServer) checkConn(
	ctx context.Context, w http.ResponseWriter, r *http.Request,
) (*restfulConn, error) {
	conn, err := s.pickConnCache(ctx)
	if err != nil {
		// s.authorization = ""
		desc := err.Error()
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return nil, err
	}
	// get dbname by context.
	paraDbName := r.FormValue("db")
	// get dbname by path.
	if paraDbName == "" {
		paraDbName = "defaultdb"
	}
	conn.h.SetDBName(paraDbName)

	// get timezone by context.
	paraTimeZone := r.FormValue("tz")
	tempStr := ""
	// get dbname by path.
	if paraTimeZone == "" {
		timezone := SQLRestfulTimeZone.Get(&s.server.cfg.Settings.SV)
		tempStr = fmt.Sprintf("%d", timezone)
	} else {
		tempStr = strings.TrimPrefix(paraTimeZone, "'")
		tempStr = strings.TrimSuffix(tempStr, "'")
		if _, err := strconv.Atoi(tempStr); err != nil {
			conn.isStrTimezone = true
		} else {
			conn.isStrTimezone = false
		}
		if strings.Contains(strings.ToLower(tempStr), "utc ") || strings.Contains(strings.ToLower(tempStr), "utc-") {
			conn.isStrTimezone = false
		}
	}
	originLoc, err := timeutil.TimeZoneStringToLocation(
		tempStr,
		timeutil.TimeZoneStringToLocationISO8601Standard,
	)
	if err != nil {
		err = pgerror.Newf(pgcode.InvalidParameterValue,
			"invalid value for parameter %q: %q", "timezone", paraTimeZone)
		desc := "pq: " + err.Error()
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return nil, err
	}
	loc, err := timeutil.TimeZoneStringToLocation(
		originLoc.String(),
		timeutil.TimeZoneStringToLocationISO8601Standard,
	)
	if err != nil {
		err = pgerror.Newf(pgcode.InvalidParameterValue,
			"invalid value for parameter %q: %q", "timezone", originLoc.String())
		desc := "pq: " + err.Error()
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return nil, err
	}
	conn.h.SetTimeZone(loc)
	return conn, nil
}

// checkFormat checks format of input
func (s *restfulServer) checkFormat(
	ctx context.Context, w http.ResponseWriter, r *http.Request, method string,
) (err error) {
	if s.server.restful.authorization != "" {
		if restAuth := r.Header.Get("Authorization"); restAuth != "" {
			s.server.restful.authorization = restAuth
		}
	}

	if r.Method != method {
		desc := "only support " + method + " method"
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return fmt.Errorf(desc)
	}
	return nil
}

// checkInput checks content of input
func (s *restfulServer) checkInput(
	ctx context.Context, w http.ResponseWriter, r *http.Request,
) (sql string, err error) {
	// Get SQL statement from the body
	sqlValue, err := s.getSQLFromReqBody(r)
	if err != nil || sqlValue == "" {
		desc := "invalid request body"
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return "", fmt.Errorf("invalid request body")
	}
	return sqlValue, nil
}

// handleSession handles session info
func (s *restfulServer) handleSession(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if r.Method != "GET" && r.Method != "DELETE" {
		desc := "only support GET/DELETE method."
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}

	if r.Method == "GET" {
		s.handleUserShow(w, r)
	} else if r.Method == "DELETE" {
		s.handleUserDelete(w, r)
	}
}

// handleUserDelete deletes conn by session id for your API endpoint
// If the current user is an admin, they can delete all connections
// If the current user is a regular user, they can only delete their own connections
func (s *restfulServer) handleUserDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	code := RestfulResponseCodeSuccess
	desc := "delete success"

	if err := s.checkFormat(ctx, w, r, "DELETE"); err != nil {
		return
	}

	uuid, err := s.checkInput(ctx, w, r)
	if err != nil {
		return
	}

	isAdmin, _, _, username, err := s.verifyUser(ctx)
	if err != nil {
		desc = err.Error()
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}
	found := false
	if isAdmin {
		// If it's an admin user, directly delete the session for that connid
		for key, value := range s.connPool.connCache {
			if value.sessionID == uuid {
				delete(s.connPool.connCache, key)
				found = true
				break
			}
		}
	} else {
		// If it's a regular user, they can only delete sessions that they themselves have created
		for key, value := range s.connPool.connCache {
			if value.sessionID == uuid {
				if value.username == username {
					delete(s.connPool.connCache, key)
					found = true
					break
				} else {
					// If it's not created by themselves, an error will occur
					desc = "do not have authority, please check."
					s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
					return
				}
			}
		}
	}
	if !found {
		desc = "no connid matching the given one was found."
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}

	responseSuccess := resultToken{Code: code, Desc: desc}
	s.sendJSONResponse(ctx, w, RestfulResponseCodeSuccess, responseSuccess, "")
}

// handleUserShow shows session info by session id for your API endpoint
func (s *restfulServer) handleUserShow(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var tokens []sessionInfo
	code := RestfulResponseCodeSuccess
	desc := "success"

	if err := s.checkFormat(ctx, w, r, "GET"); err != nil {
		return
	}

	isAdmin, method, key, username, err := s.verifyUser(ctx)
	if err != nil {
		desc = err.Error()
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}

	if isAdmin {
		for token, value := range s.connPool.connCache {
			s.addSessionInfo(&tokens, *value, s.connPool.maxLifeTime, token)
		}
	} else if method == "password" {
		for token, value := range s.connPool.connCache {
			if value.username == username {
				s.addSessionInfo(&tokens, *value, s.connPool.maxLifeTime, token)
			}
		}
	} else if method == "token" {
		if value, ok := s.connPool.connCache[key]; ok {
			s.addSessionInfo(&tokens, *value, s.connPool.maxLifeTime, key)
		}
	}

	responseSuccess := showAllSuccess{Code: code, Conns: tokens}
	s.sendJSONResponse(ctx, w, RestfulResponseCodeSuccess, responseSuccess, "")
}

// addSessionInfo adds session infos
func (s *restfulServer) addSessionInfo(
	tokens *[]sessionInfo, value restfulConn, lifeTime int64, token string,
) {
	lastLoginTime := transUnixTime(value.lastLoginTime)
	expirationTime := transUnixTime(value.lastLoginTime + lifeTime)
	truncated := token[:8]

	showtoken := truncated + strings.Repeat("*", 1)
	*tokens = append(*tokens, sessionInfo{
		Connid:         value.sessionID,
		Username:       value.username,
		Token:          showtoken,
		MaxLifeTime:    lifeTime,
		LastLoginTime:  lastLoginTime,
		ExpirationTime: expirationTime,
	})
}

// sendJSONResponse returns JSON format information
func (s *restfulServer) sendJSONResponse(
	ctx context.Context, w http.ResponseWriter, code int, responseSuccess interface{}, desc string,
) {
	if code == -1 {
		responseFail := resultToken{Code: code, Desc: desc}
		jsonResponse, err := json.Marshal(responseFail)
		if err != nil {
			log.Error(ctx, "marshal response err: %v \n", err.Error())
			return
		}
		// Set the content type header to JSON
		w.Header().Set("Content-Type", "application/json")
		// Write the JSON response
		if _, err := w.Write(jsonResponse); err != nil {
			log.Error(ctx, "write to json err: %v \n", err.Error())
			return
		}
	} else {
		jsonResponse, err := json.Marshal(responseSuccess)
		if err != nil {
			log.Error(ctx, "marshal response err: %v \n", err.Error())
			return
		}
		// Set the content type header to JSON
		w.Header().Set("Content-Type", "application/json")
		// Write the JSON response
		if _, err := w.Write(jsonResponse); err != nil {
			log.Error(ctx, "write to json err: %v \n", err.Error())
			return
		}
	}
}

// generateKey generates token by username and time when login
func (s *restfulServer) generateKey(userName string, tNow int64) (key string, err error) {
	user := RestfulUser{UserName: userName, LoginTime: tNow}

	jsonData, err := json.Marshal(user)
	if err != nil {
		return "", err
	}
	hash := md5.New()
	hash.Write(jsonData)
	hashValue := hash.Sum(nil)

	hashStr := hex.EncodeToString(hashValue)

	return hashStr[:32], nil
}

// pickConnCache finds existed restfulConn or makes a new connectionHandler for user.
func (s *restfulServer) pickConnCache(ctx context.Context) (*restfulConn, error) {
	key := ctx.Value(webCacheKey{}).(string)
	username := ctx.Value(webSessionUserKey{}).(string)
	password := ctx.Value(webSessionPassKey{}).(string)
	method := ctx.Value(webCacheMethodKey{}).(string)
	if method == "token" {
		if key != "" {
			var ok bool
			var resConn *restfulConn
			if resConn, ok = s.connPool.connCache[key]; !ok {
				return nil, fmt.Errorf("can not find token, need login first")
			}
			h, res, err := s.server.PGServer().SQLServer.NewConnectionHandler(ctx, resConn.username)
			if err != nil {
				return nil, err
			}
			resConn.h = h
			resConn.res = res
			return resConn, nil
		}
	} else if method == "password" {
		err := s.checkUser(ctx, username, password)
		if err == nil {
			connHandler, res, err := s.server.PGServer().SQLServer.NewConnectionHandler(ctx, username)
			if err != nil {
				return nil, err
			}
			return s.newRestfulConn(ctx, connHandler, res, username, timeutil.Now().Unix())
		}
	}
	return nil, fmt.Errorf("can not find token, need login first")
}

// verifyUser verifys if the user has logged in.
func (s *restfulServer) verifyUser(ctx context.Context) (bool, string, string, string, error) {
	key := ctx.Value(webCacheKey{}).(string)
	username := ctx.Value(webSessionUserKey{}).(string)
	password := ctx.Value(webSessionPassKey{}).(string)
	method := ctx.Value(webCacheMethodKey{}).(string)
	if method == "token" {
		if restConn, ok := s.connPool.connCache[key]; ok {
			if restConn.isAdmin {
				return true, method, key, username, nil
			}
			return false, method, key, username, nil
		}
	} else if method == "password" {
		if s.checkUser(ctx, username, password) == nil {
			role, err := s.isAdminRole(ctx, username)
			if err == nil {
				return role, method, key, username, nil
			}
		}
	}
	return false, "", "", "", fmt.Errorf("can not verify")
}

// anythingToNumeric cleans numeric values, and add \' (speech marks) between non-numeric values.
func anythingToNumeric(input string) (output string) {
	// regular expression for numbers.
	re := regexp.MustCompile(`^[+-]?[0-9]*[.]?[0-9]+[i]?$`)

	if re.MatchString(input) == false {
		if strings.HasPrefix(input, "'") && strings.HasSuffix(input, "'") {
			output = input
			return output
		}
		output = "'" + input + "'"
		return output
	}

	numbers := re.FindAllString(input, -1)
	// numbers should only be one match, otherwise it may not be a number
	if len(numbers) > 1 {
		output = "'" + input + "'"
		return output
	}

	output = numbers[0]
	if output[len(output)-1] == 'i' {
		output = output[0 : len(output)-1]
	}
	return output
}

// makeInsertStmt makes insert statement when telegraf.
func makeInsertStmt(stmtOriginal string) (teleInsertStmt string) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("invalid data for telegraf insert, please check the format.")
		}
	}()

	// eg. swap,host='123456' k_timestamp=123,inn=27451392,out=194539520 1687898010000000000
	// slice[slice[0] slice[1] slice[2]]

	// find truncation eg. host=
	slice := strings.Split(stmtOriginal, " ")

	// slice[0] = tableName, host, ...
	attribute := strings.Split(slice[0], ",")
	tblName := attribute[0]

	var colKey []string
	var colValue []string

	for index, keyWithValue := range attribute {
		if index >= 1 {
			initObj := strings.Split(keyWithValue, "=")
			// init clokey first
			colKey = append(colKey, initObj[0])
			// init value first
			colValue = append(colValue, anythingToNumeric(initObj[1]))
		}
	}

	colkeyValue := strings.Split(slice[1], ",")
	for _, keyValue := range colkeyValue {
		obj := strings.Split(keyValue, "=")
		colKey = append(colKey, obj[0])
		colValue = append(colValue, anythingToNumeric(obj[1]))
	}
	timeStamp := slice[2]
	if len(timeStamp) > 13 {
		timeStamp = timeStamp[0:13]
	}

	// construct insert stmt
	// eg. insert into table1(field1,field2) values(value1,value2)
	// insert keys
	insertKeyStmt := "("
	insertKeyStmt += "k_timestamp,"
	for _, insertKey := range colKey {
		insertKeyStmt += insertKey
		insertKeyStmt += ","
	}
	// insertKeyStmt += hostKey
	// drop the last character.
	insertKeyStmt = strings.TrimRight(insertKeyStmt, ",")
	insertKeyStmt += ")"

	// insert values
	insertValueStmt := "("
	insertValueStmt += timeStamp
	insertValueStmt += ","
	for _, insertValue := range colValue {
		insertValueStmt += insertValue
		insertValueStmt += ","
	}
	// drop the last character.
	// insertValueStmt += hostValue
	insertValueStmt = strings.TrimRight(insertValueStmt, ",")
	insertValueStmt += ")"
	// insert stmt
	stmtRet := "insert into " + tblName + insertKeyStmt + " values" + insertValueStmt

	return stmtRet
}

// transUnixTime formats display time.
func transUnixTime(timestamp int64) string {
	t := timeutil.Unix(timestamp, 0)

	return t.Format("2006-01-02 15:04:05")
}

// generateSessionID generates Session ID
func generateSessionID() (string, error) {
	uuid, err := uuid.NewV1()
	if err != nil {
		return "", err
	}
	return uuid.String(), nil
}

// isAdminRole determines whether the user is a member of the admin role
func (s *restfulServer) isAdminRole(ctx context.Context, member string) (bool, error) {
	ret := map[string]bool{}

	// Keep track of members we looked up.
	visited := map[string]struct{}{}
	toVisit := []string{member}
	lookupRolesStmt := `SELECT "role", "isAdmin" FROM system.role_members WHERE "member" = $1`

	for len(toVisit) > 0 {
		// Pop first element.
		m := toVisit[0]
		toVisit = toVisit[1:]
		if _, ok := visited[m]; ok {
			continue
		}
		visited[m] = struct{}{}

		rows, err := s.server.execCfg.InternalExecutor.Query(
			ctx, "expand-roles", nil, lookupRolesStmt, m,
		)
		if err != nil {
			return false, err
		}

		for _, row := range rows {
			roleName := tree.MustBeDString(row[0])
			isAdmin := row[1].(*tree.DBool)

			ret[string(roleName)] = bool(*isAdmin)

			// We need to expand this role. Let the "pop" worry about already-visited elements.
			toVisit = append(toVisit, string(roleName))
		}
	}

	if _, ok := ret[sqlbase.AdminRole]; ok {
		return true, nil
	}
	return false, nil
}

// parseSQL parses SQL of insert
func parseSQL(restInsert string) []string {
	restInsert = strings.ReplaceAll(restInsert, "\r\n", "")
	restInsert = strings.ReplaceAll(restInsert, "\n", "")
	insertStmts := strings.Split(restInsert, ";")
	return insertStmts
}

// parseSQL parses SQL of desc
func parseDesc(desc string) string {
	// erase the redundant symbols.
	desc = strings.ReplaceAll(desc, `"""`, `"`)
	desc = strings.ReplaceAll(desc, `""`, `"`)
	desc = strings.TrimRight(desc, ",")
	return desc
}

func (s *restfulServer) getUserWIthPass(
	r *http.Request,
) (userName string, passWd string, err error) {
	tokenFromHeader := s.server.restful.authorization
	tokenWithBaseAu := r.Header.Get("Authorization")

	tokenStr := ""
	if tokenWithBaseAu != "" {
		tokenStr = tokenWithBaseAu
	} else if tokenFromHeader != "" {
		tokenStr = tokenFromHeader
	}
	// token : format[Basic base64codes]
	token := ""
	// get token.
	if tokenStr == "" {
		return "", "", fmt.Errorf("can not find Basic attribute, please check")
	}

	tokenSlice := strings.Split(tokenStr, " ")
	if tokenSlice[0] != "Basic" || tokenSlice[1] == "" {
		return "", "", fmt.Errorf("can not find Basic attribute, please check")
	}
	token = tokenSlice[1]

	usernamePassword, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", "", fmt.Errorf("wrong username or password, please check")
	}
	slice := strings.Split(string(usernamePassword), ":")
	if len(slice) != 2 {
		return "", "", fmt.Errorf("wrong username or password, please check")
	}
	return slice[0], slice[1], nil
}

// determineType returns the corresponding type based on the input string
func determineType(input string) (string, string) {
	input = strings.TrimSpace(input)
	if len(input) > 1 && input[0] == '"' && input[len(input)-1] == '"' {
		length := len(input) - 2
		if length > varcharlen {
			return "'" + input[1:len(input)-1] + "'", "varchar" + "(" + strconv.Itoa(length) + ")"
		}
		return "'" + input[1:len(input)-1] + "'", "varchar"
	}
	if isFloat.MatchString(input) {
		return input, "float8"
	}
	if isNumericWithU.MatchString(input) || isNumericWithI.MatchString(input) {
		input = input[:len(input)-1]
		return input, "int8"
	}
	if val, ok := isBool[input]; ok {
		return fmt.Sprintf("%t", val), "bool"
	}
	return "", "UNKNOWN"
}

// makeInfluxDBStmt makes insert statement when telegraf.
func makeInfluxDBStmt(
	ctx context.Context, stmtOriginal string,
) (teleInsertStmt string, teleCreateStmt string) {
	defer func() {
		if err := recover(); err != nil {
			log.Error(ctx, "invalid data for Influxdb protocol, please check the format %v", err)
		}
	}()
	if stmtOriginal == "" {
		return "", ""
	}
	slice := splitStringQuotes(stmtOriginal, ' ')
	attribute := strings.Split(slice[0], ",")
	// wrap the table name in quotation marks
	tblName := "\"" + attribute[0] + "\""

	var colKey, colValue, coltagName, coltagType, colvalueType, colvalueName, hashtag []string

	for index, keyWithValue := range attribute {
		if index >= 1 {
			hashtag = append(hashtag, keyWithValue)
			initObj := strings.Split(keyWithValue, "=")
			initObj[0] = "\"" + initObj[0] + "\""
			colKey = append(colKey, initObj[0])
			coltagName = append(coltagName, initObj[0])
			if len(initObj[1]) > varcharlen {
				typ := "varchar" + "(" + strconv.Itoa(len(initObj[1])) + ")"
				coltagType = append(coltagType, typ)
			} else {
				coltagType = append(coltagType, "varchar")
			}
			initObj[1] = "'" + initObj[1] + "'"
			colValue = append(colValue, initObj[1])
		}
	}
	tagNum := len(coltagType)
	createTagStmt := strings.Builder{}
	createTagStmt.WriteString("(primary_tag varchar not null")
	for index, createKey := range coltagName {
		createTagStmt.WriteString(",")
		createTagStmt.WriteString(createKey)
		createTagStmt.WriteString(" ")
		createTagStmt.WriteString(coltagType[index])
	}
	createTagStmt.WriteString(")")
	colkeyValue := splitStringQuotes(slice[1], ',')
	for _, keyValue := range colkeyValue {
		obj := strings.SplitN(keyValue, "=", 2)
		obj[0] = "\"" + obj[0] + "\""
		colKey = append(colKey, obj[0])
		colvalueName = append(colvalueName, obj[0])
		value, col := determineType(obj[1])
		if col == "UNKNOWN" {
			return "", ""
		}
		colValue = append(colValue, value)
		colvalueType = append(colvalueType, col)
		coltagType = append(coltagType, col)
	}

	timeStamp := "now()"
	if len(slice) >= 3 {
		timeStamp = slice[2]
	}

	createColStmt := strings.Builder{}
	createColStmt.WriteString("(k_timestamp timestamptz(9) not null")
	for index, createKey := range colvalueName {
		createColStmt.WriteString(",")
		createColStmt.WriteString(createKey)
		createColStmt.WriteString(" ")
		createColStmt.WriteString(colvalueType[index])
	}
	createColStmt.WriteString(")")

	insertKeyStmt := strings.Builder{}
	insertKeyStmt.WriteString("(primary_tag varchar tag,k_timestamp timestamptz(9) column")
	for index, insertKey := range colKey {
		if index < tagNum {
			insertKeyStmt.WriteString(",")
			insertKeyStmt.WriteString(insertKey)
			insertKeyStmt.WriteString(" ")
			insertKeyStmt.WriteString(coltagType[index])
			insertKeyStmt.WriteString(" tag")
		} else {
			insertKeyStmt.WriteString(",")
			insertKeyStmt.WriteString(insertKey)
			insertKeyStmt.WriteString(" ")
			insertKeyStmt.WriteString(coltagType[index])
			insertKeyStmt.WriteString(" column")
		}
	}
	// insertKeyStmt += hostKey
	insertKeyStmt.WriteString(")")

	// insert values
	insertValueStmt := strings.Builder{}
	insertValueStmt.WriteString("(")
	insertValueStmt.WriteString("'")
	insertValueStmt.WriteString(generateHashString(hashtag))
	insertValueStmt.WriteString("'")
	insertValueStmt.WriteString(",")
	insertValueStmt.WriteString(timeStamp)
	for _, insertValue := range colValue {
		insertValueStmt.WriteString(",")
		insertValueStmt.WriteString(insertValue)
	}
	insertValueStmt.WriteString(")")
	stmtRet := "insert without schema into " + tblName + insertKeyStmt.String() + " values" + insertValueStmt.String()
	createRet := "create table " + tblName + createColStmt.String() + "tags" + createTagStmt.String() + "primary tags(primary_tag)"
	return stmtRet, createRet
}

// handleInfluxDB handles for influxdb format
func (s *restfulServer) handleInfluxDB(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()
	code := RestfulResponseCodeSuccess
	desc := ""

	if err := s.checkFormat(ctx, w, r, "POST"); err != nil {
		return
	}

	restInfluxdb, err := s.checkInput(ctx, w, r)
	if err != nil {
		return
	}
	connCache, err := s.checkConn(ctx, w, r)
	if err != nil {
		return
	}
	var rowsAffected int64
	rowsAffected = 0
	var teleResult sql.RestfulRes
	// Calculate the execution time if needed
	var executionTime float64
	// the program will get a batch of data at once, so it needs to handle it first
	statements := strings.Split(strings.ReplaceAll(restInfluxdb, "\r\n", "\n"), "\n")
	numOfStmts := len(statements)

	for i := 0; i < numOfStmts; i++ {
		insertTelegraphStmt, createTelegrafStmt := makeInfluxDBStmt(ctx, statements[i])
		TeleInsertStartTime := timeutil.Now()

		if insertTelegraphStmt == "" {
			desc += "wrong influxdb insert statement and please check;"
			code = RestfulResponseCodeFail
			continue
		}

		insertflag := strings.HasPrefix(strings.ToLower(insertTelegraphStmt), insertWithoutSchema)
		if !insertflag {
			desc += "can not find insert statement and please check;"
			code = RestfulResponseCodeFail
			continue
		}

		teleResult, err = s.executeWithRetry(ctx, connCache, insertTelegraphStmt, createTelegrafStmt)
		if err != nil {
			desc += "pq: " + err.Error() + ";"
			code = RestfulResponseCodeFail
			continue
		}

		curRowsAffected := teleResult.RowsAffected
		duration := timeutil.Now().Sub(TeleInsertStartTime)
		executionTime = executionTime + float64(duration)/float64(time.Second)
		desc += "success;"
		rowsAffected += int64(curRowsAffected)
	}

	response := teleInsertResponse{
		baseResponse: &baseResponse{
			Code: code,
			Desc: desc,
			Time: executionTime,
		},
		Rows: rowsAffected,
	}

	s.sendJSONResponse(ctx, w, RestfulResponseCodeSuccess, response, desc)

}

// handleOpenTSDBTelnet handles for opentsdb telnet format
func (s *restfulServer) handleOpenTSDBTelnet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()
	code := RestfulResponseCodeSuccess
	desc := ""

	if err := s.checkFormat(ctx, w, r, "POST"); err != nil {
		return
	}
	restTelnet, err := s.checkInput(ctx, w, r)
	if err != nil {
		return
	}
	connCache, err := s.checkConn(ctx, w, r)
	if err != nil {
		return
	}
	var rowsAffected int64
	rowsAffected = 0
	var teleResult sql.RestfulRes
	// Calculate the execution time if needed
	var executionTime float64
	// the program will get a batch of data at once, so it needs to handle it first
	statements := strings.Split(strings.ReplaceAll(restTelnet, "\r\n", "\n"), "\n")
	numOfStmts := len(statements)

	for i := 0; i < numOfStmts; i++ {
		insertStatement, createStatement, err := makeOpenTSDBTelnet(ctx, statements[i])
		TeleInsertStartTime := timeutil.Now()

		if err != nil {
			desc += "wrong opentsdb telnet insert statement: " + err.Error() + ";"
			code = RestfulResponseCodeFail
			continue
		}

		teleResult, err = s.executeWithRetry(ctx, connCache, insertStatement, createStatement)
		if err != nil {
			desc += "pq: " + err.Error() + ";"
			code = RestfulResponseCodeFail
			continue
		}

		curRowsAffected := teleResult.RowsAffected
		duration := timeutil.Now().Sub(TeleInsertStartTime)
		executionTime = executionTime + float64(duration)/float64(time.Second)
		desc += "success;"
		rowsAffected += int64(curRowsAffected)
	}

	response := teleInsertResponse{
		baseResponse: &baseResponse{
			Code: code,
			Desc: desc,
			Time: executionTime,
		},
		Rows: rowsAffected,
	}

	s.sendJSONResponse(ctx, w, RestfulResponseCodeSuccess, response, desc)
}

// handleOpenTSDBJson handles for opentsdb json format
func (s *restfulServer) handleOpenTSDBJson(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()
	code := RestfulResponseCodeSuccess
	desc := ""

	if err := s.checkFormat(ctx, w, r, "POST"); err != nil {
		return
	}

	restJSON, err := s.checkInput(ctx, w, r)
	if err != nil {
		return
	}
	connCache, err := s.checkConn(ctx, w, r)
	if err != nil {
		return
	}
	var rowsAffected int64
	rowsAffected = 0
	var teleResult sql.RestfulRes
	// Calculate the execution time if needed
	var executionTime float64
	stmt, err := makeOpenTSDBJson(ctx, restJSON)
	if err != nil {
		desc = err.Error()
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}
	if len(stmt) == 0 {
		desc = "invalid data for opentsdb json protocol, please check the format."
		s.sendJSONResponse(ctx, w, RestfulResponseCodeFail, nil, desc)
		return
	}
	// there are cases of inserting multiple pieces of data under this protocol
	for insertTelegraphStmt, createTelegrafStmt := range stmt {
		TeleInsertStartTime := timeutil.Now()

		if insertTelegraphStmt == "" {
			desc += "wrong opentsdb json insert statement and please check;"
			code = RestfulResponseCodeFail
			continue
		}

		teleResult, err = s.executeWithRetry(ctx, connCache, insertTelegraphStmt, createTelegrafStmt)
		if err != nil {
			desc += "pq: " + err.Error() + ";"
			code = RestfulResponseCodeFail
			continue
		}

		curRowsAffected := teleResult.RowsAffected
		duration := timeutil.Now().Sub(TeleInsertStartTime)
		executionTime = executionTime + float64(duration)/float64(time.Second)
		desc += "success;"
		rowsAffected += int64(curRowsAffected)
	}

	response := teleInsertResponse{
		baseResponse: &baseResponse{
			Code: code,
			Desc: desc,
			Time: executionTime,
		},
		Rows: rowsAffected,
	}

	s.sendJSONResponse(ctx, w, RestfulResponseCodeSuccess, response, desc)
}

func refreshRequestTime(connCache *restfulConn) {
	connCache.lastLoginTime = timeutil.Now().Unix()
}

// executeWithRetry handles retries in the event of a failure to write without a pattern
func (s *restfulServer) executeWithRetry(
	ctx context.Context, connCache *restfulConn, insertStmt, createStmt string,
) (sql.RestfulRes, error) {
	var execResult sql.RestfulRes
	insert, err := parser.ParseOne(insertStmt)
	if err != nil {
		return execResult, err
	}
	create, err := parser.ParseOne(createStmt)
	if err != nil {
		return execResult, err
	}

	// initialize the time of the first retry
	retryDelay := 1 * time.Second
	for attempt := 0; attempt < maxRetries; attempt++ {
		connCache.h.NewStmt()
		err = connCache.h.PushStmt(ctx, sql.ExecStmt{Statement: insert})
		if err != nil {
			return execResult, err
		}
		re, err := connCache.h.ExecRestfulStmt(ctx)
		if err != nil {
			return execResult, err
		}
		execResult = re
		schemalessError := ""
		if execResult.Err != nil {
			schemalessError = execResult.Err.Error()
		}
		if reRelationNotExist.MatchString(schemalessError) {
			// reRelationNotExist performs the following operations
			connCache.h.NewStmt()
			err = connCache.h.PushStmt(ctx, sql.ExecStmt{Statement: create})
			if err != nil {
				return execResult, err
			}
			re, err := connCache.h.ExecRestfulStmt(ctx)
			if err != nil {
				return execResult, err
			}
			execResult = re
			if execResult.Err != nil {
				createTableError := execResult.Err.Error()
				if !reRelationAlreadyExists.MatchString(createTableError) && !reWaitForSuccess.MatchString(createTableError) && !retriableErr.MatchString(createTableError) {
					return execResult, execResult.Err
				} else if reWaitForSuccess.MatchString(createTableError) || retriableErr.MatchString(createTableError) {
					time.Sleep(retryDelay)
					retryDelay *= 2
				}
			}
		} else if reWaitForSuccess.MatchString(schemalessError) || retriableErr.MatchString(schemalessError) {
			// reWaitForSuccess performs the following operations
			log.Warningf(ctx, "exec error=%v\n", schemalessError)
			time.Sleep(retryDelay)
			retryDelay *= 2
		} else {
			// other errors can be returned directly
			return execResult, execResult.Err
		}
		refreshRequestTime(connCache)
	}
	return execResult, err
}

// generateHashString generate hash byte(64)
func generateHashString(hashtag []string) string {
	sort.Strings(hashtag)

	hash := sha256.New()
	for _, str := range hashtag {
		hash.Write([]byte(str))
	}
	hashSum := hash.Sum(nil)
	return fmt.Sprintf("%x", hashSum)
}

// startsWithKeywords check keywords
func startsWithKeywords(s string) (bool, string) {
	s = strings.ToLower(s)

	for _, keyword := range queryKeyWords {
		if strings.HasPrefix(s, keyword) {
			return true, keyword
		}
	}
	return false, ""
}

// makeOpenTSDBTelnet handles data for opentsdb protocol by cut and concatenate data in telnet format to generate create and insert statements
func makeOpenTSDBTelnet(ctx context.Context, telnetStr string) (string, string, error) {
	defer func() {
		if err := recover(); err != nil {
			log.Error(ctx, "invalid data for Json, please check the format %v.", err)
		}
	}()
	// OpenTSDB telnet
	// eg: <metric> <timestamp> <value> <tagk_1>=<tagv_1>[ <tagk_n>=<tagv_n>]
	// output: insert without schema into metric(value, primary_tag, tagk_1 ...) values (value, 'XXXXXXXX', tagv_1 ...)
	parts := strings.Fields(telnetStr)

	if len(parts) < 3 {
		return "", "", fmt.Errorf("missing indicator items")
	}
	metricName := parts[0]
	timeStamp := parts[1]
	value := parts[2]
	tags := make(map[string]string)

	if !isFloat.MatchString(value) {
		return "", "", fmt.Errorf("value is not float type")
	}
	// split tag value and key
	for _, tag := range parts[3:] {
		kv := strings.SplitN(tag, "=", 2)
		if len(kv) == 2 {
			if _, exists := tags[kv[0]]; exists {
				return "", "", fmt.Errorf("duplicate tag column")
			}
			tags[kv[0]] = kv[1]
		} else {
			return "", "", fmt.Errorf("value is not just a column")
		}
	}
	var tagvalue []string
	insertKeyStmt := strings.Builder{}
	// wrap the table name in quotation marks
	tablename := "\"" + metricName + "\""
	insertKeyStmt.WriteString("(k_timestamp timestamptz column, value float8 column, primary_tag varchar tag")
	for k, v := range tags {
		val := k + "=" + "'" + v + "'"
		tagvalue = append(tagvalue, val)
	}
	// dealing with the accuracy issue of timestamp columns
	if len(timeStamp) > 13 {
		timeStamp = timeStamp[0:13]
	} else if len(timeStamp) < 13 {
		timeStamp = timeStamp + strings.Repeat("0", 13-len(timeStamp))
	}
	insertValue := strings.Builder{}
	insertValue.WriteString("(")
	insertValue.WriteString(timeStamp)
	insertValue.WriteString(",")
	insertValue.WriteString(value)
	insertValue.WriteString(",")
	insertValue.WriteString("'")
	insertValue.WriteString(generateHashString(tagvalue))
	insertValue.WriteString("'")

	createTag := strings.Builder{}
	createTag.WriteString("(primary_tag varchar not null")
	createColumn := strings.Builder{}
	createColumn.WriteString("(k_timestamp timestamptz not null, value float8 not null)")

	for k, v := range tags {
		insertKeyStmt.WriteString(",")
		insertKeyStmt.WriteString("\"")
		insertKeyStmt.WriteString(k)
		insertKeyStmt.WriteString("\"")
		if len(v) > varcharlen {
			insertKeyStmt.WriteString(" varchar")
			insertKeyStmt.WriteString("(")
			length := len(v)
			insertKeyStmt.WriteString(strconv.Itoa(length))
			insertKeyStmt.WriteString(")")
			insertKeyStmt.WriteString(" tag")
		} else {
			insertKeyStmt.WriteString(" varchar tag")
		}
		insertValue.WriteString(",")
		tagValue := "'" + v + "'"
		insertValue.WriteString(tagValue)
		createTag.WriteString(",")
		createTag.WriteString("\"")
		createTag.WriteString(k)
		createTag.WriteString("\"")
		createTag.WriteString(" varchar")
	}

	insertKeyStmt.WriteString(")")
	insertValue.WriteString(")")
	createTag.WriteString(")")
	// Splicing insert and create statements
	insertStatement := "insert without schema into " + tablename + insertKeyStmt.String() + "values" + insertValue.String()
	createStatement := "create table " + tablename + createColumn.String() + "Tags" + createTag.String() + "primary tags(primary_tag)"
	return insertStatement, createStatement, nil
}

// makeOpenTSDBJson handles data for opentsdb json protocol by deserialize and concatenate JSON formatted data to generate create and insert statements
func makeOpenTSDBJson(ctx context.Context, jsonStr string) (map[string]string, error) {
	defer func() {
		if err := recover(); err != nil {
			log.Error(ctx, "invalid data for Json, please check the format %v.", err)
		}
	}()
	// openTSDB json
	// eg:
	// 	[
	//   {
	//     "metric": "sys.cpu.nice",
	//     "timestamp": 13468146400,
	//     "value": 11,
	//     "tags": {
	//       "host": "kaiwudb01",
	//       "dc": "lga2"
	//     }
	//   }
	// ]
	jsonData := make(map[string]string)
	// deserialize to obtain data
	var data []MetricData
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		log.Error(ctx, "invalid data for Json, please check the format %v.", err.Error())
		return jsonData, err
	}

	for _, metric := range data {
		if metric.Timestamp == nil || metric.Value == nil {
			jsonData = make(map[string]string)
			return jsonData, fmt.Errorf("timestamp or value column is nil")
		}
		var tagvalue []string
		for key, value := range metric.Tags {
			switch v := value.(type) {
			case string:
				val := key + "=" + "'" + v + "'"
				tagvalue = append(tagvalue, val)
			case float64:
				val := strconv.FormatFloat(v, 'f', -1, 64)
				val = key + "=" + val
				tagvalue = append(tagvalue, val)
			}
		}
		// wrap the table name in quotation marks
		tablename := "\"" + metric.Metric + "\""
		insertKeyStmt := strings.Builder{}
		insertKeyStmt.WriteString("(primary_tag varchar tag,value float8 column,k_timestamp timestamptz column")
		value := strconv.FormatFloat(*metric.Value, 'f', -1, 64)
		timestamp := strconv.FormatInt(*metric.Timestamp, 10)
		if len(timestamp) > 13 {
			timestamp = timestamp[0:13]
		} else if len(timestamp) < 13 {
			timestamp = timestamp + strings.Repeat("0", 13-len(timestamp))
		}
		insertValue := strings.Builder{}
		insertValue.WriteString("(")
		insertValue.WriteString("'")
		insertValue.WriteString(generateHashString(tagvalue))
		insertValue.WriteString("'")
		insertValue.WriteString(",")
		insertValue.WriteString(value)
		insertValue.WriteString(",")
		insertValue.WriteString(timestamp)

		createTag := strings.Builder{}
		createTag.WriteString("(primary_tag varchar not null")
		createColumn := strings.Builder{}
		createColumn.WriteString("(k_timestamp timestamptz not null, value float8 not null)")

		for k, v := range metric.Tags {
			col, typ := determineTypeJSON(v)
			if typ == "UNKNOWN" {
				jsonData = make(map[string]string)
				return jsonData, fmt.Errorf("tags type is not support")
			}
			insertKeyStmt.WriteString(",")
			insertKeyStmt.WriteString("\"")
			insertKeyStmt.WriteString(k)
			insertKeyStmt.WriteString("\"")
			insertKeyStmt.WriteString(" ")
			insertKeyStmt.WriteString(typ)
			insertKeyStmt.WriteString(" tag")
			insertValue.WriteString(",")
			insertValue.WriteString(col)
			createTag.WriteString(",")
			createTag.WriteString("\"")
			createTag.WriteString(k)
			createTag.WriteString("\"")
			createTag.WriteString(" ")
			createTag.WriteString(typ)
		}

		insertKeyStmt.WriteString(")")
		insertValue.WriteString(")")
		createTag.WriteString(")")
		// splicing insert and create statements
		insertStatement := "insert without schema into " + tablename + insertKeyStmt.String() + "values" + insertValue.String()
		createStatement := "create table " + tablename + createColumn.String() + "Tags" + createTag.String() + "primary tags(primary_tag)"
		jsonData[insertStatement] = createStatement
	}
	return jsonData, nil
}

// determineTypeJSON returns the corresponding type based on the input string of openTSDB, return value is the column value and column type
func determineTypeJSON(input interface{}) (string, string) {
	switch v := input.(type) {
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), "float8"
	case string:
		length := len(v)
		if length > varcharlen {
			return "'" + v + "'", "varchar" + "(" + strconv.Itoa(length) + ")"
		}
		return "'" + v + "'", "varchar"
	default:
		return "", "UNKNOWN"
	}
}
