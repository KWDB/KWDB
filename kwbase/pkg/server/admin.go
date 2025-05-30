// Copyright 2014 The Cockroach Authors.
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

package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/server"
	"gitee.com/kwbasedb/kwbase/pkg/server/debug"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/ts/catalog"
	"gitee.com/kwbasedb/kwbase/pkg/util/contextutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/sysutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/errors"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	gwutil "github.com/grpc-ecosystem/grpc-gateway/utilities"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// adminPrefix is the prefix for RESTful endpoints used to provide an
	// administrative interface to the kwbase cluster.
	adminPrefix = "/_admin/v1/"

	// defaultAPIEventLimit is the default maximum number of events returned by any
	// endpoints returning events.
	defaultAPIEventLimit = 1000

	// Number of empty ranges for table descriptors that aren't actually tables,
	// e.g. descriptors 17, 18, 19, and 22 which correspond to MetaRangesID,
	// SystemRangesID, TimeseriesRangesID, and LivenessRangesID in pkg/keys.
	nonTableDescriptorRangeCount = 4
)

// apiServerMessage is the standard body for all HTTP 500 responses.
var errAdminAPIError = status.Errorf(codes.Internal, "An internal server error "+
	"has occurred. Please check your KaiwuDB logs for more details.")

var (
	// CPUAlertThreshold audit log file record switch
	CPUAlertThreshold = settings.RegisterFloatSetting(
		"alert.cpu.threshold",
		"the threshold of cpu usage alert",
		0.8,
	)

	// MemoryAlertThreshold audit log file record switch
	MemoryAlertThreshold = settings.RegisterFloatSetting(
		"alert.mem.threshold",
		"the threshold of memory usage alert",
		0.8,
	)

	// StorageAlertThreshold audit log file record switch
	StorageAlertThreshold = settings.RegisterFloatSetting(
		"alert.storage.threshold",
		"the threshold of storage usage alert",
		0.8,
	)

	// ConnectionsAlertThreshold audit log file record switch
	ConnectionsAlertThreshold = settings.RegisterFloatSetting(
		"alert.connections.threshold",
		"the threshold of connections alert, it is a percentage. If the current connection number > threshold * timeseries.sql_connections.max_limit, it will trigger the alarm on Admin UI.",
		0.9,
	)
)

// A adminServer provides a RESTful HTTP API to administration of
// the kwbase cluster.
type adminServer struct {
	server     *Server
	memMonitor mon.BytesMonitor
	memMetrics *sql.MemoryMetrics
}

// noteworthyAdminMemoryUsageBytes is the minimum size tracked by the
// admin SQL pool before the pool start explicitly logging overall
// usage growth in the log.
var noteworthyAdminMemoryUsageBytes = envutil.EnvOrDefaultInt64("KWBASE_NOTEWORTHY_ADMIN_MEMORY_USAGE", 100*1024)

// newAdminServer allocates and returns a new REST server for
// administrative APIs.
func newAdminServer(s *Server) *adminServer {
	server := &adminServer{server: s, memMetrics: &s.adminMemMetrics}
	// TODO(knz): We do not limit memory usage by admin operations
	// yet. Is this wise?
	server.memMonitor = mon.MakeUnlimitedMonitor(
		context.Background(),
		"admin",
		mon.MemoryResource,
		nil,
		nil,
		noteworthyAdminMemoryUsageBytes,
		s.ClusterSettings(),
	)
	return server
}

// RegisterService registers the GRPC service.
func (s *adminServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterAdminServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse proxy) that proxies HTTP requests
// to the appropriate gRPC endpoints.
func (s *adminServer) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	// Register the /_admin/v1/stmtbundle endpoint, which serves statement support
	// bundles as files.
	stmtBundlePattern := gwruntime.MustPattern(gwruntime.NewPattern(
		1, /* version */
		[]int{
			int(gwutil.OpLitPush), 0, int(gwutil.OpLitPush), 1, int(gwutil.OpLitPush), 2,
			int(gwutil.OpPush), 0, int(gwutil.OpConcatN), 1, int(gwutil.OpCapture), 3},
		[]string{"_admin", "v1", "stmtbundle", "id"},
		"", /* verb */
	))

	mux.Handle("GET", stmtBundlePattern, func(
		w http.ResponseWriter, req *http.Request, pathParams map[string]string,
	) {
		idStr, ok := pathParams["id"]
		if !ok {
			http.Error(w, "missing id", http.StatusBadRequest)
			return
		}
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			http.Error(w, "invalid id", http.StatusBadRequest)
			return
		}
		s.getStatementBundle(ctx, id, w)
	})

	// Register the endpoints defined in the proto.
	return serverpb.RegisterAdminHandler(ctx, mux, conn)
}

// serverError logs the provided error and returns an error that should be returned by
// the RPC endpoint method.
func (s *adminServer) serverError(err error) error {
	log.ErrorfDepth(context.TODO(), 1, "%s", err)
	return errAdminAPIError
}

// serverErrorf logs the provided error and returns an error that should be returned by
// the RPC endpoint method.
func (s *adminServer) serverErrorf(format string, args ...interface{}) error {
	log.ErrorfDepth(context.TODO(), 1, format, args...)
	return errAdminAPIError
}

// serverErrors logs the provided errors and returns an error that should be returned by
// the RPC endpoint method.
func (s *adminServer) serverErrors(errors []error) error {
	log.ErrorfDepth(context.TODO(), 1, "%v", errors)
	return errAdminAPIError
}

// isNotFoundError returns true if err is a table/database not found error.
func (s *adminServer) isNotFoundError(err error) bool {
	// TODO(cdo): Replace this crude suffix-matching with something more structured once we have
	// more structured errors.
	return err != nil && strings.HasSuffix(err.Error(), "does not exist")
}

// AllMetricMetadata returns all metrics' metadata.
func (s *adminServer) AllMetricMetadata(
	ctx context.Context, req *serverpb.MetricMetadataRequest,
) (*serverpb.MetricMetadataResponse, error) {

	resp := &serverpb.MetricMetadataResponse{
		Metadata: s.server.recorder.GetMetricsMetadata(),
	}

	return resp, nil
}

// ChartCatalog returns a catalog of Admin UI charts useful for debugging.
func (s *adminServer) ChartCatalog(
	ctx context.Context, req *serverpb.ChartCatalogRequest,
) (*serverpb.ChartCatalogResponse, error) {
	metricsMetadata := s.server.recorder.GetMetricsMetadata()

	chartCatalog, err := catalog.GenerateCatalog(metricsMetadata)

	if err != nil {
		return nil, err
	}

	resp := &serverpb.ChartCatalogResponse{
		Catalog: chartCatalog,
	}

	return resp, nil
}

// Databases is an endpoint that returns a list of databases.
func (s *adminServer) Databases(
	ctx context.Context, req *serverpb.DatabasesRequest,
) (*serverpb.DatabasesResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)

	sessionUser, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := s.server.internalExecutor.QueryEx(
		ctx, "admin-show-dbs", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: sessionUser},
		"SHOW DATABASES",
	)
	if err != nil {
		return nil, s.serverError(err)
	}

	var resp serverpb.DatabasesResponse
	for _, row := range rows {
		dbDatum, ok := tree.AsDString(row[0])
		if !ok {
			return nil, s.serverErrorf("type assertion failed on db name: %T", row[0])
		}
		dbName := string(dbDatum)
		resp.Databases = append(resp.Databases, dbName)
	}

	return &resp, nil
}

// DatabaseDetails is an endpoint that returns grants and a list of table names
// for the specified database.
func (s *adminServer) DatabaseDetails(
	ctx context.Context, req *serverpb.DatabaseDetailsRequest,
) (*serverpb.DatabaseDetailsResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)
	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	escDBName := tree.NameStringP(&req.Database)
	// Placeholders don't work with SHOW statements, so we need to manually
	// escape the database name.
	//
	// TODO(cdo): Use placeholders when they're supported by SHOW.

	// Marshal grants.
	rows, cols, err := s.server.internalExecutor.QueryWithCols(
		ctx, "admin-show-grants", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		fmt.Sprintf("SHOW GRANTS ON DATABASE %s", escDBName),
	)
	if s.isNotFoundError(err) {
		return nil, status.Errorf(codes.NotFound, "%s", err)
	}
	if err != nil {
		return nil, s.serverError(err)
	}
	var resp serverpb.DatabaseDetailsResponse
	{
		const (
			schemaCol     = "schema_name"
			userCol       = "grantee"
			privilegesCol = "privilege_type"
		)

		scanner := makeResultScanner(cols)
		for _, row := range rows {
			var schemaName string
			if err := scanner.Scan(row, schemaCol, &schemaName); err != nil {
				return nil, err
			}
			if schemaName != tree.PublicSchema {
				// We only want to list real tables.
				continue
			}

			// Marshal grant, splitting comma-separated privileges into a proper slice.
			var grant serverpb.DatabaseDetailsResponse_Grant
			var privileges string
			if err := scanner.Scan(row, userCol, &grant.User); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, privilegesCol, &privileges); err != nil {
				return nil, err
			}
			grant.Privileges = strings.Split(privileges, ",")
			resp.Grants = append(resp.Grants, grant)
		}
	}

	// Marshal table names.
	rows, cols, err = s.server.internalExecutor.QueryWithCols(
		ctx, "admin-show-tables", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		// Hide INSTANCE TABLE on Admin UI
		fmt.Sprintf("SELECT * FROM [ SHOW TABLES FROM %s ] WHERE table_type NOT IN ('INSTANCE TABLE')", escDBName),
	)
	if s.isNotFoundError(err) {
		return nil, status.Errorf(codes.NotFound, "%s", err)
	}
	if err != nil {
		return nil, s.serverError(err)
	}

	// Marshal table names.
	{
		const nameCol = "table_name"
		scanner := makeResultScanner(cols)
		if a, e := len(cols), 2; a != e {
			return nil, s.serverErrorf("show tables columns mismatch: %d != expected %d", a, e)
		}
		for _, row := range rows {
			var tableName string
			if err := scanner.Scan(row, nameCol, &tableName); err != nil {
				return nil, err
			}
			resp.TableNames = append(resp.TableNames, tableName)
		}
	}

	// Query the engine type of this database.
	rows, cols, err = s.server.internalExecutor.QueryWithCols(
		ctx, "admin-show-db-type", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		fmt.Sprintf("SELECT * FROM [ SHOW DATABASES ] WHERE database_name = '%s'", strings.Trim(escDBName, "\"")),
	)
	if s.isNotFoundError(err) {
		return nil, status.Errorf(codes.NotFound, "%s", err)
	}
	if err != nil {
		return nil, s.serverError(err)
	}
	const engineCol = "engine_type"
	if len(rows) != 1 {
		return nil, s.serverErrorf("database engine type not available.")
	}

	scanner := makeResultScanner(cols)
	var engineType string
	if err := scanner.Scan(rows[0], engineCol, &engineType); err != nil {
		return nil, err
	}
	resp.EngineType = engineType

	// Query the descriptor ID and zone configuration for this database.
	{
		path, err := s.queryDescriptorIDPath(ctx, userName, []string{req.Database})
		if err != nil {
			return &resp, nil
		}
		resp.DescriptorID = int64(path[1])

		id, zone, zoneExists, err := s.queryZonePath(ctx, userName, path)
		if err != nil {
			return nil, s.serverError(err)
		}

		if !zoneExists {
			zone = s.server.cfg.DefaultZoneConfig
		}
		resp.ZoneConfig = zone

		switch id {
		case path[1]:
			resp.ZoneConfigLevel = serverpb.ZoneConfigurationLevel_DATABASE
		default:
			resp.ZoneConfigLevel = serverpb.ZoneConfigurationLevel_CLUSTER
		}
	}

	return &resp, nil
}

// TableDetails is an endpoint that returns columns, indices, and other
// relevant details for the specified table.
func (s *adminServer) TableDetails(
	ctx context.Context, req *serverpb.TableDetailsRequest,
) (*serverpb.TableDetailsResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)
	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	escDBName := tree.NameStringP(&req.Database)
	// TODO(cdo): Use real placeholders for the table and database names when we've extended our SQL
	// grammar to allow that.
	escTableName := tree.NameStringP(&req.Table)
	escQualTable := fmt.Sprintf("%s.%s", escDBName, escTableName)

	var resp serverpb.TableDetailsResponse

	// Marshal SHOW COLUMNS result.
	rows, cols, err := s.server.internalExecutor.QueryWithCols(
		ctx, "admin-show-columns",
		nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		fmt.Sprintf("SHOW COLUMNS FROM %s", escQualTable),
	)
	if s.isNotFoundError(err) {
		return nil, status.Errorf(codes.NotFound, "%s", err)
	}
	if err != nil {
		return nil, s.serverError(err)
	}
	// TODO(cdo): protobuf v3's default behavior for fields with zero values (e.g. empty strings)
	// is to suppress them. So, if protobuf field "foo" is an empty string, "foo" won't show
	// up in the marshaled JSON. I feel that this is counterintuitive, and this should be fixed
	// for our API.
	{
		const (
			colCol     = "column_name"
			typeCol    = "data_type"
			nullCol    = "is_nullable"
			defaultCol = "column_default"
			genCol     = "generation_expression"
			hiddenCol  = "is_hidden"
		)
		scanner := makeResultScanner(cols)
		for _, row := range rows {
			var col serverpb.TableDetailsResponse_Column
			if err := scanner.Scan(row, colCol, &col.Name); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, typeCol, &col.Type); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, nullCol, &col.Nullable); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, hiddenCol, &col.Hidden); err != nil {
				return nil, err
			}
			isDefaultNull, err := scanner.IsNull(row, defaultCol)
			if err != nil {
				return nil, err
			}
			if !isDefaultNull {
				if err := scanner.Scan(row, defaultCol, &col.DefaultValue); err != nil {
					return nil, err
				}
			}
			isGenNull, err := scanner.IsNull(row, genCol)
			if err != nil {
				return nil, err
			}
			if !isGenNull {
				if err := scanner.Scan(row, genCol, &col.GenerationExpression); err != nil {
					return nil, err
				}
			}
			resp.Columns = append(resp.Columns, col)
		}
	}

	// Marshal SHOW INDEX result.
	rows, cols, err = s.server.internalExecutor.QueryWithCols(
		ctx, "admin-showindex", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		fmt.Sprintf("SHOW INDEX FROM %s", escQualTable),
	)
	if s.isNotFoundError(err) {
		return nil, status.Errorf(codes.NotFound, "%s", err)
	}
	if err != nil {
		// Because 'SHOW INDEX FROM tsTable' is not supported, an error will occur when running 'SHOW INDEX FROM tsTable'.
		// If an error occurs, only log the error and do not terminate this function because other table info needs to be returned.
		log.Warningf(ctx, "Unable to get index of table %s, error: %s", escQualTable, err)
	}
	{
		const (
			nameCol      = "index_name"
			nonUniqueCol = "non_unique"
			seqCol       = "seq_in_index"
			columnCol    = "column_name"
			directionCol = "direction"
			storingCol   = "storing"
			implicitCol  = "implicit"
		)
		scanner := makeResultScanner(cols)
		for _, row := range rows {
			// Marshal grant, splitting comma-separated privileges into a proper slice.
			var index serverpb.TableDetailsResponse_Index
			if err := scanner.Scan(row, nameCol, &index.Name); err != nil {
				return nil, err
			}
			var nonUnique bool
			if err := scanner.Scan(row, nonUniqueCol, &nonUnique); err != nil {
				return nil, err
			}
			index.Unique = !nonUnique
			if err := scanner.Scan(row, seqCol, &index.Seq); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, columnCol, &index.Column); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, directionCol, &index.Direction); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, storingCol, &index.Storing); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, implicitCol, &index.Implicit); err != nil {
				return nil, err
			}
			resp.Indexes = append(resp.Indexes, index)
		}
	}

	// Marshal SHOW GRANTS result.
	rows, cols, err = s.server.internalExecutor.QueryWithCols(
		ctx, "admin-show-grants", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		fmt.Sprintf("SHOW GRANTS ON TABLE %s", escQualTable),
	)
	if s.isNotFoundError(err) {
		return nil, status.Errorf(codes.NotFound, "%s", err)
	}
	if err != nil {
		return nil, s.serverError(err)
	}
	{
		const (
			userCol       = "grantee"
			privilegesCol = "privilege_type"
		)
		scanner := makeResultScanner(cols)
		for _, row := range rows {
			// Marshal grant, splitting comma-separated privileges into a proper slice.
			var grant serverpb.TableDetailsResponse_Grant
			var privileges string
			if err := scanner.Scan(row, userCol, &grant.User); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, privilegesCol, &privileges); err != nil {
				return nil, err
			}
			grant.Privileges = strings.Split(privileges, ",")
			resp.Grants = append(resp.Grants, grant)
		}
	}

	// Marshal SHOW CREATE result.
	rows, cols, err = s.server.internalExecutor.QueryWithCols(
		ctx, "admin-show-create", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		fmt.Sprintf("SHOW CREATE %s", escQualTable),
	)
	if s.isNotFoundError(err) {
		return nil, status.Errorf(codes.NotFound, "%s", err)
	}
	if err != nil {
		return nil, s.serverError(err)
	}
	{
		const createCol = "create_statement"
		if len(rows) != 1 {
			return nil, s.serverErrorf("create response not available.")
		}

		scanner := makeResultScanner(cols)
		var createStmt string
		if err := scanner.Scan(rows[0], createCol, &createStmt); err != nil {
			return nil, err
		}

		resp.CreateTableStatement = createStmt
	}

	var tableID sqlbase.ID
	// Query the descriptor ID and zone configuration for this table.
	{
		path, err := s.queryDescriptorIDPath(ctx, userName, []string{req.Database, req.Table})
		if err != nil {
			return nil, s.serverError(err)
		}
		tableID = path[2]
		resp.DescriptorID = int64(tableID)

		id, zone, zoneExists, err := s.queryZonePath(ctx, userName, path)
		if err != nil {
			return nil, s.serverError(err)
		}

		if !zoneExists {
			zone = s.server.cfg.DefaultZoneConfig
		}
		resp.ZoneConfig = zone

		switch id {
		case path[1]:
			resp.ZoneConfigLevel = serverpb.ZoneConfigurationLevel_DATABASE
		case path[2]:
			resp.ZoneConfigLevel = serverpb.ZoneConfigurationLevel_TABLE
		default:
			resp.ZoneConfigLevel = serverpb.ZoneConfigurationLevel_CLUSTER
		}
	}

	// Get the number of ranges in the table. We get the key span for the table
	// data. Then, we count the number of ranges that make up that key span.
	{
		tableSpan := generateTableSpan(tableID)
		tableRSpan := roachpb.RSpan{}
		var err error
		tableRSpan.Key, err = keys.Addr(tableSpan.Key)
		if err != nil {
			return nil, s.serverError(err)
		}
		tableRSpan.EndKey, err = keys.Addr(tableSpan.EndKey)
		if err != nil {
			return nil, s.serverError(err)
		}
		rangeCount, err := s.server.distSender.CountRanges(ctx, tableRSpan)
		if err != nil {
			return nil, s.serverError(err)
		}
		resp.RangeCount = rangeCount
	}

	// Get the table type of the table, such as "BASE TABLE", "TEMPLATE TABLE", "INSTANCE TABLE"
	var tableType string
	rows, cols, err = s.server.internalExecutor.QueryWithCols(
		ctx, "admin-show-table-type", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		fmt.Sprintf("SELECT * from [ SHOW TABLES FROM %s ] WHERE table_name = '%s'", escDBName, strings.Trim(escTableName, "\"")),
	)
	if s.isNotFoundError(err) {
		return nil, status.Errorf(codes.NotFound, "%s", err)
	}
	if err != nil {
		return nil, s.serverError(err)
	}
	const tbTypeCol = "table_type"
	if len(rows) != 1 {
		return nil, s.serverErrorf("table type response not available.")
	}

	scanner := makeResultScanner(cols)

	if err := scanner.Scan(rows[0], tbTypeCol, &tableType); err != nil {
		return nil, err
	}
	resp.TableType = tableType

	// Get the memory map size of TIME SERIES TABLE by system file
	var totalSize int64
	if tableType == "TIME SERIES TABLE" {
		var tablePathSubStr string = "/tsdb/" + strconv.Itoa(int(tableID)) + "/"
		pid := os.Getpid()
		mapsPath := "/proc/" + strconv.Itoa(pid) + "/maps"

		mapsFile, err := os.Open(mapsPath)
		if err != nil {
			log.Errorf(ctx, "Error opening file %s: %s", mapsPath, err)
		} else {
			defer mapsFile.Close()
			scanner := bufio.NewScanner(mapsFile)
			for scanner.Scan() {
				line := scanner.Text()
				fields := strings.Fields(line)
				if len(fields) > 5 && strings.Contains(fields[5], tablePathSubStr) {
					// Parse the size from the first field (e.g., "00400000-00452000")
					sizeStr := fields[0]
					sizeParts := strings.Split(sizeStr, "-")
					if len(sizeParts) != 2 {
						continue
					}
					start, err := strconv.ParseUint(sizeParts[0], 16, 64)
					if err != nil {
						continue
					}
					end, err := strconv.ParseUint(sizeParts[1], 16, 64)
					if err != nil {
						continue
					}
					size := end - start
					totalSize += int64(size)
				}
			}
		}
	}
	resp.MmapSize = totalSize

	// Get the size of TIME SERIES TABLE by system file
	var tsTableSize int64
	if tableType == "TIME SERIES TABLE" {
		storePath := s.server.cfg.TsStores.Specs[0].Path
		var tablePath string = storePath + "/tsdb/" + strconv.Itoa(int(tableID))

		rootDevNum, err := sysutil.GetDeviceNumber(tablePath)
		if err != nil {
			return nil, err
		}
		if errOuter := filepath.Walk(tablePath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			subDevNum, err := sysutil.GetDeviceNumber(path)
			if err != nil {
				return nil
			}
			if !info.IsDir() && subDevNum == rootDevNum {
				tsTableSize += info.Size()
			}
			return nil
		}); errOuter != nil {
			return nil, errOuter
		}
	}
	resp.TstableSize = tsTableSize

	return &resp, nil
}

// generateTableSpan generates a table's key span.
//
// NOTE: this doesn't make sense for interleaved (children) table. As of
// 03/2018, callers around here use it anyway.
func generateTableSpan(tableID sqlbase.ID) roachpb.Span {
	tablePrefix := keys.MakeTablePrefix(uint32(tableID))
	tableStartKey := roachpb.Key(tablePrefix)
	tableEndKey := tableStartKey.PrefixEnd()
	return roachpb.Span{Key: tableStartKey, EndKey: tableEndKey}
}

// TableStats is an endpoint that returns disk usage and replication statistics
// for the specified table.
func (s *adminServer) TableStats(
	ctx context.Context, req *serverpb.TableStatsRequest,
) (*serverpb.TableStatsResponse, error) {
	// TODO(someone): perform authorization based on the requesting user's
	// SELECT privilege over the requested table.
	userName, err := s.requireAdminUser(ctx)
	if err != nil {
		return nil, err
	}

	// Get table span.
	path, err := s.queryDescriptorIDPath(
		ctx, userName, []string{req.Database, req.Table},
	)
	if err != nil {
		return nil, s.serverError(err)
	}
	tableID := path[2]
	tableSpan := generateTableSpan(tableID)

	return s.statsForSpan(ctx, tableSpan)
}

// NonTableStats is an endpoint that returns disk usage and replication
// statistics for non-table parts of the system.
func (s *adminServer) NonTableStats(
	ctx context.Context, req *serverpb.NonTableStatsRequest,
) (*serverpb.NonTableStatsResponse, error) {
	if _, err := s.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	timeSeriesStats, err := s.statsForSpan(ctx, roachpb.Span{
		Key:    keys.TimeseriesPrefix,
		EndKey: keys.TimeseriesPrefix.PrefixEnd(),
	})
	if err != nil {
		return nil, err
	}
	response := serverpb.NonTableStatsResponse{
		TimeSeriesStats: timeSeriesStats,
	}

	spansForInternalUse := []roachpb.Span{
		{
			Key:    keys.LocalMax,
			EndKey: keys.TimeseriesPrefix,
		},
		{
			Key:    keys.TimeseriesKeyMax,
			EndKey: keys.TableDataMin,
		},
	}
	for _, span := range spansForInternalUse {
		nonTableStats, err := s.statsForSpan(ctx, span)
		if err != nil {
			return nil, err
		}
		if response.InternalUseStats == nil {
			response.InternalUseStats = nonTableStats
		} else {
			response.InternalUseStats.Add(nonTableStats)
		}
	}

	// There are four empty ranges for table descriptors 17, 18, 19, and 22 that
	// aren't actually tables (a.k.a. MetaRangesID, SystemRangesID,
	// TimeseriesRangesID, and LivenessRangesID in pkg/keys).
	// No data is ever really written to them since they don't have actual
	// tables. Some backend work could probably be done to eliminate these empty
	// ranges, but it may be more trouble than it's worth. In the meantime,
	// sweeping them under the general-purpose "Internal use" label in
	// the "Non-Table" section of the Databases page.
	response.InternalUseStats.RangeCount += nonTableDescriptorRangeCount

	return &response, nil
}

func (s *adminServer) statsForSpan(
	ctx context.Context, span roachpb.Span,
) (*serverpb.TableStatsResponse, error) {
	startKey, err := keys.Addr(span.Key)
	if err != nil {
		return nil, s.serverError(err)
	}
	endKey, err := keys.Addr(span.EndKey)
	if err != nil {
		return nil, s.serverError(err)
	}

	// Get current range descriptors for table. This is done by scanning over
	// meta2 keys for the range. A special case occurs if we wish to include
	// the meta1 key range itself, in which case we'll get KeyMin back and that
	// cannot be scanned (due to range-local addressing confusion). This is
	// handled appropriately by adjusting the bounds to grab the descriptors
	// for all ranges (including range1, which is not only gossiped but also
	// persisted in meta1).
	startMetaKey := keys.RangeMetaKey(startKey)
	if bytes.Equal(startMetaKey, roachpb.RKeyMin) {
		// This is the special case described above. The following key instructs
		// the code below to scan all of the addressing, i.e. grab all of the
		// descriptors including that for r1.
		startMetaKey = keys.RangeMetaKey(keys.MustAddr(keys.Meta2Prefix))
	}

	rangeDescKVs, err := s.server.db.Scan(ctx, startMetaKey, keys.RangeMetaKey(endKey), 0)
	if err != nil {
		return nil, s.serverError(err)
	}

	// This map will store the nodes we need to fan out to.
	nodeIDs := make(map[roachpb.NodeID]struct{})
	for _, kv := range rangeDescKVs {
		var rng roachpb.RangeDescriptor
		if err := kv.Value.GetProto(&rng); err != nil {
			return nil, s.serverError(err)
		}
		for _, repl := range rng.Replicas().All() {
			nodeIDs[repl.NodeID] = struct{}{}
		}
	}

	// Construct TableStatsResponse by sending an RPC to every node involved.
	tableStatResponse := serverpb.TableStatsResponse{
		NodeCount: int64(len(nodeIDs)),
		// TODO(mrtracy): The "RangeCount" returned by TableStats is more
		// accurate than the "RangeCount" returned by TableDetails, because this
		// method always consistently queries the meta2 key range for the table;
		// in contrast, TableDetails uses a method on the DistSender, which
		// queries using a range metadata cache and thus may return stale data
		// for tables that are rapidly splitting. However, one potential
		// *advantage* of using the DistSender is that it will populate the
		// DistSender's range metadata cache in the case where meta2 information
		// for this table is not already present; the query used by TableStats
		// does not populate the DistSender cache. We should consider plumbing
		// TableStats' meta2 query through the DistSender so that it will share
		// the advantage of populating the cache (without the disadvantage of
		// potentially returning stale data).
		// See Github #5435 for some discussion.
		RangeCount: int64(len(rangeDescKVs)),
	}
	type nodeResponse struct {
		nodeID roachpb.NodeID
		resp   *serverpb.SpanStatsResponse
		err    error
	}

	// Send a SpanStats query to each node.
	responses := make(chan nodeResponse, len(nodeIDs))
	for nodeID := range nodeIDs {
		nodeID := nodeID // avoid data race
		if err := s.server.stopper.RunAsyncTask(
			ctx, "server.adminServer: requesting remote stats",
			func(ctx context.Context) {
				// Set a generous timeout on the context for each individual query.
				var spanResponse *serverpb.SpanStatsResponse
				err := contextutil.RunWithTimeout(ctx, "request remote stats", 5*base.NetworkTimeout,
					func(ctx context.Context) error {
						client, err := s.server.status.dialNode(ctx, nodeID)
						if err == nil {
							req := serverpb.SpanStatsRequest{
								StartKey: startKey,
								EndKey:   endKey,
								NodeID:   nodeID.String(),
							}
							spanResponse, err = client.SpanStats(ctx, &req)
						}
						return err
					})

				// Channel is buffered, can always write.
				responses <- nodeResponse{
					nodeID: nodeID,
					resp:   spanResponse,
					err:    err,
				}
			}); err != nil {
			return nil, err
		}
	}
	for remainingResponses := len(nodeIDs); remainingResponses > 0; remainingResponses-- {
		select {
		case resp := <-responses:
			// For nodes which returned an error, note that the node's data
			// is missing. For successful calls, aggregate statistics.
			if resp.err != nil {
				tableStatResponse.MissingNodes = append(
					tableStatResponse.MissingNodes,
					serverpb.TableStatsResponse_MissingNode{
						NodeID:       resp.nodeID.String(),
						ErrorMessage: resp.err.Error(),
					},
				)
			} else {
				tableStatResponse.Stats.Add(resp.resp.TotalStats)
				tableStatResponse.ReplicaCount += int64(resp.resp.RangeCount)
				tableStatResponse.ApproximateDiskBytes += resp.resp.ApproximateDiskBytes
			}
		case <-ctx.Done():
			// Caller gave up, stop doing work.
			return nil, ctx.Err()
		}
	}

	return &tableStatResponse, nil
}

// Users returns a list of users, stripped of any passwords.
func (s *adminServer) Users(
	ctx context.Context, req *serverpb.UsersRequest,
) (*serverpb.UsersResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)
	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}
	query := `SELECT username FROM system.users WHERE "isRole" = false`
	rows, err := s.server.internalExecutor.QueryEx(
		ctx, "admin-users", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		query,
	)
	if err != nil {
		return nil, s.serverError(err)
	}

	var resp serverpb.UsersResponse
	for _, row := range rows {
		resp.Users = append(resp.Users, serverpb.UsersResponse_User{Username: string(tree.MustBeDString(row[0]))})
	}
	return &resp, nil
}

// Events is an endpoint that returns the latest event log entries, with the following
// optional URL parameters:
//
// type=STRING  returns events with this type (e.g. "create_table")
// targetID=INT returns events for that have this targetID
func (s *adminServer) Events(
	ctx context.Context, req *serverpb.EventsRequest,
) (*serverpb.EventsResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)

	userName, isAdmin, err := s.getUserAndRole(ctx)
	if err != nil {
		return nil, err
	}
	redactEvents := false
	if isAdmin {
		// We obey the redacted bit only if the user is admin.
		redactEvents = !req.UnredactedEvents
	}

	limit := req.Limit
	if limit == 0 {
		limit = defaultAPIEventLimit
	}

	// Execute the query.
	q := makeSQLQuery()
	q.Append(`SELECT timestamp, "eventType", "targetID", "reportingID", info, "uniqueID" `)
	q.Append("FROM system.eventlog ")
	q.Append("WHERE true ") // This simplifies the WHERE clause logic below.
	if len(req.Type) > 0 {
		q.Append(`AND "eventType" = $ `, req.Type)
	}
	if req.TargetId > 0 {
		q.Append(`AND "targetID" = $ `, req.TargetId)
	}
	q.Append("ORDER BY timestamp DESC ")
	if limit > 0 {
		q.Append("LIMIT $", limit)
	}
	if len(q.Errors()) > 0 {
		return nil, s.serverErrors(q.Errors())
	}
	rows, cols, err := s.server.internalExecutor.QueryWithCols(
		ctx, "admin-events", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		q.String(), q.QueryArguments()...)
	if err != nil {
		return nil, s.serverError(err)
	}

	// Marshal response.
	var resp serverpb.EventsResponse
	scanner := makeResultScanner(cols)
	for _, row := range rows {
		var event serverpb.EventsResponse_Event
		var ts time.Time
		if err := scanner.ScanIndex(row, 0, &ts); err != nil {
			return nil, err
		}
		event.Timestamp = ts
		if err := scanner.ScanIndex(row, 1, &event.EventType); err != nil {
			return nil, err
		}
		if err := scanner.ScanIndex(row, 2, &event.TargetID); err != nil {
			return nil, err
		}
		if err := scanner.ScanIndex(row, 3, &event.ReportingID); err != nil {
			return nil, err
		}
		if err := scanner.ScanIndex(row, 4, &event.Info); err != nil {
			return nil, err
		}
		if event.EventType == string(sql.EventLogSetClusterSetting) {
			if redactEvents {
				event.Info = redactSettingsChange(event.Info)
			}
		}
		if err := scanner.ScanIndex(row, 5, &event.UniqueID); err != nil {
			return nil, err
		}

		resp.Events = append(resp.Events, event)
	}
	return &resp, nil
}

// make a best-effort attempt at redacting the setting value.
func redactSettingsChange(info string) string {
	var s sql.EventLogSetClusterSettingDetail
	if err := json.Unmarshal([]byte(info), &s); err != nil {
		return ""
	}
	s.Value = "<hidden>"
	ret, err := json.Marshal(s)
	if err != nil {
		return ""
	}
	return string(ret)
}

// RangeLog is an endpoint that returns the latest range log entries.
func (s *adminServer) RangeLog(
	ctx context.Context, req *serverpb.RangeLogRequest,
) (*serverpb.RangeLogResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)

	// Range keys, even when pretty-printed, contain PII.
	userName, err := s.requireAdminUser(ctx)
	if err != nil {
		return nil, err
	}

	limit := req.Limit
	if limit == 0 {
		limit = defaultAPIEventLimit
	}

	includeRawKeys := debug.GatewayRemoteAllowed(ctx, s.server.ClusterSettings())

	// Execute the query.
	q := makeSQLQuery()
	q.Append(`SELECT timestamp, "rangeID", "storeID", "eventType", "otherRangeID", info `)
	q.Append("FROM system.rangelog ")
	if req.RangeId > 0 {
		rangeID := tree.NewDInt(tree.DInt(req.RangeId))
		q.Append(`WHERE "rangeID" = $ OR "otherRangeID" = $`, rangeID, rangeID)
	}
	if limit > 0 {
		q.Append("ORDER BY timestamp desc ")
		q.Append("LIMIT $", tree.NewDInt(tree.DInt(limit)))
	}
	if len(q.Errors()) > 0 {
		return nil, s.serverErrors(q.Errors())
	}
	rows, cols, err := s.server.internalExecutor.QueryWithCols(
		ctx, "admin-range-log", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		q.String(), q.QueryArguments()...,
	)
	if err != nil {
		return nil, s.serverError(err)
	}

	// Marshal response.
	var resp serverpb.RangeLogResponse
	if len(cols) != 6 {
		return nil, errors.Errorf("incorrect number of columns in response, expected 6, got %d", len(cols))
	}
	scanner := makeResultScanner(cols)
	for _, row := range rows {
		var event storagepb.RangeLogEvent
		var ts time.Time
		if err := scanner.ScanIndex(row, 0, &ts); err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("Timestamp didn't parse correctly: %s", row[0].String()))
		}
		event.Timestamp = ts
		var rangeID int64
		if err := scanner.ScanIndex(row, 1, &rangeID); err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("RangeID didn't parse correctly: %s", row[1].String()))
		}
		event.RangeID = roachpb.RangeID(rangeID)
		var storeID int64
		if err := scanner.ScanIndex(row, 2, &storeID); err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("StoreID didn't parse correctly: %s", row[2].String()))
		}
		event.StoreID = roachpb.StoreID(int32(storeID))
		var eventTypeString string
		if err := scanner.ScanIndex(row, 3, &eventTypeString); err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("EventType didn't parse correctly: %s", row[3].String()))
		}
		if eventType, ok := storagepb.RangeLogEventType_value[eventTypeString]; ok {
			event.EventType = storagepb.RangeLogEventType(eventType)
		} else {
			return nil, errors.Errorf("EventType didn't parse correctly: %s", eventTypeString)
		}

		var otherRangeID int64
		if row[4].String() != "NULL" {
			if err := scanner.ScanIndex(row, 4, &otherRangeID); err != nil {
				return nil, errors.Wrap(err, fmt.Sprintf("OtherRangeID didn't parse correctly: %s", row[4].String()))
			}
			event.OtherRangeID = roachpb.RangeID(otherRangeID)
		}

		var prettyInfo serverpb.RangeLogResponse_PrettyInfo
		if row[5].String() != "NULL" {
			var info string
			if err := scanner.ScanIndex(row, 5, &info); err != nil {
				return nil, errors.Wrap(err, fmt.Sprintf("info didn't parse correctly: %s", row[5].String()))
			}
			if err := json.Unmarshal([]byte(info), &event.Info); err != nil {
				return nil, errors.Wrap(err, fmt.Sprintf("info didn't parse correctly: %s", info))
			}
			if event.Info.NewDesc != nil {
				if !includeRawKeys {
					event.Info.NewDesc.StartKey = nil
					event.Info.NewDesc.EndKey = nil
				}
				prettyInfo.NewDesc = event.Info.NewDesc.String()
			}
			if event.Info.UpdatedDesc != nil {
				if !includeRawKeys {
					event.Info.UpdatedDesc.StartKey = nil
					event.Info.UpdatedDesc.EndKey = nil
				}
				prettyInfo.UpdatedDesc = event.Info.UpdatedDesc.String()
			}
			if event.Info.AddedReplica != nil {
				prettyInfo.AddedReplica = event.Info.AddedReplica.String()
			}
			if event.Info.RemovedReplica != nil {
				prettyInfo.RemovedReplica = event.Info.RemovedReplica.String()
			}
			prettyInfo.Reason = string(event.Info.Reason)
			prettyInfo.Details = event.Info.Details
		}

		resp.Events = append(resp.Events, serverpb.RangeLogResponse_Event{
			Event:      event,
			PrettyInfo: prettyInfo,
		})
	}
	return &resp, nil
}

// getUIData returns the values and timestamps for the given UI keys. Keys
// that are not found will not be returned.
func (s *adminServer) getUIData(
	ctx context.Context, userName string, keys []string,
) (*serverpb.GetUIDataResponse, error) {
	if len(keys) == 0 {
		return &serverpb.GetUIDataResponse{}, nil
	}

	// Query database.
	query := makeSQLQuery()
	query.Append(`SELECT key, value, "lastUpdated" FROM system.ui WHERE key IN (`)
	for i, key := range keys {
		if i != 0 {
			query.Append(",")
		}
		query.Append("$", tree.NewDString(makeUIKey(userName, key)))
	}
	query.Append(");")
	if err := query.Errors(); err != nil {
		return nil, s.serverErrorf("error constructing query: %v", err)
	}
	rows, err := s.server.internalExecutor.QueryEx(
		ctx, "admin-getUIData", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		query.String(), query.QueryArguments()...,
	)
	if err != nil {
		return nil, s.serverError(err)
	}

	// Marshal results.
	resp := serverpb.GetUIDataResponse{KeyValues: make(map[string]serverpb.GetUIDataResponse_Value)}
	for _, row := range rows {
		dKey, ok := tree.AsDString(row[0])
		if !ok {
			return nil, s.serverErrorf("unexpected type for UI key: %T", row[0])
		}
		_, key := splitUIKey(string(dKey))
		dKey = tree.DString(key)

		dValue, ok := row[1].(*tree.DBytes)
		if !ok {
			return nil, s.serverErrorf("unexpected type for UI value: %T", row[1])
		}
		dLastUpdated, ok := row[2].(*tree.DTimestamp)
		if !ok {
			return nil, s.serverErrorf("unexpected type for UI lastUpdated: %T", row[2])
		}

		resp.KeyValues[string(dKey)] = serverpb.GetUIDataResponse_Value{
			Value:       []byte(*dValue),
			LastUpdated: dLastUpdated.Time,
		}
	}
	return &resp, nil
}

// makeUIKey combines username and key to form a lookup key in
// system.ui.
// The username is combined to ensure that different users
// can use different customizations.
func makeUIKey(username, key string) string {
	return username + "$" + key
}

// splitUIKey is the inverse of makeUIKey.
// The caller must ensure that the value was produced by makeUIKey.
func splitUIKey(combined string) (string, string) {
	pair := strings.SplitN(combined, "$", 2)
	return pair[0], pair[1]
}

// SetUIData is an endpoint that stores the given key/value pairs in the
// system.ui table. See GetUIData for more details on semantics.
func (s *adminServer) SetUIData(
	ctx context.Context, req *serverpb.SetUIDataRequest,
) (*serverpb.SetUIDataResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)

	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	if len(req.KeyValues) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "KeyValues cannot be empty")
	}

	for key, val := range req.KeyValues {
		// Do an upsert of the key. We update each key in a separate transaction to
		// avoid long-running transactions and possible deadlocks.
		query := `UPSERT INTO system.ui (key, value, "lastUpdated") VALUES ($1, $2, now())`
		rowsAffected, err := s.server.internalExecutor.ExecEx(
			ctx, "admin-set-ui-data", nil, /* txn */
			sqlbase.InternalExecutorSessionDataOverride{
				User: security.RootUser,
			},
			query, makeUIKey(userName, key), val)
		if err != nil {
			return nil, s.serverError(err)
		}
		if rowsAffected != 1 {
			return nil, s.serverErrorf("rows affected %d != expected %d", rowsAffected, 1)
		}
	}

	return &serverpb.SetUIDataResponse{}, nil
}

// GetUIData returns data associated with the given keys, which was stored
// earlier through SetUIData.
//
// The stored values are meant to be opaque to the server. In the rare case that
// the server code needs to call this method, it should only read from keys that
// have the prefix `serverUIDataKeyPrefix`.
func (s *adminServer) GetUIData(
	ctx context.Context, req *serverpb.GetUIDataRequest,
) (*serverpb.GetUIDataResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)

	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	if len(req.Keys) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "keys cannot be empty")
	}

	resp, err := s.getUIData(ctx, userName, req.Keys)
	if err != nil {
		return nil, s.serverError(err)
	}

	return resp, nil
}

// Settings returns settings associated with the given keys.
func (s *adminServer) Settings(
	ctx context.Context, req *serverpb.SettingsRequest,
) (*serverpb.SettingsResponse, error) {
	keys := req.Keys
	if len(keys) == 0 {
		keys = settings.Keys()
	}

	sessionUser, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	isAdmin, err := s.hasAdminRole(ctx, sessionUser)
	if err != nil {
		return nil, err
	}

	var lookupPurpose settings.LookupPurpose
	if isAdmin {
		// Root accesses can customize the purpose.
		// This is used by the UI to see all values (local access)
		// and `kwbase zip` to redact the values (telemetry).
		lookupPurpose = settings.LookupForReporting
		if req.UnredactedValues {
			lookupPurpose = settings.LookupForLocalAccess
		}
	} else {
		// Non-root access cannot see the values in any case.
		lookupPurpose = settings.LookupForReporting
	}

	resp := serverpb.SettingsResponse{KeyValues: make(map[string]serverpb.SettingsResponse_Value)}
	for _, k := range keys {
		v, ok := settings.Lookup(k, lookupPurpose)
		if !ok {
			continue
		}
		resp.KeyValues[k] = serverpb.SettingsResponse_Value{
			Type: v.Typ(),
			// Note: v.String() redacts the values if the purpose is not "LocalAccess".
			Value:       v.String(&s.server.st.SV),
			Description: v.Description(),
			Public:      v.Visibility() == settings.Public,
		}
	}

	return &resp, nil
}

// Cluster returns cluster metadata.
func (s *adminServer) Cluster(
	_ context.Context, req *serverpb.ClusterRequest,
) (*serverpb.ClusterResponse, error) {
	clusterID := s.server.ClusterID()
	if clusterID == (uuid.UUID{}) {
		return nil, status.Errorf(codes.Unavailable, "cluster ID not yet available")
	}

	// Check if enterprise features are enabled.  We currently test for the
	// feature "BACKUP", although enterprise licenses do not yet distinguish
	// between different features.
	organization := sql.ClusterOrganization.Get(&s.server.st.SV)
	enterpriseEnabled := base.CheckEnterpriseEnabled(s.server.st, clusterID, organization, "BACKUP") == nil

	return &serverpb.ClusterResponse{
		ClusterID:         clusterID.String(),
		ReportingEnabled:  log.DiagnosticsReportingEnabled.Get(&s.server.st.SV),
		EnterpriseEnabled: enterpriseEnabled,
	}, nil
}

// Health returns liveness for the node target of the request.
//
// See the docstring for HealthRequest for more details about
// what this function precisely reports.
//
// Note: Health is non-privileged and non-authenticated and thus
// must not report privileged information.
func (s *adminServer) Health(
	ctx context.Context, req *serverpb.HealthRequest,
) (*serverpb.HealthResponse, error) {
	telemetry.Inc(telemetryHealthCheck)

	resp := &serverpb.HealthResponse{}
	// If Ready is not set, the client doesn't want to know whether this node is
	// ready to receive client traffic.
	if !req.Ready {
		return resp, nil
	}

	if err := s.checkReadinessForHealthCheck(); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *adminServer) checkReadinessForHealthCheck() error {
	serveMode := s.server.grpc.mode.get()
	switch serveMode {
	case modeInitializing:
		return status.Error(codes.Unavailable, "node is waiting for cluster initialization")
	case modeDraining:
		// grpc.mode is set to modeDraining when the Drain(DrainMode_CLIENT) has
		// been called (client connections are to be drained).
		return status.Errorf(codes.Unavailable, "node is shutting down")
	case modeOperational:
		break
	default:
		return s.serverError(errors.Newf("unknown mode: %v", serveMode))
	}

	// TODO(knz): update this code when progress is made on
	// https://gitee.com/kwbasedb/kwbase/issues/45123
	l, err := s.server.nodeLiveness.GetLiveness(s.server.NodeID())
	if err != nil {
		return s.serverError(err)
	}
	if !l.IsLive(s.server.clock.Now().GoTime()) {
		return status.Errorf(codes.Unavailable, "node is not healthy")
	}
	if l.Draining {
		// l.Draining indicates that the node is draining leases.
		// This is set when Drain(DrainMode_LEASES) is called.
		// It's possible that l.Draining is set without
		// grpc.mode being modeDraining, if a RPC client
		// has requested DrainMode_LEASES but not DrainMode_CLIENT.
		return status.Errorf(codes.Unavailable, "node is shutting down")
	}

	return nil
}

// getLivenessStatusMap generates a map from NodeID to LivenessStatus for all
// nodes known to gossip. Nodes that haven't pinged their liveness record for
// more than server.time_until_store_dead are considered dead.
//
// To include all nodes (including ones not in the gossip network), callers
// should consider calling (statusServer).NodesWithLiveness() instead where
// possible.
//
// getLivenessStatusMap() includes removed nodes (dead + decommissioned).
func getLivenessStatusMap(
	nl *kvserver.NodeLiveness, now time.Time, st *cluster.Settings,
) map[roachpb.NodeID]storagepb.NodeLivenessStatus {
	livenesses := nl.GetLivenesses()
	threshold := kvserver.TimeUntilStoreDead.Get(&st.SV)

	statusMap := make(map[roachpb.NodeID]storagepb.NodeLivenessStatus, len(livenesses))
	for _, liveness := range livenesses {
		status := kvserver.LivenessStatus(liveness, now, threshold)
		statusMap[liveness.NodeID] = status
	}
	return statusMap
}

// Liveness returns the liveness state of all nodes on the cluster
// known to gossip. To reach all nodes in the cluster, consider
// using (statusServer).NodesWithLiveness instead.
func (s *adminServer) Liveness(
	context.Context, *serverpb.LivenessRequest,
) (*serverpb.LivenessResponse, error) {
	clock := s.server.clock
	statusMap := getLivenessStatusMap(
		s.server.nodeLiveness, clock.Now().GoTime(), s.server.st)
	livenesses := s.server.nodeLiveness.GetLivenesses()
	return &serverpb.LivenessResponse{
		Livenesses: livenesses,
		Statuses:   statusMap,
	}, nil
}

func (s *adminServer) Jobs(
	ctx context.Context, req *serverpb.JobsRequest,
) (*serverpb.JobsResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)

	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	q := makeSQLQuery()
	q.Append(`
      SELECT job_id, job_type, description, statement, user_name, descriptor_ids, status,
						 running_status, created, started, finished, modified,
						 fraction_completed, high_water_timestamp, error
        FROM kwdb_internal.jobs
       WHERE true
	`)
	if req.Status != "" {
		q.Append(" AND status = $", req.Status)
	}
	if req.Type != jobspb.TypeUnspecified {
		q.Append(" AND job_type = $", req.Type.String())
	} else {
		// Don't show auto stats jobs in the overview page.
		q.Append(" AND (job_type != $ OR job_type IS NULL)", jobspb.TypeAutoCreateStats.String())
	}
	q.Append("ORDER BY created DESC")
	if req.Limit > 0 {
		q.Append(" LIMIT $", tree.DInt(req.Limit))
	}
	rows, cols, err := s.server.internalExecutor.QueryWithCols(
		ctx, "admin-jobs", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		q.String(), q.QueryArguments()...,
	)
	if err != nil {
		return nil, s.serverError(err)
	}

	scanner := makeResultScanner(cols)
	resp := serverpb.JobsResponse{
		Jobs: make([]serverpb.JobsResponse_Job, len(rows)),
	}
	for i, row := range rows {
		job := &resp.Jobs[i]
		var fractionCompletedOrNil *float32
		var highwaterOrNil *apd.Decimal
		var runningStatusOrNil *string
		if err := scanner.ScanAll(
			row,
			&job.ID,
			&job.Type,
			&job.Description,
			&job.Statement,
			&job.Username,
			&job.DescriptorIDs,
			&job.Status,
			&runningStatusOrNil,
			&job.Created,
			&job.Started,
			&job.Finished,
			&job.Modified,
			&fractionCompletedOrNil,
			&highwaterOrNil,
			&job.Error,
		); err != nil {
			return nil, s.serverError(err)
		}
		if highwaterOrNil != nil {
			highwaterTimestamp, err := tree.DecimalToHLC(highwaterOrNil)
			if err != nil {
				return nil, s.serverError(errors.Wrap(err, "highwater timestamp had unexpected format"))
			}
			goTime := highwaterTimestamp.GoTime()
			job.HighwaterTimestamp = &goTime
			job.HighwaterDecimal = highwaterOrNil.String()
		}
		if fractionCompletedOrNil != nil {
			job.FractionCompleted = *fractionCompletedOrNil
		}
		if runningStatusOrNil != nil {
			job.RunningStatus = *runningStatusOrNil
		}
	}

	return &resp, nil
}

func (s *adminServer) Queries(
	ctx context.Context, req *serverpb.QueriesRequest,
) (*serverpb.QueriesResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)
	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	q := makeSQLQuery()
	q.Append(`
      SELECT node_id, user_name, start, query, phase, application_name
		FROM kwdb_internal.cluster_queries
		WHERE true
	`)
	q.Append(" AND application_name != $", "$ internal-admin-queries")
	rows, cols, err := s.server.internalExecutor.QueryWithCols(
		ctx, "admin-queries", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		q.String(),
		q.QueryArguments()...,
	)
	if nil != err {
		return nil, err
	}

	scanner := makeResultScanner(cols)
	resp := serverpb.QueriesResponse{
		Queries: make([]serverpb.QueriesResponse_Query, len(rows)),
	}
	for i, row := range rows {
		query := &resp.Queries[i]
		if err := scanner.ScanAll(
			row,
			&query.NodeId,
			&query.UserName,
			&query.Start,
			&query.Query,
			&query.Phase,
			&query.ApplicationName,
		); nil != err {
			return nil, s.serverError(err)
		}
	}

	return &resp, nil
}

func (s *adminServer) Locations(
	ctx context.Context, req *serverpb.LocationsRequest,
) (*serverpb.LocationsResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)

	_, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	q := makeSQLQuery()
	q.Append(`SELECT "localityKey", "localityValue", latitude, longitude FROM system.locations`)
	rows, cols, err := s.server.internalExecutor.QueryWithCols(
		ctx, "admin-locations", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		q.String(),
	)
	if err != nil {
		return nil, s.serverError(err)
	}

	scanner := makeResultScanner(cols)
	resp := serverpb.LocationsResponse{
		Locations: make([]serverpb.LocationsResponse_Location, len(rows)),
	}
	for i, row := range rows {
		loc := &resp.Locations[i]
		lat, lon := new(apd.Decimal), new(apd.Decimal)
		if err := scanner.ScanAll(
			row, &loc.LocalityKey, &loc.LocalityValue, lat, lon); err != nil {
			return nil, s.serverError(err)
		}
		if loc.Latitude, err = lat.Float64(); err != nil {
			return nil, s.serverError(err)
		}
		if loc.Longitude, err = lon.Float64(); err != nil {
			return nil, s.serverError(err)
		}
	}

	return &resp, nil
}

// QueryPlan returns a JSON representation of a distsql physical query
// plan.
func (s *adminServer) QueryPlan(
	ctx context.Context, req *serverpb.QueryPlanRequest,
) (*serverpb.QueryPlanResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)

	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	// As long as there's only one query provided it's safe to construct the
	// explain query.
	stmts, err := parser.Parse(req.Query)
	if err != nil {
		return nil, s.serverError(err)
	}
	if len(stmts) > 1 {
		return nil, s.serverErrorf("more than one query provided")
	}

	explain := fmt.Sprintf(
		"SELECT json FROM [EXPLAIN (DISTSQL) %s]",
		strings.Trim(req.Query, ";"))
	rows, err := s.server.internalExecutor.QueryEx(
		ctx, "admin-query-plan", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		explain,
	)
	if err != nil {
		return nil, s.serverError(err)
	}

	row := rows[0]
	dbDatum, ok := tree.AsDString(row[0])
	if !ok {
		return nil, s.serverErrorf("type assertion failed on json: %T", row)
	}

	return &serverpb.QueryPlanResponse{
		DistSQLPhysicalQueryPlan: string(dbDatum),
	}, nil
}

// getStatementBundle retrieves the statement bundle with the given id and
// writes it out as an attachment.
func (s *adminServer) getStatementBundle(ctx context.Context, id int64, w http.ResponseWriter) {
	sessionUser, err := userFromContext(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	row, err := s.server.internalExecutor.QueryRowEx(
		ctx, "admin-stmt-bundle", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: sessionUser},
		"SELECT bundle_chunks FROM system.statement_diagnostics WHERE id=$1 AND bundle_chunks IS NOT NULL",
		id,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if row == nil {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	// Put together the entire bundle. Ideally we would stream it in chunks,
	// but it's hard to return errors once we start.
	var bundle bytes.Buffer
	chunkIDs := row[0].(*tree.DArray).Array
	for _, chunkID := range chunkIDs {
		chunkRow, err := s.server.internalExecutor.QueryRowEx(
			ctx, "admin-stmt-bundle", nil, /* txn */
			sqlbase.InternalExecutorSessionDataOverride{User: sessionUser},
			"SELECT data FROM system.statement_bundle_chunks WHERE id=$1",
			chunkID,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if row == nil {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}
		data := chunkRow[0].(*tree.DBytes)
		bundle.WriteString(string(*data))
	}

	w.Header().Set(
		"Content-Disposition",
		fmt.Sprintf("attachment; filename=stmt-bundle-%d.zip", id),
	)

	_, _ = io.Copy(w, &bundle)
}

// AuditLog log audit data when node quit
func (s *adminServer) AuditLog(ctx context.Context, auditLog *server.AuditInfo) {
	if err := s.server.auditServer.LogAudit(ctx, nil, auditLog); err != nil {
		log.Warningf(ctx, "unable to log %s event, error:%s", auditLog.GetEvent(), err)
	}
}

// DecommissionStatus returns the DecommissionStatus for all or the given nodes.
func (s *adminServer) DecommissionStatus(
	ctx context.Context, req *serverpb.DecommissionStatusRequest,
) (*serverpb.DecommissionStatusResponse, error) {
	// Get the number of replicas on each node. We *may* not need all of them,
	// but that would be more complicated than seems worth it right now.
	ns, err := s.server.status.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, errors.Wrap(err, "loading node statuses")
	}

	nodeIDs := req.NodeIDs
	// If no nodeIDs given, use all nodes.
	if len(nodeIDs) == 0 {
		for _, status := range ns.Nodes {
			nodeIDs = append(nodeIDs, status.Desc.NodeID)
		}
	}

	// Compute the replica counts for the target nodes only. This map doubles as
	// a lookup table to check whether we care about a given node.
	var replicaCounts map[roachpb.NodeID]int64
	if err := s.server.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		const pageSize = 10000
		replicaCounts = make(map[roachpb.NodeID]int64)
		for _, nodeID := range nodeIDs {
			replicaCounts[nodeID] = 0
		}
		return txn.Iterate(ctx, keys.MetaMin, keys.MetaMax, pageSize,
			func(rows []kv.KeyValue) error {
				rangeDesc := roachpb.RangeDescriptor{}
				for _, row := range rows {
					if err := row.ValueProto(&rangeDesc); err != nil {
						return errors.Wrapf(err, "%s: unable to unmarshal range descriptor", row.Key)
					}
					for _, r := range rangeDesc.Replicas().All() {
						if _, ok := replicaCounts[r.NodeID]; ok {
							replicaCounts[r.NodeID]++
						}
					}
				}
				return nil
			})
	}); err != nil {
		return nil, err
	}

	var res serverpb.DecommissionStatusResponse

	for nodeID := range replicaCounts {
		l, err := s.server.nodeLiveness.GetLiveness(nodeID)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to get liveness for %d", nodeID)
		}

		nodeResp := serverpb.DecommissionStatusResponse_Status{
			NodeID:          l.NodeID,
			ReplicaCount:    replicaCounts[l.NodeID],
			Decommissioning: l.Decommissioning,
			Draining:        l.Draining,
		}
		if l.IsLive(s.server.clock.Now().GoTime()) {
			nodeResp.IsLive = true
		}

		res.Status = append(res.Status, nodeResp)
	}

	sort.Slice(res.Status, func(i, j int) bool {
		return res.Status[i].NodeID < res.Status[j].NodeID
	})

	return &res, nil
}

// Decommission sets the decommission flag to the specified value on the specified node(s).
func (s *adminServer) Decommission(
	ctx context.Context, req *serverpb.DecommissionRequest,
) (*serverpb.DecommissionStatusResponse, error) {
	if s.server.execCfg.StartMode == sql.StartSingleReplica {
		return nil, errors.New("node cannot be decommissioned in mpp mode")
	}
	nodeIDs := req.NodeIDs
	if nodeIDs == nil {
		// If no NodeIDs are specified, decommission the current node. This is
		// used by `quit --decommission`.
		// TODO(knz): This behavior is deprecated in 20.1. Remove in 20.2.
		nodeIDs = []roachpb.NodeID{s.server.NodeID()}
	}

	// Mark the target nodes as decommissioning. They'll find out as they
	// heartbeat their liveness.
	if err := s.server.Decommission(ctx, req.Decommissioning, nodeIDs); err != nil {
		return nil, err
	}
	return s.DecommissionStatus(ctx, &serverpb.DecommissionStatusRequest{NodeIDs: nodeIDs})
}

// UpgradeStatus returns the UpgradeStatus for all or the given nodes.
func (s *adminServer) UpgradeStatus(
	ctx context.Context, req *serverpb.UpgradeStatusRequest,
) (*serverpb.UpgradeStatusResponse, error) {
	// Get the number of replicas on each node. We *may* not need all of them,
	// but that would be more complicated than seems worth it right now.
	ns, err := s.server.status.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, errors.Wrap(err, "loading node statuses")
	}

	nodeIDs := req.NodeIDs
	// If no nodeIDs given, use all nodes.
	if len(nodeIDs) == 0 {
		for _, status := range ns.Nodes {
			nodeIDs = append(nodeIDs, status.Desc.NodeID)
		}
	}

	// Compute the replica counts for the target nodes only. This map doubles as
	// a lookup table to check whether we care about a given node.
	var replicaCounts map[roachpb.NodeID]int64
	if err := s.server.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		const pageSize = 10000
		replicaCounts = make(map[roachpb.NodeID]int64)
		for _, nodeID := range nodeIDs {
			replicaCounts[nodeID] = 0
		}
		return txn.Iterate(ctx, keys.MetaMin, keys.MetaMax, pageSize,
			func(rows []kv.KeyValue) error {
				rangeDesc := roachpb.RangeDescriptor{}
				for _, row := range rows {
					if err := row.ValueProto(&rangeDesc); err != nil {
						return errors.Wrapf(err, "%s: unable to unmarshal range descriptor", row.Key)
					}
					for _, r := range rangeDesc.Replicas().All() {
						if _, ok := replicaCounts[r.NodeID]; ok {
							replicaCounts[r.NodeID]++
						}
					}
				}
				return nil
			})
	}); err != nil {
		return nil, err
	}

	var res serverpb.UpgradeStatusResponse

	for nodeID := range replicaCounts {
		l, err := s.server.nodeLiveness.GetLiveness(nodeID)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to get liveness for %d", nodeID)
		}

		nodeResp := serverpb.UpgradeStatusResponse_Status{
			NodeID:       l.NodeID,
			ReplicaCount: replicaCounts[l.NodeID],
			Upgrading:    l.Upgrading,
			Draining:     l.Draining,
		}
		if l.IsLive(s.server.clock.Now().GoTime()) {
			nodeResp.IsLive = true
		}

		res.Status = append(res.Status, nodeResp)
	}

	sort.Slice(res.Status, func(i, j int) bool {
		return res.Status[i].NodeID < res.Status[j].NodeID
	})

	return &res, nil
}

// Upgrade sets the upgrade flag to the specified value on the specified node(s).
func (s *adminServer) Upgrade(
	ctx context.Context, req *serverpb.UpgradeRequest,
) (*serverpb.UpgradeStatusResponse, error) {
	nodeIDs := req.NodeIDs
	if nodeIDs == nil {
		// If no NodeIDs are specified, decommission the current node. This is
		// used by `quit --decommission`.
		// TODO(knz): This behavior is deprecated in 20.1. Remove in 20.2.
		nodeIDs = []roachpb.NodeID{s.server.NodeID()}
	}

	// Mark the target nodes as upgrading. They'll find out as they
	// heartbeat their liveness.
	if err := s.server.Upgrade(ctx, req.Upgrading, nodeIDs); err != nil {
		return nil, err
	}
	return s.UpgradeStatus(ctx, &serverpb.UpgradeStatusRequest{NodeIDs: nodeIDs})
}

// DataDistribution returns a count of replicas on each node for each table.
func (s *adminServer) DataDistribution(
	ctx context.Context, req *serverpb.DataDistributionRequest,
) (*serverpb.DataDistributionResponse, error) {
	if _, err := s.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	resp := &serverpb.DataDistributionResponse{
		DatabaseInfo: make(map[string]serverpb.DataDistributionResponse_DatabaseInfo),
		ZoneConfigs:  make(map[string]serverpb.DataDistributionResponse_ZoneConfig),
	}

	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	// Get ids and names for databases and tables.
	// Set up this structure in the response.

	// This relies on kwdb_internal.tables returning data even for newly added tables
	// and deleted tables (as opposed to e.g. information_schema) because we are interested
	// in the data for all ranges, not just ranges for visible tables.
	tablesQuery := `SELECT name, table_id, database_name, drop_time FROM "".kwdb_internal.tables WHERE schema_name = 'public'`
	rows1, err := s.server.internalExecutor.QueryEx(
		ctx, "admin-replica-matrix", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		tablesQuery,
	)
	if err != nil {
		return nil, s.serverError(err)
	}

	// Used later when we're scanning Meta2 and only have IDs, not names.
	tableInfosByTableID := map[uint64]serverpb.DataDistributionResponse_TableInfo{}

	for _, row := range rows1 {
		tableName := (*string)(row[0].(*tree.DString))
		tableID := uint64(tree.MustBeDInt(row[1]))
		dbName := (*string)(row[2].(*tree.DString))
		// Look at whether it was dropped.
		var droppedAtTime *time.Time
		droppedAtDatum, ok := row[3].(*tree.DTimestamp)
		if ok {
			droppedAtTime = &droppedAtDatum.Time
		}

		// Insert database if it doesn't exist.
		dbInfo, ok := resp.DatabaseInfo[*dbName]
		if !ok {
			dbInfo = serverpb.DataDistributionResponse_DatabaseInfo{
				TableInfo: make(map[string]serverpb.DataDistributionResponse_TableInfo),
			}
			resp.DatabaseInfo[*dbName] = dbInfo
		}

		// Get zone config for table.
		zcID := int64(0)

		if droppedAtTime == nil {
			// TODO(vilterp): figure out a way to get zone configs for tables that are dropped
			zoneConfigQuery := fmt.Sprintf(
				`SELECT zone_id FROM [SHOW ZONE CONFIGURATION FOR TABLE %s.%s]`,
				(*tree.Name)(dbName), (*tree.Name)(tableName),
			)
			rows, err := s.server.internalExecutor.QueryEx(
				ctx, "admin-replica-matrix", nil, /* txn */
				sqlbase.InternalExecutorSessionDataOverride{User: userName},
				zoneConfigQuery,
			)
			if err != nil {
				return nil, s.serverError(err)
			}

			if len(rows) != 1 {
				return nil, s.serverError(fmt.Errorf(
					"could not get zone config for table %s; %d rows returned", *tableName, len(rows),
				))
			}
			zcRow := rows[0]
			zcID = int64(tree.MustBeDInt(zcRow[0]))
		}

		// Insert table.
		tableInfo := serverpb.DataDistributionResponse_TableInfo{
			ReplicaCountByNodeId: make(map[roachpb.NodeID]int64),
			ZoneConfigId:         zcID,
			DroppedAt:            droppedAtTime,
		}
		dbInfo.TableInfo[*tableName] = tableInfo
		tableInfosByTableID[tableID] = tableInfo
	}

	// Get replica counts.
	if err := s.server.db.Txn(ctx, func(txnCtx context.Context, txn *kv.Txn) error {
		acct := s.memMonitor.MakeBoundAccount()
		defer acct.Close(txnCtx)

		kvs, err := sql.ScanMetaKVs(ctx, txn, roachpb.Span{
			Key:    keys.UserTableDataMin,
			EndKey: keys.MaxKey,
		})
		if err != nil {
			return err
		}

		// Group replicas by table and node, accumulate counts.
		var rangeDesc roachpb.RangeDescriptor
		for _, kv := range kvs {
			if err := acct.Grow(txnCtx, int64(len(kv.Key)+len(kv.Value.RawBytes))); err != nil {
				return err
			}
			if err := kv.ValueProto(&rangeDesc); err != nil {
				return err
			}

			_, tableID, err := keys.DecodeTablePrefix(rangeDesc.StartKey.AsRawKey())
			if err != nil {
				return err
			}

			for _, replicaDesc := range rangeDesc.Replicas().All() {
				tableInfo, ok := tableInfosByTableID[tableID]
				if !ok {
					// This is a database, skip.
					continue
				}
				tableInfo.ReplicaCountByNodeId[replicaDesc.NodeID]++
			}
		}
		return nil
	}); err != nil {
		return nil, s.serverError(err)
	}

	// Get zone configs.
	// TODO(vilterp): this can be done in parallel with getting table/db names and replica counts.
	zoneConfigsQuery := `
		SELECT target, raw_config_sql, raw_config_protobuf
		FROM kwdb_internal.zones
		WHERE target IS NOT NULL
	`
	rows2, err := s.server.internalExecutor.QueryEx(
		ctx, "admin-replica-matrix", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		zoneConfigsQuery)
	if err != nil {
		return nil, s.serverError(err)
	}

	for _, row := range rows2 {
		target := string(tree.MustBeDString(row[0]))
		zcSQL := tree.MustBeDString(row[1])
		zcBytes := tree.MustBeDBytes(row[2])
		var zcProto zonepb.ZoneConfig
		if err := protoutil.Unmarshal([]byte(zcBytes), &zcProto); err != nil {
			return nil, s.serverError(err)
		}

		resp.ZoneConfigs[target] = serverpb.DataDistributionResponse_ZoneConfig{
			Target:    target,
			Config:    zcProto,
			ConfigSQL: string(zcSQL),
		}
	}

	return resp, nil
}

// EnqueueRange runs the specified range through the specified queue, returning
// the detailed trace and error information from doing so.
func (s *adminServer) EnqueueRange(
	ctx context.Context, req *serverpb.EnqueueRangeRequest,
) (*serverpb.EnqueueRangeResponse, error) {
	if _, err := s.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	if !debug.GatewayRemoteAllowed(ctx, s.server.ClusterSettings()) {
		return nil, remoteDebuggingErr
	}

	ctx = propagateGatewayMetadata(ctx)
	ctx = s.server.AnnotateCtx(ctx)

	if req.NodeID < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "node_id must be non-negative; got %d", req.NodeID)
	}
	if req.Queue == "" {
		return nil, status.Errorf(codes.InvalidArgument, "queue name must be non-empty")
	}
	if req.RangeID <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "range_id must be positive; got %d", req.RangeID)
	}

	// If the request is targeted at this node, serve it directly. Otherwise,
	// forward it to the appropriate node(s). If no node was specified, forward
	// it to all nodes.
	if req.NodeID == s.server.NodeID() {
		return s.enqueueRangeLocal(ctx, req)
	} else if req.NodeID != 0 {
		admin, err := s.dialNode(ctx, req.NodeID)
		if err != nil {
			return nil, err
		}
		return admin.EnqueueRange(ctx, req)
	}

	response := &serverpb.EnqueueRangeResponse{}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error) {
		admin := client.(serverpb.AdminClient)
		req := *req
		req.NodeID = nodeID
		return admin.EnqueueRange(ctx, &req)
	}
	responseFn := func(_ roachpb.NodeID, nodeResp interface{}) {
		nodeDetails := nodeResp.(*serverpb.EnqueueRangeResponse)
		response.Details = append(response.Details, nodeDetails.Details...)
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		errDetail := &serverpb.EnqueueRangeResponse_Details{
			NodeID: nodeID,
			Error:  err.Error(),
		}
		response.Details = append(response.Details, errDetail)
	}

	if err := contextutil.RunWithTimeout(ctx, "enqueue range", time.Minute, func(ctx context.Context) error {
		return s.server.status.iterateNodes(
			ctx, fmt.Sprintf("enqueue r%d in queue %s", req.RangeID, req.Queue),
			dialFn, nodeFn, responseFn, errorFn,
		)
	}); err != nil {
		if len(response.Details) == 0 {
			return nil, err
		}
		response.Details = append(response.Details, &serverpb.EnqueueRangeResponse_Details{
			Error: err.Error(),
		})
	}

	return response, nil
}

// enqueueRangeLocal checks whether the local node has a replica for the
// requested range that can be run through the queue, running it through the
// queue and returning trace/error information if so. If not, returns an empty
// response.
func (s *adminServer) enqueueRangeLocal(
	ctx context.Context, req *serverpb.EnqueueRangeRequest,
) (*serverpb.EnqueueRangeResponse, error) {
	response := &serverpb.EnqueueRangeResponse{
		Details: []*serverpb.EnqueueRangeResponse_Details{
			{
				NodeID: s.server.NodeID(),
			},
		},
	}

	var store *kvserver.Store
	var repl *kvserver.Replica
	if err := s.server.node.stores.VisitStores(func(s *kvserver.Store) error {
		r, err := s.GetReplica(req.RangeID)
		if roachpb.IsRangeNotFoundError(err) {
			return nil
		}
		if err != nil {
			return err
		}
		repl = r
		store = s
		return nil
	}); err != nil {
		response.Details[0].Error = err.Error()
		return response, nil
	}

	if store == nil || repl == nil {
		response.Details[0].Error = fmt.Sprintf("n%d has no replica for r%d", s.server.NodeID(), req.RangeID)
		return response, nil
	}

	traceSpans, processErr, err := store.ManuallyEnqueue(ctx, req.Queue, repl, req.SkipShouldQueue)
	if err != nil {
		response.Details[0].Error = err.Error()
		return response, nil
	}
	response.Details[0].Events = recordedSpansToTraceEvents(traceSpans)
	response.Details[0].Error = processErr
	return response, nil
}

func (s *adminServer) getLocalRangeAndStore(
	ctx context.Context, key roachpb.RKey, evictToken *kvcoord.EvictionToken,
) (*roachpb.RangeDescriptor, roachpb.StoreID, error) {
	var storeID roachpb.StoreID = -1
	rangeDesc, returnedToken, err :=
		s.server.execCfg.RangeDescriptorCache.LookupRangeDescriptorWithEvictionToken(ctx, key, evictToken, false)
	if err != nil {
		log.Errorf(ctx, "failed get range for key %v", key)
		return nil, 0, err
	}
	for _, replDesc := range rangeDesc.InternalReplicas {
		if replDesc.NodeID == s.server.NodeID() {
			storeID = replDesc.StoreID
			break
		}
	}
	if storeID < 0 {
		if evictToken == nil {
			_ = returnedToken.Evict(ctx)
			return s.getLocalRangeAndStore(ctx, key, returnedToken)
		}
		err = errors.New("get no replica from the node")
		log.Errorf(ctx, "get no replica from the node")
		return nil, 0, err
	}
	return rangeDesc, storeID, nil
}

// sqlQuery allows you to incrementally build a SQL query that uses
// placeholders. Instead of specific placeholders like $1, you instead use the
// temporary placeholder $.
type sqlQuery struct {
	buf   bytes.Buffer
	pidx  int
	qargs []interface{}
	errs  []error
}

func makeSQLQuery() *sqlQuery {
	res := &sqlQuery{}
	return res
}

// String returns the full query.
func (q *sqlQuery) String() string {
	if len(q.errs) > 0 {
		return "couldn't generate query: please check Errors()"
	}
	return q.buf.String()
}

// Errors returns a slice containing all errors that have happened during the
// construction of this query.
func (q *sqlQuery) Errors() []error {
	return q.errs
}

// QueryArguments returns a filled map of placeholders containing all arguments
// provided to this query through Append.
func (q *sqlQuery) QueryArguments() []interface{} {
	return q.qargs
}

// Append appends the provided string and any number of query parameters.
// Instead of using normal placeholders (e.g. $1, $2), use meta-placeholder $.
// This method rewrites the query so that it uses proper placeholders.
//
// For example, suppose we have the following calls:
//
//	query.Append("SELECT * FROM foo WHERE a > $ AND a < $ ", arg1, arg2)
//	query.Append("LIMIT $", limit)
//
// The query is rewritten into:
//
//	SELECT * FROM foo WHERE a > $1 AND a < $2 LIMIT $3
//	/* $1 = arg1, $2 = arg2, $3 = limit */
//
// Note that this method does NOT return any errors. Instead, we queue up
// errors, which can later be accessed. Returning an error here would make
// query construction code exceedingly tedious.
func (q *sqlQuery) Append(s string, params ...interface{}) {
	var placeholders int
	for _, r := range s {
		q.buf.WriteRune(r)
		if r == '$' {
			q.pidx++
			placeholders++
			q.buf.WriteString(strconv.Itoa(q.pidx)) // SQL placeholders are 1-based
		}
	}

	if placeholders != len(params) {
		q.errs = append(q.errs,
			errors.Errorf("# of placeholders %d != # of params %d", placeholders, len(params)))
	}
	q.qargs = append(q.qargs, params...)
}

// resultScanner scans columns from sql.ResultRow instances into variables,
// performing the appropriate casting and error detection along the way.
type resultScanner struct {
	colNameToIdx map[string]int
}

func makeResultScanner(cols []sqlbase.ResultColumn) resultScanner {
	rs := resultScanner{
		colNameToIdx: make(map[string]int),
	}
	for i, col := range cols {
		rs.colNameToIdx[col.Name] = i
	}
	return rs
}

// IsNull returns whether the specified column of the given row contains
// a SQL NULL value.
func (rs resultScanner) IsNull(row tree.Datums, col string) (bool, error) {
	idx, ok := rs.colNameToIdx[col]
	if !ok {
		return false, errors.Errorf("result is missing column %s", col)
	}
	return row[idx] == tree.DNull, nil
}

// ScanIndex scans the given column index of the given row into dst.
func (rs resultScanner) ScanIndex(row tree.Datums, index int, dst interface{}) error {
	src := row[index]

	if dst == nil {
		return errors.Errorf("nil destination pointer passed in")
	}

	switch d := dst.(type) {
	case *string:
		s, ok := tree.AsDString(src)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		*d = string(s)

	case **string:
		s, ok := tree.AsDString(src)
		if !ok {
			if src != tree.DNull {
				return errors.Errorf("source type assertion failed")
			}
			*d = nil
			break
		}
		val := string(s)
		*d = &val

	case *bool:
		s, ok := src.(*tree.DBool)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		*d = bool(*s)

	case *float32:
		s, ok := src.(*tree.DFloat)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		*d = float32(*s)

	case **float32:
		s, ok := src.(*tree.DFloat)
		if !ok {
			if src != tree.DNull {
				return errors.Errorf("source type assertion failed")
			}
			*d = nil
			break
		}
		val := float32(*s)
		*d = &val

	case *int64:
		s, ok := tree.AsDInt(src)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		*d = int64(s)

	case *[]sqlbase.ID:
		s, ok := tree.AsDArray(src)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		for i := 0; i < s.Len(); i++ {
			id, ok := tree.AsDInt(s.Array[i])
			if !ok {
				return errors.Errorf("source type assertion failed on index %d", i)
			}
			*d = append(*d, sqlbase.ID(id))
		}

	case *time.Time:
		s, ok := src.(*tree.DTimestamp)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		*d = s.Time

	// Passing a **time.Time instead of a *time.Time means the source is allowed
	// to be NULL, in which case nil is stored into *src.
	case **time.Time:
		s, ok := src.(*tree.DTimestamp)
		if !ok {
			if src != tree.DNull {
				return errors.Errorf("source type assertion failed")
			}
			*d = nil
			break
		}
		*d = &s.Time

	case *[]byte:
		s, ok := src.(*tree.DBytes)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		// Yes, this copies, but this probably isn't in the critical path.
		*d = []byte(*s)

	case *apd.Decimal:
		s, ok := src.(*tree.DDecimal)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		*d = s.Decimal

	case **apd.Decimal:
		s, ok := src.(*tree.DDecimal)
		if !ok {
			if src != tree.DNull {
				return errors.Errorf("source type assertion failed")
			}
			*d = nil
			break
		}
		*d = &s.Decimal

	default:
		return errors.Errorf("unimplemented type for scanCol: %T", dst)
	}

	return nil
}

// ScanAll scans all the columns from the given row, in order, into dsts.
func (rs resultScanner) ScanAll(row tree.Datums, dsts ...interface{}) error {
	if len(row) != len(dsts) {
		return fmt.Errorf(
			"ScanAll: row has %d columns but %d dests provided", len(row), len(dsts))
	}
	for i := 0; i < len(row); i++ {
		if err := rs.ScanIndex(row, i, dsts[i]); err != nil {
			return err
		}
	}
	return nil
}

// Scan scans the column with the given name from the given row into dst.
func (rs resultScanner) Scan(row tree.Datums, colName string, dst interface{}) error {
	idx, ok := rs.colNameToIdx[colName]
	if !ok {
		return errors.Errorf("result is missing column %s", colName)
	}
	return rs.ScanIndex(row, idx, dst)
}

// TODO(mrtracy): The following methods, used to look up the zone configuration
// for a database or table, use the same algorithm as a set of methods in
// cli/zone.go for the same purpose. However, as that code connects to the
// server with a SQL connections, while this code uses the InternalExecutor, the
// code cannot be commonized.
//
// queryZone retrieves the specific ZoneConfig associated with the supplied ID,
// if it exists.
func (s *adminServer) queryZone(
	ctx context.Context, userName string, id sqlbase.ID,
) (zonepb.ZoneConfig, bool, error) {
	const query = `SELECT kwdb_internal.get_zone_config($1)`
	rows, cols, err := s.server.internalExecutor.QueryWithCols(
		ctx,
		"admin-query-zone",
		nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		query,
		id,
	)
	if err != nil {
		return *zonepb.NewZoneConfig(), false, err
	}

	if len(rows) != 1 {
		return *zonepb.NewZoneConfig(), false, errors.Errorf("invalid number of rows returned: %s (%d)", query, id)
	}

	var zoneBytes []byte
	scanner := makeResultScanner(cols)
	if isNull, err := scanner.IsNull(rows[0], cols[0].Name); err != nil {
		return *zonepb.NewZoneConfig(), false, err
	} else if isNull {
		return *zonepb.NewZoneConfig(), false, nil
	}

	err = scanner.ScanIndex(rows[0], 0, &zoneBytes)
	if err != nil {
		return *zonepb.NewZoneConfig(), false, err
	}

	var zone zonepb.ZoneConfig
	if err := protoutil.Unmarshal(zoneBytes, &zone); err != nil {
		return *zonepb.NewZoneConfig(), false, err
	}
	return zone, true, nil
}

// queryZonePath queries a path of sql object IDs, as generated by
// queryDescriptorIDPath(), for a ZoneConfig. It returns the most specific
// ZoneConfig specified for the object IDs in the path.
func (s *adminServer) queryZonePath(
	ctx context.Context, userName string, path []sqlbase.ID,
) (sqlbase.ID, zonepb.ZoneConfig, bool, error) {
	for i := len(path) - 1; i >= 0; i-- {
		zone, zoneExists, err := s.queryZone(ctx, userName, path[i])
		if err != nil || zoneExists {
			return path[i], zone, true, err
		}
	}
	return 0, *zonepb.NewZoneConfig(), false, nil
}

// queryNamespaceID queries for the ID of the namespace with the given name and
// parent ID.
func (s *adminServer) queryNamespaceID(
	ctx context.Context, userName string, parentID sqlbase.ID, name string,
) (sqlbase.ID, error) {
	const query = `SELECT kwdb_internal.get_namespace_id($1, $2)`
	rows, cols, err := s.server.internalExecutor.QueryWithCols(
		ctx, "admin-query-namespace-ID", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: userName},
		query, parentID, name,
	)
	if err != nil {
		return 0, err
	}

	if len(rows) != 1 {
		return 0, errors.Errorf("invalid number of rows returned: %s (%d, %s)", query, parentID, name)
	}

	var id int64
	scanner := makeResultScanner(cols)
	if isNull, err := scanner.IsNull(rows[0], cols[0].Name); err != nil {
		return 0, err
	} else if isNull {
		return 0, errors.Errorf("namespace %s with ParentID %d not found", name, parentID)
	}

	err = scanner.ScanIndex(rows[0], 0, &id)
	if err != nil {
		return 0, err
	}

	return sqlbase.ID(id), nil
}

// queryDescriptorIDPath converts a path of namespaces into a path of namespace
// IDs. For example, if this function is called with a database/table name pair,
// it will return a list of IDs consisting of the root namespace ID, the
// databases ID, and the table ID (in that order).
func (s *adminServer) queryDescriptorIDPath(
	ctx context.Context, userName string, names []string,
) ([]sqlbase.ID, error) {
	path := []sqlbase.ID{keys.RootNamespaceID}
	for _, name := range names {
		id, err := s.queryNamespaceID(ctx, userName, path[len(path)-1], name)
		if err != nil {
			return nil, err
		}
		path = append(path, id)
	}
	return path, nil
}

func (s *adminServer) dialNode(
	ctx context.Context, nodeID roachpb.NodeID,
) (serverpb.AdminClient, error) {
	addr, err := s.server.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return nil, err
	}
	conn, err := s.server.rpcContext.GRPCDialNode(
		addr.String(), nodeID, rpc.DefaultClass).Connect(ctx)
	if err != nil {
		return nil, err
	}
	return serverpb.NewAdminClient(conn), nil
}

func (s *adminServer) requireAdminUser(ctx context.Context) (userName string, err error) {
	userName, isAdmin, err := s.getUserAndRole(ctx)
	if err != nil {
		return "", err
	}
	if !isAdmin {
		return "", errInsufficientPrivilege
	}
	return userName, nil
}

func (s *adminServer) getUserAndRole(
	ctx context.Context,
) (userName string, isAdmin bool, err error) {
	userName, err = userFromContext(ctx)
	if err != nil {
		return "", false, err
	}
	isAdmin, err = s.hasAdminRole(ctx, userName)
	return userName, isAdmin, err
}

func (s *adminServer) hasAdminRole(ctx context.Context, sessionUser string) (bool, error) {
	if sessionUser == security.RootUser {
		// Shortcut.
		return true, nil
	}
	rows, cols, err := s.server.internalExecutor.QueryWithCols(
		ctx, "check-is-admin", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: sessionUser},
		"SELECT kwdb_internal.is_admin()")
	if err != nil {
		return false, err
	}
	if len(rows) != 1 || len(cols) != 1 {
		return false, errors.AssertionFailedf("hasAdminRole: expected 1 row, got %d", len(rows))
	}
	dbDatum, ok := tree.AsDBool(rows[0][0])
	if !ok {
		return false, errors.AssertionFailedf("hasAdminRole: expected bool, got %T", rows[0][0])
	}
	return bool(dbDatum), nil
}

func (s *adminServer) GetAlertThreshold(
	ctx context.Context, req *serverpb.AlertRequest,
) (*serverpb.AlertResponse, error) {
	var resp serverpb.AlertResponse
	resp.CPUAlertThreshold = float32(CPUAlertThreshold.Get(&s.server.st.SV))
	resp.MemAlertThreshold = float32(MemoryAlertThreshold.Get(&s.server.st.SV))
	resp.StorageAlertThreshold = float32(StorageAlertThreshold.Get(&s.server.st.SV))
	resp.ConnectionsAlertThreshold = float32(ConnectionsAlertThreshold.Get(&s.server.st.SV))
	resp.SqlConnectionMaxLimit = int32(pgwire.SQLConnectionMaxLimit.Get(&s.server.st.SV))
	return &resp, nil
}

// CreateTSTable create ts table
func (s *adminServer) CreateTSTable(
	ctx context.Context, req *serverpb.CreateTSTableRequest,
) (*serverpb.CreateTSTableResponse, error) {
	exist, _ := s.server.tsEngine.TSIsTsTableExist(req.TableID)
	if !exist {
		err := s.createTSTable(ctx, req.TableID, req.HashNum, req.Meta)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// createTSTable create ts table
func (s *adminServer) createTSTable(
	ctx context.Context, tableID uint64, hashNum uint64, meta []byte,
) error {
	rangeGroups := []api.RangeGroup{{RangeGroupID: 1}}
	err := s.server.tsEngine.CreateTsTable(tableID, hashNum, meta, rangeGroups)
	if err != nil {
		log.Errorf(ctx, "CreateTSTable failed %v", tableID)
		return err
	}
	return nil
}

var errInsufficientPrivilege = status.Error(codes.PermissionDenied, "this operation requires admin privilege")
