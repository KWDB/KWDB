package ape

import (
	"context"
	"errors"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	duck "github.com/duckdb-go-bindings"
)

// Appender holds the DuckDB appender. It allows efficient bulk loading into a DuckDB database.
type Appender struct {
	conn     *duck.Connection
	schema   string
	table    string
	appender duck.Appender
	closed   bool

	// The chunk to append to.
	chunk DataChunk
	// The column types of the table to append to.
	types []duck.LogicalType
	// The number of appended rows.
	rowCount int
}

// DataChunk storage of a DuckDB table.
type DataChunk struct {
	// data holds the underlying duckdb data chunk.
	chunk duck.DataChunk
	// columns is a helper slice providing direct access to all columns.
	columns []vector
	// columnNames holds the column names, if known.
	columnNames []string
	// size caches the size after initialization.
	size int
}

func DuckInsert(ctx context.Context, r *Engine, dbName, tabName string, rowVals []tree.Datums) error {
	// fmt.Println("DuckInsert start")
	db := duck.Database{}
	var state duck.State
	state = duck.Open(r.GetDBPath()+"/"+dbName, &db)
	if state != duck.StateSuccess {
		return errors.New(fmt.Sprintf("failed to opend the ap database \"%s\"" + dbName))
	}

	conn := duck.Connection{}
	state = duck.Connect(db, &conn)
	if state != duck.StateSuccess {
		return pgerror.Newf(pgcode.Internal, "failed to connect to ap database \"%s\""+dbName)
	}
	defer func() {
		duck.Disconnect(&conn)
		duck.Close(&db)
	}()
	a, err := NewAppender(&conn, dbName, tree.PublicSchema, tabName)
	if err != nil {
		return pgerror.Newf(pgcode.Internal, "could not create new appender: %s", err.Error())
	}
	for i := range rowVals {
		if err := a.AppendRow(rowVals[i]); err != nil {
			return pgerror.Newf(pgcode.Internal, "could not append row to \"%s\": %s", tabName, err.Error())
		}
	}

	if err := a.Close(); err != nil {
		return pgerror.Newf(pgcode.Internal, "could not flush and close appender: %s", err.Error())
	}
	//var res duck.Result
	//query := fmt.Sprintf("SELECT * FROM %s", tabName)
	//duck.Query(conn, query, &res)
	//count := duck.RowCount(&res)
	//fmt.Printf("select count= %s:%d\n", tabName, int(count))
	//fmt.Println("DuckInsert done")
	return nil
}

// NewAppender returns a new Appender from a DuckDB driver connection.
func NewAppender(driverConn *duck.Connection, catalog, schema, table string) (*Appender, error) {

	var appender duck.Appender
	state := duck.AppenderCreateExt(*driverConn, catalog, schema, table, &appender)
	if state == duck.StateError {
		err := duck.AppenderError(appender)
		duck.AppenderDestroy(&appender)
		return nil, errors.New(err)
	}

	a := &Appender{
		conn:     driverConn,
		schema:   schema,
		table:    table,
		appender: appender,
		rowCount: 0,
	}

	// Get the column types.
	columnCount := duck.AppenderColumnCount(appender)
	for i := duck.IdxT(0); i < columnCount; i++ {
		colType := duck.AppenderColumnType(appender, i)
		a.types = append(a.types, colType)

		// Ensure that we only create an appender for supported column types.
		t := duck.GetTypeId(colType)
		name, found := typeToStringMap[t]
		if !found {
			// todo(zxy)
			_ = name
			err := addIndexToError(errors.New("unsupportedTypeError(name)"), int(i)+1)
			destroyTypeSlice(a.types)
			duck.AppenderDestroy(&appender)
			return nil, err
		}
	}

	// Initialize the data chunk.
	if err := a.chunk.initFromTypes(a.types, true); err != nil {
		a.chunk.close()
		destroyTypeSlice(a.types)
		duck.AppenderDestroy(&appender)
		return nil, pgerror.Wrap(err, "", "errAppenderCreation")
	}

	return a, nil
}

var typeToStringMap = map[duck.Type]string{
	duck.TypeInvalid:  "INVALID",
	duck.TypeBoolean:  "BOOLEAN",
	duck.TypeTinyInt:  "TINYINT",
	duck.TypeSmallInt: "SMALLINT",
	duck.TypeInteger:  "INTEGER",
	duck.TypeBigInt:   "BIGINT",
	duck.TypeFloat:    "FLOAT",
	duck.TypeDouble:   "DOUBLE",
	duck.TypeVarchar:  "VARCHAR",
	duck.TypeBlob:     "BLOB",
	duck.TypeDecimal:  "DECIMAL",
	duck.TypeDate:     "DATE",
	//TYPE_UTINYINT:     "UTINYINT",
	//TYPE_USMALLINT:    "USMALLINT",
	//TYPE_UINTEGER:     "UINTEGER",
	//TYPE_UBIGINT:      "UBIGINT",
	//TYPE_TIMESTAMP:    "TIMESTAMP",
	//TYPE_TIME:         "TIME",
	//TYPE_INTERVAL:     "INTERVAL",
	//TYPE_HUGEINT:      "HUGEINT",
	//TYPE_UHUGEINT:     "UHUGEINT",
	//TYPE_TIMESTAMP_S:  "TIMESTAMP_S",
	//TYPE_TIMESTAMP_MS: "TIMESTAMP_MS",
	//TYPE_TIMESTAMP_NS: "TIMESTAMP_NS",
	//TYPE_ENUM:         "ENUM",
	//TYPE_LIST:         "LIST",
	//TYPE_STRUCT:       "STRUCT",
	//TYPE_MAP:          "MAP",
	//TYPE_ARRAY:        "ARRAY",
	//TYPE_UUID:         "UUID",
	//TYPE_UNION:        "UNION",
	//TYPE_BIT:          "BIT",
	//TYPE_TIME_TZ:      "TIMETZ",
	//TYPE_TIMESTAMP_TZ: "TIMESTAMPTZ",
	//TYPE_ANY:          "ANY",
	//TYPE_VARINT:       "VARINT",
	//TYPE_SQLNULL:      "SQLNULL",
}

func addIndexToError(err error, idx int) error {
	return fmt.Errorf("%w: index: %d", err, idx)
}

func destroyTypeSlice(slice []duck.LogicalType) {
	for _, t := range slice {
		duck.DestroyLogicalType(&t)
	}
}

func (chunk *DataChunk) initFromTypes(types []duck.LogicalType, writable bool) error {
	// NOTE: initFromTypes does not initialize the column names.
	columnCount := len(types)

	// Initialize the callback functions to read and write values.
	chunk.columns = make([]vector, columnCount)
	var err error
	for i := 0; i < columnCount; i++ {
		if err = chunk.columns[i].init(types[i], i); err != nil {
			break
		}
	}
	if err != nil {
		return err
	}

	chunk.chunk = duck.CreateDataChunk(types)
	chunk.initVectors(writable)

	return nil
}

func (chunk *DataChunk) initVectors(writable bool) {
	duck.DataChunkSetSize(chunk.chunk, duck.IdxT(GetDataChunkCapacity()))

	for i := 0; i < len(chunk.columns); i++ {
		v := duck.DataChunkGetVector(chunk.chunk, duck.IdxT(i))
		chunk.columns[i].initVectors(v, writable)
	}
}

// GetDataChunkCapacity returns the capacity of a data chunk.
func GetDataChunkCapacity() int {
	return int(duck.VectorSize())
}

// SetValue writes a single value to a column in a data chunk.
// Note that this requires casting the type for each invocation.
// NOTE: Custom ENUM types must be passed as string.
func (chunk *DataChunk) SetValue(colIdx int, rowIdx int, val tree.Datum) error {
	if colIdx >= len(chunk.columns) {
		return getError(errAPI, fmt.Errorf("%s: expected %d, got %d", columnCountErrMsg, colIdx, len(chunk.columns)))
	}
	column := &chunk.columns[colIdx]

	return column.setFn(column, duck.IdxT(rowIdx), val)
}

// SetSize sets the internal size of the data chunk. Cannot exceed GetCapacity().
func (chunk *DataChunk) SetSize(size int) error {
	if size > GetDataChunkCapacity() {
		return getError(errAPI, errVectorSize)
	}
	duck.DataChunkSetSize(chunk.chunk, duck.IdxT(size))
	return nil
}

func (chunk *DataChunk) reset(writable bool) {
	duck.DataChunkReset(chunk.chunk)
	chunk.initVectors(writable)
}

func (chunk *DataChunk) close() {
	duck.DestroyDataChunk(&chunk.chunk)
}

var (
	driverErrMsg                = "database/sql/driver"
	errAppenderAppendAfterClose = fmt.Errorf("%w: appender already closed", errAppenderAppendRow)
	errAppenderAppendRow        = errors.New("could not append row")
	columnCountErrMsg           = "invalid column count"
	errAPI                      = errors.New("API error")
	errVectorSize               = errors.New("data chunks cannot exceed duckdb's internal vector size")
	errAppenderDoubleClose      = fmt.Errorf("%w: already closed", errAppenderClose)
	errAppenderClose            = errors.New("could not close appender")
)

func getError(errDriver error, err error) error {
	if err == nil {
		return fmt.Errorf("%s: %w", driverErrMsg, errDriver)
	}
	return fmt.Errorf("%s: %w: %s", driverErrMsg, errDriver, err.Error())
}

func apEngineErr(msg string) error {
	return errors.New(fmt.Sprintf("APEngineErr: %s", msg))
}

// AppendRow loads a row of values into the appender. The values are provided as separate arguments.
func (a *Appender) AppendRow(args tree.Datums) error {
	if a.closed {
		return getError(errAppenderAppendAfterClose, nil)
	}

	err := a.appendRowSlice(args)
	if err != nil {
		return getError(errAppenderAppendRow, err)
	}

	return nil
}

func (a *Appender) appendRowSlice(args tree.Datums) error {
	// Early-out, if the number of args does not match the column count.
	if len(args) != len(a.types) {
		return fmt.Errorf("%s: expected %d, got %d", columnCountErrMsg, len(args), len(a.types))
	}

	// Create a new data chunk if the current chunk is full.
	if a.rowCount == GetDataChunkCapacity() {
		if err := a.appendDataChunk(); err != nil {
			return err
		}
	}

	// Set all values.
	for i, val := range args {
		err := a.chunk.SetValue(i, a.rowCount, val)
		if err != nil {
			return err
		}
	}
	a.rowCount++

	return nil
}

func (a *Appender) appendDataChunk() error {
	if a.rowCount == 0 {
		// Nothing to append.
		return nil
	}
	if err := a.chunk.SetSize(a.rowCount); err != nil {
		return err
	}
	if duck.AppendDataChunk(a.appender, a.chunk.chunk) == duck.StateError {
		return apEngineErr(duck.AppenderError(a.appender))
	}

	a.chunk.reset(true)
	a.rowCount = 0

	return nil
}

// Close the appender. This will flush the appender to the underlying table.
// It is vital to call this when you are done with the appender to avoid leaking memory.
func (a *Appender) Close() error {
	if a.closed {
		return getError(errAppenderDoubleClose, nil)
	}
	a.closed = true

	// Append all remaining chunks.
	errAppend := a.appendDataChunk()
	a.chunk.close()

	// We flush before closing to get a meaningful error message.
	var errFlush error
	if duck.AppenderFlush(a.appender) == duck.StateError {
		errFlush = apEngineErr(duck.AppenderError(a.appender))
	}

	// Destroy all appender data and the appender.
	destroyTypeSlice(a.types)
	var errClose error
	if duck.AppenderDestroy(&a.appender) == duck.StateError {
		errClose = errAppenderClose
	}
	if errAppend != nil || errFlush != nil || errClose != nil {
		return fmt.Errorf("errAppend: %s, errFlush: %s, errClose: %s", errAppend, errFlush, errClose)
	}
	return nil
}
