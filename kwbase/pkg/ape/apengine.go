package ape

import (
	"errors"

	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	duck "github.com/duckdb-go-bindings"
)

type ApEngine struct {
	stopper    *stop.Stopper
	db         *duck.Database
	Connection *duck.Connection
	DbPath     string
}

func NewApEngine(stopper *stop.Stopper, dbPath string) (*ApEngine, error) {
	db := duck.Database{}
	var state duck.State
	state = duck.Open(dbPath+"/ap_database.db", &db)
	if state != duck.StateSuccess {
		return nil, errors.New("failed to opend the ap database")
	}

	connection := duck.Connection{}
	state = duck.Connect(db, &connection)
	if state != duck.StateSuccess {
		return nil, errors.New("failed to connect to ap database")
	}
	return &ApEngine{
		stopper:    stopper,
		db:         &db,
		Connection: &connection,
		DbPath:     dbPath,
	}, nil
}

// Close close TsEngine
func (r *ApEngine) Close() {
	duck.Close(r.db)
	duck.Disconnect(r.Connection)
}
