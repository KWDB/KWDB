// Copyright 2019 The Cockroach Authors.
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

package sql

import (
	"context"
	"fmt"
	"io"
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/blobs"
	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

const (
	fileUploadTable = "file_upload"
	copyOptionDest  = "destination"
)

var copyFileOptionExpectValues = map[string]sqlconst.KVStringOptValidate{
	copyOptionDest: sqlconst.KVStringOptRequireValue,
}

var _ copyMachineInterface = &fileUploadMachine{}

type fileUploadMachine struct {
	c              *copyMachine
	writeToFile    *io.PipeWriter
	wg             *sync.WaitGroup
	failureCleanup func()
}

func newFileUploadMachine(
	ctx context.Context,
	conn pgwirebase.Conn,
	n *tree.CopyFrom,
	txnOpt copyTxnOpt,
	execCfg *ExecutorConfig,
) (f *fileUploadMachine, retErr error) {
	if len(n.Columns) != 0 {
		return nil, errors.New("expected 0 columns specified for file uploads")
	}
	c := &copyMachine{
		conn: conn,
		// The GenericPlanner will be prepared before use.
		p: GenericPlanner{execCfg: execCfg},
	}
	f = &fileUploadMachine{
		c:  c,
		wg: &sync.WaitGroup{},
	}

	// We need a GenericPlanner to do the initial planning, even if a GenericPlanner
	// is not required after that.
	cleanup := c.p.preparePlannerForCopy(ctx, txnOpt)
	defer func() {
		retErr = cleanup(ctx, retErr)
	}()
	c.parsingEvalCtx = c.p.EvalContext()

	if err := c.p.RequireAdminRole(ctx, "upload to nodelocal"); err != nil {
		return nil, err
	}

	optsFn, err := f.c.p.TypeAsStringOpts(n.Options, copyFileOptionExpectValues)
	if err != nil {
		return nil, err
	}
	opts, err := optsFn()
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	localStorage, err := blobs.NewLocalStorage(c.p.execCfg.Settings.ExternalIODir)
	if err != nil {
		return nil, err
	}
	// Check that the file does not already exist
	_, err = localStorage.Stat(opts[copyOptionDest])
	if err == nil {
		return nil, fmt.Errorf("destination file already exists for %s", opts[copyOptionDest])
	}
	f.wg.Add(1)
	go func() {
		err := localStorage.WriteFile(opts[copyOptionDest], pr)
		if err != nil {
			_ = pr.CloseWithError(err)
		}
		f.wg.Done()
	}()
	f.writeToFile = pw
	f.failureCleanup = func() {
		// Ignoring this error because deletion would only fail
		// if the file was not created in the first place.
		_ = localStorage.Delete(opts[copyOptionDest])
	}

	c.resultColumns = make(sqlbase.ResultColumns, 1)
	c.resultColumns[0] = sqlbase.ResultColumn{Typ: types.Bytes}
	c.parsingEvalCtx = c.p.EvalContext()
	c.rowsMemAcc = c.p.extendedEvalCtx.Mon.MakeBoundAccount()
	c.bufMemAcc = c.p.extendedEvalCtx.Mon.MakeBoundAccount()
	c.processRows = f.writeFile
	return
}

// CopyInFileStmt creates a COPY FROM statement which can be used
// to upload files, and be prepared with Tx.Prepare().
func CopyInFileStmt(destination, schema, table string) string {
	return fmt.Sprintf(
		`COPY %s.%s FROM STDIN WITH destination = %s`,
		pq.QuoteIdentifier(schema),
		pq.QuoteIdentifier(table),
		lex.EscapeSQLString(destination),
	)
}

func (f *fileUploadMachine) run(ctx context.Context) (err error) {
	err = f.c.run(ctx)
	_ = f.writeToFile.Close()
	if err != nil {
		f.failureCleanup()
	}
	f.wg.Wait()
	return
}

func (f *fileUploadMachine) writeFile(ctx context.Context) error {
	for _, r := range f.c.rows {
		b := []byte(*r[0].(*tree.DBytes))
		n, err := f.writeToFile.Write(b)
		if err != nil {
			return err
		}
		if n < len(b) {
			return io.ErrShortWrite
		}
	}

	// Issue a final zero-byte write to ensure we observe any errors in the pipe.
	_, err := f.writeToFile.Write(nil)
	if err != nil {
		return err
	}
	f.c.insertedRows += len(f.c.rows)
	f.c.rows = f.c.rows[:0]
	f.c.rowsMemAcc.Clear(ctx)
	return nil
}
