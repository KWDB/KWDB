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

package tse

import (
	"context"
	"fmt"
	"os"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

func TestTSOpen(t *testing.T) {
	_ = os.RemoveAll("./tsdb1")
	st := cluster.MakeClusterSettings()
	settings.SetCanonicalValuesContainer(&st.SV)
	cfg := TsEngineConfig{
		Dir:            "./tsdb1",
		Settings:       st,
		ThreadPoolSize: 10,
		TaskQueueSize:  10,
		BufferPoolSize: 4096,
		BRPCAddr:       "127.0.0.1:0",
	}
	stopper := stop.NewStopper()
	tsDB, err := NewTsEngine(context.Background(), cfg, stopper)
	if err != nil {
		panic(err)
	}
	if err = tsDB.Open(nil); err != nil {
		panic(err)
	}
	stopper.AddCloser(tsDB)
	fmt.Println("TsEngine open success")
	stopper.Stop(context.Background())
	_ = os.RemoveAll("./tsdb1")
}

type TestCol struct {
	name  string
	cType sqlbase.DataType
	cLen  uint64
}

func makeTestObjectTable(
	tName string, tableID uint64, columns []TestCol, hashNum uint64,
) sqlbase.CreateTsTable {
	var kColDescs []sqlbase.KWDBKTSColumn
	var KColumnsID []uint32

	cOffset := uint64(0)
	for i, col := range columns {
		kColDesc := sqlbase.KWDBKTSColumn{
			ColumnId:           uint32(i),
			Name:               col.name,
			Nullable:           false,
			StorageType:        col.cType,
			StorageLen:         col.cLen,
			ColOffset:          cOffset,
			VariableLengthType: 0,
		}
		cOffset += col.cLen
		kColDescs = append(kColDescs, kColDesc)
		KColumnsID = append(KColumnsID, uint32(i))
	}
	tableName := tree.Name(tName)
	kObjectTable := sqlbase.KWDBTsTable{
		TsTableId:    tableID,
		DatabaseId:   uint32(1),
		LifeTime:     0,
		ActiveTime:   0,
		KColumnsId:   KColumnsID,
		RowSize:      cOffset,
		BitmapOffset: 0,
		TableName:    tableName.String(),
		Sde:          false,
		TsVersion:    1,
		HashNum:      hashNum,
	}

	return sqlbase.CreateTsTable{
		TsTable: kObjectTable,
		KColumn: kColDescs,
	}
}

func TestTSPut(t *testing.T) {
	_ = os.RemoveAll("./tsdb2")

	st := cluster.MakeClusterSettings()
	settings.SetCanonicalValuesContainer(&st.SV)

	cfg := TsEngineConfig{
		Dir:            "./tsdb2",
		Settings:       st,
		ThreadPoolSize: 10,
		TaskQueueSize:  10,
		BufferPoolSize: 4096,
		BRPCAddr:       "127.0.0.1:0",
	}
	stopper := stop.NewStopper()
	tsDB, err := NewTsEngine(context.Background(), cfg, stopper)
	if err != nil {
		panic(err)
	}
	if err = tsDB.Open(nil); err != nil {
		panic(err)
	}
	stopper.AddCloser(tsDB)
	fmt.Println("TsEngine open success")

	// create table
	tableID := uint64(1001)
	hashNum := 2000

	tsCols := []TestCol{
		{"k_time", sqlbase.DataType_TIMESTAMP, 8},
		{"col1", sqlbase.DataType_INT, 4},
		{"col2", sqlbase.DataType_DOUBLE, 8},
	}
	tsTable := makeTestObjectTable("table1", tableID, tsCols, uint64(hashNum))
	meta, _ := protoutil.Marshal(&tsTable)
	rangeGroups := []api.RangeGroup{
		{101, api.ReplicaType_Follower},
	}
	err = tsDB.CreateTsTable(tableID, uint64(hashNum), meta, rangeGroups)
	if err != nil {
		panic(err)
	}
	// close ts engine
	stopper.Stop(context.Background())
	_ = os.RemoveAll("./tsdb2")
}
