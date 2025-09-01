// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
//
//	http://license.coscl.org.cn/MulanPSL2
//
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package engine

// #cgo CPPFLAGS: -I../../../common/src/include
// #cgo LDFLAGS: -lcommon  -lstdc++
// #cgo linux LDFLAGS: -lrt -lpthread
//
// #include <stdlib.h>
// #include <string.h>
// #include "libcommon.h"
import "C"
import (
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
)

type Interface interface {
	TableExist(tableID uint64) (bool, error)

	CreateTable(tableID uint64, hashNum uint64, meta []byte, rangeGroups []api.RangeGroup) error

	SetRaftLogCombinedWAL(combined bool)

	DropLeftTsTableGarbage() error

	Open(rangeIndex []roachpb.RangeIndex) error

	CheckpointForTable(tableID uint32) error

	IsOpen() bool
}

type Helper struct {
	tsEngine Interface
	apEngine Interface
}

func (e *Helper) GetTSEngine() Interface {
	return e.tsEngine
}

func (e *Helper) TSEngineCreated() bool {
	return e.tsEngine != nil
}

func (e *Helper) SetTSEngine(en Interface) {
	e.tsEngine = en
}

func (e *Helper) APEngineCreated() bool {
	return e.apEngine != nil
}

func (e *Helper) GetAPEngine() Interface {
	return e.apEngine
}

func (e *Helper) SetAPEngine(en Interface) {
	e.apEngine = en
}
