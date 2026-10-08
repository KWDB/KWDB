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

package relational

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
)

// NewInMem allocates and returns a new, opened in-memory engine. The caller
// must call the engine's Close method when the engine is no longer needed.
//
// FIXME(tschottdorf): make the signature similar to NewRocksDB (require a cfg).
func NewInMem(
	ctx context.Context, engine enginepb.EngineType, attrs roachpb.Attributes, cacheSize int64,
) Engine {
	switch engine {
	case enginepb.EngineTypeDefault, enginepb.EngineTypeRocksDB:
		return newRocksDBInMem(attrs, cacheSize)
	default:
		panic(fmt.Sprintf("unknown engine type: %d", engine))
	}
}

// NewDefaultInMem allocates and returns a new, opened in-memory engine with
// the default configuration. The caller must call the engine's Close method
// when the engine is no longer needed.
func NewDefaultInMem() Engine {
	return NewInMem(context.Background(),
		DefaultStorageEngine, roachpb.Attributes{}, 1<<20 /* 1 MB */)
}
