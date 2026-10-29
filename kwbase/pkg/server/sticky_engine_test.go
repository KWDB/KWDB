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

package server

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestStickyEngines(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	engineType := enginepb.EngineTypeRocksDB
	attrs := roachpb.Attributes{}
	cacheSize := int64(1 << 20)

	engine1, err := getOrCreateStickyInMemEngine(ctx, "engine1", engineType, attrs, cacheSize)
	require.NoError(t, err)
	require.False(t, engine1.Closed())

	engine2, err := getOrCreateStickyInMemEngine(ctx, "engine2", engineType, attrs, cacheSize)
	require.NoError(t, err)
	require.False(t, engine2.Closed())

	// Regetting the engine whilst it is not closed will fail.
	_, err = getOrCreateStickyInMemEngine(ctx, "engine1", engineType, attrs, cacheSize)
	require.EqualError(t, err, "sticky engine engine1 has not been closed")

	// Close the engine, which allows it to be refetched.
	engine1.Close()
	require.True(t, engine1.Closed())
	require.False(t, engine1.(*stickyInMemEngine).Engine.Closed())

	// Refetching the engine should give back the same engine.
	engine1Refetched, err := getOrCreateStickyInMemEngine(ctx, "engine1", engineType, attrs, cacheSize)
	require.NoError(t, err)
	require.Equal(t, engine1, engine1Refetched)
	require.False(t, engine1.Closed())

	// Closing an engine that does not exist will error.
	err = CloseStickyInMemEngine("engine3")
	require.EqualError(t, err, "sticky in-mem engine engine3 does not exist")

	// Cleaning up the engine should result in a new engine.
	err = CloseStickyInMemEngine("engine1")
	require.NoError(t, err)
	require.True(t, engine1.Closed())
	require.True(t, engine1.(*stickyInMemEngine).Engine.Closed())

	newEngine1, err := getOrCreateStickyInMemEngine(ctx, "engine1", engineType, attrs, cacheSize)
	require.NoError(t, err)
	require.NotEqual(t, engine1, newEngine1)

	// Cleaning up everything asserts everything is closed.
	CloseAllStickyInMemEngines()
	require.Len(t, stickyInMemEnginesRegistry.entries, 0)
	for _, engine := range []relational.Engine{engine1, newEngine1, engine2} {
		require.True(t, engine.Closed())
		require.True(t, engine.(*stickyInMemEngine).Engine.Closed())
	}
}
