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

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/pkg/errors"
)

// stickyInMemEngine extends a normal engine, but does not allow them to be
// closed using the normal Close() method, instead keeping the engines in
// memory until CloseAllStickyInMemEngines is called, hence being "sticky".
// This prevents users of the in memory engine from having to special
// case "sticky" engines on every instance of "Close".
// It is intended for use in demos and/or tests, where we want in-memory
// storage nodes to persist between killed nodes.
type stickyInMemEngine struct {
	// id is the unique identifier for this sticky engine.
	id string
	// closed indicates whether the current engine has been closed.
	closed bool

	// Engine extends the Engine interface.
	relational.Engine
}

// stickyInMemEngine implements Engine.
var _ relational.Engine = &stickyInMemEngine{}

// Close overwrites the default Engine interface to not close the underlying
// engine if called. We mark the state as closed to reflect a correct result
// in Closed().
func (e *stickyInMemEngine) Close() {
	e.closed = true
}

// Closed overwrites the default Engine interface.
func (e *stickyInMemEngine) Closed() bool {
	return e.closed
}

// stickyInMemEnginesRegistryImpl is the bookkeeper for all active
// sticky engines, keyed by their id.
type stickyInMemEnginesRegistryImpl struct {
	entries map[string]*stickyInMemEngine
	mu      syncutil.Mutex
}

var stickyInMemEnginesRegistry = &stickyInMemEnginesRegistryImpl{
	entries: map[string]*stickyInMemEngine{},
}

// getOrCreateStickyInMemEngine returns an engine associated with the given id.
// It will create a new in-memory engine if one does not already exist.
// At most one engine with a given id can be active in
// "getOrCreateStickyInMemEngine" at any given time.
// Note that if you re-create an existing sticky engine the new attributes
// and cache size will be ignored.
// One must Close() on the sticky engine before another can be fetched.
func getOrCreateStickyInMemEngine(
	ctx context.Context,
	id string,
	engineType enginepb.EngineType,
	attrs roachpb.Attributes,
	cacheSize int64,
) (relational.Engine, error) {
	stickyInMemEnginesRegistry.mu.Lock()
	defer stickyInMemEnginesRegistry.mu.Unlock()

	if engine, ok := stickyInMemEnginesRegistry.entries[id]; ok {
		if !engine.closed {
			return nil, errors.Errorf("sticky engine %s has not been closed", id)
		}

		log.Infof(ctx, "re-using sticky in-mem engine %s", id)
		engine.closed = false
		return engine, nil
	}

	log.Infof(ctx, "creating new sticky in-mem engine %s", id)
	engine := &stickyInMemEngine{
		id:     id,
		closed: false,
		Engine: relational.NewInMem(ctx, engineType, attrs, cacheSize),
	}
	stickyInMemEnginesRegistry.entries[id] = engine
	return engine, nil
}

// CloseStickyInMemEngine closes the underlying engine and
// removes the sticky engine keyed by the given id.
// It will error if it does not exist.
func CloseStickyInMemEngine(id string) error {
	stickyInMemEnginesRegistry.mu.Lock()
	defer stickyInMemEnginesRegistry.mu.Unlock()

	if engine, ok := stickyInMemEnginesRegistry.entries[id]; ok {
		engine.closed = true
		engine.Engine.Close()
		delete(stickyInMemEnginesRegistry.entries, id)
		return nil
	}
	return errors.Errorf("sticky in-mem engine %s does not exist", id)
}

// CloseAllStickyInMemEngines closes and removes all sticky in memory engines.
func CloseAllStickyInMemEngines() {
	stickyInMemEnginesRegistry.mu.Lock()
	defer stickyInMemEnginesRegistry.mu.Unlock()

	for _, engine := range stickyInMemEnginesRegistry.entries {
		engine.closed = true
		engine.Engine.Close()
	}

	for id := range stickyInMemEnginesRegistry.entries {
		delete(stickyInMemEnginesRegistry.entries, id)
	}
}
