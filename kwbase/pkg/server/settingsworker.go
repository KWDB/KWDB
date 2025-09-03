// Copyright 2017 The Cockroach Authors.
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
	"bytes"
	"context"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

// RefreshSettings starts a settings-changes listener.
func (s *Server) refreshSettings() {
	tbl := &sqlbase.SettingsTable

	a := &sqlbase.DatumAlloc{}
	settingsTablePrefix := keys.MakeTablePrefix(uint32(tbl.ID))
	colIdxMap := row.ColIDtoRowIndexFromCols(tbl.Columns)

	processKV := func(ctx context.Context, kv roachpb.KeyValue, u settings.Updater) error {
		if !bytes.HasPrefix(kv.Key, settingsTablePrefix) {
			return nil
		}

		var k, v, t string
		// First we need to decode the setting name field from the index key.
		{
			types := []types.T{tbl.Columns[0].Type}
			nameRow := make([]sqlbase.EncDatum, 1)
			_, matches, _, err := sqlbase.DecodeIndexKey(tbl, &tbl.PrimaryIndex, types, nameRow, nil, kv.Key)
			if err != nil {
				return errors.Wrap(err, "failed to decode key")
			}
			if !matches {
				return errors.Errorf("unexpected non-settings KV with settings prefix: %v", kv.Key)
			}
			if err := nameRow[0].EnsureDecoded(&types[0], a); err != nil {
				return err
			}
			k = string(tree.MustBeDString(nameRow[0].Datum))
		}

		// The rest of the columns are stored as a family, packed with diff-encoded
		// column IDs followed by their values.
		{
			// column valueType can be null (missing) so we default it to "s".
			t = "s"
			bytes, err := kv.Value.GetTuple()
			if err != nil {
				return err
			}
			var colIDDiff uint32
			var lastColID sqlbase.ColumnID
			var res tree.Datum
			for len(bytes) > 0 {
				_, _, colIDDiff, _, err = encoding.DecodeValueTag(bytes)
				if err != nil {
					return err
				}
				colID := lastColID + sqlbase.ColumnID(colIDDiff)
				lastColID = colID
				if idx, ok := colIdxMap[colID]; ok {
					res, bytes, err = sqlbase.DecodeTableValue(a, &tbl.Columns[idx].Type, bytes)
					if err != nil {
						return err
					}
					switch colID {
					case tbl.Columns[1].ID: // value
						v = string(tree.MustBeDString(res))
					case tbl.Columns[3].ID: // valueType
						t = string(tree.MustBeDString(res))
					case tbl.Columns[2].ID: // lastUpdated
						// TODO(dt): we could decode just the len and then seek `bytes` past
						// it, without allocating/decoding the unused timestamp.
					default:
						return errors.Errorf("unknown column: %v", colID)
					}
				}
			}
		}

		if err := u.Set(k, v, t); err != nil {
			log.Warningf(ctx, "setting %q to %q failed: %+v", k, v, err)
		}
		return nil
	}

	ctx := s.AnnotateCtx(context.Background())
	s.stopper.RunWorker(ctx, func(ctx context.Context) {
		gossipUpdateC := s.gossip.RegisterSystemConfigChannel()
		// No new settings can be defined beyond this point.
		for {
			select {
			case <-gossipUpdateC:
				cfg := s.gossip.GetSystemConfig()
				u := s.st.MakeUpdater()
				ok := true
				for _, kv := range cfg.Values {
					if err := processKV(ctx, kv, u); err != nil {
						log.Warningf(ctx, `error decoding settings data: %+v
								this likely indicates the settings table structure or encoding has been altered;
								skipping settings updates`, err)
						ok = false
						break
					}
				}
				if ok {
					u.ResetRemaining()
				}
				// TODO(whz): we open tsengine async to wait for updated cluster settings.
				// This affects all start modes, and some immediate ops may block to wait
				// for opening of tsengine. It depends on the speed of refreshing cluster
				// setting, usually quick enough. We will optimize it later if necessary.
				if !s.engineHelper.TSEngineCreated() {
					log.Warningf(ctx, "ts engine is not created")
				} else if !s.GetTSEngine().IsOpen() {
					log.Infof(ctx, "try open ts engine")
					if err := s.GetTSEngine().Open(s.node.Descriptor.RangeIndex); err != nil {
						panic(errors.Errorf("failed create tsEngine, err: %+v", err))
					}
				}

				if !s.engineHelper.APEngineCreated() {
					log.Warningf(ctx, "ap engine is not created")
				} else if !s.GetAPEngine().IsOpen() {
					log.Infof(ctx, "try open ap engine")
					if err := s.GetAPEngine().Open(s.node.Descriptor.RangeIndex); err != nil {
						panic(errors.Errorf("failed create ap Engine, err: %+v", err))
					}
				}
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
}
