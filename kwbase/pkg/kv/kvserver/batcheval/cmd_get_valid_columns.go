// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package batcheval

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/batcheval/result"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/spanset"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

func init() {
	RegisterReadOnlyCommand(roachpb.TsGetValidColumns, declareKeysGetValidColumns, GetValidColumns)
}

func declareKeysGetValidColumns(
	_ *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeLeaseKey(header.RangeID)})
}

// GetValidColumns returns IDs of valid columns.
func GetValidColumns(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	req := cArgs.Args.(*roachpb.TsGetValidColumnsRequest)
	tableID := req.TableId
	version := req.Version
	ids, err := cArgs.EvalCtx.TsEngine().GetValidColumns(tableID, version, req.PrimaryTags)
	if err != nil {
		log.Infof(ctx, "get valid columns failed: %s", err.Error())
		return result.Result{}, err
	}

	reply := resp.(*roachpb.TsGetValidColumnsResponse)
	reply.ValidIDs = ids

	return result.Result{}, nil
}
