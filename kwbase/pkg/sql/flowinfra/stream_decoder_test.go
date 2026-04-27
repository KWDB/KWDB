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

package flowinfra

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestStreamDecoderTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var sd StreamDecoder

	// Create a producer message with typing information
	msg := &execinfrapb.ProducerMessage{
		Header: &execinfrapb.ProducerHeader{},
		Typing: []execinfrapb.DatumInfo{{Type: *types.Int}, {Type: *types.String}},
	}

	// Add the message to the decoder
	err := sd.AddMessage(ctx, msg)
	if err != nil {
		t.Fatalf("expected AddMessage() to succeed, got: %v", err)
	}

	// Test Types()
	typs := sd.Types()
	if len(typs) != 2 {
		t.Errorf("expected 2 types, got: %d", len(typs))
	}
	if typs[0].Family() != types.IntFamily {
		t.Error("expected first type to be Int")
	}
	if typs[1].Family() != types.StringFamily {
		t.Error("expected second type to be String")
	}
}
