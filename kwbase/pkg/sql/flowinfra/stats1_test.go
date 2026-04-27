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

package flowinfra_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/flowinfra"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestOutboxStatsRAI(t *testing.T) {
	defer leaktest.AfterTest(t)()

	obj1 := &flowinfra.OutboxStats{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}
	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()
	obj1.XXX_Merge(obj1)
	obj2 := &flowinfra.OutboxStats{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}
