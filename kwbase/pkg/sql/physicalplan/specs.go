// Copyright 2018 The Cockroach Authors.
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

package physicalplan

import (
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
)

var flowSpecPool = sync.Pool{
	New: func() interface{} {
		return &execinfrapb.FlowSpec{}
	},
}

// NewFlowSpec returns a new FlowSpec, which may have non-zero capacity in its
// slice fields.
func NewFlowSpec(flowID execinfrapb.FlowID, gateway roachpb.NodeID) *execinfrapb.FlowSpec {
	spec := flowSpecPool.Get().(*execinfrapb.FlowSpec)
	spec.FlowID = flowID
	spec.Gateway = gateway
	return spec
}

// ReleaseFlowSpec returns this FlowSpec back to the pool of FlowSpecs. It may
// not be used again after this call.
func ReleaseFlowSpec(spec *execinfrapb.FlowSpec) {
	*spec = execinfrapb.FlowSpec{
		Processors: spec.Processors[:0],
	}
	flowSpecPool.Put(spec)
}

var trSpecPool = sync.Pool{
	New: func() interface{} {
		return &execinfrapb.TableReaderSpec{}
	},
}

// NewTableReaderSpec returns a new TableReaderSpec.
func NewTableReaderSpec() *execinfrapb.TableReaderSpec {
	return trSpecPool.Get().(*execinfrapb.TableReaderSpec)
}

var trAPSpecPool = sync.Pool{
	New: func() interface{} {
		return &execinfrapb.APTableReaderSpec{}
	},
}

// NewAPTableReaderSpec returns a new APTableReader.
func NewAPTableReaderSpec() *execinfrapb.APTableReaderSpec {
	return trAPSpecPool.Get().(*execinfrapb.APTableReaderSpec)
}

// ReleaseTableReaderSpec puts this TableReaderSpec back into its sync pool. It
// may not be used again after Release returns.
func ReleaseTableReaderSpec(s *execinfrapb.TableReaderSpec) {
	s.Reset()
	trSpecPool.Put(s)
}

// ReleaseAPTableReaderSpec puts this TableReaderSpec back into its sync pool. It
// may not be used again after Release returns.
func ReleaseAPTableReaderSpec(s *execinfrapb.APTableReaderSpec) {
	s.Reset()
	trAPSpecPool.Put(s)
}

// ReleaseSetupFlowRequest releases the resources of this SetupFlowRequest,
// putting them back into their respective object pools.
func ReleaseSetupFlowRequest(s *execinfrapb.SetupFlowRequest) {
	if s == nil {
		return
	}
	for i := range s.Flow.Processors {
		if tr := s.Flow.Processors[i].Core.TableReader; tr != nil {
			ReleaseTableReaderSpec(tr)
		} else if tr1 := s.Flow.Processors[i].Core.ApTableReader; tr != nil {
			ReleaseAPTableReaderSpec(tr1)
		}
	}
	ReleaseFlowSpec(&s.Flow)
}
