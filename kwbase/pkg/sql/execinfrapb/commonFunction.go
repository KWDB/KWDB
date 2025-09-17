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

package execinfrapb

// ExecInTSEngine returns true for execute in time series
func (s *ProcessorSpec) ExecInTSEngine() bool {
	return s.Engine == ProcessorSpec_TimeSeries
}

// ExecInAPEngine returns true for execute in analytical
func (s *ProcessorSpec) ExecInAPEngine() bool {
	return s.Engine == ProcessorSpec_Analytical
}

// ExecInMEEngine returns true for execute in me
func (s *ProcessorSpec) ExecInMEEngine() bool {
	return s.Engine == ProcessorSpec_Relation
}
