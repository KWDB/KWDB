// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sql

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/physicalplan"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// createTsInsertWithCDCNodeForSingleMode construct TsInsertWithCDCProSpec for SingeMode and processors.
func createTsInsertWithCDCNodeForSingleMode(n *tsInsertWithCDCNode) (PhysicalPlan, error) {
	var p PhysicalPlan
	stageID := p.NewStageID()

	p.ResultRouters = make([]physicalplan.ProcessorIdx, len(n.nodeIDs))
	p.Processors = make([]physicalplan.Processor, 0, len(n.nodeIDs))

	// Construct a processor for the payload of each node.
	// For Single mode, n.nodeIDs only contain local node.
	for i := 0; i < len(n.nodeIDs); i++ {
		var tsInsert = &execinfrapb.TsInsertWithCDCProSpec{}
		tsInsert.PayLoad = make([][]byte, len(n.allNodePayloadInfos[i]))
		tsInsert.RowNums = make([]uint32, len(n.allNodePayloadInfos[i]))
		tsInsert.PrimaryTagKey = make([][]byte, len(n.allNodePayloadInfos[i]))
		tsInsert.CDCData = buildCDCDataProto(n.CDCData)

		for j := range n.allNodePayloadInfos[i] {
			tsInsert.PayLoad[j] = n.allNodePayloadInfos[i][j].Payload
			tsInsert.RowNums[j] = n.allNodePayloadInfos[i][j].RowNum
			tsInsert.PrimaryTagKey[j] = n.allNodePayloadInfos[i][j].PrimaryTagKey
		}

		proc := physicalplan.Processor{
			Node: n.nodeIDs[i],
			Spec: execinfrapb.ProcessorSpec{
				Core:    execinfrapb.ProcessorCoreUnion{TsInsertWithCDC: tsInsert},
				Output:  []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}
		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	p.GateNoopInput = len(n.nodeIDs)
	p.TsOperator = execinfrapb.OperatorType_TsInsert

	return p, nil
}

// createTsInsertWithCDCNodeForSingeMode construct TsInsertWithCDCProSpec for DistributeMode and processors.
func createTsTsInsertWithCDCNodeForDistributeMode(n *tsInsertWithCDCNode) (PhysicalPlan, error) {
	var p PhysicalPlan
	stageID := p.NewStageID()

	p.ResultRouters = make([]physicalplan.ProcessorIdx, len(n.nodeIDs))
	p.Processors = make([]physicalplan.Processor, 0, len(n.nodeIDs))

	// Construct a processor for the payload of each node.
	for i := 0; i < len(n.nodeIDs); i++ {
		var tsInsert = &execinfrapb.TsInsertWithCDCProSpec{}
		payloadNum := len(n.allNodePayloadInfos[i])
		tsInsert.RowNums = make([]uint32, payloadNum)
		tsInsert.PrimaryTagKey = make([][]byte, payloadNum)
		tsInsert.AllPayload = make([]*execinfrapb.PayloadForDistributeMode, payloadNum)
		tsInsert.PayloadPrefix = make([][]byte, payloadNum)
		tsInsert.CDCData = buildCDCDataProto(n.CDCData)

		for j := range n.allNodePayloadInfos[i] {
			tsInsert.RowNums[j] = n.allNodePayloadInfos[i][j].RowNum
			tsInsert.PrimaryTagKey[j] = n.allNodePayloadInfos[i][j].PrimaryTagKey
			tsInsert.PayloadPrefix[j] = n.allNodePayloadInfos[i][j].Payload
			payloadForDistributeMode := &execinfrapb.PayloadForDistributeMode{
				Row:        n.allNodePayloadInfos[i][j].RowBytes,
				TimeStamps: n.allNodePayloadInfos[i][j].RowTimestamps,
				StartKey:   n.allNodePayloadInfos[i][j].StartKey,
				EndKey:     n.allNodePayloadInfos[i][j].EndKey,
				ValueSize:  n.allNodePayloadInfos[i][j].ValueSize,
			}
			tsInsert.AllPayload[j] = payloadForDistributeMode
		}
		proc := physicalplan.Processor{
			Node: n.nodeIDs[i],
			Spec: execinfrapb.ProcessorSpec{
				Core:    execinfrapb.ProcessorCoreUnion{TsInsertWithCDC: tsInsert},
				Output:  []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}
		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	p.GateNoopInput = len(n.nodeIDs)
	p.TsOperator = execinfrapb.OperatorType_TsInsert

	return p, nil
}

// buildCDCDataProto builds CDC data to Protobuf format.
func buildCDCDataProto(cdcData *sqlbase.CDCData) execinfrapb.CDCData {
	var reconstructData []*execinfrapb.CDCPushData
	for _, data := range cdcData.PushData {
		pushData := execinfrapb.CDCPushData{
			TaskID:   data.TaskID,
			TaskType: data.TaskType,
			Data:     data.Rows,
		}

		reconstructData = append(reconstructData, &pushData)
	}

	return execinfrapb.CDCData{
		TableID:      cdcData.TableID,
		MinTimestamp: cdcData.MinTimestamp,
		PushData:     reconstructData,
	}
}
