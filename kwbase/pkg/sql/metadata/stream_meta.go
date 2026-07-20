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

package metadata

import (
	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/json"
)

// CDCWatermark saved the LowWatermark of table for cdc.
type CDCWatermark struct {
	TableID      uint64
	TaskID       uint64
	TaskType     sqlbase.CDCInstanceType
	InternalType int32
	LowWatermark int64
	ClientID     interface{}
}

// StreamMetadata records a list of stream info to run stream
type StreamMetadata struct {
	ID            uint64
	Name          tree.Name
	CreateBy      string
	CreateAt      tree.DTimestamp
	Status        string
	TargetTableID uint64
	SourceTableID uint64
	LowWaterMark  int64
	JobID         int64
	Parameters    json.JSON
	RunInfo       json.JSON

	StreamParameters sqlutil.StreamParameters
	RunInfoList      []sqlutil.RunInfo
}

// Decode decode JSON to struct.
func (p *StreamMetadata) Decode() error {
	var err error
	p.StreamParameters, err = sqlutil.UnmarshalStreamParameters(p.Parameters)
	if err != nil {
		return err
	}

	p.RunInfoList, err = sqlutil.UnmarshalStreamRunInfo(p.RunInfo)
	if err != nil {
		return err
	}

	return nil
}

// PipeMetadata records a list of pipe info to run pipe
type PipeMetadata struct {
	ID               uint64
	Name             tree.Name
	Parameters       json.JSON
	CreateAt         tree.DTimestamp
	CreateBy         string
	Status           string
	RunInfo          json.JSON
	JobID            int64
	DatabaseID       uint64
	LowWaterMark     int64
	CdcWatermarkList []CDCWatermark

	ParaInfo    cdcpb.PipeParameters
	RunInfoList []cdcpb.RunInfo
}

// Decode decode JSON to struct.
func (p *PipeMetadata) Decode() error {
	var err error
	p.ParaInfo, err = cdcpb.UnmarshalPipeParameters(p.Parameters)
	if err != nil {
		return err
	}

	if p.RunInfo != nil {
		p.RunInfoList, err = cdcpb.UnmarshalRunInfo(p.RunInfo)
		if err != nil {
			return err
		}
	}

	return nil
}
