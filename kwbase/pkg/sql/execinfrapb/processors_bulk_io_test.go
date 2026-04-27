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

import "testing"

func TestBackfillerSpecRAI(t *testing.T) {
	obj1 := &BackfillerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestJobProgressRAI(t *testing.T) {
	obj1 := &JobProgress{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestReadImportDataSpecRAI(t *testing.T) {
	obj1 := &ReadImportDataSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestCSVWriterSpecRAI(t *testing.T) {
	obj1 := &CSVWriterSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestBulkRowWriterSpecRAI(t *testing.T) {
	obj1 := &BulkRowWriterSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestReplicationIngestionPartitionSpecRAI(t *testing.T) {
	obj1 := &ReplicationIngestionPartitionSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestReplicationIngestionDataSpecRAI(t *testing.T) {
	obj1 := &ReplicationIngestionDataSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestReplicationIngestionFrontierSpecRAI(t *testing.T) {
	obj1 := &ReplicationIngestionFrontierSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestReplicationRecvPartitionSpecRAI(t *testing.T) {
	obj1 := &ReplicationRecvPartitionSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestReplicationRecvDataSpecRAI(t *testing.T) {
	obj1 := &ReplicationRecvDataSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestReplicationRecvFrontierSpecRAI(t *testing.T) {
	obj1 := &ReplicationRecvFrontierSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}
