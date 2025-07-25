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

package jobspb

import (
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/stats"
)

// Details is a marker interface for job details proto structs.
type Details interface{}

var _ Details = BackupDetails{}
var _ Details = RestoreDetails{}
var _ Details = SchemaChangeDetails{}
var _ Details = ChangefeedDetails{}
var _ Details = CreateStatsDetails{}
var _ Details = SchemaChangeGCDetails{}
var _ Details = RestartDetails{}
var _ Details = ExportDetails{}
var _ Details = SqlScheduleDetails{}
var _ Details = ReplicaRebanalceDetails{}
var _ Details = TsTxnDetails{}

// ProgressDetails is a marker interface for job progress details proto structs.
type ProgressDetails interface{}

var _ ProgressDetails = BackupProgress{}
var _ ProgressDetails = RestoreProgress{}
var _ ProgressDetails = SchemaChangeProgress{}
var _ ProgressDetails = ChangefeedProgress{}
var _ ProgressDetails = CreateStatsProgress{}
var _ ProgressDetails = SchemaChangeGCProgress{}
var _ ProgressDetails = RestartHistoryProgress{}
var _ ProgressDetails = ExportProgress{}
var _ ProgressDetails = SqlScheduleProgress{}
var _ ProgressDetails = ReplicaRebanalceProgress{}
var _ ProgressDetails = TsTxnProgress{}

// Type returns the payload's job type.
func (p *Payload) Type() Type {
	return DetailsType(p.Details)
}

// DetailsType returns the type for a payload detail.
func DetailsType(d isPayload_Details) Type {
	switch d := d.(type) {
	case *Payload_Backup:
		return TypeBackup
	case *Payload_Restore:
		return TypeRestore
	case *Payload_SchemaChange:
		return TypeSchemaChange
	case *Payload_Import:
		return TypeImport
	case *Payload_Changefeed:
		return TypeChangefeed
	case *Payload_CreateStats:
		createStatsName := d.CreateStats.Name
		if createStatsName == stats.AutoStatsName {
			return TypeAutoCreateStats
		}
		return TypeCreateStats
	case *Payload_SchemaChangeGC:
		return TypeSchemaChangeGC
	case *Payload_SyncMetaCache:
		return TypeSyncMetaCache
	case *Payload_ReplicationIngestion:
		return TypeReplicationIngestion
	case *Payload_ReplicationStream:
		return TypeReplicationStream
	case *Payload_RestartHistory:
		return TypeRestartHistory
	case *Payload_Export:
		return TypeExport
	case *Payload_SqlSchedule:
		return TypeSqlSchedule
	case *Payload_ReplicaRebalance:
		return TypeReplicaRebanalce
	case *Payload_TsTxn:
		return TypeTsTxn
	default:
		panic(fmt.Sprintf("Payload.Type called on a payload with an unknown details type: %T", d))
	}
}

// WrapProgressDetails wraps a ProgressDetails object in the protobuf wrapper
// struct necessary to make it usable as the Details field of a Progress.
//
// Providing an unknown details type indicates programmer error and so causes a
// panic.
func WrapProgressDetails(details ProgressDetails) interface {
	isProgress_Details
} {
	switch d := details.(type) {
	case BackupProgress:
		return &Progress_Backup{Backup: &d}
	case RestoreProgress:
		return &Progress_Restore{Restore: &d}
	case SchemaChangeProgress:
		return &Progress_SchemaChange{SchemaChange: &d}
	case ImportProgress:
		return &Progress_Import{Import: &d}
	case ChangefeedProgress:
		return &Progress_Changefeed{Changefeed: &d}
	case CreateStatsProgress:
		return &Progress_CreateStats{CreateStats: &d}
	case SchemaChangeGCProgress:
		return &Progress_SchemaChangeGC{SchemaChangeGC: &d}
	case SyncMetaCacheProgress:
		return &Progress_SyncMetaCache{SyncMetaCache: &d}
	case ReplicationIngestionProgress:
		return &Progress_ReplicationIngest{ReplicationIngest: &d}
	case ReplicationStreamProgress:
		return &Progress_ReplicationStream{ReplicationStream: &d}
	case RestartHistoryProgress:
		return &Progress_Restarthistory{Restarthistory: &d}
	case ExportProgress:
		return &Progress_Export{Export: &d}
	case SqlScheduleProgress:
		return &Progress_SqlSchedule{SqlSchedule: &d}
	case ReplicaRebanalceProgress:
		return &Progress_ReplicaRebalance{ReplicaRebalance: &d}
	case TsTxnProgress:
		return &Progress_TsTxn{TsTxn: &d}
	default:
		panic(fmt.Sprintf("WrapProgressDetails: unknown details type %T", d))
	}
}

// UnwrapDetails returns the details object stored within the payload's Details
// field, discarding the protobuf wrapper struct.
func (p *Payload) UnwrapDetails() Details {
	switch d := p.Details.(type) {
	case *Payload_Backup:
		return *d.Backup
	case *Payload_Restore:
		return *d.Restore
	case *Payload_SchemaChange:
		return *d.SchemaChange
	case *Payload_Import:
		return *d.Import
	case *Payload_Changefeed:
		return *d.Changefeed
	case *Payload_CreateStats:
		return *d.CreateStats
	case *Payload_SchemaChangeGC:
		return *d.SchemaChangeGC
	case *Payload_SyncMetaCache:
		return *d.SyncMetaCache
	case *Payload_ReplicationIngestion:
		return *d.ReplicationIngestion
	case *Payload_ReplicationStream:
		return *d.ReplicationStream
	case *Payload_SqlSchedule:
		return *d.SqlSchedule
	case *Payload_ReplicaRebalance:
		return *d.ReplicaRebalance
	case *Payload_TsTxn:
		return *d.TsTxn
	default:
		return nil
	}
}

// UnwrapDetails returns the details object stored within the progress' Details
// field, discarding the protobuf wrapper struct.
func (p *Progress) UnwrapDetails() ProgressDetails {
	switch d := p.Details.(type) {
	case *Progress_Backup:
		return *d.Backup
	case *Progress_Restore:
		return *d.Restore
	case *Progress_SchemaChange:
		return *d.SchemaChange
	case *Progress_Import:
		return *d.Import
	case *Progress_Changefeed:
		return *d.Changefeed
	case *Progress_CreateStats:
		return *d.CreateStats
	case *Progress_SchemaChangeGC:
		return *d.SchemaChangeGC
	default:
		return nil
	}
}

func (t Type) String() string {
	// Protobufs, by convention, use CAPITAL_SNAKE_CASE for enum identifiers.
	// Since Type's string representation is used as a SHOW JOBS output column, we
	// simply swap underscores for spaces in the identifier for very SQL-esque
	// names, like "BACKUP" and "SCHEMA CHANGE".
	return strings.Replace(Type_name[int32(t)], "_", " ", -1)
}

// WrapPayloadDetails wraps a Details object in the protobuf wrapper struct
// necessary to make it usable as the Details field of a Payload.
//
// Providing an unknown details type indicates programmer error and so causes a
// panic.
func WrapPayloadDetails(details Details) interface {
	isPayload_Details
} {
	switch d := details.(type) {
	case BackupDetails:
		return &Payload_Backup{Backup: &d}
	case RestoreDetails:
		return &Payload_Restore{Restore: &d}
	case SchemaChangeDetails:
		return &Payload_SchemaChange{SchemaChange: &d}
	case ImportDetails:
		return &Payload_Import{Import: &d}
	case ChangefeedDetails:
		return &Payload_Changefeed{Changefeed: &d}
	case CreateStatsDetails:
		return &Payload_CreateStats{CreateStats: &d}
	case SchemaChangeGCDetails:
		return &Payload_SchemaChangeGC{SchemaChangeGC: &d}
	case SyncMetaCacheDetails:
		return &Payload_SyncMetaCache{SyncMetaCache: &d}
	case ReplicationIngestionDetails:
		return &Payload_ReplicationIngestion{ReplicationIngestion: &d}
	case ReplicationStreamDetails:
		return &Payload_ReplicationStream{ReplicationStream: &d}
	case RestartDetails:
		return &Payload_RestartHistory{RestartHistory: &d}
	case ExportDetails:
		return &Payload_Export{Export: &d}
	case SqlScheduleDetails:
		return &Payload_SqlSchedule{SqlSchedule: &d}
	case ReplicaRebanalceDetails:
		return &Payload_ReplicaRebalance{ReplicaRebalance: &d}
	case TsTxnDetails:
		return &Payload_TsTxn{TsTxn: &d}
	default:
		panic(fmt.Sprintf("jobs.WrapPayloadDetails: unknown details type %T", d))
	}
}

// ChangefeedTargets is a set of id targets with metadata.
type ChangefeedTargets map[sqlbase.ID]ChangefeedTarget

// SchemaChangeDetailsFormatVersion is the format version for
// SchemaChangeDetails.
type SchemaChangeDetailsFormatVersion uint32

const (
	// BaseFormatVersion corresponds to the initial version of
	// SchemaChangeDetails, intended for the original version of schema change
	// jobs which were meant to be updated by a SchemaChanger instead of being run
	// as jobs by the job registry.
	BaseFormatVersion SchemaChangeDetailsFormatVersion = iota
	// JobResumerFormatVersion corresponds to the introduction of the schema
	// change job resumer. This version introduces the TableID and MutationID
	// fields, and, more generally, flags the job as being suitable for the job
	// registry to adopt.
	JobResumerFormatVersion

	// Silence unused warning.
	_ = BaseFormatVersion
)
