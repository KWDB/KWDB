// Copyright 2014 The Cockroach Authors.
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

package roachpb

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/concurrency/lock"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pkg/errors"
)

// UserPriority is a custom type for transaction's user priority.
type UserPriority float64

func (up UserPriority) String() string {
	switch up {
	case MinUserPriority:
		return "low"
	case UnspecifiedUserPriority, NormalUserPriority:
		return "normal"
	case MaxUserPriority:
		return "high"
	default:
		return fmt.Sprintf("%g", float64(up))
	}
}

const (
	// MinUserPriority is the minimum allowed user priority.
	MinUserPriority UserPriority = 0.001
	// UnspecifiedUserPriority means NormalUserPriority.
	UnspecifiedUserPriority UserPriority = 0
	// NormalUserPriority is set to 1, meaning ops run through the database
	// are all given equal weight when a random priority is chosen. This can
	// be set specifically via client.NewDBWithPriority().
	NormalUserPriority UserPriority = 1
	// MaxUserPriority is the maximum allowed user priority.
	MaxUserPriority UserPriority = 1000
)

// RequiresReadLease returns whether the ReadConsistencyType requires
// that a read-only request be performed on an active valid leaseholder.
func (rc ReadConsistencyType) RequiresReadLease() bool {
	switch rc {
	case CONSISTENT:
		return true
	case READ_UNCOMMITTED:
		return true
	case INCONSISTENT:
		return false
	}
	panic("unreachable")
}

// SupportsBatch determines whether the methods in the provided batch
// are supported by the ReadConsistencyType, returning an error if not.
func (rc ReadConsistencyType) SupportsBatch(ba BatchRequest) error {
	switch rc {
	case CONSISTENT:
		return nil
	case READ_UNCOMMITTED, INCONSISTENT:
		for _, ru := range ba.Requests {
			m := ru.GetInner().Method()
			switch m {
			case Get, Scan, ReverseScan:
			default:
				return errors.Errorf("method %s not allowed with %s batch", m, rc)
			}
		}
		return nil
	}
	panic("unreachable")
}

const (
	isAdmin             = 1 << iota // admin cmds don't go through raft, but run on lease holder
	isRead                          // read-only cmds don't go through raft, but may run on lease holder
	isWrite                         // write cmds go through raft and must be proposed on lease holder
	isTxn                           // txn commands may be part of a transaction
	isLocking                       // locking cmds acquire locks for their transaction  (implies isTxn)
	isIntentWrite                   // intent write cmds leave intents when they succeed (implies isWrite and isLocking)
	isRange                         // range commands may span multiple keys
	isReverse                       // reverse commands traverse ranges in descending direction
	isAlone                         // requests which must be alone in a batch
	isPrefix                        // requests which should be grouped with the next request in a batch
	isUnsplittable                  // range command that must not be split during sending
	skipLeaseCheck                  // commands which skip the check that the evaluting replica has a valid lease
	consultsTSCache                 // mutating commands which write data at a timestamp
	updatesTSCache                  // commands which update the timestamp cache
	updatesTSCacheOnErr             // commands which make read data available on errors
	needsRefresh                    // commands which require refreshes to avoid serializable retries
	canBackpressure                 // commands which deserve backpressure when a Range grows too large
)

// IsReadOnly returns true iff the request is read-only. A request is
// read-only if it does not go through raft, meaning that it cannot
// change any replicated state. However, read-only requests may still
// acquire locks with an unreplicated durability level; see IsLocking.
func IsReadOnly(args Request) bool {
	flags := args.flags()
	return (flags&isRead) != 0 && (flags&isWrite) == 0
}

// IsBlindWrite returns true iff the request is a blind-write. A request is a
// blind-write if it is a write that does not observe any key-value state when
// modifying that state (such as puts and deletes). This is in contrast with
// read-write requests, which do observe key-value state when modifying the
// state and may base their modifications off of this existing state (such as
// conditional puts and increments).
//
// As a result of being "blind", blind-writes are allowed to be more freely
// re-ordered with other writes. In practice, this means that they can be
// evaluated with a read timestamp below another write and a write timestamp
// above that write without needing to re-evaluate. This allows the WriteTooOld
// error that is generated during such an occurrence to be deferred.
func IsBlindWrite(args Request) bool {
	flags := args.flags()
	return (flags&isRead) == 0 && (flags&isWrite) != 0
}

// IsTransactional returns true if the request may be part of a
// transaction.
func IsTransactional(args Request) bool {
	return (args.flags() & isTxn) != 0
}

// IsLocking returns true if the request acquires locks when used within
// a transaction.
func IsLocking(args Request) bool {
	return (args.flags() & isLocking) != 0
}

// LockingDurability returns the durability of the locks acquired by the
// request. The function assumes that IsLocking(args).
func LockingDurability(args Request) lock.Durability {
	if IsReadOnly(args) {
		return lock.Unreplicated
	}
	return lock.Replicated
}

// IsIntentWrite returns true if the request produces write intents at
// the request's sequence number when used within a transaction.
func IsIntentWrite(args Request) bool {
	return (args.flags() & isIntentWrite) != 0
}

// IsRange returns true if the command is range-based and must include
// a start and an end key.
func IsRange(args Request) bool {
	return (args.flags() & isRange) != 0
}

// ConsultsTimestampCache returns whether the command must consult
// the timestamp cache to determine whether a mutation is safe at
// a proposed timestamp or needs to move to a higher timestamp to
// avoid re-writing history.
func ConsultsTimestampCache(args Request) bool {
	return (args.flags() & consultsTSCache) != 0
}

// UpdatesTimestampCache returns whether the command must update
// the timestamp cache in order to set a low water mark for the
// timestamp at which mutations to overlapping key(s) can write
// such that they don't re-write history.
func UpdatesTimestampCache(args Request) bool {
	return (args.flags() & updatesTSCache) != 0
}

// UpdatesTimestampCacheOnError returns whether the command must
// update the timestamp cache even on error, as in some cases the data
// which was read is returned (e.g. ConditionalPut ConditionFailedError).
func UpdatesTimestampCacheOnError(args Request) bool {
	return (args.flags() & updatesTSCacheOnErr) != 0
}

// NeedsRefresh returns whether the command must be refreshed in
// order to avoid client-side retries on serializable transactions.
func NeedsRefresh(args Request) bool {
	return (args.flags() & needsRefresh) != 0
}

// CanBackpressure returns whether the command can be backpressured
// when waiting for a Range to split after it has grown too large.
func CanBackpressure(args Request) bool {
	return (args.flags() & canBackpressure) != 0
}

// Request is an interface for RPC requests.
type Request interface {
	protoutil.Message
	// Header returns the request header.
	Header() RequestHeader
	// SetHeader sets the request header.
	SetHeader(RequestHeader)
	// Method returns the request method.
	Method() Method
	// ShallowCopy returns a shallow copy of the receiver.
	ShallowCopy() Request
	flags() int
}

// leaseRequestor is implemented by requests dealing with leases.
// Implementors return the previous lease at the time the request
// was proposed.
type leaseRequestor interface {
	prevLease() Lease
}

var _ leaseRequestor = &RequestLeaseRequest{}

func (rlr *RequestLeaseRequest) prevLease() Lease {
	return rlr.PrevLease
}

var _ leaseRequestor = &TransferLeaseRequest{}

func (tlr *TransferLeaseRequest) prevLease() Lease {
	return tlr.PrevLease
}

// Response is an interface for RPC responses.
type Response interface {
	protoutil.Message
	// Header returns the response header.
	Header() ResponseHeader
	// SetHeader sets the response header.
	SetHeader(ResponseHeader)
	// Verify verifies response integrity, as applicable.
	Verify(req Request) error
}

// combinable is implemented by response types whose corresponding
// requests may cross range boundaries, such as Scan or DeleteRange.
// combine() allows responses from individual ranges to be aggregated
// into a single one.
type combinable interface {
	combine(combinable) error
}

// CombineResponses attempts to combine the two provided responses. If both of
// the responses are combinable, they will be combined. If neither are
// combinable, the function is a no-op and returns a nil error. If one of the
// responses is combinable and the other isn't, the function returns an error.
func CombineResponses(left, right Response) error {
	cLeft, lOK := left.(combinable)
	cRight, rOK := right.(combinable)
	if lOK && rOK {
		return cLeft.combine(cRight)
	} else if lOK != rOK {
		return errors.Errorf("can not combine %T and %T", left, right)
	}
	return nil
}

// combine is used by range-spanning Response types (e.g. Scan or DeleteRange)
// to merge their headers.
func (rh *ResponseHeader) combine(otherRH ResponseHeader) error {
	if rh.Txn != nil && otherRH.Txn == nil {
		rh.Txn = nil
	}
	if rh.ResumeSpan != nil {
		return errors.Errorf("combining %+v with %+v", rh.ResumeSpan, otherRH.ResumeSpan)
	}
	rh.ResumeSpan = otherRH.ResumeSpan
	rh.ResumeReason = otherRH.ResumeReason
	rh.NumKeys += otherRH.NumKeys
	rh.NumBytes += otherRH.NumBytes
	rh.RangeInfos = append(rh.RangeInfos, otherRH.RangeInfos...)
	return nil
}

// combine implements the combinable interface.
func (sr *ScanResponse) combine(c combinable) error {
	otherSR := c.(*ScanResponse)
	if sr != nil {
		sr.Rows = append(sr.Rows, otherSR.Rows...)
		sr.IntentRows = append(sr.IntentRows, otherSR.IntentRows...)
		sr.BatchResponses = append(sr.BatchResponses, otherSR.BatchResponses...)
		if err := sr.ResponseHeader.combine(otherSR.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &AdminVerifyProtectedTimestampResponse{}

func (avptr *AdminVerifyProtectedTimestampResponse) combine(c combinable) error {
	other := c.(*AdminVerifyProtectedTimestampResponse)
	if avptr != nil {
		avptr.FailedRanges = append(avptr.FailedRanges, other.FailedRanges...)
		if err := avptr.ResponseHeader.combine(other.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &ScanResponse{}

// combine implements the combinable interface.
func (sr *ReverseScanResponse) combine(c combinable) error {
	otherSR := c.(*ReverseScanResponse)
	if sr != nil {
		sr.Rows = append(sr.Rows, otherSR.Rows...)
		sr.IntentRows = append(sr.IntentRows, otherSR.IntentRows...)
		sr.BatchResponses = append(sr.BatchResponses, otherSR.BatchResponses...)
		if err := sr.ResponseHeader.combine(otherSR.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &ReverseScanResponse{}

// combine implements the combinable interface.
func (dr *DeleteRangeResponse) combine(c combinable) error {
	otherDR := c.(*DeleteRangeResponse)
	if dr != nil {
		dr.Keys = append(dr.Keys, otherDR.Keys...)
		if err := dr.ResponseHeader.combine(otherDR.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &DeleteRangeResponse{}

// combine implements the combinable interface.
func (dr *TsDeleteMultiEntitiesDataResponse) combine(c combinable) error {
	otherDR := c.(*TsDeleteMultiEntitiesDataResponse)
	if dr != nil {
		//dr.NumKeys += otherDR.NumKeys
		if err := dr.ResponseHeader.combine(otherDR.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &TsDeleteMultiEntitiesDataResponse{}

// combine implements the combinable interface.
func (dr *TsDeleteResponse) combine(c combinable) error {
	otherDR := c.(*TsDeleteResponse)
	if dr != nil {
		//dr.NumKeys += otherDR.NumKeys
		if err := dr.ResponseHeader.combine(otherDR.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &TsDeleteResponse{}

// combine implements the combinable interface.
func (dr *TsDeleteEntityResponse) combine(c combinable) error {
	otherDR := c.(*TsDeleteEntityResponse)
	if dr != nil {
		//dr.NumKeys += otherDR.NumKeys
		if err := dr.ResponseHeader.combine(otherDR.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &TsDeleteEntityResponse{}

// combine implements the combinable interface.
func (dr *RevertRangeResponse) combine(c combinable) error {
	otherDR := c.(*RevertRangeResponse)
	if dr != nil {
		if err := dr.ResponseHeader.combine(otherDR.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &RevertRangeResponse{}

// combine implements the combinable interface.
func (rr *ResolveIntentRangeResponse) combine(c combinable) error {
	otherRR := c.(*ResolveIntentRangeResponse)
	if rr != nil {
		if err := rr.ResponseHeader.combine(otherRR.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &ResolveIntentRangeResponse{}

// Combine implements the combinable interface.
func (cc *CheckConsistencyResponse) combine(c combinable) error {
	if cc != nil {
		otherCC := c.(*CheckConsistencyResponse)
		cc.Result = append(cc.Result, otherCC.Result...)
		if err := cc.ResponseHeader.combine(otherCC.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &CheckConsistencyResponse{}

// Combine implements the combinable interface.
func (grs *GetReplicaStatusResponse) combine(c combinable) error {
	if grs != nil {
		otherGRS := c.(*GetReplicaStatusResponse)
		grs.ReplicaStatus = append(grs.ReplicaStatus, otherGRS.ReplicaStatus...)
		if err := grs.ResponseHeader.combine(otherGRS.Header()); err != nil {
			return err
		}
	}
	return nil
}

var _ combinable = &GetReplicaStatusResponse{}

// Combine implements the combinable interface.
func (er *ExportResponse) combine(c combinable) error {
	if er != nil {
		otherER := c.(*ExportResponse)
		if err := er.ResponseHeader.combine(otherER.Header()); err != nil {
			return err
		}
		er.Files = append(er.Files, otherER.Files...)
	}
	return nil
}

var _ combinable = &ExportResponse{}

// Combine implements the combinable interface.
func (r *AdminScatterResponse) combine(c combinable) error {
	if r != nil {
		otherR := c.(*AdminScatterResponse)
		if err := r.ResponseHeader.combine(otherR.Header()); err != nil {
			return err
		}
		r.Ranges = append(r.Ranges, otherR.Ranges...)
	}
	return nil
}

var _ combinable = &AdminScatterResponse{}

// Header implements the Request interface.
func (rh RequestHeader) Header() RequestHeader {
	return rh
}

// SetHeader implements the Request interface.
func (rh *RequestHeader) SetHeader(other RequestHeader) {
	*rh = other
}

// Span returns the key range that the Request operates over.
func (rh RequestHeader) Span() Span {
	return Span{Key: rh.Key, EndKey: rh.EndKey}
}

// SetSpan addresses the RequestHeader to the specified key span.
func (rh *RequestHeader) SetSpan(s Span) {
	rh.Key = s.Key
	rh.EndKey = s.EndKey
}

// RequestHeaderFromSpan creates a RequestHeader addressed at the specified key
// span.
func RequestHeaderFromSpan(s Span) RequestHeader {
	return RequestHeader{Key: s.Key, EndKey: s.EndKey}
}

func (h *BatchResponse_Header) combine(o BatchResponse_Header) error {
	if h.Error != nil || o.Error != nil {
		return errors.Errorf(
			"can't combine batch responses with errors, have errors %q and %q",
			h.Error, o.Error,
		)
	}
	h.Timestamp.Forward(o.Timestamp)
	if txn := o.Txn; txn != nil {
		if h.Txn == nil {
			h.Txn = txn.Clone()
		} else {
			h.MaybeUpdateWriteKeyCount(o.Txn)
			h.Txn.Update(txn)
		}
	}
	h.Now.Forward(o.Now)
	h.CollectedSpans = append(h.CollectedSpans, o.CollectedSpans...)
	return nil
}

// MaybeUpdateWriteKeyCount add BatchWriteKeyCount with o.BatchWriteKeyCount.
func (h *BatchResponse_Header) MaybeUpdateWriteKeyCount(o *Transaction) {
	if h.Txn == nil || o == nil {
		return
	}
	h.Txn.BatchWriteKeyCount += o.BatchWriteKeyCount
}

// SetHeader implements the Response interface.
func (rh *ResponseHeader) SetHeader(other ResponseHeader) {
	*rh = other
}

// Header implements the Response interface for ResponseHeader.
func (rh ResponseHeader) Header() ResponseHeader {
	return rh
}

// Verify implements the Response interface for ResopnseHeader with a
// default noop. Individual response types should override this method
// if they contain checksummed data which can be verified.
func (rh *ResponseHeader) Verify(req Request) error {
	return nil
}

// Verify verifies the integrity of the get response value.
func (gr *GetResponse) Verify(req Request) error {
	if gr.Value != nil {
		return gr.Value.Verify(req.Header().Key)
	}
	return nil
}

// Verify verifies the integrity of every value returned in the scan.
func (sr *ScanResponse) Verify(req Request) error {
	for _, kv := range sr.Rows {
		if err := kv.Value.Verify(kv.Key); err != nil {
			return err
		}
	}
	return nil
}

// Verify verifies the integrity of every value returned in the reverse scan.
func (sr *ReverseScanResponse) Verify(req Request) error {
	for _, kv := range sr.Rows {
		if err := kv.Value.Verify(kv.Key); err != nil {
			return err
		}
	}
	return nil
}

// MustSetInner sets the Request contained in the union. It panics if the
// request is not recognized by the union type. The RequestUnion is reset
// before being repopulated.
func (ru *RequestUnion) MustSetInner(args Request) {
	ru.Reset()
	if !ru.SetInner(args) {
		panic(fmt.Sprintf("%T excludes %T", ru, args))
	}
}

// MustSetInner sets the Response contained in the union. It panics if the
// response is not recognized by the union type. The ResponseUnion is reset
// before being repopulated.
func (ru *ResponseUnion) MustSetInner(reply Response) {
	ru.Reset()
	if !ru.SetInner(reply) {
		panic(fmt.Sprintf("%T excludes %T", ru, reply))
	}
}

// Method implements the Request interface.
func (*GetRequest) Method() Method { return Get }

// Method implements the Request interface.
func (*PutRequest) Method() Method { return Put }

// Method implements the Request interface.
func (*ConditionalPutRequest) Method() Method { return ConditionalPut }

// Method implements the Request interface.
func (*InitPutRequest) Method() Method { return InitPut }

// Method implements the Request interface.
func (*IncrementRequest) Method() Method { return Increment }

// Method implements the Request interface.
func (*DeleteRequest) Method() Method { return Delete }

// Method implements the Request interface.
func (*DeleteRangeRequest) Method() Method { return DeleteRange }

// Method implements the Request interface.
func (*ClearRangeRequest) Method() Method { return ClearRange }

// Method implements the Request interface.
func (*RevertRangeRequest) Method() Method { return RevertRange }

// Method implements the Request interface.
func (*ScanRequest) Method() Method { return Scan }

// Method implements the Request interface.
func (*ReverseScanRequest) Method() Method { return ReverseScan }

// Method implements the Request interface.
func (*CheckConsistencyRequest) Method() Method { return CheckConsistency }

// Method implements the Request interface.
func (*GetReplicaStatusRequest) Method() Method { return GetReplicaStatus }

// Method implements the Request interface.
func (*EndTxnRequest) Method() Method { return EndTxn }

// Method implements the Request interface.
func (*AdminSplitRequest) Method() Method { return AdminSplit }

// Method implements the ts Request interface.
func (*AdminSplitForTsRequest) Method() Method { return AdminSplit }

// Method implements the Request interface.
func (*AdminUnsplitRequest) Method() Method { return AdminUnsplit }

// Method implements the Request interface.
func (*AdminMergeRequest) Method() Method { return AdminMerge }

// Method implements the Request interface.
func (*AdminTransferLeaseRequest) Method() Method { return AdminTransferLease }

// Method implements the Request interface.
func (*AdminChangeReplicasRequest) Method() Method { return AdminChangeReplicas }

// Method implements the Request interface.
func (*AdminRelocateRangeRequest) Method() Method { return AdminRelocateRange }

// Method implements the Request interface.
func (*HeartbeatTxnRequest) Method() Method { return HeartbeatTxn }

// Method implements the Request interface.
func (*GCRequest) Method() Method { return GC }

// Method implements the Request interface.
func (*PushTxnRequest) Method() Method { return PushTxn }

// Method implements the Request interface.
func (*RecoverTxnRequest) Method() Method { return RecoverTxn }

// Method implements the Request interface.
func (*QueryTxnRequest) Method() Method { return QueryTxn }

// Method implements the Request interface.
func (*QueryIntentRequest) Method() Method { return QueryIntent }

// Method implements the Request interface.
func (*ResolveIntentRequest) Method() Method { return ResolveIntent }

// Method implements the Request interface.
func (*ResolveIntentRangeRequest) Method() Method { return ResolveIntentRange }

// Method implements the Request interface.
func (*MergeRequest) Method() Method { return Merge }

// Method implements the Request interface.
func (*TruncateLogRequest) Method() Method { return TruncateLog }

// Method implements the Request interface.
func (*RequestLeaseRequest) Method() Method { return RequestLease }

// Method implements the Request interface.
func (*TransferLeaseRequest) Method() Method { return TransferLease }

// Method implements the Request interface.
func (*LeaseInfoRequest) Method() Method { return LeaseInfo }

// Method implements the Request interface.
func (*ComputeChecksumRequest) Method() Method { return ComputeChecksum }

// Method implements the Request interface.
func (*WriteBatchRequest) Method() Method { return WriteBatch }

// Method implements the Request interface.
func (*ExportRequest) Method() Method { return Export }

// Method implements the Request interface.
func (*ImportRequest) Method() Method { return Import }

// Method implements the Request interface.
func (*AdminScatterRequest) Method() Method { return AdminScatter }

// Method implements the Request interface.
func (*AddSSTableRequest) Method() Method { return AddSSTable }

// Method implements the Request interface.
func (*RecomputeStatsRequest) Method() Method { return RecomputeStats }

// Method implements the Request interface.
func (*RefreshRequest) Method() Method { return Refresh }

// Method implements the Request interface.
func (*RefreshRangeRequest) Method() Method { return RefreshRange }

// Method implements the Request interface.
func (*SubsumeRequest) Method() Method { return Subsume }

// Method implements the Request interface.
func (*RangeStatsRequest) Method() Method { return RangeStats }

// Method implements the Request interface.
func (*AdminVerifyProtectedTimestampRequest) Method() Method { return AdminVerifyProtectedTimestamp }

// Method implements the Request interface.
func (*TsPutRequest) Method() Method { return TsPut }

// Method implements the Request interface.
func (*TsRowPutRequest) Method() Method { return TsRowPut }

// Method implements the Request interface.
func (*TsPutTagRequest) Method() Method { return TsPutTag }

// Method implements the Request interface.
func (*TsDeleteRequest) Method() Method { return TsDelete }

// Method implements the Request interface.
func (*TsDeleteMultiEntitiesDataRequest) Method() Method { return TsDeleteMultiEntitiesData }

// Method implements the Request interface.
func (*TsDeleteEntityRequest) Method() Method { return TsDeleteEntity }

// Method implements the Request interface.
func (*TsTagUpdateRequest) Method() Method { return TsTagUpdate }

// ShallowCopy implements the Request interface.
func (gr *GetRequest) ShallowCopy() Request {
	shallowCopy := *gr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (pr *PutRequest) ShallowCopy() Request {
	shallowCopy := *pr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (cpr *ConditionalPutRequest) ShallowCopy() Request {
	shallowCopy := *cpr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (pr *InitPutRequest) ShallowCopy() Request {
	shallowCopy := *pr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (ir *IncrementRequest) ShallowCopy() Request {
	shallowCopy := *ir
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (dr *DeleteRequest) ShallowCopy() Request {
	shallowCopy := *dr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (drr *DeleteRangeRequest) ShallowCopy() Request {
	shallowCopy := *drr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (crr *ClearRangeRequest) ShallowCopy() Request {
	shallowCopy := *crr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (crr *RevertRangeRequest) ShallowCopy() Request {
	shallowCopy := *crr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (sr *ScanRequest) ShallowCopy() Request {
	shallowCopy := *sr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (rsr *ReverseScanRequest) ShallowCopy() Request {
	shallowCopy := *rsr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (ccr *CheckConsistencyRequest) ShallowCopy() Request {
	shallowCopy := *ccr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (grs *GetReplicaStatusRequest) ShallowCopy() Request {
	shallowCopy := *grs
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (etr *EndTxnRequest) ShallowCopy() Request {
	shallowCopy := *etr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (asr *AdminSplitRequest) ShallowCopy() Request {
	shallowCopy := *asr
	return &shallowCopy
}

// ShallowCopy implements the ts Request interface.
func (asr *AdminSplitForTsRequest) ShallowCopy() Request {
	shallowCopy := *asr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (aur *AdminUnsplitRequest) ShallowCopy() Request {
	shallowCopy := *aur
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (amr *AdminMergeRequest) ShallowCopy() Request {
	shallowCopy := *amr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (atlr *AdminTransferLeaseRequest) ShallowCopy() Request {
	shallowCopy := *atlr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (acrr *AdminChangeReplicasRequest) ShallowCopy() Request {
	shallowCopy := *acrr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (acrr *AdminRelocateRangeRequest) ShallowCopy() Request {
	shallowCopy := *acrr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (htr *HeartbeatTxnRequest) ShallowCopy() Request {
	shallowCopy := *htr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (gcr *GCRequest) ShallowCopy() Request {
	shallowCopy := *gcr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (ptr *PushTxnRequest) ShallowCopy() Request {
	shallowCopy := *ptr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (rtr *RecoverTxnRequest) ShallowCopy() Request {
	shallowCopy := *rtr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (qtr *QueryTxnRequest) ShallowCopy() Request {
	shallowCopy := *qtr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (pir *QueryIntentRequest) ShallowCopy() Request {
	shallowCopy := *pir
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (rir *ResolveIntentRequest) ShallowCopy() Request {
	shallowCopy := *rir
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (rirr *ResolveIntentRangeRequest) ShallowCopy() Request {
	shallowCopy := *rirr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (mr *MergeRequest) ShallowCopy() Request {
	shallowCopy := *mr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (tlr *TruncateLogRequest) ShallowCopy() Request {
	shallowCopy := *tlr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (rlr *RequestLeaseRequest) ShallowCopy() Request {
	shallowCopy := *rlr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (tlr *TransferLeaseRequest) ShallowCopy() Request {
	shallowCopy := *tlr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (lt *LeaseInfoRequest) ShallowCopy() Request {
	shallowCopy := *lt
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (ccr *ComputeChecksumRequest) ShallowCopy() Request {
	shallowCopy := *ccr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *WriteBatchRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (ekr *ExportRequest) ShallowCopy() Request {
	shallowCopy := *ekr
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *ImportRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *AdminScatterRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *AddSSTableRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *RecomputeStatsRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *RefreshRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *RefreshRangeRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *SubsumeRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *RangeStatsRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *AdminVerifyProtectedTimestampRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *TsPutRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *TsRowPutRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *TsPutTagRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *TsDeleteRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *TsDeleteMultiEntitiesDataRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *TsDeleteEntityRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// ShallowCopy implements the Request interface.
func (r *TsTagUpdateRequest) ShallowCopy() Request {
	shallowCopy := *r
	return &shallowCopy
}

// NewGet returns a Request initialized to get the value at key.
func NewGet(key Key) Request {
	return &GetRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
	}
}

// NewIncrement returns a Request initialized to increment the value at
// key by increment.
func NewIncrement(key Key, increment int64) Request {
	return &IncrementRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Increment: increment,
	}
}

// NewPut returns a Request initialized to put the value at key.
func NewPut(key Key, value Value) Request {
	value.InitChecksum(key)
	return &PutRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Value: value,
	}
}

// NewPutInline returns a Request initialized to put the value at key
// using an inline value.
func NewPutInline(key Key, value Value) Request {
	value.InitChecksum(key)
	return &PutRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Value:  value,
		Inline: true,
	}
}

// NewConditionalPut returns a Request initialized to put value as a byte
// slice at key if the existing value at key equals expValueBytes.
func NewConditionalPut(key Key, value, expValue Value, allowNotExist bool) Request {
	value.InitChecksum(key)
	var expValuePtr *Value
	if expValue.RawBytes != nil {
		// Make a copy to avoid forcing expValue itself on to the heap.
		expValueTmp := expValue
		expValuePtr = &expValueTmp
		expValue.InitChecksum(key)
	}
	return &ConditionalPutRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Value:               value,
		ExpValue:            expValuePtr,
		AllowIfDoesNotExist: allowNotExist,
	}
}

// NewInitPut returns a Request initialized to put the value at key, as long as
// the key doesn't exist, returning a ConditionFailedError if the key exists and
// the existing value is different from value. If failOnTombstones is set to
// true, tombstones count as mismatched values and will cause a
// ConditionFailedError.
func NewInitPut(key Key, value Value, failOnTombstones bool) Request {
	value.InitChecksum(key)
	return &InitPutRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Value:            value,
		FailOnTombstones: failOnTombstones,
	}
}

// NewDelete returns a Request initialized to delete the value at key.
func NewDelete(key Key) Request {
	return &DeleteRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
	}
}

// NewDeleteRange returns a Request initialized to delete the values in
// the given key range (excluding the endpoint).
func NewDeleteRange(startKey, endKey Key, returnKeys bool) Request {
	return &DeleteRangeRequest{
		RequestHeader: RequestHeader{
			Key:    startKey,
			EndKey: endKey,
		},
		ReturnKeys: returnKeys,
	}
}

// NewScan returns a Request initialized to scan from start to end keys.
// If forUpdate is true, unreplicated, exclusive locks are acquired on
// each of the resulting keys.
func NewScan(key, endKey Key, forUpdate bool) Request {
	return &ScanRequest{
		RequestHeader: RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
		KeyLocking: scanLockStrength(forUpdate),
	}
}

// NewScanAllMvcc returns a Request initialized to scan from start to end keys.
// If forUpdate is true, unreplicated, exclusive locks are acquired on
// each of the resulting keys.
func NewScanAllMvcc(key, endKey Key, forUpdate bool) Request {
	return &ScanRequest{
		RequestHeader: RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
		KeyLocking:                  scanLockStrength(forUpdate),
		IsScanAllMvccVerForOneTable: true,
	}
}

// NewReverseScan returns a Request initialized to reverse scan from end.
// If forUpdate is true, unreplicated, exclusive locks are acquired on
// each of the resulting keys.
func NewReverseScan(key, endKey Key, forUpdate bool) Request {
	return &ReverseScanRequest{
		RequestHeader: RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
		KeyLocking: scanLockStrength(forUpdate),
	}
}

func scanLockStrength(forUpdate bool) lock.Strength {
	if forUpdate {
		return lock.Exclusive
	}
	return lock.None
}

func (*GetRequest) flags() int {
	return isRead | isTxn | updatesTSCache | needsRefresh
}

func (*PutRequest) flags() int {
	return isWrite | isTxn | isLocking | isIntentWrite | consultsTSCache | canBackpressure
}

func (*TsPutRequest) flags() int {
	return isWrite | isTxn | isLocking | consultsTSCache | canBackpressure
}

func (*TsRowPutRequest) flags() int {
	return isWrite | isTxn | isLocking | consultsTSCache | canBackpressure | isRange
}

func (*TsPutTagRequest) flags() int {
	return isWrite | isTxn | isLocking | consultsTSCache | canBackpressure | isRange
}

func (*TsDeleteRequest) flags() int {
	return isWrite | isTxn | isLocking | consultsTSCache | canBackpressure | isRange
}

func (*TsDeleteMultiEntitiesDataRequest) flags() int {
	return isWrite | isTxn | isLocking | consultsTSCache | canBackpressure | isRange
}

func (*TsDeleteEntityRequest) flags() int {
	return isWrite | isTxn | isLocking | consultsTSCache | canBackpressure | isRange
}

func (*TsTagUpdateRequest) flags() int {
	return isWrite | isTxn | isLocking | consultsTSCache | canBackpressure | isRange
}

// ConditionalPut effectively reads without writing if it hits a
// ConditionFailedError, so it must update the timestamp cache in this case.
// ConditionalPuts do not require a refresh because on write-too-old errors,
// they return an error immediately instead of continuing a serializable
// transaction to be retried at end transaction.
func (*ConditionalPutRequest) flags() int {
	return isRead | isWrite | isTxn | isLocking | isIntentWrite | consultsTSCache | updatesTSCache | updatesTSCacheOnErr | canBackpressure
}

// InitPut, like ConditionalPut, effectively reads without writing if it hits a
// ConditionFailedError, so it must update the timestamp cache in this case.
// InitPuts do not require a refresh because on write-too-old errors, they
// return an error immediately instead of continuing a serializable transaction
// to be retried at end transaction.
func (*InitPutRequest) flags() int {
	return isRead | isWrite | isTxn | isLocking | isIntentWrite | consultsTSCache | updatesTSCache | updatesTSCacheOnErr | canBackpressure
}

// Increment reads the existing value, but always leaves an intent so
// it does not need to update the timestamp cache. Increments do not
// require a refresh because on write-too-old errors, they return an
// error immediately instead of continuing a serializable transaction
// to be retried at end transaction.
func (*IncrementRequest) flags() int {
	return isRead | isWrite | isTxn | isLocking | isIntentWrite | consultsTSCache | canBackpressure
}

func (*DeleteRequest) flags() int {
	return isWrite | isTxn | isLocking | isIntentWrite | consultsTSCache | canBackpressure
}

func (drr *DeleteRangeRequest) flags() int {
	// DeleteRangeRequest has different properties if the "inline" flag is set.
	// This flag indicates that the request is deleting inline MVCC values,
	// which cannot be deleted transactionally - inline DeleteRange will thus
	// fail if executed as part of a transaction. This alternate flag set
	// is needed to prevent the command from being automatically wrapped into a
	// transaction by TxnCoordSender, which can occur if the command spans
	// multiple ranges.
	//
	// TODO(mrtracy): The behavior of DeleteRangeRequest with "inline" set has
	// likely diverged enough that it should be promoted into its own command.
	// However, it is complicated to plumb a new command through the system,
	// while this special case in flags() fixes all current issues succinctly.
	// This workaround does not preclude us from creating a separate
	// "DeleteInlineRange" command at a later date.
	if drr.Inline {
		return isWrite | isRange | isAlone
	}
	// DeleteRange updates the timestamp cache as it doesn't leave intents or
	// tombstones for keys which don't yet exist, but still wants to prevent
	// anybody from writing under it. Note that, even if we didn't update the ts
	// cache, deletes of keys that exist would not be lost (since the DeleteRange
	// leaves intents on those keys), but deletes of "empty space" would.
	return isWrite | isTxn | isLocking | isIntentWrite | isRange | consultsTSCache | updatesTSCache | needsRefresh | canBackpressure
}

// Note that ClearRange commands cannot be part of a transaction as
// they clear all MVCC versions.
func (*ClearRangeRequest) flags() int { return isWrite | isRange | isAlone }

// Note that RevertRange commands cannot be part of a transaction as
// they clear all MVCC versions above their target time.
func (*RevertRangeRequest) flags() int { return isWrite | isRange }

func (sr *ScanRequest) flags() int {
	maybeLocking := 0
	if sr.KeyLocking != lock.None {
		maybeLocking = isLocking
	}
	return isRead | isRange | isTxn | maybeLocking | updatesTSCache | needsRefresh
}

func (rsr *ReverseScanRequest) flags() int {
	maybeLocking := 0
	if rsr.KeyLocking != lock.None {
		maybeLocking = isLocking
	}
	return isRead | isRange | isReverse | isTxn | maybeLocking | updatesTSCache | needsRefresh
}

// EndTxn updates the timestamp cache to prevent replays.
// Replays for the same transaction key and timestamp will have
// Txn.WriteTooOld=true and must retry on EndTxn.
func (*EndTxnRequest) flags() int              { return isWrite | isTxn | isAlone | updatesTSCache }
func (*AdminSplitRequest) flags() int          { return isAdmin | isAlone }
func (*AdminSplitForTsRequest) flags() int     { return isAdmin | isAlone }
func (*AdminUnsplitRequest) flags() int        { return isAdmin | isAlone }
func (*AdminMergeRequest) flags() int          { return isAdmin | isAlone }
func (*AdminTransferLeaseRequest) flags() int  { return isAdmin | isAlone }
func (*AdminChangeReplicasRequest) flags() int { return isAdmin | isAlone }
func (*AdminRelocateRangeRequest) flags() int  { return isAdmin | isAlone }
func (*HeartbeatTxnRequest) flags() int        { return isWrite | isTxn }
func (*GCRequest) flags() int                  { return isWrite | isRange }

// PushTxnRequest updates different marker keys in the timestamp cache when
// pushing a transaction's timestamp and when aborting a transaction.
func (*PushTxnRequest) flags() int {
	return isWrite | isAlone | updatesTSCache | updatesTSCache
}
func (*RecoverTxnRequest) flags() int { return isWrite | isAlone | updatesTSCache }
func (*QueryTxnRequest) flags() int   { return isRead | isAlone }

// QueryIntent only updates the timestamp cache when attempting to prevent an
// intent that is found missing from ever being written in the future. See
// QueryIntentRequest_PREVENT.
func (*QueryIntentRequest) flags() int {
	return isRead | isPrefix | updatesTSCache | updatesTSCacheOnErr
}
func (*ResolveIntentRequest) flags() int      { return isWrite }
func (*ResolveIntentRangeRequest) flags() int { return isWrite | isRange }
func (*TruncateLogRequest) flags() int        { return isWrite }
func (*MergeRequest) flags() int              { return isWrite | canBackpressure }
func (*RequestLeaseRequest) flags() int       { return isWrite | isAlone | skipLeaseCheck }

// LeaseInfoRequest is usually executed in an INCONSISTENT batch, which has the
// effect of the `skipLeaseCheck` flag that lease write operations have.
func (*LeaseInfoRequest) flags() int { return isRead | isAlone }
func (*TransferLeaseRequest) flags() int {
	// TransferLeaseRequest requires the lease, which is checked in
	// `AdminTransferLease()` before the TransferLeaseRequest is created and sent
	// for evaluation and in the usual way at application time (i.e.
	// replica.processRaftCommand() checks that the lease hasn't changed since the
	// command resulting from the evaluation of TransferLeaseRequest was
	// proposed).
	//
	// But we're marking it with skipLeaseCheck because `redirectOnOrAcquireLease`
	// can't be used before evaluation as, by the time that call would be made,
	// the store has registered that a transfer is in progress and
	// `redirectOnOrAcquireLease` would already tentatively redirect to the future
	// lease holder.
	return isWrite | isAlone | skipLeaseCheck
}
func (*RecomputeStatsRequest) flags() int                { return isWrite | isAlone }
func (*ComputeChecksumRequest) flags() int               { return isWrite }
func (*CheckConsistencyRequest) flags() int              { return isAdmin | isRange }
func (*GetReplicaStatusRequest) flags() int              { return isAdmin | isRange }
func (*WriteBatchRequest) flags() int                    { return isWrite | isRange }
func (*ExportRequest) flags() int                        { return isRead | isRange | updatesTSCache }
func (*ImportRequest) flags() int                        { return isAdmin | isAlone }
func (*AdminScatterRequest) flags() int                  { return isAdmin | isRange | isAlone }
func (*AdminVerifyProtectedTimestampRequest) flags() int { return isAdmin | isRange | isAlone }
func (*AddSSTableRequest) flags() int {
	return isWrite | isRange | isAlone | isUnsplittable | canBackpressure
}

// RefreshRequest and RefreshRangeRequest both determine which timestamp cache
// they update based on their Write parameter.
func (r *RefreshRequest) flags() int {
	return isRead | isTxn | updatesTSCache
}
func (r *RefreshRangeRequest) flags() int {
	return isRead | isTxn | isRange | updatesTSCache
}

func (*SubsumeRequest) flags() int    { return isRead | isAlone | updatesTSCache }
func (*RangeStatsRequest) flags() int { return isRead }

// IsParallelCommit returns whether the EndTxn request is attempting to perform
// a parallel commit. See txn_interceptor_committer.go for a discussion about
// parallel commits.
func (etr *EndTxnRequest) IsParallelCommit() bool {
	return etr.Commit && len(etr.InFlightWrites) > 0
}

// Keys returns credentials in an aws.Config.
func (b *ExternalStorage_S3) Keys() *aws.Config {
	return &aws.Config{
		Credentials: credentials.NewStaticCredentials(b.AccessKey, b.Secret, b.TempToken),
	}
}

// InsertRangeInfo inserts ri into a slice of RangeInfo's if a descriptor for
// the same range is not already present. If it is present, it's overwritten;
// the rationale being that ri is newer information than what we had before.
func InsertRangeInfo(ris []RangeInfo, ri RangeInfo) []RangeInfo {
	for i := range ris {
		if ris[i].Desc.RangeID == ri.Desc.RangeID {
			ris[i] = ri
			return ris
		}
	}
	return append(ris, ri)
}

// BulkOpSummaryID returns the key within a BulkOpSummary's EntryCounts map for
// the given table and index ID. This logic is mirrored in c++ in rowcounter.cc.
func BulkOpSummaryID(tableID, indexID uint64) uint64 {
	return (tableID << 32) | indexID
}

// Add combines the values from other, for use on an accumulator BulkOpSummary.
func (b *BulkOpSummary) Add(other BulkOpSummary) {
	b.DataSize += other.DataSize
	b.DeprecatedRows += other.DeprecatedRows
	b.DeprecatedIndexEntries += other.DeprecatedIndexEntries
	b.TimeSeriesCounts += other.TimeSeriesCounts
	b.RejectedCounts += other.RejectedCounts
	b.AbandonCounts += other.AbandonCounts
	if other.EntryCounts != nil && b.EntryCounts == nil {
		b.EntryCounts = make(map[uint64]int64, len(other.EntryCounts))
	}
	for i := range other.EntryCounts {
		b.EntryCounts[i] += other.EntryCounts[i]
	}
}

// MustSetValue is like SetValue, except it resets the enum and panics if the
// provided value is not a valid variant type.
func (e *RangeFeedEvent) MustSetValue(value interface{}) {
	e.Reset()
	if !e.SetValue(value) {
		panic(fmt.Sprintf("%T excludes %T", e, value))
	}
}

// ShallowCopy returns a shallow copy of the receiver and its variant type.
func (e *RangeFeedEvent) ShallowCopy() *RangeFeedEvent {
	cpy := *e
	switch t := cpy.GetValue().(type) {
	case *RangeFeedValue:
		cpyVal := *t
		cpy.MustSetValue(&cpyVal)
	case *RangeFeedCheckpoint:
		cpyChk := *t
		cpy.MustSetValue(&cpyChk)
	case *RangeFeedError:
		cpyErr := *t
		cpy.MustSetValue(&cpyErr)
	case *Commit:
		cpyCmt := *t
		cpy.MustSetValue(&cpyCmt)

	default:
		panic(fmt.Sprintf("unexpected RangeFeedEvent variant: %v", t))
	}
	return &cpy
}

// MakeReplicationChanges returns a slice of changes of the given type with an
// item for each target.
func MakeReplicationChanges(
	changeType ReplicaChangeType, targets ...ReplicationTarget,
) []ReplicationChange {
	chgs := make([]ReplicationChange, 0, len(targets))
	for _, target := range targets {
		chgs = append(chgs, ReplicationChange{
			ChangeType: changeType,
			Target:     target,
		})
	}
	return chgs
}

// AddChanges adds a batch of changes to the request in a backwards-compatible
// way.
func (acrr *AdminChangeReplicasRequest) AddChanges(chgs ...ReplicationChange) {
	acrr.InternalChanges = append(acrr.InternalChanges, chgs...)

	acrr.DeprecatedChangeType = chgs[0].ChangeType
	for _, chg := range chgs {
		acrr.DeprecatedTargets = append(acrr.DeprecatedTargets, chg.Target)
	}
}

// ReplicationChanges is a slice of ReplicationChange.
type ReplicationChanges []ReplicationChange

func (rc ReplicationChanges) byType(typ ReplicaChangeType) []ReplicationTarget {
	var sl []ReplicationTarget
	for _, chg := range rc {
		if chg.ChangeType == typ {
			sl = append(sl, chg.Target)
		}
	}
	return sl
}

// Additions returns a slice of all contained replication changes that add replicas.
func (rc ReplicationChanges) Additions() []ReplicationTarget {
	return rc.byType(ADD_REPLICA)
}

// Removals returns a slice of all contained replication changes that remove replicas.
func (rc ReplicationChanges) Removals() []ReplicationTarget {
	return rc.byType(REMOVE_REPLICA)
}

// Changes returns the changes requested by this AdminChangeReplicasRequest, taking
// the deprecated method of doing so into account.
func (acrr *AdminChangeReplicasRequest) Changes() []ReplicationChange {
	if len(acrr.InternalChanges) > 0 {
		return acrr.InternalChanges
	}

	sl := make([]ReplicationChange, len(acrr.DeprecatedTargets))
	for _, target := range acrr.DeprecatedTargets {
		sl = append(sl, ReplicationChange{
			ChangeType: acrr.DeprecatedChangeType,
			Target:     target,
		})
	}
	return sl
}

// AsLockUpdate creates a lock update message corresponding to the given resolve
// intent request.
func (rir *ResolveIntentRequest) AsLockUpdate() LockUpdate {
	return LockUpdate{
		Span:           rir.Span(),
		Txn:            rir.IntentTxn,
		Status:         rir.Status,
		IgnoredSeqNums: rir.IgnoredSeqNums,
	}
}

// AsLockUpdate creates a lock update message corresponding to the given resolve
// intent range request.
func (rirr *ResolveIntentRangeRequest) AsLockUpdate() LockUpdate {
	return LockUpdate{
		Span:           rirr.Span(),
		Txn:            rirr.IntentTxn,
		Status:         rirr.Status,
		IgnoredSeqNums: rirr.IgnoredSeqNums,
	}
}
