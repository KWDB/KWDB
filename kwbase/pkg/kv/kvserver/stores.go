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

package kvserver

import (
	"context"
	"fmt"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/pkg/errors"
)

// Stores provides methods to access a collection of stores. There's
// a visitor pattern and also an implementation of the client.Sender
// interface which directs a call to the appropriate store based on
// the call's key range. Stores also implements the gossip.Storage
// interface, which allows gossip bootstrap information to be
// persisted consistently to every store and the most recent bootstrap
// information to be read at node startup.
type Stores struct {
	log.AmbientContext
	clock    *hlc.Clock
	storeMap syncutil.IntMap // map[roachpb.StoreID]*Store
	// These two versions are usually
	// clusterversion.binary{,MinimumSupported}Version, respectively. They are
	// changed in some tests.
	binaryVersion             roachpb.Version
	binaryMinSupportedVersion roachpb.Version

	mu struct {
		syncutil.Mutex
		biLatestTS hlc.Timestamp         // Timestamp of gossip bootstrap info
		latestBI   *gossip.BootstrapInfo // Latest cached bootstrap info
	}
}

var _ kv.Sender = &Stores{}      // Stores implements the client.Sender interface
var _ gossip.Storage = &Stores{} // Stores implements the gossip.Storage interface

// NewStores returns a local-only sender which directly accesses
// a collection of stores.
func NewStores(
	ambient log.AmbientContext,
	clock *hlc.Clock,
	binaryVersion, binaryMinSupportedVersion roachpb.Version,
) *Stores {
	return &Stores{
		AmbientContext:            ambient,
		clock:                     clock,
		binaryVersion:             binaryVersion,
		binaryMinSupportedVersion: binaryMinSupportedVersion,
	}
}

// IsMeta1Leaseholder returns whether the specified stores owns
// the meta1 lease. Returns an error if any.
func (ls *Stores) IsMeta1Leaseholder(now hlc.Timestamp) (bool, error) {
	repl, _, err := ls.GetReplicaForRangeID(1)
	if roachpb.IsRangeNotFoundError(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return repl.OwnsValidLease(now), nil
}

// GetStoreCount returns the number of stores this node is exporting.
func (ls *Stores) GetStoreCount() int {
	var count int
	ls.storeMap.Range(func(_ int64, _ unsafe.Pointer) bool {
		count++
		return true
	})
	return count
}

// HasStore returns true if the specified store is owned by this Stores.
func (ls *Stores) HasStore(storeID roachpb.StoreID) bool {
	_, ok := ls.storeMap.Load(int64(storeID))
	return ok
}

// GetStore looks up the store by store ID. Returns an error
// if not found.
func (ls *Stores) GetStore(storeID roachpb.StoreID) (*Store, error) {
	if value, ok := ls.storeMap.Load(int64(storeID)); ok {
		return (*Store)(value), nil
	}
	return nil, roachpb.NewStoreNotFoundError(storeID)
}

// AddStore adds the specified store to the store map.
func (ls *Stores) AddStore(s *Store) {
	if _, loaded := ls.storeMap.LoadOrStore(int64(s.Ident.StoreID), unsafe.Pointer(s)); loaded {
		panic(fmt.Sprintf("cannot add store twice: %+v", s.Ident))
	}
	// If we've already read the gossip bootstrap info, ensure that
	// all stores have the most recent values.
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.mu.biLatestTS != (hlc.Timestamp{}) {
		if err := ls.updateBootstrapInfoLocked(ls.mu.latestBI); err != nil {
			ctx := ls.AnnotateCtx(context.TODO())
			log.Errorf(ctx, "failed to update bootstrap info on newly added store: %+v", err)
		}
	}
}

// RemoveStore removes the specified store from the store map.
func (ls *Stores) RemoveStore(s *Store) {
	ls.storeMap.Delete(int64(s.Ident.StoreID))
}

// VisitStores implements a visitor pattern over stores in the
// storeMap. The specified function is invoked with each store in
// turn. Care is taken to invoke the visitor func without the lock
// held to avoid inconsistent lock orderings, as some visitor
// functions may call back into the Stores object. Stores are visited
// in random order.
func (ls *Stores) VisitStores(visitor func(s *Store) error) error {
	var err error
	ls.storeMap.Range(func(k int64, v unsafe.Pointer) bool {
		err = visitor((*Store)(v))
		return err == nil
	})
	return err
}

// GetReplicaForRangeID returns the replica and store which contains the
// specified range. If the replica is not found on any store then
// roachpb.RangeNotFoundError will be returned.
func (ls *Stores) GetReplicaForRangeID(
	rangeID roachpb.RangeID,
) (replica *Replica, store *Store, err error) {
	err = ls.VisitStores(func(s *Store) error {
		switch r, err := s.GetReplica(rangeID); err.(type) {
		case nil:
			replica, store = r, s
			return nil
		case *roachpb.RangeNotFoundError:
			return nil
		default:
			return err
		}
	})
	if err != nil {
		return nil, nil, err
	}
	if replica == nil {
		return nil, nil, roachpb.NewRangeNotFoundError(rangeID, 0)
	}
	return replica, store, nil
}

// Send implements the client.Sender interface. The store is looked up from the
// store map using the ID specified in the request.
func (ls *Stores) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	if err := ba.ValidateForEvaluation(); err != nil {
		log.Fatalf(ctx, "invalid batch (%s): %s", ba, err)
	}
	if ba.Txn != nil && ba.Txn.WriteTooOld && (ba.Txn.ReadTimestamp.Equal(ba.Txn.WriteTimestamp)) {
		// This is a Fatal in 20.2+, and it used to be a Fatal in 20.1 too until
		// someone ran into it while upgrading a 19.2 cluster. I'm not sure why it
		// happens, but I'm not that surprised; the client used to be less
		// disciplined about this field in 19.2 It's possible that it'd be safe to
		// let the request go through; I'm not sure. Let's be safe and reject it.
		return nil, roachpb.NewError(errors.Errorf(
			"WriteTooOld set but no offset in timestamps. txn: %s (ba: %s)", ba.Txn.String(), ba.String()))
	}

	store, err := ls.GetStore(ba.Replica.StoreID)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	br, pErr := store.Send(ctx, ba)
	if br != nil && br.Error != nil {
		panic(roachpb.ErrorUnexpectedlySet(store, br))
	}
	return br, pErr
}

// RangeFeed registers a rangefeed over the specified span. It sends updates to
// the provided stream and returns with an optional error when the rangefeed is
// complete.
func (ls *Stores) RangeFeed(
	args *roachpb.RangeFeedRequest, stream roachpb.Internal_RangeFeedServer,
) *roachpb.Error {
	ctx := stream.Context()
	if args.RangeID == 0 {
		log.Fatal(ctx, "rangefeed request missing range ID")
	} else if args.Replica.StoreID == 0 {
		log.Fatal(ctx, "rangefeed request missing store ID")
	}

	store, err := ls.GetStore(args.Replica.StoreID)
	if err != nil {
		return roachpb.NewError(err)
	}

	return store.RangeFeed(args, stream)
}

// ReadBootstrapInfo implements the gossip.Storage interface. Read
// attempts to read gossip bootstrap info from every known store and
// finds the most recent from all stores to initialize the bootstrap
// info argument. Returns an error on any issues reading data for the
// stores (but excluding the case in which no data has been persisted
// yet).
func (ls *Stores) ReadBootstrapInfo(bi *gossip.BootstrapInfo) error {
	var latestTS hlc.Timestamp

	ctx := ls.AnnotateCtx(context.TODO())
	var err error

	// Find the most recent bootstrap info.
	ls.storeMap.Range(func(k int64, v unsafe.Pointer) bool {
		s := (*Store)(v)
		var storeBI gossip.BootstrapInfo
		var ok bool
		ok, err = storage.MVCCGetProto(ctx, s.engine, keys.StoreGossipKey(), hlc.Timestamp{}, &storeBI,
			storage.MVCCGetOptions{})
		if err != nil {
			return false
		}
		if ok && latestTS.Less(storeBI.Timestamp) {
			latestTS = storeBI.Timestamp
			*bi = storeBI
		}
		return true
	})
	if err != nil {
		return err
	}
	log.Infof(ctx, "read %d node addresses from persistent storage", len(bi.Addresses))

	ls.mu.Lock()
	defer ls.mu.Unlock()
	return ls.updateBootstrapInfoLocked(bi)
}

// WriteBootstrapInfo implements the gossip.Storage interface. Write
// persists the supplied bootstrap info to every known store. Returns
// nil on success; otherwise returns first error encountered writing
// to the stores.
func (ls *Stores) WriteBootstrapInfo(bi *gossip.BootstrapInfo) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	bi.Timestamp = ls.clock.Now()
	if err := ls.updateBootstrapInfoLocked(bi); err != nil {
		return err
	}
	ctx := ls.AnnotateCtx(context.TODO())
	log.Infof(ctx, "wrote %d node addresses to persistent storage", len(bi.Addresses))
	return nil
}

func (ls *Stores) updateBootstrapInfoLocked(bi *gossip.BootstrapInfo) error {
	if bi.Timestamp.Less(ls.mu.biLatestTS) {
		return nil
	}
	ctx := ls.AnnotateCtx(context.TODO())
	// Update the latest timestamp and set cached version.
	ls.mu.biLatestTS = bi.Timestamp
	ls.mu.latestBI = protoutil.Clone(bi).(*gossip.BootstrapInfo)
	// Update all stores.
	var err error
	ls.storeMap.Range(func(k int64, v unsafe.Pointer) bool {
		s := (*Store)(v)
		err = storage.MVCCPutProto(ctx, s.engine, nil, keys.StoreGossipKey(), hlc.Timestamp{}, nil, bi)
		return err == nil
	})
	return err
}

// ReadVersionFromEngineOrZero reads the persisted cluster version from the
// engine, falling back to the zero value.
func ReadVersionFromEngineOrZero(
	ctx context.Context, e storage.Engine,
) (clusterversion.ClusterVersion, error) {
	var cv clusterversion.ClusterVersion
	cv, err := ReadClusterVersion(ctx, e)
	if err != nil {
		return clusterversion.ClusterVersion{}, err
	}
	return cv, nil
}

// WriteClusterVersionToEngines writes the given version to the given engines,
// without any sanity checks.
func WriteClusterVersionToEngines(
	ctx context.Context, engines []storage.Engine, cv clusterversion.ClusterVersion,
) error {
	for _, eng := range engines {
		if err := WriteClusterVersion(ctx, eng, cv); err != nil {
			return errors.Wrapf(err, "error writing version to engine %s", eng)
		}
	}
	return nil
}

// SynthesizeClusterVersionFromEngines returns the cluster version that was read
// from the engines or, if there's no bootstrapped engines, returns
// binaryMinSupportedVersion.
//
// Args:
//
//	binaryMinSupportedVersion: The minimum version supported by this binary. An error
//	  is returned if any engine has a version lower that this. This version is
//	  written to the engines if no store has a version in it.
//	binaryVersion: The version of this binary. An error is returned if
//	  any engine has a higher version.
func SynthesizeClusterVersionFromEngines(
	ctx context.Context,
	engines []storage.Engine,
	binaryVersion, binaryMinSupportedVersion roachpb.Version,
) (clusterversion.ClusterVersion, error) {
	// Find the most recent bootstrap info.
	type originVersion struct {
		roachpb.Version
		origin string
	}

	maxPossibleVersion := roachpb.Version{Major: 999999} // Sort above any real version.
	minStoreVersion := originVersion{
		Version: maxPossibleVersion,
		origin:  "(no store)",
	}

	// We run this twice because it's only after having seen all the versions
	// that we can decide whether the node catches a version error. However, we
	// also want to name at least one engine that violates the version
	// constraints, which at the latest the second loop will achieve (because
	// then minStoreVersion don't change any more).
	for _, eng := range engines {
		var cv clusterversion.ClusterVersion
		cv, err := ReadVersionFromEngineOrZero(ctx, eng)
		if err != nil {
			return clusterversion.ClusterVersion{}, err
		}
		if cv.Version == (roachpb.Version{}) {
			// This is needed when a node first joins an existing cluster, in
			// which case it won't know what version to use until the first
			// Gossip update comes in.
			cv.Version = binaryMinSupportedVersion
		}

		// Avoid running a binary with a store that is too new. For example,
		// restarting into 1.1 after having upgraded to 1.2 doesn't work.
		if binaryVersion.Less(cv.Version) {
			return clusterversion.ClusterVersion{}, errors.Errorf(
				"kwbase version v%s is incompatible with data in store %s; use version v%s or later",
				binaryVersion, eng, cv.Version)
		}

		// Track smallest use version encountered.
		if cv.Version.Less(minStoreVersion.Version) {
			minStoreVersion.Version = cv.Version
			minStoreVersion.origin = fmt.Sprint(eng)
		}
	}

	// If no use version was found, fall back to our binaryMinSupportedVersion. This
	// is the case when a brand new node is joining an existing cluster (which
	// may be on any older version this binary supports).
	if minStoreVersion.Version == maxPossibleVersion {
		minStoreVersion.Version = binaryMinSupportedVersion
	}

	cv := clusterversion.ClusterVersion{
		Version: minStoreVersion.Version,
	}
	log.Eventf(ctx, "read ClusterVersion %+v", cv)

	// Avoid running a binary too new for this store. This is what you'd catch
	// if, say, you restarted directly from 1.0 into 1.2 (bumping the min
	// version) without going through 1.1 first. It would also be what you catch if
	// you are starting 1.1 for the first time (after 1.0), but it crashes
	// half-way through the startup sequence (so now some stores have 1.1, but
	// some 1.0), in which case you are expected to run 1.1 again (hopefully
	// without the crash this time) which would then rewrite all the stores.
	//
	// We only verify this now because as we iterate through the stores, we
	// may not yet have picked up the final versions we're actually planning
	// to use.
	if minStoreVersion.Version.Less(binaryMinSupportedVersion) {
		return clusterversion.ClusterVersion{}, errors.Errorf("store %s, last used with kwbase version v%s, "+
			"is too old for running version v%s (which requires data from v%s or later)",
			minStoreVersion.origin, minStoreVersion.Version, binaryVersion, binaryMinSupportedVersion)
	}

	// Write the "actual" version back to all stores. This is almost always a
	// no-op, but will backfill the information for 1.0.x clusters, and also
	// smoothens out inconsistent state that can crop up during an ill-timed
	// crash or when new stores are being added.
	return cv, WriteClusterVersionToEngines(ctx, engines, cv)
}

// SynthesizeClusterVersion reads and returns the ClusterVersion protobuf
// (written to any of the configured stores (all of which are bootstrapped)).
// The returned value is also replicated to all stores for consistency, in case
// a new store was added or an old store re-configured. In case of non-identical
// versions across the stores, returns a version that carries the smallest
// Version.
//
// If there aren't any stores, returns the minimum supported version of the binary.
func (ls *Stores) SynthesizeClusterVersion(
	ctx context.Context,
) (clusterversion.ClusterVersion, error) {
	var engines []storage.Engine
	ls.storeMap.Range(func(_ int64, v unsafe.Pointer) bool {
		engines = append(engines, (*Store)(v).engine)
		return true // want more
	})
	cv, err := SynthesizeClusterVersionFromEngines(ctx, engines, ls.binaryVersion, ls.binaryMinSupportedVersion)
	if err != nil {
		return clusterversion.ClusterVersion{}, err
	}
	return cv, nil
}

// WriteClusterVersion persists the supplied ClusterVersion to every
// configured store. Returns nil on success; otherwise returns first
// error encountered writing to the stores.
//
// WriteClusterVersion makes no attempt to validate the supplied version.
func (ls *Stores) WriteClusterVersion(ctx context.Context, cv clusterversion.ClusterVersion) error {
	// Update all stores.
	engines := ls.engines()
	ls.storeMap.Range(func(_ int64, v unsafe.Pointer) bool {
		engines = append(engines, (*Store)(v).Engine())
		return true // want more
	})
	return WriteClusterVersionToEngines(ctx, engines, cv)
}

func (ls *Stores) engines() []storage.Engine {
	var engines []storage.Engine
	ls.storeMap.Range(func(_ int64, v unsafe.Pointer) bool {
		engines = append(engines, (*Store)(v).Engine())
		return true // want more
	})
	return engines
}

// OnClusterVersionChange is invoked when the running node receives a notification
// indicating that the cluster version has changed. It checks the currently persisted
// version and updates if it is older than the provided update.
func (ls *Stores) OnClusterVersionChange(
	ctx context.Context, cv clusterversion.ClusterVersion,
) error {
	// Grab a lock to make sure that there aren't two interleaved invocations of
	// this method that result in clobbering of an update.
	ls.mu.Lock()
	defer ls.mu.Unlock()

	// We're going to read the cluster version from any engine - all the engines
	// are always kept in sync so it doesn't matter which one we read from.
	var someEngine storage.Engine
	ls.storeMap.Range(func(_ int64, v unsafe.Pointer) bool {
		someEngine = (*Store)(v).engine
		return false // don't iterate any more
	})
	if someEngine == nil {
		// If we haven't bootstrapped any engines yet, there's nothing for us to do.
		return nil
	}
	synthCV, err := ReadClusterVersion(ctx, someEngine)
	if err != nil {
		return errors.Wrap(err, "error reading persisted cluster version")
	}
	// If the update downgrades the version, ignore it. Must be a
	// reordering (this method is called from multiple goroutines via
	// `(*Node).onClusterVersionChange)`). Note that we do carry out the upgrade if
	// the MinVersion is identical, to backfill the engines that may still need it.
	if cv.Version.Less(synthCV.Version) {
		return nil
	}
	if err := ls.WriteClusterVersion(ctx, cv); err != nil {
		return errors.Wrap(err, "writing cluster version")
	}

	return nil
}
