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

package rpc

import (
	"context"
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/pkg/errors"
)

func (r RemoteOffset) measuredAt() time.Time {
	return timeutil.Unix(0, r.MeasuredAt)
}

// String formats the RemoteOffset for human readability.
func (r RemoteOffset) String() string {
	return fmt.Sprintf("off=%s, err=%s, at=%s", time.Duration(r.Offset), time.Duration(r.Uncertainty), r.measuredAt())
}

// A HeartbeatService exposes a method to echo its request params. It doubles
// as a way to measure the offset of the server from other nodes. It uses the
// clock to return the server time every heartbeat. It also keeps track of
// remote clocks sent to it by storing them in the remoteClockMonitor.
type HeartbeatService struct {
	// Provides the nanosecond unix epoch timestamp of the processor.
	clock *hlc.Clock
	// A pointer to the RemoteClockMonitor configured in the RPC Context,
	// shared by rpc clients, to keep track of remote clock measurements.
	remoteClockMonitor *RemoteClockMonitor

	clusterID *base.ClusterIDContainer
	nodeID    *base.NodeIDContainer
	settings  *cluster.Settings

	clusterName                    string
	disableClusterNameVerification bool

	// TestingAllowNamedRPCToAnonymousServer, when defined (in tests),
	// disables errors in case a heartbeat requests a specific node ID but
	// the remote node doesn't have a node ID yet. This testing knob is
	// currently used by the multiTestContext which does not suitably
	// populate separate node IDs for each heartbeat service.
	testingAllowNamedRPCToAnonymousServer bool
}

func checkClusterName(clusterName string, peerName string) error {
	if clusterName != peerName {
		var err error
		if clusterName == "" {
			err = errors.Errorf("peer node expects cluster name %q, use --cluster-name to configure", peerName)
		} else if peerName == "" {
			err = errors.New("peer node does not have a cluster name configured, cannot use --cluster-name")
		} else {
			err = errors.Errorf(
				"local cluster name %q does not match peer cluster name %q", clusterName, peerName)
		}
		log.Shout(context.Background(), log.Severity_ERROR, err)
		return err
	}
	return nil
}

func checkVersion(ctx context.Context, st *cluster.Settings, peerVersion roachpb.Version) error {
	activeVersion := st.Version.ActiveVersionOrEmpty(ctx)
	if activeVersion == (clusterversion.ClusterVersion{}) {
		// Cluster version has not yet been determined.
		return nil
	}
	if peerVersion == (roachpb.Version{}) {
		return errors.Errorf(
			"cluster requires at least version %s, but peer did not provide a version", activeVersion)
	}
	if peerVersion.Less(activeVersion.Version) {
		return errors.Errorf(
			"cluster requires at least version %s, but peer has version %s", activeVersion, peerVersion)
	}
	return nil
}

// Ping echos the contents of the request to the response, and returns the
// server's current clock value, allowing the requester to measure its clock.
// The requester should also estimate its offset from this server along
// with the requester's address.
func (hs *HeartbeatService) Ping(ctx context.Context, args *PingRequest) (*PingResponse, error) {
	if log.V(2) {
		log.Infof(ctx, "received heartbeat: %+v vs local cluster %+v node %+v", args, hs.clusterID, hs.nodeID)
	}
	// Check that cluster IDs match.
	clusterID := hs.clusterID.Get()
	if args.ClusterID != nil && *args.ClusterID != uuid.Nil && clusterID != uuid.Nil {
		// There is a cluster ID on both sides. Use that to verify the connection.
		//
		// Note: we could be checking the cluster name here too, however
		// for UX reason it is better to check it on the other side (the side
		// initiating the connection), so that the user of a newly started
		// node gets a chance to see a cluster name mismatch as an error message
		// on their side.
		if *args.ClusterID != clusterID {
			return nil, errors.Errorf(
				"client cluster ID %q doesn't match server cluster ID %q", args.ClusterID, clusterID)
		}
	}
	// Check that node IDs match.
	var nodeID roachpb.NodeID
	if hs.nodeID != nil {
		nodeID = hs.nodeID.Get()
	}
	if args.NodeID != 0 && (!hs.testingAllowNamedRPCToAnonymousServer || nodeID != 0) && args.NodeID != nodeID {
		// If nodeID != 0, the situation is clear (we are checking that
		// the other side is talking to the right node).
		//
		// If nodeID == 0 this means that this node (serving the
		// heartbeat) doesn't have a node ID yet. Then we can't serve
		// connections for other nodes that want a specific node ID,
		// however we can still serve connections that don't need a node
		// ID, e.g. during initial gossip.
		return nil, errors.Errorf(
			"client requested node ID %d doesn't match server node ID %d", args.NodeID, nodeID)
	}

	// Check version compatibility.
	if err := checkVersion(ctx, hs.settings, args.ServerVersion); err != nil {
		return nil, errors.Wrap(err, "version compatibility check failed on ping request")
	}

	// Enforce that clock max offsets are identical between nodes.
	// Commit suicide in the event that this is ever untrue.
	// This check is ignored if either offset is set to 0 (for unittests).
	// Note that we validated this connection already. Different clusters
	// could very well have different max offsets.
	mo, amo := hs.clock.MaxOffset(), time.Duration(args.MaxOffsetNanos)
	if mo != 0 && amo != 0 && mo != amo {
		panic(fmt.Sprintf("locally configured maximum clock offset (%s) "+
			"does not match that of node %s (%s)", mo, args.Addr, amo))
	}

	serverOffset := args.Offset
	// The server offset should be the opposite of the client offset.
	serverOffset.Offset = -serverOffset.Offset
	hs.remoteClockMonitor.UpdateOffset(ctx, args.Addr, serverOffset, 0 /* roundTripLatency */)
	return &PingResponse{
		Pong:                           args.Ping,
		ServerTime:                     hs.clock.PhysicalNow(),
		ServerVersion:                  hs.settings.Version.BinaryVersion(),
		ClusterName:                    hs.clusterName,
		DisableClusterNameVerification: hs.disableClusterNameVerification,
	}, nil
}
