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

package gossip

import (
	"regexp"
	"strconv"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"github.com/pkg/errors"
)

// separator is used to separate the non-prefix components of a
// Gossip key, facilitating the automated generation of regular
// expressions for various prefixes.
// It must not be contained in any of the other keys defined here.
const separator = ":"

// Constants for gossip keys.
const (
	// KeyClusterID is the unique UUID for this Cockroach cluster.
	// The value is a string UUID for the cluster.  The cluster ID is
	// gossiped by all nodes that contain a replica of the first range,
	// and it serves as a check for basic gossip connectivity. The
	// Gossip.Connected channel is closed when we see this key.
	KeyClusterID = "cluster-id"

	// KeyStorePrefix is the key prefix for gossiping stores in the network.
	// The suffix is a store ID and the value is roachpb.StoreDescriptor.
	KeyStorePrefix = "store"

	// KeyNodeIDPrefix is the key prefix for gossiping node id
	// addresses. The actual key is suffixed with the decimal
	// representation of the node id and the value is the host:port
	// string address of the node. E.g. node:1 => 127.0.0.1:24001
	KeyNodeIDPrefix = "node"

	// KeyHealthAlertPrefix is the key prefix for gossiping health alerts. The
	// value is a proto of type HealthCheckResult.
	KeyNodeHealthAlertPrefix = "health-alert"

	// KeyNodeLivenessPrefix is the key prefix for gossiping node liveness info.
	KeyNodeLivenessPrefix = "liveness"

	// KeySentinel is a key for gossip which must not expire or
	// else the node considers itself partitioned and will retry with
	// bootstrap hosts.  The sentinel is gossiped by the node that holds
	// the range lease for the first range.
	KeySentinel = "sentinel"

	// KeyFirstRangeDescriptor is the descriptor for the "first"
	// range. The "first" range contains the meta1 key range, the first
	// level of the bi-level key addressing scheme. The value is a slice
	// of storage.Replica structs.
	KeyFirstRangeDescriptor = "first-range"

	// KeySystemConfig is the gossip key for the system DB span.
	// The value if a config.SystemConfig which holds all key/value
	// pairs in the system DB span.
	KeySystemConfig = "system-db"

	// KeyDistSQLNodeVersionKeyPrefix is key prefix for each node's DistSQL
	// version.
	KeyDistSQLNodeVersionKeyPrefix = "distsql-version"

	// KeyDistSQLDrainingPrefix is the key prefix for each node's DistSQL
	// draining state.
	KeyDistSQLDrainingPrefix = "distsql-draining"

	// KeyTableStatAddedPrefix is the prefix for keys that indicate a new table
	// statistic was computed. The statistics themselves are not stored in gossip;
	// the keys are used to notify nodes to invalidate table statistic caches.
	KeyTableStatAddedPrefix = "table-stat-added"

	// KeyGossipClientsPrefix is the prefix for keys that indicate which gossip
	// client connections a node has open. This is used by other nodes in the
	// cluster to build a map of the gossip network.
	KeyGossipClientsPrefix = "gossip-clients"

	// KeyGossipStatementDiagnosticsRequest is the gossip key for new statement
	// diagnostics requests. The values is the id of the request that generated
	// the notification, as a little-endian-encoded uint64.
	// stmtDiagnosticsRequestRegistry listens for notifications and responds by
	// polling for new requests.
	KeyGossipStatementDiagnosticsRequest = "stmt-diag-req"

	//table-query-hint-add-cache
	KeyQueryHintAddCache = "table-query-hint-add-cache"

	//cuckoo-filter-update
	KeyCuckooFilterAddCache = "cuckoo-filter-update"

	// KeyGossipMLCache is the gossip key for ml predict privilege cache
	KeyGossipMLCache = "ml-predict"

	// KeyUDFUpdatedPrefix is the prefix for keys that indicate a new udf
	// was created.
	KeyUDFUpdatedPrefix = "udf-added"

	// KeyUDFDeletedPrefix is the prefix for keys that indicate a udf
	// was deleted.
	KeyUDFDeletedPrefix = "udf-deleted"
)

// MakeKey creates a canonical key under which to gossip a piece of
// information. The first argument will typically be one of the key constants
// defined in this package.
func MakeKey(components ...string) string {
	return strings.Join(components, separator)
}

// MakePrefixPattern returns a regular expression pattern that
// matches precisely the Gossip keys created by invocations of
// MakeKey with multiple arguments for which the first argument
// is equal to the given prefix.
func MakePrefixPattern(prefix string) string {
	return regexp.QuoteMeta(prefix+separator) + ".*"
}

// MakeNodeIDKey returns the gossip key for node ID info.
func MakeNodeIDKey(nodeID roachpb.NodeID) string {
	return MakeKey(KeyNodeIDPrefix, nodeID.String())
}

// IsNodeIDKey returns true iff the provided key is a valid node ID key.
func IsNodeIDKey(key string) bool {
	return strings.HasPrefix(key, KeyNodeIDPrefix+separator)
}

// NodeIDFromKey attempts to extract a NodeID from the provided key after
// stripping the provided prefix. Returns an error if the key is not of the
// correct type or is not parsable.
func NodeIDFromKey(key string, prefix string) (roachpb.NodeID, error) {
	trimmedKey, err := removePrefixFromKey(key, prefix)
	if err != nil {
		return 0, err
	}
	nodeID, err := strconv.ParseInt(trimmedKey, 10 /* base */, 64 /* bitSize */)
	if err != nil {
		return 0, errors.Wrapf(err, "failed parsing NodeID from key %q", key)
	}
	return roachpb.NodeID(nodeID), nil
}

// MakeGossipClientsKey returns the gossip client key for the given node.
func MakeGossipClientsKey(nodeID roachpb.NodeID) string {
	return MakeKey(KeyGossipClientsPrefix, nodeID.String())
}

// MakeNodeHealthAlertKey returns the gossip key under which the given node can
// gossip health alerts.
func MakeNodeHealthAlertKey(nodeID roachpb.NodeID) string {
	return MakeKey(KeyNodeHealthAlertPrefix, strconv.Itoa(int(nodeID)))
}

// MakeNodeLivenessKey returns the gossip key for node liveness info.
func MakeNodeLivenessKey(nodeID roachpb.NodeID) string {
	return MakeKey(KeyNodeLivenessPrefix, nodeID.String())
}

// MakeStoreKey returns the gossip key for the given store.
func MakeStoreKey(storeID roachpb.StoreID) string {
	return MakeKey(KeyStorePrefix, storeID.String())
}

// StoreIDFromKey attempts to extract a StoreID from the provided key after
// stripping the provided prefix. Returns an error if the key is not of the
// correct type or is not parsable.
func StoreIDFromKey(storeKey string) (roachpb.StoreID, error) {
	trimmedKey, err := removePrefixFromKey(storeKey, KeyStorePrefix)
	if err != nil {
		return 0, err
	}
	storeID, err := strconv.ParseInt(trimmedKey, 10 /* base */, 64 /* bitSize */)
	if err != nil {
		return 0, errors.Wrapf(err, "failed parsing StoreID from key %q", storeKey)
	}
	return roachpb.StoreID(storeID), nil
}

// MakeDistSQLNodeVersionKey returns the gossip key for the given store.
func MakeDistSQLNodeVersionKey(nodeID roachpb.NodeID) string {
	return MakeKey(KeyDistSQLNodeVersionKeyPrefix, nodeID.String())
}

// MakeDistSQLDrainingKey returns the gossip key for the given node's distsql
// draining state.
func MakeDistSQLDrainingKey(nodeID roachpb.NodeID) string {
	return MakeKey(KeyDistSQLDrainingPrefix, nodeID.String())
}

// MakeTableStatAddedKey returns the gossip key used to notify that a new
// statistic is available for the given table.
func MakeTableStatAddedKey(tableID uint32) string {
	return MakeKey(KeyTableStatAddedPrefix, strconv.FormatUint(uint64(tableID), 10 /* base */))
}

// MakeUdfAddedKey returns the gossip key used to notify that a new
// udf or definition is available.
func MakeUdfAddedKey(udfName string) string {
	return MakeKey(KeyUDFUpdatedPrefix, udfName)
}

// MakeUdfDeletedKey returns the gossip key used to notify that
// udf or definition is unavailable.
func MakeUdfDeletedKey(udfName string) string {
	return MakeKey(KeyUDFDeletedPrefix, udfName)
}

// MakeQueryHintAddedKey returns the gossip key used to synchronize the pseudocatalog
// cache.
func MakeQueryHintAddedKey(optype string, message string) string {
	return MakeKey(KeyQueryHintAddCache, optype, message)
}

// MakeCuckooFilterAddedKey returns the gossip key used to synchronize the cuckoofilter
func MakeCuckooFilterAddedKey(optype string, queryFingerprint string) string {
	return MakeKey(KeyCuckooFilterAddCache, optype, queryFingerprint)
}

// QueryFingerprintFromTableAddedKey attempts to extract the query fingerprint from the
// provided key.
func QueryFingerprintFromTableAddedKey(key string) (string, error) {
	trimmedKey, err := removePrefixFromKey(key, KeyQueryHintAddCache)
	if err != nil {
		return "", err
	}
	return trimmedKey, nil
}

// QueryFingerprintFromCuckooFilterAddedKey attempts to extract the query fingerprint from the
// provided key.
func QueryFingerprintFromCuckooFilterAddedKey(key string) (string, error) {
	trimmedKey, err := removePrefixFromKey(key, KeyCuckooFilterAddCache)
	if err != nil {
		return "", err
	}
	return trimmedKey, nil
}

// TableIDFromTableStatAddedKey attempts to extract the table ID from the
// provided key.
// The key should have been constructed by MakeTableStatAddedKey.
// Returns an error if the key is not of the correct type or is not parsable.
func TableIDFromTableStatAddedKey(key string) (uint32, error) {
	trimmedKey, err := removePrefixFromKey(key, KeyTableStatAddedPrefix)
	if err != nil {
		return 0, err
	}
	tableID, err := strconv.ParseUint(trimmedKey, 10 /* base */, 32 /* bitSize */)
	if err != nil {
		return 0, errors.Wrapf(err, "failed parsing table ID from key %q", key)
	}
	return uint32(tableID), nil
}

// removePrefixFromKey removes the key prefix and separator and returns what's
// left. Returns an error if the key doesn't have this prefix.
func removePrefixFromKey(key, prefix string) (string, error) {
	trimmedKey := strings.TrimPrefix(key, prefix+separator)
	if trimmedKey == key {
		return "", errors.Errorf("%q does not have expected prefix %q%s", key, prefix, separator)
	}
	return trimmedKey, nil
}

// UDFNameFromUDFUpdatedKey attempts to extract the UDF name from the
// provided key.
func UDFNameFromUDFUpdatedKey(key string) (string, error) {
	trimmedKey, err := removePrefixFromKey(key, KeyUDFUpdatedPrefix)
	if err != nil {
		return "", err
	}

	return trimmedKey, nil
}

// UDFNameFromUDFDeletedKey attempts to extract the UDF name from the
// provided key.
func UDFNameFromUDFDeletedKey(key string) (string, error) {
	trimmedKey, err := removePrefixFromKey(key, KeyUDFDeletedPrefix)
	if err != nil {
		return "", err
	}

	return trimmedKey, nil
}
