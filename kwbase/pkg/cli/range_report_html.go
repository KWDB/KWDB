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

package cli

import (
	"fmt"
	"html"
	"sort"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

type rangeReportField struct {
	key     string
	label   string
	compare bool
}

type reportCell struct {
	text      string
	className string
}

var rangeReportFields = []rangeReportField{
	{"id", "ID", false},
	{"keyRange", "Key Range", true},
	{"problems", "Problems", true},
	{"raftState", "Raft State", false},
	{"quiescent", "Quiescent", true},
	{"ticking", "Ticking", true},
	{"leaseType", "Lease Type", true},
	{"leaseState", "Lease State", true},
	{"leaseHolder", "Lease Holder", true},
	{"leaseEpoch", "Lease Epoch", true},
	{"leaseStart", "Lease Start", true},
	{"leaseExpiration", "Lease Expiration", true},
	{"leaseAppliedIndex", "Lease Applied Index", true},
	{"raftLeader", "Raft Leader", true},
	{"vote", "Vote", false},
	{"term", "Term", true},
	{"leadTransferee", "Lead Transferee", false},
	{"applied", "Applied", true},
	{"commit", "Commit", true},
	{"lastIndex", "Last Index", true},
	{"logSize", "Log Size", false},
	{"leaseHolderQPS", "Lease Holder QPS", false},
	{"keysWrittenPS", "Average Keys Written Per Second", false},
	{"approxProposalQuota", "Approx Proposal Quota", false},
	{"pendingCommands", "Pending Commands", false},
	{"droppedCommands", "Dropped Commands", false},
	{"truncatedIndex", "Truncated Index", true},
	{"truncatedTerm", "Truncated Term", true},
	{"mvccLastUpdate", "MVCC Last Update", true},
	{"GCAvgAge", "Dead Value average age", true},
	{"GCBytesAge", "GC Bytes Age (score)", true},
	{"NumIntents", "Intents", true},
	{"IntentAvgAge", "Intent Average Age", true},
	{"IntentAge", "Intent Age (score)", true},
	{"mvccLiveBytesCount", "MVCC Live Bytes/Count", true},
	{"mvccKeyBytesCount", "MVCC Key Bytes/Count", true},
	{"mvccValueBytesCount", "MVCC Value Bytes/Count", true},
	{"mvccIntentBytesCount", "MVCC Intent Bytes/Count", true},
	{"mvccSystemBytesCount", "MVCC System Bytes/Count", true},
	{"rangeMaxBytes", "Max Range Size Before Split", true},
	{"writeLatches", "Write Latches Local/Global", false},
	{"readLatches", "Read Latches Local/Global", false},
}

func escHTML(s string) string {
	return html.EscapeString(s)
}

func fmtReplica(node roachpb.ReplicaDescriptor) string {
	return fmt.Sprintf("n%d/s%d/r%d", node.NodeID, node.StoreID, node.ReplicaID)
}

func cleanRaftState(state string) string {
	switch strings.ToLower(state) {
	case "statedormant":
		return "dormant"
	case "stateleader":
		return "leader"
	case "statefollower":
		return "follower"
	case "statecandidate":
		return "candidate"
	case "stateprecandidate":
		return "precandidate"
	default:
		return "unknown"
	}
}

func fmtHLCTimestamp(ts hlc.Timestamp) string {
	if ts.WallTime == 0 {
		return "no timestamp"
	}
	out := ts.GoTime().UTC().Format("2006-01-02 15:04:05")
	if ts.Logical != 0 {
		out += fmt.Sprintf(".%d", ts.Logical)
	}
	return out
}

func fmtHLCTimestampPtr(ts *hlc.Timestamp) string {
	if ts == nil {
		return "no timestamp"
	}
	return fmtHLCTimestamp(*ts)
}

func fmtNanosTimestamp(nanos int64) string {
	if nanos == 0 {
		return "no timestamp"
	}
	t := timeutil.Unix(0, nanos).UTC()
	return t.Format("2006-01-02 15:04:05")
}

func fmtDurationNS(ns int64) string {
	if ns <= 0 {
		return "0s"
	}
	d := time.Duration(ns)
	if d < time.Second {
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	m := int(d.Minutes())
	s := int(d.Seconds()) % 60
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", m, s)
	}
	h := int(d.Hours())
	return fmt.Sprintf("%dh%dm", h, m%60)
}

func mvccPair(bytesVal, countVal int64) string {
	return fmt.Sprintf("%s / %d count", humanizeutil.IBytes(bytesVal), countVal)
}

func getLocalReplica(info *serverpb.RangeInfo) *roachpb.ReplicaDescriptor {
	raftRep := info.RaftState.ReplicaID
	for i := range info.State.Desc.InternalReplicas {
		rep := &info.State.Desc.InternalReplicas[i]
		if rep.ReplicaID == roachpb.ReplicaID(raftRep) {
			return rep
		}
	}
	return nil
}

func isEpochLease(lease roachpb.Lease) bool {
	return lease.Epoch != 0
}

func buildStoreContent(info *serverpb.RangeInfo) map[string]reportCell {
	local := getLocalReplica(info)
	awaitingGC := local == nil
	lease := info.State.Lease
	if lease == nil {
		lease = &roachpb.Lease{}
	}
	stats := info.State.Stats
	raft := info.RaftState
	localRepID := roachpb.ReplicaID(0)
	if local != nil {
		localRepID = local.ReplicaID
	}
	isLeader := localRepID != 0 && roachpb.ReplicaID(raft.Lead) == localRepID
	isLeaseHolder := localRepID != 0 && lease.Replica.ReplicaID == localRepID
	raftRole := cleanRaftState(raft.State)
	dormant := raftRole == "dormant"
	epoch := isEpochLease(*lease)

	problems := []string{}
	if info.Problems.NoLease {
		problems = append(problems, "Invalid Lease")
	}
	if info.Problems.LeaderNotLeaseHolder {
		problems = append(problems, "Leader is Not Lease holder")
	}
	if info.Problems.Underreplicated {
		problems = append(problems, "Underreplicated (or slow)")
	}
	if info.Problems.Overreplicated {
		problems = append(problems, "Overreplicated")
	}
	if info.Problems.NoRaftLeader {
		problems = append(problems, "No Raft Leader")
	}
	if info.Problems.Unavailable {
		problems = append(problems, "Unavailable")
	}
	if info.Problems.QuiescentEqualsTicking {
		problems = append(problems, "Quiescent equals ticking")
	}
	if info.Problems.RaftLogTooLarge {
		problems = append(problems, "Raft log too large")
	}
	if awaitingGC {
		problems = append(problems, "Awaiting GC")
	}
	problemText := "-"
	problemCls := ""
	if len(problems) > 0 {
		problemText = strings.Join(problems, "; ")
		problemCls = "range-table__cell--warning"
	}

	leaseState := strings.ToLower(info.LeaseStatus.State.String())
	leaseStateCls := ""
	if info.LeaseStatus.State != storagepb.LeaseState_VALID {
		leaseStateCls = "range-table__cell--warning"
	}

	maybeDormantInt := func(v uint64) string {
		if dormant {
			return "-"
		}
		if v == 0 {
			return "0"
		}
		return fmt.Sprintf("%d", v)
	}

	writeLocal := info.LatchesLocal.WriteCount
	writeGlobal := info.LatchesGlobal.WriteCount
	readLocal := info.LatchesLocal.ReadCount
	readGlobal := info.LatchesGlobal.ReadCount
	writeLatches := fmt.Sprintf("%d local / %d global", writeLocal, writeGlobal)
	readLatches := fmt.Sprintf("%d local / %d global", readLocal, readGlobal)
	latchCls := ""
	if !isLeader && (writeLocal != 0 || writeGlobal != 0 || readLocal != 0 || readGlobal != 0) {
		latchCls = "range-table__cell--warning"
	}

	idText := "-"
	if local != nil {
		idText = fmtReplica(*local)
	}
	keyRange := fmt.Sprintf("%s to %s", info.Span.StartKey, info.Span.EndKey)

	deadBytes := stats.KeyBytes + stats.ValBytes - stats.LiveBytes
	gcAvgAge := "0s"
	if deadBytes > 0 {
		gcAvgAge = fmtDurationNS(int64(float64(stats.GCBytesAge) / float64(deadBytes) * 1e9))
	}
	intentAvgAge := "0s"
	if stats.IntentCount > 0 {
		intentAvgAge = fmtDurationNS(int64(float64(stats.IntentAge) / float64(stats.IntentCount) * 1e9))
	}

	logCls := ""
	if !info.State.RaftLogSizeTrusted {
		logCls = "range-table__cell--dormant"
	}

	leaseHolderText := fmtReplica(lease.Replica)
	leaseHolderCls := "range-table__cell--lease-follower"
	if isLeaseHolder {
		leaseHolderCls = "range-table__cell--lease-holder"
	}
	leaderCls := "range-table__cell--raftstate-follower"
	if isLeader {
		leaderCls = "range-table__cell--raftstate-leader"
	}

	quiescentCls := ""
	quiescentText := "-"
	if info.Quiescent {
		quiescentText = "quiescent"
		quiescentCls = "range-table__cell--quiescent"
	}

	leaseExpiration := "-"
	if !epoch {
		leaseExpiration = fmtHLCTimestampPtr(lease.Expiration)
	}

	droppedCls := ""
	if info.State.NumDropped > 0 {
		droppedCls = "range-table__cell--warning"
	}

	leaseHolderQPS := "-"
	if isLeaseHolder {
		leaseHolderQPS = fmt.Sprintf("%.4f", info.Stats.QueriesPerSecond)
	}
	approxQuota := "-"
	if isLeader {
		approxQuota = humanizeutil.IBytes(info.State.ApproximateProposalQuota)
	}

	writeLatchText := "-"
	if isLeader || writeLocal != 0 || writeGlobal != 0 {
		writeLatchText = writeLatches
	}
	readLatchText := "-"
	if isLeader || readLocal != 0 || readGlobal != 0 {
		readLatchText = readLatches
	}

	return map[string]reportCell{
		"id":                   {text: idText},
		"keyRange":             {text: keyRange},
		"problems":             {text: problemText, className: problemCls},
		"raftState":            {text: raftRole, className: "range-table__cell--raftstate-" + raftRole},
		"quiescent":            {text: quiescentText, className: quiescentCls},
		"ticking":              {text: fmt.Sprintf("%t", info.Ticking)},
		"leaseType":            {text: map[bool]string{true: "epoch", false: "expiration"}[epoch]},
		"leaseState":           {text: leaseState, className: leaseStateCls},
		"leaseHolder":          {text: leaseHolderText, className: leaseHolderCls},
		"leaseEpoch":           {text: map[bool]string{true: fmt.Sprintf("%d", lease.Epoch), false: "-"}[epoch]},
		"leaseStart":           {text: fmtHLCTimestamp(lease.Start)},
		"leaseExpiration":      {text: leaseExpiration},
		"leaseAppliedIndex":    {text: fmt.Sprintf("%d", info.State.LeaseAppliedIndex)},
		"raftLeader":           {text: maybeDormantInt(raft.Lead), className: leaderCls},
		"vote":                 {text: maybeDormantInt(raft.HardState.Vote)},
		"term":                 {text: maybeDormantInt(raft.HardState.Term)},
		"leadTransferee":       {text: maybeDormantInt(raft.LeadTransferee)},
		"applied":              {text: maybeDormantInt(raft.Applied)},
		"commit":               {text: maybeDormantInt(raft.HardState.Commit)},
		"lastIndex":            {text: fmt.Sprintf("%d", info.State.LastIndex)},
		"logSize":              {text: humanizeutil.IBytes(info.State.RaftLogSize), className: logCls},
		"leaseHolderQPS":       {text: leaseHolderQPS},
		"keysWrittenPS":        {text: fmt.Sprintf("%.4f", info.Stats.WritesPerSecond)},
		"approxProposalQuota":  {text: approxQuota},
		"pendingCommands":      {text: fmt.Sprintf("%d", info.State.NumPending)},
		"droppedCommands":      {text: fmt.Sprintf("%d", info.State.NumDropped), className: droppedCls},
		"truncatedIndex":       {text: fmt.Sprintf("%d", info.State.TruncatedState.Index)},
		"truncatedTerm":        {text: fmt.Sprintf("%d", info.State.TruncatedState.Term)},
		"mvccLastUpdate":       {text: fmtNanosTimestamp(stats.LastUpdateNanos)},
		"GCAvgAge":             {text: gcAvgAge},
		"GCBytesAge":           {text: fmt.Sprintf("%d", stats.GCBytesAge)},
		"NumIntents":           {text: fmt.Sprintf("%d", stats.IntentCount)},
		"IntentAvgAge":         {text: intentAvgAge},
		"IntentAge":            {text: fmt.Sprintf("%d", stats.IntentAge)},
		"mvccLiveBytesCount":   {text: mvccPair(stats.LiveBytes, stats.LiveCount)},
		"mvccKeyBytesCount":    {text: mvccPair(stats.KeyBytes, stats.KeyCount)},
		"mvccValueBytesCount":  {text: mvccPair(stats.ValBytes, stats.ValCount)},
		"mvccIntentBytesCount": {text: mvccPair(stats.IntentBytes, stats.IntentCount)},
		"mvccSystemBytesCount": {text: mvccPair(stats.SysBytes, stats.SysCount)},
		"rangeMaxBytes":        {text: humanizeutil.IBytes(info.State.RangeMaxBytes)},
		"writeLatches":         {text: writeLatchText, className: latchCls},
		"readLatches":          {text: readLatchText, className: latchCls},
	}
}

func collectRangeInfos(data *rangeReportData) []*serverpb.RangeInfo {
	var infos []*serverpb.RangeInfo
	for _, nodeResp := range data.Range.ResponsesByNodeID {
		if nodeResp.ErrorMessage != "" {
			continue
		}
		for i := range nodeResp.Infos {
			info := nodeResp.Infos[i]
			infos = append(infos, &info)
		}
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].SourceStoreID < infos[j].SourceStoreID
	})
	return infos
}

func collectInternalReplicas(infos []*serverpb.RangeInfo) []roachpb.ReplicaDescriptor {
	seen := map[roachpb.ReplicaID]struct{}{}
	var replicas []roachpb.ReplicaDescriptor
	for _, info := range infos {
		for _, rep := range info.State.Desc.InternalReplicas {
			if _, ok := seen[rep.ReplicaID]; ok {
				continue
			}
			seen[rep.ReplicaID] = struct{}{}
			replicas = append(replicas, rep)
		}
	}
	sort.Slice(replicas, func(i, j int) bool {
		return replicas[i].ReplicaID < replicas[j].ReplicaID
	})
	return replicas
}

func renderReportCell(text, className string) string {
	cls := "range-table__cell"
	if className != "" {
		cls += " " + className
	}
	return fmt.Sprintf(`<td class="%s">%s</td>`, escHTML(cls), escHTML(text))
}

func renderRangeTable(data *rangeReportData) string {
	infos := collectRangeInfos(data)
	replicas := collectInternalReplicas(infos)
	storeIDs := make([]roachpb.StoreID, 0, len(infos))
	storeMap := map[roachpb.StoreID]map[string]reportCell{}
	replicaPresence := map[roachpb.StoreID]map[roachpb.ReplicaID]roachpb.ReplicaDescriptor{}
	dormantStores := map[roachpb.StoreID]struct{}{}

	for _, info := range infos {
		storeID := info.SourceStoreID
		storeIDs = append(storeIDs, storeID)
		content := buildStoreContent(info)
		storeMap[storeID] = content
		if content["raftState"].className == "range-table__cell--raftstate-dormant" {
			dormantStores[storeID] = struct{}{}
		}
		replicaPresence[storeID] = map[roachpb.ReplicaID]roachpb.ReplicaDescriptor{}
		for _, rep := range info.State.Desc.InternalReplicas {
			replicaPresence[storeID][rep.ReplicaID] = rep
		}
	}

	var rows strings.Builder
	for _, field := range rangeReportFields {
		leaderStore := storeIDs[0]
		leaderVal := storeMap[leaderStore][field.key].text
		headerCls := "range-table__cell range-table__cell--header"
		if field.compare {
			for _, sid := range storeIDs[1:] {
				if _, dormant := dormantStores[sid]; dormant {
					continue
				}
				if storeMap[sid][field.key].text != leaderVal {
					headerCls += " range-table__cell--header-warning"
					break
				}
			}
		}
		rows.WriteString(`<tr class="range-table__row">`)
		rows.WriteString(fmt.Sprintf(`<th class="%s">%s</th>`, escHTML(headerCls), escHTML(field.label)))
		for _, sid := range storeIDs {
			cell := storeMap[sid][field.key]
			extra := cell.className
			if field.compare && sid != leaderStore {
				if _, dormant := dormantStores[sid]; !dormant && cell.text != leaderVal {
					if extra != "" {
						extra += " "
					}
					extra += "range-table__cell--different-from-leader-warning"
				}
			}
			rows.WriteString(renderReportCell(cell.text, extra))
		}
		rows.WriteString("</tr>")
	}

	for _, replica := range replicas {
		header := fmt.Sprintf("Replica %d - (%s)", replica.ReplicaID, fmtReplica(replica))
		rows.WriteString(`<tr class="range-table__row">`)
		rows.WriteString(fmt.Sprintf(`<th class="range-table__cell range-table__cell--header">%s</th>`, escHTML(header)))
		for _, sid := range storeIDs {
			_, dormant := dormantStores[sid]
			rep, ok := replicaPresence[sid][replica.ReplicaID]
			if !ok {
				cls := "range-table__cell--different-from-leader-warning"
				if dormant {
					cls = "range-table__cell--dormant"
				}
				rows.WriteString(renderReportCell("-", cls))
				continue
			}
			cls := ""
			if rep.StoreID == sid {
				cls = "range-table__cell--local-replica"
			}
			if dormant {
				if cls != "" {
					cls += " "
				}
				cls += "range-table__cell--dormant"
			}
			rows.WriteString(renderReportCell(fmtReplica(rep), cls))
		}
		rows.WriteString("</tr>")
	}
	return `<table class="range-table"><tbody>` + rows.String() + `</tbody></table>`
}

func renderLeaseHistory(info *serverpb.RangeInfo, rangeID roachpb.RangeID) string {
	local := getLocalReplica(info)
	title := "Lease History (from -)"
	if local != nil {
		title = fmt.Sprintf("Lease History (from %s)", fmtReplica(*local))
	}
	if len(info.LeaseHistory) == 0 {
		return fmt.Sprintf(`<div><h2 class="base-heading">%s</h2><h3>There is no lease history for this range</h3></div>`, escHTML(title))
	}
	history := append([]roachpb.Lease(nil), info.LeaseHistory...)
	for i, j := 0, len(history)-1; i < j; i, j = i+1, j-1 {
		history[i], history[j] = history[j], history[i]
	}
	epoch := isEpochLease(history[0])
	var head strings.Builder
	head.WriteString(`<tr class="lease-table__row lease-table__row--header">`)
	head.WriteString(`<th class="lease-table__cell lease-table__cell--header">Replica</th>`)
	if epoch {
		head.WriteString(`<th class="lease-table__cell lease-table__cell--header">Epoch</th>`)
	}
	head.WriteString(`<th class="lease-table__cell lease-table__cell--header">Proposed</th>`)
	head.WriteString(`<th class="lease-table__cell lease-table__cell--header">Start</th>`)
	head.WriteString(`<th class="lease-table__cell lease-table__cell--header">Expiration</th>`)
	head.WriteString(`</tr>`)

	writeCell := func(b *strings.Builder, value string) {
		b.WriteString(fmt.Sprintf(`<td class="lease-table__cell">%s</td>`, escHTML(value)))
	}

	var body strings.Builder
	for _, item := range history {
		expiration := fmtHLCTimestampPtr(item.Expiration)
		if epoch {
			expiration = "-"
		}
		body.WriteString(`<tr class="lease-table__row">`)
		writeCell(&body, fmtReplica(item.Replica))
		if epoch {
			writeCell(&body, fmt.Sprintf("%d", item.Epoch))
		}
		writeCell(&body, fmtHLCTimestampPtr(item.ProposedTS))
		writeCell(&body, fmtHLCTimestamp(item.Start))
		writeCell(&body, expiration)
		body.WriteString(`</tr>`)
	}
	return fmt.Sprintf(`<div><h2 class="base-heading">%s</h2><table class="lease-table"><tbody>%s%s</tbody></table></div>`, escHTML(title), head.String(), body.String())
}

func renderConnections(data *rangeReportData) string {
	nodeIDs := make([]roachpb.NodeID, 0, len(data.Range.ResponsesByNodeID))
	for nodeID := range data.Range.ResponsesByNodeID {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })
	title := "Connections"
	if data.Range.NodeID != 0 {
		title = fmt.Sprintf("Connections (via n%d)", data.Range.NodeID)
	}
	var rows strings.Builder
	for _, nodeID := range nodeIDs {
		resp := data.Range.ResponsesByNodeID[nodeID]
		rowCls := "connections-table__row"
		if !resp.Response || resp.ErrorMessage != "" {
			rowCls += " connections-table__row--warning"
		}
		valid := "error"
		if resp.Response {
			valid = "ok"
		}
		rows.WriteString(fmt.Sprintf(`<tr class="%s">`, escHTML(rowCls)))
		rows.WriteString(fmt.Sprintf(`<td class="connections-table__cell">n%d</td>`, nodeID))
		rows.WriteString(fmt.Sprintf(`<td class="connections-table__cell">%s</td>`, escHTML(valid)))
		rows.WriteString(fmt.Sprintf(`<td class="connections-table__cell">%d</td>`, len(resp.Infos)))
		rows.WriteString(fmt.Sprintf(`<td class="connections-table__cell">%s</td>`, escHTML(resp.ErrorMessage)))
		rows.WriteString(`</tr>`)
	}
	return fmt.Sprintf(`<h2 class="base-heading">%s</h2><table class="connections-table"><tbody><tr class="connections-table__row connections-table__row--header"><th class="connections-table__cell connections-table__cell--header">Node</th><th class="connections-table__cell connections-table__cell--header">Valid</th><th class="connections-table__cell connections-table__cell--header">Replicas</th><th class="connections-table__cell connections-table__cell--header">Error</th></tr>%s</tbody></table>`, escHTML(title), rows.String())
}

func renderAllocator(data *rangeReportData) string {
	title := "Simulated Allocator Output"
	if data.Allocator.NodeID != 0 {
		title = fmt.Sprintf("Simulated Allocator Output (from n%d)", data.Allocator.NodeID)
	}
	if data.Allocator.DryRun == nil || len(data.Allocator.DryRun.Events) == 0 {
		return fmt.Sprintf(`<h2 class="base-heading">%s</h2><p>No simulated allocator output was returned.</p>`, escHTML(title))
	}
	var rows strings.Builder
	for _, event := range data.Allocator.DryRun.Events {
		ts := event.Time.UTC().Format("2006-01-02 15:04:05")
		rows.WriteString(`<tr class="allocator-table__row">`)
		rows.WriteString(fmt.Sprintf(`<td class="allocator-table__cell allocator-table__cell--date">%s</td>`, escHTML(ts)))
		rows.WriteString(fmt.Sprintf(`<td class="allocator-table__cell">%s</td>`, escHTML(event.Message)))
		rows.WriteString(`</tr>`)
	}
	return fmt.Sprintf(`<h2 class="base-heading">%s</h2><table class="allocator-table"><tbody><tr class="allocator-table__row allocator-table__row--header"><th class="allocator-table__cell allocator-table__cell--header">Timestamp</th><th class="allocator-table__cell allocator-table__cell--header">Message</th></tr>%s</tbody></table>`, escHTML(title), rows.String())
}

func rangeLogEventTypeName(t storagepb.RangeLogEventType) string {
	switch t {
	case storagepb.RangeLogEventType_add:
		return "Add"
	case storagepb.RangeLogEventType_remove:
		return "Remove"
	case storagepb.RangeLogEventType_split:
		return "Split"
	case storagepb.RangeLogEventType_merge:
		return "Merge"
	default:
		return fmt.Sprintf("Unknown(%d)", t)
	}
}

func renderRangeLog(data *rangeReportData) string {
	if len(data.RangeLog.Events) == 0 {
		return `<h2 class="base-heading">Range Log</h2><p>No range log events were returned.</p>`
	}
	events := append([]serverpb.RangeLogResponse_Event(nil), data.RangeLog.Events...)
	sort.Slice(events, func(i, j int) bool {
		return events[i].Event.Timestamp.After(events[j].Event.Timestamp)
	})
	var rows strings.Builder
	for _, item := range events {
		event := item.Event
		pretty := item.PrettyInfo
		ts := event.Timestamp.UTC().Format("2006-01-02 15:04:05")
		rows.WriteString(`<tr class="log-table__row">`)
		rows.WriteString(fmt.Sprintf(`<td class="log-table__cell log-table__cell--date">%s</td>`, escHTML(ts)))
		rows.WriteString(fmt.Sprintf(`<td class="log-table__cell">s%d</td>`, event.StoreID))
		rows.WriteString(fmt.Sprintf(`<td class="log-table__cell">%s</td>`, escHTML(rangeLogEventTypeName(event.EventType))))
		rows.WriteString(fmt.Sprintf(`<td class="log-table__cell">%s</td>`, escHTML(formatRangeLogID(event.RangeID, data.Range.RangeID))))
		rows.WriteString(fmt.Sprintf(`<td class="log-table__cell">%s</td>`, escHTML(formatRangeLogID(event.OtherRangeID, data.Range.RangeID))))
		rows.WriteString(fmt.Sprintf(`<td class="log-table__cell">%s</td>`, renderLogPrettyInfo(pretty)))
		rows.WriteString(`</tr>`)
	}
	return `<h2 class="base-heading">Range Log</h2><table class="log-table"><tbody><tr class="log-table__row log-table__row--header"><th class="log-table__cell log-table__cell--header">Timestamp</th><th class="log-table__cell log-table__cell--header">Store</th><th class="log-table__cell log-table__cell--header">Event Type</th><th class="log-table__cell log-table__cell--header">Range</th><th class="log-table__cell log-table__cell--header">Other Range</th><th class="log-table__cell log-table__cell--header">Info</th></tr>` + rows.String() + `</tbody></table>`
}

func formatRangeLogID(id, current roachpb.RangeID) string {
	if id == 0 {
		return ""
	}
	if id == current {
		return fmt.Sprintf("r%d", id)
	}
	return fmt.Sprintf("r%d", id)
}

func renderLogPrettyInfo(p serverpb.RangeLogResponse_PrettyInfo) string {
	var items []string
	for _, pair := range []struct {
		label string
		value string
	}{
		{"Updated Range Descriptor", p.UpdatedDesc},
		{"New Range Descriptor", p.NewDesc},
		{"Added Replica", p.AddedReplica},
		{"Removed Replica", p.RemovedReplica},
		{"Reason", p.Reason},
		{"Details", p.Details},
	} {
		if pair.value == "" {
			continue
		}
		items = append(items, fmt.Sprintf("<li>%s: %s</li>", escHTML(pair.label), escHTML(pair.value)))
	}
	if len(items) == 0 {
		return escHTML("-")
	}
	return `<ul class="log-entries-list">` + strings.Join(items, "") + `</ul>`
}

const rangeReportHTMLStyles = `
body {
  margin: 24px;
  font-family: SourceSansPro-Regular, Helvetica Neue, Helvetica, Arial, sans-serif;
  color: #394455;
  background: #fff;
}
.section { margin-bottom: 24px; }
.meta { color: #5f6c87; margin-bottom: 24px; font-size: 14px; }
.base-heading { margin: 0 0 16px; font-size: 18px; font-weight: 600; color: #00294d; }
.range-table, .lease-table, .allocator-table, .connections-table, .log-table {
  margin: 0 0 40px;
  display: table;
  border-collapse: collapse;
  border-spacing: 0;
}
.range-table__row, .lease-table__row, .allocator-table__row, .connections-table__row, .log-table__row {
  display: table-row;
  background-color: #ededed;
}
.connections-table__row--warning { background-color: #fff5f5; }
.range-table__cell, .lease-table__cell, .allocator-table__cell, .connections-table__cell, .log-table__cell {
  background-color: #fff;
  padding: 6px 12px;
  display: table-cell;
  height: 20px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 260px;
  border-width: 1px 1px 0 0;
  border-color: rgba(0, 0, 0, 0.1);
  border-style: solid;
  font-size: 14px;
  vertical-align: top;
}
.log-table__cell { white-space: normal; max-width: 420px; }
.log-entries-list { margin: 0; padding-left: 18px; white-space: normal; }
.range-table__cell--header, .lease-table__cell--header, .allocator-table__cell--header,
.connections-table__cell--header, .log-table__cell--header {
  background-color: #3a7de1;
  color: #fff;
  font-weight: 900;
  text-align: right;
  max-width: none;
  white-space: normal;
}
.connections-table__cell { text-align: center; width: 6em; }
.range-table__cell--header-warning { color: #ff3b4e; }
.range-table__cell--raftstate-leader, .range-table__cell--lease-holder, .range-table__cell--local-replica { color: #008000; }
.range-table__cell--raftstate-follower, .range-table__cell--lease-follower { color: #00f; }
.range-table__cell--raftstate-candidate { color: #ffa500; }
.range-table__cell--raftstate-precandidate { color: #ff8c00; }
.range-table__cell--raftstate-dormant, .range-table__cell--dormant { color: #d3d3d3; }
.range-table__cell--raftstate-unknown, .range-table__cell--different-from-leader-warning { color: #f00; }
.range-table__cell--quiescent { color: #ee82ee; }
.range-table__cell--warning { color: #ffa500; }
a { color: #3a7de1; text-decoration: none; }
`

func generateRangeReportHTML(data *rangeReportData, sourceURL string) string {
	rangeID := data.Range.RangeID
	now := timeutil.Now().UTC().Format("2006-01-02 15:04:05")
	infos := collectRangeInfos(data)

	var sections strings.Builder
	sections.WriteString(fmt.Sprintf(`<h1 class="base-heading">Range Report for r%d</h1>`, rangeID))
	sections.WriteString(fmt.Sprintf(`<h2 class="base-heading">Range r%d at %s UTC</h2>`, rangeID, escHTML(now)))
	sections.WriteString(renderRangeTable(data))
	for _, info := range infos {
		sections.WriteString(renderLeaseHistory(info, rangeID))
	}
	sections.WriteString(renderConnections(data))
	sections.WriteString(renderAllocator(data))
	sections.WriteString(renderRangeLog(data))

	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Range Report r%d | KaiwuDB Console</title>
<style>%s</style>
</head>
<body>
<div class="section">
<p class="meta">来源: <a href="%s">%s</a></p>
%s
</div>
</body>
</html>`, rangeID, rangeReportHTMLStyles, escHTML(sourceURL), escHTML(sourceURL), sections.String())
}
