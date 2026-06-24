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

package cli

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cli/cliflags"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var demoCmd = &cobra.Command{
	Use:   "demo",
	Short: "open a demo sql shell (not suitable for time-series scenario)",
	Long: `
Start an in-memory, standalone, single-node KwDB instance, and open an
interactive SQL prompt to it.

kwbase demo attempts to connect to a Kw Labs server to obtain a
temporary enterprise license for demoing enterprise features and enable
telemetry back to Kw Labs. In order to disable this behavior, set the
environment variable "KWBASE_SKIP_ENABLING_DIAGNOSTIC_REPORTING" to true.
`,
	Example: `  kwbase demo`,
	Args:    cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(func(cmd *cobra.Command, _ []string) error {
		return runDemo(cmd)
	}),
}

const demoOrg = "Kw Demo"

const defaultRootPassword = "admin"

// maxNodeInitTime is the maximum amount of time to wait for nodes to be connected.
const maxNodeInitTime = 30 * time.Second

var defaultLocalities = demoLocalityList{
	// Default localities for a 3 node cluster
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east1"}, {Key: "az", Value: "b"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east1"}, {Key: "az", Value: "c"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east1"}, {Key: "az", Value: "d"}}},
	// Default localities for a 6 node cluster
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-west1"}, {Key: "az", Value: "a"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-west1"}, {Key: "az", Value: "b"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-west1"}, {Key: "az", Value: "c"}}},
	// Default localities for a 9 node cluster
	{Tiers: []roachpb.Tier{{Key: "region", Value: "europe-west1"}, {Key: "az", Value: "b"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "europe-west1"}, {Key: "az", Value: "c"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "europe-west1"}, {Key: "az", Value: "d"}}},
}

var demoNodeCacheSizeValue = newBytesOrPercentageValue(
	&demoCtx.cacheSize,
	memoryPercentResolver,
)
var demoNodeSQLMemSizeValue = newBytesOrPercentageValue(
	&demoCtx.sqlPoolMemorySize,
	memoryPercentResolver,
)

type regionPair struct {
	regionA string
	regionB string
}

var regionToRegionToLatency map[string]map[string]int

func insertPair(pair regionPair, latency int) {
	regionToLatency, ok := regionToRegionToLatency[pair.regionA]
	if !ok {
		regionToLatency = make(map[string]int)
		regionToRegionToLatency[pair.regionA] = regionToLatency
	}
	regionToLatency[pair.regionB] = latency
}

func init() {
	regionToRegionToLatency = make(map[string]map[string]int)
	// Latencies collected from http://cloudping.co on 2019-09-11.
	for pair, latency := range map[regionPair]int{
		{regionA: "us-east1", regionB: "us-west1"}:     66,
		{regionA: "us-east1", regionB: "europe-west1"}: 64,
		{regionA: "us-west1", regionB: "europe-west1"}: 146,
	} {
		insertPair(pair, latency)
		insertPair(regionPair{
			regionA: pair.regionB,
			regionB: pair.regionA,
		}, latency)
	}
}

// GetAndApplyLicense is not implemented in order to keep OSS/BSL builds successful.
// The cliccl package sets this function if enterprise features are available to demo.
var GetAndApplyLicense func(dbConn *gosql.DB, clusterID uuid.UUID, org string) (bool, error)

func incrementTelemetryCounters(cmd *cobra.Command) {
	incrementDemoCounter(demo)
	if flagSetForCmd(cmd).Lookup(cliflags.DemoNodes.Name).Changed {
		incrementDemoCounter(nodes)
	}
	if demoCtx.localities != nil {
		incrementDemoCounter(demoLocality)
	}
}

func checkDemoConfiguration(cmd *cobra.Command) error {
	// Make sure the number of nodes is valid.
	if demoCtx.nodes <= 0 {
		return errors.Newf("--nodes has invalid value (expected positive, got %d)", demoCtx.nodes)
	}

	// If artificial latencies were requested, then the user cannot supply their own localities.
	if demoCtx.simulateLatency && demoCtx.localities != nil {
		return errors.New("--global cannot be used with --demo-locality")
	}

	demoCtx.disableTelemetry = cluster.TelemetryOptOut()
	// disableLicenseAcquisition can also be set by the the user as an
	// input flag, so make sure it include it when considering the final
	// value of disableLicenseAcquisition.
	demoCtx.disableLicenseAcquisition =
		demoCtx.disableTelemetry || (GetAndApplyLicense == nil) || demoCtx.disableLicenseAcquisition

	return nil
}

func runDemo(cmd *cobra.Command) (err error) {
	if err = checkDemoConfiguration(cmd); err != nil {
		return err
	}
	// Record some telemetry about what flags are being used.
	incrementTelemetryCounters(cmd)

	ctx := context.Background()

	if err := checkTzDatabaseAvailability(ctx); err != nil {
		return err
	}

	c, err := setupTransientCluster(ctx, cmd)
	defer c.cleanup(ctx)
	if err != nil {
		return checkAndMaybeShout(err)
	}
	demoCtx.transientCluster = &c

	checkInteractive()

	if cliCtx.isInteractive {
		fmt.Printf(`#
# Welcome to the KwDB demo database!
#
# You are connected to a temporary, in-memory KwDB cluster of %d node%s.
`, demoCtx.nodes, util.Pluralize(int64(demoCtx.nodes)))

		if demoCtx.disableTelemetry {
			fmt.Println("#\n# Telemetry and automatic license acquisition disabled by configuration.")
		} else if demoCtx.disableLicenseAcquisition {
			fmt.Println("#\n# Enterprise features disabled by OSS-only build.")
		} else {
			fmt.Println("#\n# This demo session will attempt to enable enterprise features\n" +
				"# by acquiring a temporary license from Kw Labs in the background.\n" +
				"# To disable this behavior, set the environment variable\n" +
				"# KWBASE_SKIP_ENABLING_DIAGNOSTIC_REPORTING=true.")
		}
	}

	// Start license acquisition in the background.
	licenseDone, err := c.acquireDemoLicense(ctx)
	if err != nil {
		return checkAndMaybeShout(err)
	}

	if cliCtx.isInteractive {
		fmt.Println(`#
# Reminder: your changes to data stored in the demo session will not be saved!
#
# Connection parameters:`)
		var nodeList strings.Builder
		c.listDemoNodes(&nodeList, true /* justOne */)
		fmt.Println("#", strings.ReplaceAll(nodeList.String(), "\n", "\n# "))

		if demoCtx.insecure {
			fmt.Printf(
				"# Kw demo is running in insecure mode.\n" +
					"# Run with --insecure=false to use security related features.\n" +
					"# Note: Starting in secure mode will become the default in v20.2.\n#\n",
			)
		} else {
			fmt.Printf(
				"# The user %q with password %q has been created. Use it to access the Web UI!\n#\n",
				security.RootUser,
				defaultRootPassword,
			)
		}
		go func() {
			if err := waitForLicense(licenseDone); err != nil {
				_ = checkAndMaybeShout(err)
			}
		}()
	} else {
		// If we are not running an interactive shell, we need to wait to ensure
		// that license acquisition is successful. If license acquisition is
		// disabled, then a read on this channel will return immediately.
		if err := waitForLicense(licenseDone); err != nil {
			return checkAndMaybeShout(err)
		}
	}

	conn := makeSQLConn(c.connURL)
	defer conn.Close()

	return runClient(cmd, conn)
}

func waitForLicense(licenseDone <-chan error) error {
	err := <-licenseDone
	return err
}
