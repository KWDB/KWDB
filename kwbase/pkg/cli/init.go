// Copyright 2017 The Cockroach Authors.
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
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/contextutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "initialize a cluster",
	Long: `
Perform one-time-only initialization of a KwDB cluster.

After starting one or more nodes with --join flags, run the init
command on one node (passing the same --host and certificate flags
you would use for the sql command). The target of the init command
must appear in the --join flags of other nodes.

A node started without the --join flag initializes itself as a
single-node cluster, so the init command is not used in that case.
`,
	Args: cobra.NoArgs,
	RunE: maybeShoutError(MaybeDecorateGRPCError(runInit)),
}

func runInit(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wait for the node to be ready for initialization.
	conn, finish, err := waitForClientReadinessAndGetClientGRPCConn(ctx)
	if err != nil {
		return err
	}
	defer finish()

	// Actually perform cluster initialization.
	c := serverpb.NewInitClient(conn)

	if _, err = c.Bootstrap(ctx, &serverpb.BootstrapRequest{}); err != nil {
		if strings.Contains(err.Error(), server.ErrClusterInitialized.Error()) {
			// We really want to use errors.Is() here but this would require
			// error serialization support in gRPC.
			// This is not yet performed in KwDB even though
			// the error library now has infrastructure to do so, see:
			// https://github.com/cockroachdb/errors/pull/14
			return errors.WithHint(err,
				"Please ensure all your start commands are using --join.")
		}
		return err
	}

	fmt.Fprintln(os.Stdout, "Cluster successfully initialized")
	return nil
}

// waitForClientReadinessAndGetClientGRPCConn waits for the node to
// be ready for initialization. This check ensures that the `init`
// command is less likely to fail because it was issued too
// early. In general, retrying the `init` command is dangerous [0],
// so we make a best effort at minimizing chances for users to
// arrive in an uncomfortable situation.
//
// [0]: https://gitee.com/kwbasedb/kwbase/pull/19753#issuecomment-341561452
func waitForClientReadinessAndGetClientGRPCConn(
	ctx context.Context,
) (conn *grpc.ClientConn, finish func(), err error) {
	defer func() {
		// If we're returning with an error, tear down the gRPC connection
		// that's been established, if any.
		if finish != nil && err != nil {
			finish()
		}
	}()

	retryOpts := retry.Options{InitialBackoff: time.Second, MaxBackoff: time.Second}
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		if err = contextutil.RunWithTimeout(ctx, "init-open-conn", 5*time.Second,
			func(ctx context.Context) error {
				// (Attempt to) establish the gRPC connection. If that fails,
				// it may be that the server hasn't started to listen yet, in
				// which case we'll retry.
				conn, _, finish, err = getClientGRPCConn(ctx, serverCfg)
				if err != nil {
					return err
				}

				// Access the /health endpoint. Until/unless this succeeds, the
				// node is not yet fully initialized and ready to accept
				// Bootstrap requests.
				ac := serverpb.NewAdminClient(conn)
				_, err := ac.Health(ctx, &serverpb.HealthRequest{})
				return err
			}); err != nil {
			err = errors.Wrapf(err, "node not ready to perform cluster initialization")
			fmt.Fprintln(stderr, "warning:", err, "(retrying)")

			// We're going to retry; first cancel the connection that's
			// been established, if any.
			if finish != nil {
				finish()
				finish = nil
			}
			// Then retry.
			continue
		}

		// No error - connection was established and health endpoint is
		// ready.
		return conn, finish, err
	}
	err = errors.New("maximum number of retries exceeded")
	return
}
