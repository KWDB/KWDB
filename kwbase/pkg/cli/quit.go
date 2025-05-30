// Copyright 2020 The Cockroach Authors.
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
	"io"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/contextutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/grpcutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// quitCmd command shuts down the node server.
var quitCmd = &cobra.Command{
	Use:   "quit",
	Short: "drain and shut down a node\n",
	Long: `
Shut down the server. The first stage is drain, where the server
stops accepting client connections, then stops extant
connections, and finally pushes range leases onto other nodes,
subject to various timeout parameters configurable via
cluster settings. After the first stage completes,
the server process is shut down.

See also 'kwbase node drain' to drain a server
without stopping the server process.
`,
	Args:   cobra.NoArgs,
	Hidden: true,
	RunE:   MaybeDecorateGRPCError(runQuit),
}

// runQuit accesses the quit shutdown path.
func runQuit(cmd *cobra.Command, args []string) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// At the end, we'll report "ok" if there was no error.
	defer func() {
		if err == nil {
			fmt.Println("ok")
		}
	}()

	// Establish a RPC connection.
	c, finish, err := getAdminClient(ctx, serverCfg)
	if err != nil {
		return err
	}
	defer finish()

	// If --decommission was passed, perform the decommission as first
	// step. (Note that this flag is deprecated. It will be removed.)
	if quitCtx.serverDecommission {
		var myself []roachpb.NodeID // will remain empty, which means target yourself
		if err := runDecommissionNodeImpl(ctx, c, nodeDecommissionWaitAll, myself); err != nil {
			log.Warningf(ctx, "%v", err)
			if server.IsWaitingForInit(err) {
				err = errors.New("node cannot be decommissioned before it has been initialized")
			}
			return err
		}
	}

	return drainAndShutdown(ctx, c)
}

// drainAndShutdown attempts to drain the server and then shut it
// down. When given an empty onModes slice, it's a hard shutdown.
func drainAndShutdown(ctx context.Context, c serverpb.AdminClient) (err error) {
	hardError, remainingWork, err := doDrain(ctx, c)
	if hardError {
		return err
	}

	if remainingWork {
		log.Warningf(ctx, "graceful shutdown may not have completed successfully; check the node's logs for details.")
	}

	if err != nil {
		log.Warningf(ctx, "drain did not complete successfully; hard shutdown may cause disruption")
	}
	// We have already performed the drain above. We use a nil array
	// of drain modes to indicate no further drain needs to be attempted
	// and go straight to shutdown. We try two times just in case there
	// is a transient error.
	hardErr, err := doShutdown(ctx, c)
	if err != nil && !hardErr {
		log.Warningf(ctx, "hard shutdown attempt failed, retrying: %v", err)
		_, err = doShutdown(ctx, c)
	}
	return errors.Wrap(err, "hard shutdown failed")
}

// doDrain calls a graceful drain.
//
// If the function returns hardError true, then the caller should not
// proceed with an alternate strategy (it's likely the server has gone
// away).
func doDrain(
	ctx context.Context, c serverpb.AdminClient,
) (hardError, remainingWork bool, err error) {
	// The next step is to drain. The timeout is configurable
	// via --drain-wait.
	if quitCtx.drainWait == 0 {
		return doDrainNoTimeout(ctx, c)
	}

	err = contextutil.RunWithTimeout(ctx, "drain", quitCtx.drainWait, func(ctx context.Context) (err error) {
		hardError, remainingWork, err = doDrainNoTimeout(ctx, c)
		return err
	})
	if _, ok := err.(*contextutil.TimeoutError); ok || grpcutil.IsTimeout(err) {
		log.Infof(ctx, "drain timed out: %v", err)
		err = errors.New("drain timeout")
	}
	return
}

func doDrainNoTimeout(
	ctx context.Context, c serverpb.AdminClient,
) (hardError, remainingWork bool, err error) {
	defer func() {
		if server.IsWaitingForInit(err) {
			log.Infof(ctx, "%v", err)
			err = errors.New("node cannot be drained before it has been initialized")
		}
	}()

	remainingWork = true
	for {
		// Tell the user we're starting to drain. This enables the user to
		// mentally prepare for something to take some time, as opposed to
		// wondering why nothing is happening.
		fmt.Fprintf(stderr, "node is draining... ") // notice no final newline.

		// Send a drain request with the drain bit set and the shutdown bit
		// unset.
		stream, err := c.Drain(ctx, &serverpb.DrainRequest{
			DeprecatedProbeIndicator: server.DeprecatedDrainParameter,
			DoDrain:                  true,
		})
		if err != nil {
			fmt.Fprintf(stderr, "\n") // finish the line started above.
			return !grpcutil.IsTimeout(err), remainingWork, errors.Wrap(err, "error sending drain request")
		}
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				// Done.
				break
			}
			if err != nil {
				// Unexpected error.
				fmt.Fprintf(stderr, "\n") // finish the line started above.
				log.Infof(ctx, "graceful shutdown failed: %v", err)
				return false, remainingWork, err
			}

			if resp.IsDraining {
				// We want to assert that the node is quitting, and tell the
				// story about how much work was performed in logs for
				// debugging.
				finalString := ""
				if resp.DrainRemainingIndicator == 0 {
					finalString = " (complete)"
				}
				// We use stderr so that 'kwbase quit''s stdout remains a
				// simple 'ok' in case of success (for compatibility with
				// scripts).
				fmt.Fprintf(stderr, "remaining: %d%s\n",
					resp.DrainRemainingIndicator, finalString)
				remainingWork = resp.DrainRemainingIndicator > 0
			} else {
				// Either the server has decided it wanted to stop quitting; or
				// we're running a pre-20.1 node which doesn't populate IsDraining.
				// In either case, we need to stop sending drain requests.
				remainingWork = false
				fmt.Fprintf(stderr, "done\n")
			}

			if resp.DrainRemainingDescription != "" {
				// Only show this information in the log; we'd use this for debugging.
				// (This can be revealed e.g. via --logtostderr.)
				log.Infof(ctx, "drain details: %s\n", resp.DrainRemainingDescription)
			}

			// Iterate until end of stream, which indicates the drain is
			// complete.
		}
		if !remainingWork {
			break
		}
		// Avoid a busy wait with high CPU/network usage if the server
		// replies with an incomplete drain too quickly.
		time.Sleep(200 * time.Millisecond)
	}
	return false, remainingWork, nil
}

// doShutdown attempts to trigger a server shutdown *without*
// draining. Use doDrain() prior to perform a drain, or
// drainAndShutdown() to combine both.
func doShutdown(ctx context.Context, c serverpb.AdminClient) (hardError bool, err error) {
	defer func() {
		if server.IsWaitingForInit(err) {
			log.Infof(ctx, "encountered error: %v", err)
			err = errors.New("node cannot be shut down before it has been initialized")
			err = errors.WithHint(err, "You can still stop the process using a service manager or a signal.")
			hardError = true
		}
		if grpcutil.IsClosedConnection(err) || grpcConfusedErrConnClosedByPeer(err) {
			// This most likely means that we shut down successfully. Note
			// that sometimes the connection can be shut down even before a
			// DrainResponse gets sent back to us, so we don't require a
			// response on the stream (see #14184).
			err = nil
		}
	}()

	// We use a shorter timeout because a shutdown request has nothing
	// else to do than shut down the node immediately.
	err = contextutil.RunWithTimeout(ctx, "hard shutdown", 10*time.Second, func(ctx context.Context) error {
		// Send a drain request with the drain bit unset (no drain).
		// and the shutdown bit set.
		stream, err := c.Drain(ctx, &serverpb.DrainRequest{Shutdown: true})
		if err != nil {
			return errors.Wrap(err, "error sending shutdown request")
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
		}
	})
	if _, ok := err.(*contextutil.TimeoutError); !ok {
		hardError = true
	}
	return hardError, err
}

// getAdminClient returns an AdminClient and a closure that must be invoked
// to free associated resources.
func getAdminClient(ctx context.Context, cfg server.Config) (serverpb.AdminClient, func(), error) {
	conn, _, finish, err := getClientGRPCConn(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed to connect to the node")
	}
	return serverpb.NewAdminClient(conn), finish, nil
}

// grpcConfusedErrConnClosedByPeer returns true if the given error
// has been likely produced by a gRPC handshake that was confused
// by the remote end closing the connection.
// This situation occurs semi-frequently (10-15% of cases) in
// go 1.13, and may have been eliminated in 1.14.
func grpcConfusedErrConnClosedByPeer(err error) bool {
	err = errors.Cause(err)
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch {
	case s.Code() == codes.Internal && strings.Contains(err.Error(), "compressed flag set with identity or empty encoding"):
		return true
	case s.Code() == codes.Unimplemented && strings.Contains(err.Error(), "Decompressor is not installed"):
		return true
	default:
		return false
	}
}
