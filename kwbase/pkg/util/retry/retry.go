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

package retry

import (
	"context"
	"math"
	"math/rand"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// Options provides reusable configuration of Retry objects.
type Options struct {
	InitialBackoff      time.Duration   // Default retry backoff interval
	MaxBackoff          time.Duration   // Maximum retry backoff interval
	Multiplier          float64         // Default backoff constant
	MaxRetries          int             // Maximum number of attempts (0 for infinite)
	RandomizationFactor float64         // Randomize the backoff interval by constant
	Closer              <-chan struct{} // Optionally end retry loop channel close.
}

// Retry implements the public methods necessary to control an exponential-
// backoff retry loop.
type Retry struct {
	opts           Options
	ctxDoneChan    <-chan struct{}
	currentAttempt int
	isReset        bool
}

// Start returns a new Retry initialized to some default values. The Retry can
// then be used in an exponential-backoff retry loop.
func Start(opts Options) Retry {
	return StartWithCtx(context.Background(), opts)
}

// StartWithCtx returns a new Retry initialized to some default values. The
// Retry can then be used in an exponential-backoff retry loop. If the provided
// context is canceled (see Context.Done), the retry loop ends early.
func StartWithCtx(ctx context.Context, opts Options) Retry {
	if opts.InitialBackoff == 0 {
		opts.InitialBackoff = 50 * time.Millisecond
	}
	if opts.MaxBackoff == 0 {
		opts.MaxBackoff = 2 * time.Second
	}
	if opts.RandomizationFactor == 0 {
		opts.RandomizationFactor = 0.15
	}
	if opts.Multiplier == 0 {
		opts.Multiplier = 2
	}

	r := Retry{opts: opts}
	r.ctxDoneChan = ctx.Done()
	r.Reset()
	return r
}

// Reset resets the Retry to its initial state, meaning that the next call to
// Next will return true immediately and subsequent calls will behave as if
// they had followed the very first attempt (i.e. their backoffs will be
// short).
func (r *Retry) Reset() {
	select {
	case <-r.opts.Closer:
		// When the closer has fired, you can't keep going.
		return
	case <-r.ctxDoneChan:
		// When the context was canceled, you can't keep going.
		return
	default:
	}
	r.currentAttempt = 0
	r.isReset = true
}

func (r Retry) retryIn() time.Duration {
	backoff := float64(r.opts.InitialBackoff) * math.Pow(r.opts.Multiplier, float64(r.currentAttempt))
	if maxBackoff := float64(r.opts.MaxBackoff); backoff > maxBackoff {
		backoff = maxBackoff
	}

	var delta = r.opts.RandomizationFactor * backoff
	// Get a random value from the range [backoff - delta, backoff + delta].
	// The formula used below has a +1 because time.Duration is an int64, and the
	// conversion floors the float64.
	return time.Duration(backoff - delta + rand.Float64()*(2*delta+1))
}

// Next returns whether the retry loop should continue, and blocks for the
// appropriate length of time before yielding back to the caller. If a stopper
// is present, Next will eagerly return false when the stopper is stopped.
func (r *Retry) Next() bool {
	if r.isReset {
		r.isReset = false
		return true
	}

	if r.opts.MaxRetries > 0 && r.currentAttempt >= r.opts.MaxRetries {
		return false
	}

	// Wait before retry.
	select {
	case <-time.After(r.retryIn()):
		r.currentAttempt++
		return true
	case <-r.opts.Closer:
		return false
	case <-r.ctxDoneChan:
		return false
	}
}

// CurrentAttempts returns the currentAttempt
func (r *Retry) CurrentAttempts() int {
	return r.currentAttempt
}

// closedC is returned from Retry.NextCh whenever a retry
// can begin immediately.
var closedC = func() chan time.Time {
	c := make(chan time.Time)
	close(c)
	return c
}()

// NextCh returns a channel which will receive when the next retry
// interval has expired.
func (r *Retry) NextCh() <-chan time.Time {
	if r.isReset {
		r.isReset = false
		return closedC
	}
	r.currentAttempt++
	if r.opts.MaxRetries > 0 && r.currentAttempt > r.opts.MaxRetries {
		return nil
	}
	return time.After(r.retryIn())
}

// WithMaxAttempts is a helper that runs fn N times and collects the last err.
// It guarantees fn will run at least once. Otherwise, an error will be returned.
func WithMaxAttempts(ctx context.Context, opts Options, n int, fn func() error) error {
	if n <= 0 {
		return errors.Errorf("max attempts should not be 0 or below, got: %d", n)
	}

	opts.MaxRetries = n - 1
	var err error
	for r := StartWithCtx(ctx, opts); r.Next(); {
		err = fn()
		if err == nil {
			return nil
		}
	}
	if err == nil {
		if ctx.Err() != nil {
			err = errors.Wrap(ctx.Err(), "did not run function due to context completion")
		} else {
			err = errors.New("did not run function due to closed opts.Closer")
		}
	}
	return err
}

// ForDuration will retry the given function until it either returns
// without error, or the given duration has elapsed. The function is invoked
// immediately at first and then successively with an exponential backoff
// starting at 1ns and ending at the specified duration.
//
// This function is DEPRECATED! Please use one of the other functions in this
// package that takes context cancellation into account.
//
// TODO(benesch): remove this function and port its callers to a context-
// sensitive API.
func ForDuration(duration time.Duration, fn func() error) error {
	deadline := timeutil.Now().Add(duration)
	var lastErr error
	for wait := time.Duration(1); timeutil.Now().Before(deadline); wait *= 2 {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if wait > time.Second {
			wait = time.Second
		}
		time.Sleep(wait)
	}
	return lastErr
}
