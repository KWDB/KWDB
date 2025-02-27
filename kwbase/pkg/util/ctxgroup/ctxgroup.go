// Copyright 2018 The Cockroach Authors.
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

/*
Package ctxgroup wraps golang.org/x/sync/errgroup with a context func.

This package extends and modifies the errgroup API slightly to
make context variables more explicit. WithContext no longer returns
a context. Instead, the GoCtx method explicitly passes one to the
invoked func. The goal is to make misuse of context vars with errgroups
more difficult. Example usage:

	ctx := context.Background()
	g := ctxgroup.WithContext(ctx)
	ch := make(chan bool)
	g.GoCtx(func(ctx context.Context) error {
		defer close(ch)
		for _, val := range []bool{true, false} {
			select {
			case ch <- val:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	g.GoCtx(func(ctx context.Context) error {
		for val := range ch {
			if err := api.Call(ctx, val); err != nil {
				return err
			}
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	api.Call(ctx, "done")

Problems with errgroup

The bugs this package attempts to prevent are: misuse of shadowed
ctx variables after errgroup closure and confusion in the face of
multiple ctx variables when trying to prevent shadowing. The following
are all example bugs that Cockroach has had during its use of errgroup:

	ctx := context.Background()
	g, ctx := errgroup.WithContext(ctx)
	ch := make(chan bool)
	g.Go(func() error {
		defer close(ch)
		for _, val := range []bool{true, false} {
			select {
			case ch <- val:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	g.Go(func() error {
		for val := range ch {
			if err := api.Call(ctx, val); err != nil {
				return err
			}
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	api.Call(ctx, "done")

The ctx used by the final api.Call is already closed because the
errgroup has returned. This happened because of the desire to not
create another ctx variable, and so we shadowed the original ctx var,
but then incorrectly continued to use it after the errgroup had closed
its context. So we make a modification and create new gCtx variable
that doesn't shadow the original ctx:

	ctx := context.Background()
	g, gCtx := errgroup.WithContext(ctx)
	ch := make(chan bool)
	g.Go(func() error {
		defer close(ch)
		for _, val := range []bool{true, false} {
			select {
			case ch <- val:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	g.Go(func() error {
		for val := range ch {
			if err := api.Call(ctx, val); err != nil {
				return err
			}
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	api.Call(ctx, "done")

Now the final api.Call is correct. But the other api.Call is incorrect
and the ctx.Done receive is incorrect because they are using the wrong
context and thus won't correctly exit early if the errgroup needs to
exit early.

*/
package ctxgroup

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// Group wraps errgroup.
type Group struct {
	wrapped *errgroup.Group
	ctx     context.Context
}

// Wait blocks until all function calls from the Go method have returned, then
// returns the first non-nil error (if any) from them. If Wait() is invoked
// after the context (originally supplied to WithContext) is canceled, Wait
// returns an error, even if no Go invocation did. In particular, calling
// Wait() after Done has been closed is guaranteed to return an error.
func (g Group) Wait() error {
	ctxErr := g.ctx.Err()
	err := g.wrapped.Wait()
	if err != nil {
		return err
	}
	return ctxErr
}

// WithContext returns a new Group and an associated Context derived from ctx.
func WithContext(ctx context.Context) Group {
	grp, ctx := errgroup.WithContext(ctx)
	return Group{
		wrapped: grp,
		ctx:     ctx,
	}
}

// Go calls the given function in a new goroutine.
func (g Group) Go(f func() error) {
	g.wrapped.Go(f)
}

// GoCtx calls the given function in a new goroutine.
func (g Group) GoCtx(f func(ctx context.Context) error) {
	g.wrapped.Go(func() error {
		return f(g.ctx)
	})
}

// GroupWorkers runs num worker go routines in an errgroup.
func GroupWorkers(ctx context.Context, num int, f func(context.Context, int) error) error {
	group := WithContext(ctx)
	for i := 0; i < num; i++ {
		workerID := i
		group.GoCtx(func(ctx context.Context) error { return f(ctx, workerID) })
	}
	return group.Wait()
}
