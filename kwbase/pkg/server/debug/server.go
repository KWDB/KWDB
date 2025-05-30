// Copyright 2016 The Cockroach Authors.
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

package debug

import (
	"context"
	"expvar"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/server/debug/goroutineui"
	"gitee.com/kwbasedb/kwbase/pkg/server/debug/pprofui"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"
	"golang.org/x/net/trace"
	"google.golang.org/grpc/metadata"
)

func init() {
	// Disable the net/trace auth handler.
	trace.AuthRequest = func(r *http.Request) (allowed, sensitive bool) {
		return true, true
	}
}

// RemoteMode controls who can access /debug/requests.
type RemoteMode string

const (
	// RemoteOff disallows access to /debug/requests.
	RemoteOff RemoteMode = "off"
	// RemoteLocal allows only host-local access to /debug/requests.
	RemoteLocal RemoteMode = "local"
	// RemoteAny allows all access to /debug/requests.
	RemoteAny RemoteMode = "any"
)

// Endpoint is the entry point under which the debug tools are housed.
const Endpoint = "/debug/"

// DebugRemote controls which clients are allowed to access certain
// confidential debug pages, such as those served under the /debug/ prefix.
var DebugRemote = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		"server.remote_debugging.mode",
		"set to enable remote debugging, localhost-only or disable (any, local, off)",
		"local",
		func(sv *settings.Values, s string) error {
			switch RemoteMode(strings.ToLower(s)) {
			case RemoteOff, RemoteLocal, RemoteAny:
				return nil
			default:
				return errors.Errorf("invalid mode: '%s'", s)
			}
		},
	)
	s.SetReportable(true)
	s.SetVisibility(settings.Public)
	return s
}()

// Server serves the /debug/* family of tools.
type Server struct {
	st  *cluster.Settings
	mux *http.ServeMux
	spy logSpy
}

// NewServer sets up a debug server.
func NewServer(st *cluster.Settings, hbaConfDebugFn http.HandlerFunc) *Server {
	mux := http.NewServeMux()

	// Install a redirect to the UI's collection of debug tools.
	mux.HandleFunc(Endpoint, handleLanding)

	// Cribbed straight from pprof's `init()` method. See:
	// https://golang.org/src/net/http/pprof/pprof.go
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Cribbed straight from trace's `init()` method. See:
	// https://github.com/golang/net/blob/master/trace/trace.go
	mux.HandleFunc("/debug/requests", trace.Traces)
	mux.HandleFunc("/debug/events", trace.Events)

	// This registers a superset of the variables exposed through the
	// /debug/vars endpoint onto the /debug/metrics endpoint. It includes all
	// expvars registered globally and all metrics registered on the
	// DefaultRegistry.
	mux.Handle("/debug/metrics", exp.ExpHandler(metrics.DefaultRegistry))
	// Also register /debug/vars (even though /debug/metrics is better).
	mux.Handle("/debug/vars", expvar.Handler())

	if hbaConfDebugFn != nil {
		// Expose the processed HBA configuration through the debug
		// interface for inspection during troubleshooting.
		mux.HandleFunc("/debug/hba_conf", hbaConfDebugFn)
	}

	// Register the stopper endpoint, which lists all active tasks.
	mux.HandleFunc("/debug/stopper", stop.HandleDebug)

	// Set up the log spy, a tool that allows inspecting filtered logs at high
	// verbosity.
	spy := logSpy{
		setIntercept: log.Intercept,
	}
	mux.HandleFunc("/debug/logspy", spy.handleDebugLogSpy)

	ps := pprofui.NewServer(pprofui.NewMemStorage(1, 0), func(profile string, labels bool, do func()) {
		tBegin := timeutil.Now()

		extra := ""
		if profile == "profile" && labels {
			extra = " (enabling profiler labels)"
			st.SetCPUProfiling(true)
			defer st.SetCPUProfiling(false)
		}
		log.Infof(context.Background(), "pprofui: recording %s%s", profile, extra)

		do()

		log.Infof(context.Background(), "pprofui: recorded %s in %.2fs", profile, timeutil.Since(tBegin).Seconds())
	})
	mux.Handle("/debug/pprof/ui/", http.StripPrefix("/debug/pprof/ui", ps))

	mux.HandleFunc("/debug/pprof/goroutineui/", func(w http.ResponseWriter, req *http.Request) {
		dump := goroutineui.NewDump(timeutil.Now())

		_ = req.ParseForm()
		switch req.Form.Get("sort") {
		case "count":
			dump.SortCountDesc()
		case "wait":
			dump.SortWaitDesc()
		default:
		}
		_ = dump.HTML(w)
	})

	mux.HandleFunc("/debug/threads", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Add("Content-type", "text/plain")
		fmt.Fprint(w, storage.ThreadStacks())
	})

	return &Server{
		st:  st,
		mux: mux,
		spy: spy,
	}
}

// ServeHTTP serves various tools under the /debug endpoint. It restricts access
// according to the `server.remote_debugging.mode` cluster variable.
func (ds *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if authed := ds.authRequest(r); !authed {
		http.Error(w, "not allowed (due to the 'server.remote_debugging.mode' setting)",
			http.StatusForbidden)
		return
	}

	handler, _ := ds.mux.Handler(r)
	handler.ServeHTTP(w, r)
}

// authRequest restricts access to /debug/*.
func (ds *Server) authRequest(r *http.Request) bool {
	return authRequest(r.RemoteAddr, ds.st)
}

// authRequest restricts access according to the DebugRemote setting.
func authRequest(remoteAddr string, st *cluster.Settings) bool {
	switch RemoteMode(strings.ToLower(DebugRemote.Get(&st.SV))) {
	case RemoteAny:
		return true
	case RemoteLocal:
		return isLocalhost(remoteAddr)
	default:
		return false
	}
}

// isLocalhost returns true if the remoteAddr represents a client talking to
// us via localhost.
func isLocalhost(remoteAddr string) bool {
	// RemoteAddr is commonly in the form "IP" or "IP:port".
	// If it is in the form "IP:port", split off the port.
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}

// GatewayRemoteAllowed returns whether a request that has been passed through
// the grpc gateway should be allowed accessed to privileged debugging
// information. Because this function assumes the presence of a context field
// populated by the grpc gateway, it's not applicable for other uses.
func GatewayRemoteAllowed(ctx context.Context, st *cluster.Settings) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		// This should only happen for direct grpc connections, which are allowed.
		return true
	}
	peerAddr, ok := md["x-forwarded-for"]
	if !ok || len(peerAddr) == 0 {
		// This should only happen for direct grpc connections, which are allowed.
		return true
	}

	return authRequest(peerAddr[0], st)
}

func handleLanding(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != Endpoint {
		http.Redirect(w, r, Endpoint, http.StatusMovedPermanently)
		return
	}

	// The explicit header is necessary or (at least Chrome) will try to
	// download a gzipped file (Content-type comes back application/x-gzip).
	w.Header().Add("Content-type", "text/html")

	fmt.Fprint(w, `
<html>
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="1; url=/#/debug">
<script type="text/javascript">
	window.location.href = "/#/debug"
</script>
<title>Page Redirection</title>
</head>
<body>
This page has moved.
If you are not redirected automatically, follow this <a href='/#/debug'>link</a>.
</body>
</html>
`)
}
