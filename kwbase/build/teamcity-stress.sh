#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

run export BUILDER_HIDE_GOPATH_SRC=1
run mkdir -p artifacts
definitely_ccache

env=(
  "KWBASE_NIGHTLY_STRESS=true"
  "PKG=$PKG"
  "GOFLAGS=${GOFLAGS:-}"
  "TAGS=${TAGS:-}"
  "STRESSFLAGS=${STRESSFLAGS:-}"
  "TZ=America/New_York"
)

build/builder.sh env "${env[@]}" bash <<'EOF'
set -euxo pipefail

# We've set pipefail, so the exit status is going to come from stress if there
# are test failures.
# Use an `if` so that the `-e` option doesn't stop the script on error.
if ! stdbuf -oL -eL \
  make stress PKG="$PKG" TESTTIMEOUT=40m GOFLAGS="$GOFLAGS" TAGS="$TAGS" STRESSFLAGS="-maxruns 100 -maxtime 1h -maxfails 1 -stderr $STRESSFLAGS" 2>&1 \
  | tee artifacts/stress.log; then
  exit_status=${PIPESTATUS[0]}
  exit $exit_status
fi

EOF
