#!/bin/bash
# Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
#
# This software (KWDB) is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

#! /bin/bash
set -euo pipefail

export cur_path="$(pwd)"
echo "${cur_path}"

curdate="$(date +%Y-%m-%d,%H:%M:%S)"

export TSBS_PATH="${QA_DIR}/tsbs_test/bin"
arch="$(uname -m)"
tsbs_branch="${1:-master}"
tsbs_repo_url="${TSBS_REPO_URL:-https://gitee.com/kwdb/kwdb-tsbs.git}"
tsbs_repo_dir="${TSBS_REPO_DIR:-${GOPATH:-}/src/github.com/kwdb-tsbs}"
tsbs_repo_fallback_dir="${tsbs_repo_dir}_auto_${tsbs_branch}"
tsbs_commit_file="${TSBS_PATH}/TSBS_COMMIT_${arch}"

required_binaries=(
  "tsbs_generate_data"
  "tsbs_generate_queries"
  "tsbs_load_kwdb"
  "tsbs_run_queries_kwdb"
)

log() {
  echo "[tsbs_setup] $*"
}

die() {
  echo "[tsbs_setup] ERROR: $*" >&2
  exit 1
}

require_gopath() {
  if [[ -z "${GOPATH:-}" ]]; then
    die "GOPATH is not set"
  fi
}

cleanup_kwbase_processes() {
  log "pkill existing kwbase processes before test setup"
  pkill -9 kwbase || true
}

ensure_repo_exists() {
  if [[ -d "${tsbs_repo_dir}/.git" ]]; then
    return
  fi

  log "local tsbs repo not found, cloning ${tsbs_repo_url} (${tsbs_branch})"
  mkdir -p "$(dirname "${tsbs_repo_dir}")"
  git clone "${tsbs_repo_url}" -b "${tsbs_branch}" "${tsbs_repo_dir}"
}

ensure_clean_repo_dir() {
  if [[ ! -d "${tsbs_repo_dir}/.git" ]]; then
    return
  fi

  if [[ -n "$(git -C "${tsbs_repo_dir}" status --short 2>/dev/null)" ]]; then
    log "detected local changes in ${tsbs_repo_dir}, use clean repo ${tsbs_repo_fallback_dir} for auto build"
    tsbs_repo_dir="${tsbs_repo_fallback_dir}"
    ensure_repo_exists
  fi
}

binary_available() {
  local binary_path
  for binary in "${required_binaries[@]}"; do
    binary_path="${TSBS_PATH}/${binary}_${arch}"
    if [[ ! -f "${binary_path}" ]]; then
      log "binary ${binary_path} not found"
      return 1
    fi
    if [[ ! -x "${binary_path}" ]]; then
      log "binary ${binary_path} is not executable"
      return 1
    fi
  done
  return 0
}

get_remote_commit() {
  git ls-remote --heads "${tsbs_repo_url}" "${tsbs_branch}" | awk '{print $1}'
}

sync_repo_to_latest() {
  local remote_commit="$1"

  cd "${tsbs_repo_dir}"
  git fetch origin "${tsbs_branch}"

  local current_branch
  current_branch="$(git rev-parse --abbrev-ref HEAD)"
  if [[ "${current_branch}" != "${tsbs_branch}" ]]; then
    log "switch tsbs branch from ${current_branch} to ${tsbs_branch}"
    git checkout "${tsbs_branch}"
  fi

  local local_commit
  local_commit="$(git rev-parse HEAD)"
  if [[ "${local_commit}" != "${remote_commit}" ]]; then
    log "update tsbs repo from ${local_commit} to ${remote_commit}"
    git reset --hard "${remote_commit}"
  else
    log "tsbs repo is already at latest commit ${local_commit}"
  fi
}

compiled_commit_matches() {
  if [[ ! -f "${tsbs_commit_file}" ]]; then
    return 1
  fi

  local expected_commit="$1"
  local compiled_commit
  compiled_commit="$(cat "${tsbs_commit_file}")"
  [[ "${compiled_commit}" == "${expected_commit}" ]]
}

compile_and_install() {
  cd "${tsbs_repo_dir}"
  log "compile tsbs binaries on branch ${tsbs_branch}"
  make

  mkdir -p "${TSBS_PATH}"
  log "copy binaries to ${TSBS_PATH}"
  for filename in "${required_binaries[@]}"; do
    local binary="${tsbs_repo_dir}/bin/${filename}"
    cp "${binary}" "${TSBS_PATH}/${filename}_${arch}"
    chmod +x "${TSBS_PATH}/${filename}_${arch}"
    log "copy ${filename} to ${filename}_${arch}"
  done

  git rev-parse HEAD > "${tsbs_commit_file}"
}

require_gopath
cleanup_kwbase_processes
ensure_repo_exists
ensure_clean_repo_dir

remote_commit="$(get_remote_commit)"
if [[ -z "${remote_commit}" ]]; then
  die "failed to fetch remote commit for ${tsbs_repo_url} branch ${tsbs_branch}"
fi

sync_repo_to_latest "${remote_commit}"

if binary_available && compiled_commit_matches "${remote_commit}"; then
  log "all required binaries exist and match latest commit ${remote_commit}"
else
  log "recompile tsbs binaries for latest commit ${remote_commit}"
  compile_and_install
fi

echo "TSBS_PATH: ${TSBS_PATH}"
exit 0
