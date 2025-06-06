#!/usr/bin/env bash

set -euxo pipefail

source "$(dirname "${0}")/teamcity-support.sh"


if [[ -n "${PUBLISH_LATEST}" && -n "$PRE_RELEASE" ]]; then
  echo "Invalid parameter combination: PUBLISH_LATEST and PRE_RELEASE can't both be set."
  exit 6
fi


tc_start_block "Variable Setup"
export BUILDER_HIDE_GOPATH_SRC=1

# Matching the version name regex from within the kwbase code except
# for the `metadata` part at the end because Docker tags don't support
# `+` in the tag name.
# https://gitee.com/kwbasedb/kwbase/blob/4c6864b44b9044874488cfedee3a31e6b23a6790/pkg/util/version/version.go#L75
build_name="$(echo "${NAME}" | grep -E -o '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[-.0-9A-Za-z]+)?$')"
#                                         ^major           ^minor           ^patch         ^preRelease

if [[ -z "$build_name" ]] ; then
    echo "Invalid NAME \"${NAME}\". Must be of the format \"vMAJOR.MINOR.PATCH(-PRERELEASE)?\"."
    exit 1
fi

release_branch=$(echo ${build_name} | grep -E -o '^v[0-9]+\.[0-9]+')

if [[ -z "${DRY_RUN}" ]] ; then
  bucket="${BUCKET:-binaries.kwbasedb.com}"
  google_credentials="$GOOGLE_KWBASE_CLOUD_IMAGES_CREDENTIALS"
  if [[ -z "${PRE_RELEASE}" ]] ; then
    dockerhub_repository="docker.io/kwbasedb/kwbase"
  else
    dockerhub_repository="docker.io/kwbasedb/kwbase-unstable"
  fi
  gcr_repository="us.gcr.io/kwbase-cloud-images/kwbase"
  s3_download_hostname="${bucket}"
  git_repo_for_tag="kwbasedb/kwbase"
else
  bucket="${BUCKET:-kwbase-builds-test}"
  google_credentials="$GOOGLE_KWBASE_RELEASE_CREDENTIALS"
  dockerhub_repository="docker.io/kwbasedb/kwbase-misc"
  gcr_repository="us.gcr.io/kwbase-release/kwbase-test"
  s3_download_hostname="${bucket}.s3.amazonaws.com"
  git_repo_for_tag="cockroachlabs/release-staging"
  if [[ -z "$(echo ${build_name} | grep -E -o '^v[0-9]+\.[0-9]+\.[0-9]+$')" ]] ; then
    # Using `.` to match how we usually format the pre-release portion of the
    # version string using '.' separators.
    # ex: v20.2.0-rc.2.dryrun
    build_name="${build_name}.dryrun"
  else
    # Using `-` to put dryrun in the pre-release portion of the version string.
    # ex: v20.2.0-dryrun
    build_name="${build_name}-dryrun"
  fi
fi

# Used for docker login for gcloud
gcr_hostname="us.gcr.io"

tc_end_block "Variable Setup"


tc_start_block "Tag the release"
git tag "${build_name}"
tc_end_block "Tag the release"


tc_start_block "Compile publish-provisional-artifacts"
build/builder.sh go install ./pkg/cmd/publish-provisional-artifacts
tc_end_block "Compile publish-provisional-artifacts"


tc_start_block "Make and publish release S3 artifacts"
# Using publish-provisional-artifacts here is funky. We're directly publishing
# the official binaries, not provisional ones. Legacy naming. To clean up...
build/builder.sh env \
  AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  TC_BUILD_BRANCH="$build_name" \
  publish-provisional-artifacts -provisional -release -bucket "$bucket"
tc_end_block "Make and publish release S3 artifacts"


tc_start_block "Make and push docker images"
configure_docker_creds
docker_login_with_google
docker_login

# TODO: update publish-provisional-artifacts with option to leave one or more kwbase binaries in the local filesystem?
curl -f -s -S -o- "https://${s3_download_hostname}/kwbase-${build_name}.linux-amd64.tgz" | tar ixfz - --strip-components 1
cp kwbase build/deploy

docker build --no-cache --tag=${dockerhub_repository}:{"$build_name",latest,latest-"${release_branch}"} --tag=${gcr_repository}:${build_name} build/deploy

docker push "${dockerhub_repository}:${build_name}"
docker push "${gcr_repository}:${build_name}"
tc_end_block "Make and push docker images"


tc_start_block "Push release tag to GitHub"
github_ssh_key="${GITHUB_KWBASE_TEAMCITY_PRIVATE_SSH_KEY}"
configure_git_ssh_key
push_to_git "ssh://git@github.com/${git_repo_for_tag}.git" "$build_name"
tc_end_block "Push release tag to GitHub"


tc_start_block "Publish S3 binaries and archive as latest-RELEASE_BRANCH"
# example: v20.1-latest
if [[ -z "$PRE_RELEASE" ]]; then
  #TODO: implement me!
  echo "Pushing latest-RELEASE_BRANCH S3 binaries and archive is not implemented."
else
  echo "Pushing latest-RELEASE_BRANCH S3 binaries and archive is not implemented."
fi
tc_end_block "Publish S3 binaries and archive as latest-RELEASE_BRANCH"


tc_start_block "Publish S3 binaries and archive as latest"
# Only push the "latest" for our most recent release branch.
# https://gitee.com/kwbasedb/kwbase/issues/41067
if [[ -n "${PUBLISH_LATEST}" && -z "${PRE_RELEASE}" ]]; then
  build/builder.sh env \
    AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    TC_BUILD_BRANCH="$build_name" \
    publish-provisional-artifacts -bless -release -bucket "${bucket}"
else
  echo "The latest S3 binaries and archive were _not_ updated."
fi
tc_end_block "Publish S3 binaries and archive as latest"


tc_start_block "Tag docker image as latest-RELEASE_BRANCH"
if [[ -z "$PRE_RELEASE" ]]; then
  docker push "${dockerhub_repository}:latest-${release_branch}"
else
  echo "The ${dockerhub_repository}:latest-${release_branch} docker image tag was _not_ pushed."
fi
tc_end_block "Tag docker image as latest-RELEASE_BRANCH"


tc_start_block "Tag docker image as latest"
# Only push the "latest" tag for our most recent release branch.
# https://gitee.com/kwbasedb/kwbase/issues/41067
if [[ -n "${PUBLISH_LATEST}" && -z "$PRE_RELEASE" ]]; then
  docker push "${dockerhub_repository}:latest"
else
  echo "The ${dockerhub_repository}:latest docker image tag was _not_ pushed."
fi
tc_end_block "Tag docker image as latest"
