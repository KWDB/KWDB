#!/usr/bin/env bash
# regen_proto.sh — regenerate mlagent gRPC stubs from *.proto.
#
# This script uses the modern protoc plugins (protoc-gen-go +
# protoc-gen-go-grpc) and targets google.golang.org/protobuf (newer
# API) and google.golang.org/grpc (1.21.2, vendored in kwdb).
#
# Run from kwbase/pkg/sql/mlagent/ (the directory that contains
# serverpb/).
#
# Requires:
#   - protoc 3.6+                (/usr/bin/protoc)
#   - protoc-gen-go v1.34+       (/usr/local/bin/protoc-gen-go)
#   - protoc-gen-go-grpc v1.2.0  (build into /tmp/protoc-gen-go-grpc)

set -euo pipefail

# Build a compatible older protoc-gen-go-grpc plugin
build_plugin() {
    if [ ! -x /tmp/protoc-gen-go-grpc ]; then
        echo "Building protoc-gen-go-grpc v1.2.0..."
        local builddir=/tmp/proto-grpc-build
        rm -rf $builddir
        mkdir -p $builddir
        cp -r /root/go/pkg/mod/google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0/* $builddir/
        cat > $builddir/go.mod <<'EOF'
module protocgen

go 1.18

require google.golang.org/protobuf v1.27.1
EOF
        cd $builddir
        rm -rf vendor
        go mod tidy
        go build -o /tmp/protoc-gen-go-grpc .
    fi
}

build_plugin
export PATH=/tmp:$PATH

cd "$(dirname "$0")"

PROTO_DIR=serverpb
KWDB_ROOT=/home/inspur/src/gitee.com/kwdb
KWDB_BASE=$KWDB_ROOT/kwbase
KWBASE_VENDOR=/home/inspur/src/gitee.com/kwbasedb/kwbase/vendor

# Use -I to point to the kwdb pkg/ root (where proto files live).
# paths=import means the output is placed under the import-relative
# path (sql/mlagent/serverpb/), which we then move to pkg/.

rm -f $PROTO_DIR/*.pb.go

protoc -I$KWDB_BASE/pkg \
    -I$KWBASE_VENDOR \
    --go_out=$KWDB_BASE --go_opt=paths=import \
    --go-grpc_out=$KWDB_BASE --go-grpc_opt=paths=import \
    $PROTO_DIR/common.proto \
    $PROTO_DIR/pipeline.proto \
    $PROTO_DIR/isvc.proto \
    $PROTO_DIR/mlagent.proto \
    $PROTO_DIR/ml_error.proto \
    $PROTO_DIR/predict.proto

# protoc-gen-go writes to <output>/<import-relative-path>/ which is
# sql/mlagent/serverpb/. Move them next to the .proto files.
if [ -d $KWDB_BASE/sql/mlagent/serverpb ]; then
    mv $KWDB_BASE/sql/mlagent/serverpb/*.go $PROTO_DIR/
    rm -rf $KWDB_BASE/sql
fi

# Re-write the package name in the generated files. protoc-gen-go
# sometimes emits a fully qualified import path in the header comment;
# we leave that as is. The hand-written grpc_clients.go provides
# MLAgentClient / IsvcServiceClient / KPredictionServiceClient, so
# the *_grpc.pb.go files generated here are not strictly needed for
# the build (the hand-written wrappers are compatible with the vendored
# google.golang.org/grpc 1.21.2). We keep them for source-of-truth
# parity with the .proto definitions.

# Re-format
gofmt -s -w $PROTO_DIR/*.go

echo "OK: regenerated $(ls $PROTO_DIR/*.pb.go | wc -l) .pb.go files in $PROTO_DIR/"
echo "    $(ls $PROTO_DIR/*_grpc.pb.go 2>/dev/null | wc -l) _grpc.pb.go files (may not be used at runtime)"
