#!/bin/bash

compile_protos() {
  SRC_DIR=proto
  DST_DIR=proto

  rm ./proto/client.pb.go ./proto/client_grpc.pb.go ./proto/general.pb.go ./proto/pbft.pb.go ./proto/pbft_grpc.pb.go ./proto/view_change.pb.go ./proto/view_change_grpc.pb.go || true

  protoc -I="$SRC_DIR" --go_out="$DST_DIR" --go_opt=paths=source_relative \
    --go-grpc_out="$DST_DIR" --go-grpc_opt=paths=source_relative "$SRC_DIR"/*.proto --experimental_allow_proto3_optional
}

compile_protos
