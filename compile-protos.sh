#!/bin/bash

compile_protos() {
  SRC_DIR=proto
  DST_DIR=proto

  protoc -I="$SRC_DIR" --go_out="$DST_DIR" --go_opt=paths=source_relative \
    --go-grpc_out="$DST_DIR" --go-grpc_opt=paths=source_relative "$SRC_DIR"/*.proto --experimental_allow_proto3_optional
}

compile_protos
