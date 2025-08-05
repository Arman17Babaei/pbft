#!/bin/bash
# How to use:
# bash compile-protos.sh deleteAll (delete all generated .pb.go and _grpc.pb.go files)
# bash compile-protos.sh compileAll (compile all .proto files)
# bash compile-protos.sh (execute deleteAll first, and then compileAll)

deleteAll() {
  echo "Deleting all generated .pb.go and _grpc.pb.go files..."
  find ./proto -name "*.pb.go" -type f -delete
  echo "All generated files deleted."
}

compileAll() {
  echo "Compiling all .proto files..."
  SRC_DIR=proto
  DST_DIR=proto

  # Iterate through all .proto files and compile them
  for proto_file in "$SRC_DIR"/*.proto; do
    if [ -f "$proto_file" ]; then
      echo "Compiling: $proto_file"
      protoc -I="$SRC_DIR" --go_out="$DST_DIR" --go_opt=paths=source_relative \
        --go-grpc_out="$DST_DIR" --go-grpc_opt=paths=source_relative "$proto_file" --experimental_allow_proto3_optional
    fi
  done
  echo "All .proto files compiled successfully."
}

# Main function: delete first, then compile
main() {
  deleteAll
  compileAll
}

# Execute corresponding function based on arguments
case "${1:-}" in
  "deleteAll")
    deleteAll
    ;;
  "compileAll")
    compileAll
    ;;
  *)
    # Default: execute full process
    main
    ;;
esac