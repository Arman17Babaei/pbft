# PBFT Simple Implementation

## Execution
```
cd proto
protoc --go_out=. --go-grpc_out=. pbft.proto
cd ..
go run cmd/node/main.go --cluster
```

