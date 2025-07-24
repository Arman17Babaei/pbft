GO_CMD=go
PKG_PATH=github.com/Arman17Babaei/pbft/cmd/load_test

# go run github.com/Arman17Babaei/pbft/cmd/load_test
test:
	go run github.com/Arman17Babaei/pbft/cmd/load_test

up:
	bash compile-protos.sh
