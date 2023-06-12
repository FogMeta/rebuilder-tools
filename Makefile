build:
	go build -ldflags "-s -w" -o rebuildctl ./cmd/rebuilder/main.go
.PHONY: build

## FFI

ffi: 
	./extern/filecoin-ffi/install-filcrypto
.PHONY: ffi

test:
	go test -v ./...
.PHONY: test
