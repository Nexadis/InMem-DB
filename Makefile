BINDIR=bin
NAME=inmem

build:
	go build -o ${BINDIR}/server ./cmd/server/main.go
	go build -o ${BINDIR}/client ./cmd/client/main.go

test:
	go test -race -v -count=1 -cover ./...

run-server:
	CONFIG_FILE="configs/local.yaml" go run ./cmd/server/main.go

run-client:
	go run ./cmd/client/main.go

