BINDIR=bin
NAME=inmem

build:
	go build -o ${BINDIR}/server ./cmd/server/main.go
	go build -o ${BINDIR}/client ./cmd/client/main.go

test:
	go test -race -v -count=1 -cover ./...

run-master:
	CONFIG_FILE="configs/master.yaml" go run ./cmd/server/main.go

run-slave:
	CONFIG_FILE="configs/slave.yaml" go run ./cmd/server/main.go

run-client:
	go run ./cmd/client/main.go

run-client-slave:
	go run ./cmd/client/main.go -address localhost:3224

