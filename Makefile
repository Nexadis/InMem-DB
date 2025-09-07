BINDIR=bin
NAME=inmem

build:
	go build -o ${BINDIR}/${NAME} ./cmd/main.go

test:
	go test -cover ./...

run:
	CONFIG_FILE="configs/local.yaml" go run ./cmd/main.go
