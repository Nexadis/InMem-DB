package storage

import (
	"inmem-db/internal/server/tcp"
)

func NewMasterServer(addr string, segmenter SegmentsGetter) *tcp.Server {
	cfg := tcp.DefaultConfig
	cfg.Address = addr

	server := tcp.NewServer(cfg, senderFactory(segmenter))
	return server
}
