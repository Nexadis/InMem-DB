package storage

import (
	"inmem-db/internal/config"
)

type option func(*Storage)

func WithMasterConnect(cfg config.Replication, wal segmentManager, e Engine) option {
	return func(s *Storage) {
		s.isSlave = true

		client := newReplicationClient(cfg, wal, e)
		s.masterConnect = client
	}
}

func WithMasterServer(addr string, wal SegmentsGetter) option {
	return func(s *Storage) {
		s.server = newMasterServer(addr, wal)
	}
}
