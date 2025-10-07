package storage

import "inmem-db/internal/server/tcp"

type option func(*Storage)

func WithReplicationClient(client *replicationClient) option {
	return func(s *Storage) {
		s.isSlave = true
		s.client = client
	}
}

func WithMasterServer(masterServer *tcp.Server) option {
	return func(s *Storage) {
		s.isSlave = false
		s.server = masterServer
	}
}
