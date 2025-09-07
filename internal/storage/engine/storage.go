package engine

import (
	"context"
	"errors"
	"sync"
)

var ErrNotFound = errors.New("value not found")

type storage struct {
	mu   sync.RWMutex
	data map[string]string
}

func newStorage() *storage {
	return &storage{
		data: make(map[string]string, 50),
	}
}

func (s *storage) Set(ctx context.Context, name string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[name] = value

	return nil
}

func (s *storage) Get(ctx context.Context, name string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.data[name]
	if !ok {
		return "", ErrNotFound
	}

	return v, nil
}

func (s *storage) Del(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, name)

	return nil
}
