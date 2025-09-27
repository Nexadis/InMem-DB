package storage

import (
	"context"
	"fmt"

	"inmem-db/internal/domain/command"
)

type Engine interface {
	Do(ctx context.Context, cmd command.Command) (string, error)
}

type WAL interface {
	Save(ctx context.Context, cmd command.Command) error
	Load(ctx context.Context) ([]command.Command, error)
}

type Storage struct {
	e Engine
	w WAL
}

func New(e Engine, w WAL) *Storage {
	s := Storage{
		e: e,
		w: w,
	}

	return &s
}

// Do оборачивает engine для записи в engine и wal
func (s *Storage) Do(ctx context.Context, cmd command.Command) (string, error) {
	if cmd.Type != command.CommandGET {
		err := s.w.Save(ctx, cmd)
		if err != nil {
			return "", fmt.Errorf("wal save: %w", err)
		}
	}

	res, err := s.e.Do(ctx, cmd)
	if err != nil {
		return "", fmt.Errorf("engine do: %w", err)
	}
	return res, nil
}

func (s *Storage) Restore(ctx context.Context) error {
	cmds, err := s.w.Load(ctx)
	if err != nil {
		return fmt.Errorf("load commands: %w", err)
	}
	for _, cmd := range cmds {
		_, err = s.e.Do(ctx, cmd)
		if err != nil {
			return fmt.Errorf("engine do: %w", err)
		}
	}
	return nil
}
