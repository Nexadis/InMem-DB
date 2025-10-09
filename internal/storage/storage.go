package storage

import (
	"context"
	"fmt"
	"log/slog"

	"inmem-db/internal/domain/command"
	"inmem-db/internal/server/tcp"

	"golang.org/x/sync/errgroup"
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

	isSlave bool
	client  *replicationClient
	server  *tcp.Server
}

func New(e Engine, w WAL, options ...option) *Storage {
	s := Storage{
		e: e,
		w: w,
	}

	for _, o := range options {
		o(&s)
	}

	return &s
}

// Do оборачивает engine для записи в engine и wal
func (s *Storage) Do(ctx context.Context, cmd command.Command) (string, error) {
	if cmd.Type != command.CommandGET {
		if s.isSlave {
			return "", ErrReadOnly
		}

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

func (s *Storage) Start(ctx context.Context) error {
	grp, ctx := errgroup.WithContext(ctx)

	if s.isSlave {
		slog.InfoContext(ctx, "storage is slave")
		grp.Go(func() error {
			err := s.client.Start(ctx)
			slog.InfoContext(ctx, "close connection to master")
			return err
		})
	}

	if s.server != nil {
		grp.Go(func() error {
			err := s.server.Start(ctx)
			slog.InfoContext(ctx, "close master server")
			return err
		})
	}
	return grp.Wait()
}
