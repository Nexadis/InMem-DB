package app

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"inmem-db/internal/compute/parser"
	"inmem-db/internal/config"
	"inmem-db/internal/server/cli"
	"inmem-db/internal/server/tcp"
	"inmem-db/internal/storage/engine"
	"inmem-db/internal/storage/wal"

	"golang.org/x/sync/errgroup"
)

type App struct {
	server *tcp.Server
	wal    *wal.WAL
}

func New(cfg config.Server) (App, error) {
	err := initLog(cfg.Logging)
	if err != nil {
		return App{}, err
	}
	a := App{}

	p := parser.Parser{}
	s := engine.New()
	factory := cli.NewFactory(p, s)

	if cfg.Wal != nil {
		w, err := wal.New(*cfg.Wal, s)
		if err != nil {
			return App{}, fmt.Errorf("new wal: %w", err)
		}
		a.wal = w

		factory = cli.NewFactory(p, w)
	}

	server := tcp.NewServer(cfg.Network, factoryAdapter(factory))
	a.server = server

	return a, nil
}

func (a *App) Start(ctx context.Context) error {
	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		if a.wal == nil {
			return nil
		}
		a.wal.Start(ctx)
		return nil
	})
	grp.Go(func() error {
		return a.server.Start(ctx)
	})

	return grp.Wait()
}

func initLog(logConfig config.Logging) error {
	level := slog.LevelInfo
	switch logConfig.Level {
	case config.LevelDebug:
		level = slog.LevelDebug
	case config.LevelInfo:
		level = slog.LevelInfo
	case config.LevelError:
		level = slog.LevelError
	}

	w, err := os.Create(logConfig.Output)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	h := slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: level,
	})

	l := slog.New(h)
	slog.SetDefault(l)
	return nil
}

func factoryAdapter(f cli.Factory) tcp.HandlerFactory {
	return func(r io.Reader, w io.Writer) tcp.Starter {
		return f(r, w)
	}
}
