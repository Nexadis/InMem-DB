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
	"inmem-db/internal/storage"
	"inmem-db/internal/storage/engine"
	"inmem-db/internal/storage/wal"

	"golang.org/x/sync/errgroup"
)

type App struct {
	server  *tcp.Server
	storage *storage.Storage

	beforeStart func(ctx context.Context) error
	cleanup     func()
}

func New(cfg config.Server) (App, error) {
	err := initLog(cfg.Logging)
	if err != nil {
		return App{}, err
	}

	a := App{}
	p := parser.Parser{}
	e := engine.New()

	factory := cli.NewFactory(p, e)

	if cfg.Wal != nil {
		w, err := wal.New(*cfg.Wal)
		if err != nil {
			return App{}, fmt.Errorf("new wal: %w", err)
		}

		s := newStorage(e, w, cfg.Replication)
		factory = cli.NewFactory(p, s)

		a.beforeStart = func(ctx context.Context) error {
			return s.Restore(ctx)
		}

		a.cleanup = func() {
			w.Close()
		}

		a.storage = s
	}

	server := tcp.NewServer(cfg.Network, factoryAdapter(factory))
	a.server = server

	return a, nil
}

func (a *App) Start(ctx context.Context) error {
	if a.beforeStart != nil {
		err := a.beforeStart(ctx)
		if err != nil {
			return err
		}
	}
	defer a.cleanup()

	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		return a.server.Start(ctx)
	})
	if a.storage != nil {
		grp.Go(func() error {
			return a.storage.Start(ctx)
		})
	}

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

func newStorage(e *engine.Engine, w *wal.WAL, cfg *config.Replication) *storage.Storage {
	if cfg != nil {
		switch cfg.ReplicaType {

		case config.MasterReplica:
			server := storage.NewMasterServer(cfg.MasterAddress, w)
			return storage.New(e, w, storage.WithMasterServer(server))

		case config.SlaveReplica:
			client := storage.NewReplicationClient(*cfg, w, e)
			return storage.New(e, w, storage.WithReplicationClient(client))
		}
	}

	return storage.New(e, w)
}
