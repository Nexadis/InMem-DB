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
)

type App struct {
	server *tcp.Server
}

func New(cfg config.Server) (App, error) {
	err := initLog(cfg.Logging)
	if err != nil {
		return App{}, err
	}

	p := parser.Parser{}
	s := engine.New()
	factory := cli.NewFactory(p, s)
	server := tcp.NewServer(cfg.Network, factoryAdapter(factory))

	return App{server: server}, nil
}

func (a *App) Start(ctx context.Context) error {
	return a.server.Start(ctx)
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
