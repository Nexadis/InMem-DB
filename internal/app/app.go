package app

import (
	"context"
	"inmem-db/internal/cli"
	"inmem-db/internal/compute/parser"
	"inmem-db/internal/config"
	"inmem-db/internal/storage/engine"
	"log/slog"
	"os"
)

type App struct {
	cli cli.Cli
}

func New(cfg config.Config) App {
	if cfg.Env == config.EnvDev {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	p := parser.Parser{}
	s := engine.New()

	c := cli.New(os.Stdin, os.Stdout, p, s)
	return App{
		cli: c,
	}
}

func (a *App) Start(ctx context.Context) error {
	return a.cli.Start(ctx)
}
