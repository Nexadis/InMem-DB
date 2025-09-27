package wal

import (
	"context"
	"fmt"
	"log/slog"

	"inmem-db/internal/config"
	"inmem-db/internal/domain/command"
	"inmem-db/internal/storage/wal/fstore"
	"inmem-db/pkg/concurrent"
)

type WAL struct {
	cfg config.WAL

	store *fstore.FStore
	batch *concurrent.Batch[command.Command]
}

func New(cfg config.WAL) (*WAL, error) {
	store, err := fstore.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("new store: %w", err)
	}

	w := WAL{
		cfg:   cfg,
		store: store,
	}
	w.batch = concurrent.NewBatch(
		int(cfg.BatchSize),
		cfg.BatchTimeout,
		w.writeBatch)

	return &w, nil
}

func (w *WAL) Save(ctx context.Context, cmd command.Command) error {
	f := w.batch.Add(ctx, cmd)
	return f.Get()
}

func (w *WAL) writeBatch(batch []command.Command) error {
	data, err := encodeCommands(batch)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	_, err = w.store.Write(data)
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}

func (w *WAL) Load(ctx context.Context) ([]command.Command, error) {
	data, err := w.store.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("load files: %w", err)
	}

	cmds, err := decodeCommands(data)
	if err != nil {
		return nil, fmt.Errorf("decode commands: %w", err)
	}

	slog.Debug("wal loaded")
	return cmds, nil
}

func (w *WAL) Close() {
	w.batch.Close()
}
