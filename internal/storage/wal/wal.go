package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"inmem-db/internal/config"
	"inmem-db/internal/domain/command"
	"inmem-db/internal/storage/wal/fstore"
	"inmem-db/pkg/concurrent"
)

type WAL struct {
	cfg config.WAL

	mu       sync.RWMutex
	segments map[ID]Segment
	maxID    ID

	store *fstore.FStore
	batch *concurrent.Batch[command.Command]
}

func New(cfg config.WAL) (*WAL, error) {
	store, err := fstore.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("new store: %w", err)
	}

	w := WAL{
		cfg:      cfg,
		store:    store,
		segments: make(map[ID]Segment, 10),
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
	if len(batch) == 0 {
		return nil
	}

	segment := w.makeSegment(batch)
	err := w.SaveSegment(segment)
	if err != nil {
		return fmt.Errorf("save segment: %w", err)
	}
	return nil
}

func (w *WAL) Load(ctx context.Context) ([]command.Command, error) {
	data, err := w.store.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("load files: %w", err)
	}

	segments, err := decodeSegments(data)
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	commands := make([]command.Command, 0, len(segments)*10)
	for _, segment := range segments {
		commands = append(commands, segment.commands...)
		w.addSegment(segment)
	}

	slog.Debug("wal loaded")
	return commands, nil
}

func (w *WAL) Close() {
	w.batch.Close()
}

func decodeSegments(data []byte) ([]Segment, error) {
	buf := bytes.NewBuffer(data)

	segments := make([]Segment, 0, 100)
	for {
		segment, err := DecodeSegment(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return segments, nil
			}
			return nil, fmt.Errorf("decode cmd: %w", err)
		}
		segments = append(segments, segment)
	}
}
