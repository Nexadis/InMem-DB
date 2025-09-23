package wal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"inmem-db/internal/config"
	"inmem-db/internal/domain/command"
	"inmem-db/internal/storage/wal/fstore"
)

type job struct {
	id  uint32
	cmd command.Command
}

type doRes struct {
	res string
	err error
}

type WAL struct {
	cfg config.WAL
	e   Engine

	mu  sync.Mutex
	cnt uint32

	workers *semaphore

	jobs    chan job
	results []doRes

	resultGot   chan struct{}
	resultReady chan struct{}

	store *fstore.FStore
}

type Engine interface {
	Do(ctx context.Context, cmd command.Command) (string, error)
}

func New(cfg config.WAL, e Engine) (*WAL, error) {
	store, err := fstore.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("new store: %w", err)
	}

	w := WAL{
		cfg:   cfg,
		e:     e,
		store: store,

		jobs:    make(chan job),
		results: make([]doRes, cfg.BatchSize),

		resultGot:   make(chan struct{}),
		resultReady: make(chan struct{}),
		workers:     newSema(int(cfg.BatchSize)),
	}

	err = w.restore()
	if err != nil {
		return nil, err
	}

	return &w, nil
}

// Do оборачивает engine для записи в engine и на диск
func (w *WAL) Do(ctx context.Context, cmd command.Command) (string, error) {
	defer w.workers.Release()
	w.workers.Acquire()

	w.mu.Lock()
	id := w.cnt
	w.cnt = (w.cnt + 1) % uint32(w.cfg.BatchSize)
	w.mu.Unlock()

	select {
	case <-ctx.Done():
		return "", nil
	case w.jobs <- job{id: id, cmd: cmd}:
	}

	select {
	case <-ctx.Done():
		return "", nil
	case <-w.resultReady:
	}

	res := w.results[id]

	w.resultGot <- struct{}{}

	return res.res, res.err
}

// Start в цикле обрабатывает батчи, блокирующаяся функция
func (w *WAL) Start(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.BatchTimeout)
	defer ticker.Stop()
	batch := make([]job, 0, w.cfg.BatchSize)

loop:
	for {
		select {
		case <-ctx.Done():
			w.store.Close()
			return
		case job := <-w.jobs:

			batch = append(batch, job)
			if len(batch) != int(w.cfg.BatchSize) {
				continue loop
			}
			slog.DebugContext(ctx, "batch full")
		case <-ticker.C:
		}

		w.batchDo(ctx, batch)
		batch = batch[:0]
	}
}

func (w *WAL) batchDo(ctx context.Context, batch []job) {
	if len(batch) == 0 {
		return
	}

	slog.DebugContext(ctx, "wal store on disk", slog.Int("batch_size", len(batch)))
	commands := make([]command.Command, len(batch))
	for i, j := range batch {
		commands[i] = j.cmd
	}
	storeErr := w.store.WriteCommands(commands)

	wg := sync.WaitGroup{}
	wg.Add(len(batch))
	for _, job := range batch {
		go func() {
			defer wg.Done()
			r, err := w.e.Do(ctx, job.cmd)
			res := doRes{
				res: r,
				err: errors.Join(err, storeErr),
			}

			w.results[job.id] = res
		}()
	}
	wg.Wait()

	fill(ctx.Done(), w.resultReady, len(batch))
	wait(ctx.Done(), w.resultGot, len(batch))
}

func (w *WAL) restore() error {
	cmds, err := w.store.LoadFiles()
	if err != nil {
		return fmt.Errorf("load files: %w", err)
	}

	slog.Debug("wal loaded")

	for _, cmd := range cmds {
		_, err := w.e.Do(context.Background(), cmd)
		if err != nil {
			return fmt.Errorf("engine do: %w", err)
		}
	}
	slog.Debug("wal commands applied")

	return nil
}
