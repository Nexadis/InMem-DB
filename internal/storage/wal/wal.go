package wal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
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
	cnt atomic.Uint32

	workers *semaphore

	jobs    chan job
	results []chan doRes

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
		results: make([]chan doRes, cfg.BatchSize),

		resultGot:   make(chan struct{}),
		resultReady: make(chan struct{}),
		workers:     newSema(int(cfg.BatchSize)),
	}
	for i := range w.results {
		w.results[i] = make(chan doRes)
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

	id := w.cnt.Add(1) - 1
	id = id % uint32(w.cfg.BatchSize)
	j := job{id: id, cmd: cmd}

	var res doRes
	select {
	case <-ctx.Done():
		return "", nil
	case w.jobs <- j:
	}

	select {
	case <-ctx.Done():
		return "", nil
	case res = <-w.results[id]:
	}

	return res.res, res.err
}

// Start в цикле обрабатывает батчи, блокирующаяся функция
func (w *WAL) Start(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.BatchTimeout)
	defer ticker.Stop()

	batch := make([]job, 0, w.cfg.BatchSize)
	commands := make([]command.Command, w.cfg.BatchSize)
	results := make([]doRes, w.cfg.BatchSize)

	recvJobs := func() {
		for {
			select {
			case j := <-w.jobs:
				batch = append(batch, j)
				if len(batch) == int(w.cfg.BatchSize) {
					return
				}
			case <-ticker.C:
				return
			}
		}
	}

	copyCommands := func() {
		for i := range batch {
			commands[i] = batch[i].cmd
		}
	}

	sendResults := func() {
		for i, j := range batch {
			w.results[j.id] <- results[i]
		}
		batch = batch[:0]
	}

	for {
		if ctx.Err() != nil {
			return
		}

		recvJobs()
		copyCommands()
		w.batchDo(ctx, commands, results)
		sendResults()
	}
}

func (w *WAL) batchDo(ctx context.Context, batch []command.Command, results []doRes) {
	if len(batch) == 0 {
		return
	}

	slog.DebugContext(ctx, "wal store on disk", slog.Int("batch_size", len(batch)))
	storeErr := w.store.WriteCommands(batch)

	wg := sync.WaitGroup{}
	wg.Add(len(batch))
	for i, cmd := range batch {
		go func() {
			defer wg.Done()
			r, err := w.e.Do(ctx, cmd)
			res := doRes{
				res: r,
				err: errors.Join(err, storeErr),
			}
			results[i] = res
		}()
	}
	wg.Wait()
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
