package wal

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"inmem-db/internal/compute/parser"
	"inmem-db/internal/config"
	"inmem-db/internal/domain/command"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWAL_WithManyWorkers(t *testing.T) {
	t.Parallel()
	cfg := config.WAL{
		BatchSize:      100,
		BatchTimeout:   time.Millisecond * 100,
		MaxSegmentSize: "10MB",
		DataDir:        t.TempDir(),
	}

	w, err := New(cfg)
	require.NoError(t, err)
	t.Log("wal created")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	const workers = 1000

	mu := sync.Mutex{}
	wantCommands := make([]command.Command, 0, workers)

	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := range workers {
		go func() {
			defer wg.Done()
			cmdStr := fmt.Sprintf("SET name%d val%d\r\n", i, i)
			p := parser.Parser{}
			cmd, err := p.Parse(ctx, cmdStr)
			require.NoError(t, err)

			mu.Lock()
			wantCommands = append(wantCommands, cmd)
			mu.Unlock()

			err = w.Save(ctx, cmd)
			require.NoError(t, err)
		}()
	}

	wg.Wait()
	w.Close()

	w, err = New(cfg)
	require.NoError(t, err)

	gotCommands, err := w.Load(ctx)
	require.NoError(t, err)

	assert.ElementsMatch(t, wantCommands, gotCommands)
}
