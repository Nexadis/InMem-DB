package wal

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"inmem-db/internal/compute/parser"
	"inmem-db/internal/config"
	"inmem-db/internal/storage/engine"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWAL_WithManyWorkers(t *testing.T) {
	t.Parallel()
	e := engine.New()

	cfg := config.WAL{
		BatchSize:      100,
		BatchTimeout:   time.Millisecond * 100,
		MaxSegmentSize: "10MB",
		DataDir:        t.TempDir(),
	}

	w, err := New(cfg, e)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	go w.Start(ctx)

	const workers = 5000

	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := range workers {
		go func() {
			defer wg.Done()
			cmdStr := fmt.Sprintf("SET name%d val%d\r\n", i, i)
			p := parser.Parser{}
			cmd, err := p.Parse(ctx, cmdStr)
			require.NoError(t, err)

			_, err = w.Do(ctx, cmd)
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	wg.Add(workers)
	for i := range workers {
		go func() {
			defer wg.Done()
			cmdStr := fmt.Sprintf("GET name%d", i)
			p := parser.Parser{}
			cmd, err := p.Parse(ctx, cmdStr)
			require.NoError(t, err)

			ans, err := w.Do(ctx, cmd)
			require.NoError(t, err)
			assert.Contains(t, ans, fmt.Sprintf("val%d", i))
		}()
	}
	wg.Wait()
}
