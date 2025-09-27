package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"inmem-db/internal/compute/parser"
	"inmem-db/internal/config"
	"inmem-db/internal/storage/engine"
	"inmem-db/internal/storage/wal"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDo_concurrently(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	const workers = 1000

	e := engine.New()

	cfg := config.WAL{
		BatchSize:      100,
		BatchTimeout:   time.Millisecond,
		MaxSegmentSize: "10B",
		DataDir:        t.TempDir(),
	}
	w, err := wal.New(cfg)
	require.NoError(t, err)

	s := New(e, w)
	wg := sync.WaitGroup{}
	wg.Add(workers)

	for i := range workers {
		go func() {
			defer wg.Done()
			cmdStr := fmt.Sprintf("SET name%d val%d\r\n", i, i)
			p := parser.Parser{}
			cmd, err := p.Parse(ctx, cmdStr)
			require.NoError(t, err)

			_, err = s.Do(ctx, cmd)
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	w.Close()

	e = engine.New()
	w, err = wal.New(cfg)
	require.NoError(t, err)

	s = New(e, w)
	s.Restore(ctx)

	wg = sync.WaitGroup{}
	wg.Add(workers)

	for i := range workers {
		go func() {
			defer wg.Done()
			cmdStr := fmt.Sprintf("GET name%d", i)
			p := parser.Parser{}
			cmd, err := p.Parse(ctx, cmdStr)
			require.NoError(t, err)

			got, err := s.Do(ctx, cmd)
			require.NoError(t, err)

			assert.Contains(t, got, fmt.Sprintf("val%d", i))
		}()
	}
	wg.Wait()
	w.Close()
}
