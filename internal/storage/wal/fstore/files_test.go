package fstore

import (
	"crypto/rand"
	"testing"
	"time"

	"inmem-db/internal/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFiles(t *testing.T) {
	t.Parallel()

	cfg := config.WAL{
		BatchSize:      100,
		BatchTimeout:   time.Millisecond * 5,
		MaxSegmentSize: "10B",
		DataDir:        t.TempDir(),
	}

	s, err := New(cfg)
	require.NoError(t, err)

	data := make([]byte, 1000)
	_, err = rand.Read(data)
	require.NoError(t, err)

	_, err = s.Write(data)
	require.NoError(t, err)
	err = s.Close()
	require.NoError(t, err)

	s, err = New(cfg)
	require.NoError(t, err)
	readData, err := s.ReadAll()
	require.NoError(t, err)
	err = s.Close()
	require.NoError(t, err)

	assert.Equal(t, data, readData)
}
