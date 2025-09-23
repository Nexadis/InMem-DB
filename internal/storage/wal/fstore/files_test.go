package fstore

import (
	"testing"
	"time"

	"inmem-db/internal/config"
	"inmem-db/internal/domain/command"

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

	cmds := []command.Command{
		{
			Type: command.CommandSET,
			Name: "name",
			Set:  command.SetArgs{Value: "value"},
		},
		{
			Type: command.CommandSET,
			Name: "name1",
			Set:  command.SetArgs{Value: "value1"},
		},
		{
			Type: command.CommandDEL,
			Name: "name",
		},
	}

	err = s.WriteCommands(cmds)
	require.NoError(t, err)
	err = s.Close()
	require.NoError(t, err)

	s, err = New(cfg)
	require.NoError(t, err)
	loadedCmds, err := s.LoadFiles()
	require.NoError(t, err)

	assert.Equal(t, cmds, loadedCmds)
}
