package wal

import (
	"bytes"
	"testing"

	"inmem-db/internal/domain/command"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeSegment(t *testing.T) {
	t.Parallel()
	commands := []command.Command{
		{
			Type: command.CommandSET,
			Name: "test_name1",
			Set: command.SetArgs{
				Value: "test_value1",
			},
		},
		{
			Type: command.CommandDEL,
			Name: "test_name2",
		},
	}
	segment := newSegment(ID(123), commands)
	buf := bytes.Buffer{}
	err := EncodeSegment(&buf, segment)
	require.NoError(t, err)
	gotSegment, err := DecodeSegment(&buf)
	require.NoError(t, err)
	assert.Equal(t, segment, gotSegment)
}
