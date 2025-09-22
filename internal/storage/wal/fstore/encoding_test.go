package fstore

import (
	"bytes"
	"testing"

	"inmem-db/internal/domain/command"

	"github.com/stretchr/testify/assert"
)

func TestEncodeCmd(t *testing.T) {
	t.Parallel()

	type test struct {
		cmd       command.Command
		wantBytes []byte
		wantErr   bool
	}

	tests := map[string]test{
		"get encode": {
			cmd:     command.Command{Type: command.CommandGET, Name: "name"},
			wantErr: false,
		},
		"set encode": {
			cmd: command.Command{Type: command.CommandSET, Name: "name", Set: command.SetArgs{
				Value: "value1",
			}},
			wantBytes: []byte{
				CmdType2Byte[string(command.CommandSET)], // тип команды
				0x00, 0x04,                               // размер имени
				'n', 'a', 'm', 'e',
				0x00, 0x06, // размер значения
				'v', 'a', 'l', 'u', 'e', '1',
			},
			wantErr: false,
		},
		"del encode": {
			cmd: command.Command{Type: command.CommandDEL, Name: "name"},
			wantBytes: []byte{
				CmdType2Byte[string(command.CommandDEL)], // тип команды
				0x00, 0x04,                               // размер имени
				'n', 'a', 'm', 'e',
			},
			wantErr: false,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			buf := &bytes.Buffer{}

			err := encodeCmd(buf, test.cmd)
			if test.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, test.wantBytes, buf.Bytes())
		})
	}
}

func TestDecodeCmd(t *testing.T) {
	t.Parallel()

	type test struct {
		bytes []byte

		gotCmd  command.Command
		wantErr bool
	}

	tests := map[string]test{
		"get decode": {
			bytes: []byte{
				CmdType2Byte[string(command.CommandGET)], // тип команды
				0x00, 0x04,                               // размер имени
				'n', 'a', 'm', 'e',
			},
			wantErr: true,
		},
		"set decode": {
			bytes: []byte{
				CmdType2Byte[string(command.CommandSET)], // тип команды
				0x00, 0x04,                               // размер имени
				'n', 'a', 'm', 'e',
				0x00, 0x06, // размер значения
				'v', 'a', 'l', 'u', 'e', '1',
			},

			gotCmd: command.Command{Type: command.CommandSET, Name: "name", Set: command.SetArgs{
				Value: "value1",
			}},
			wantErr: false,
		},
		"del decode": {
			bytes: []byte{
				CmdType2Byte[string(command.CommandDEL)], // тип команды
				0x00, 0x04,                               // размер имени
				'n', 'a', 'm', 'e',
			},

			gotCmd:  command.Command{Type: command.CommandDEL, Name: "name"},
			wantErr: false,
		},
		"invalid cmd type": {
			bytes: []byte{
				0xFF,       // тип команды
				0x00, 0x04, // размер имени
				'n', 'a', 'm', 'e',
			},

			wantErr: true,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			buf := bytes.NewBuffer(test.bytes)
			cmd, err := decodeCmd(buf)
			if test.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, test.gotCmd, cmd)
		})
	}
}
