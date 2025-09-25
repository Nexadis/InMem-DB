package wal

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"inmem-db/internal/domain/command"
	"inmem-db/internal/storage/wal/encode"
)

func decodeCommands(data []byte) ([]command.Command, error) {
	buf := bytes.NewBuffer(data)

	cmds := make([]command.Command, 0, 100)
	for {
		cmd, err := encode.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return cmds, nil
			}
			return nil, fmt.Errorf("decode cmd: %w", err)
		}
		cmds = append(cmds, cmd)
	}
}

func encodeCommands(commands []command.Command) ([]byte, error) {
	buf := &bytes.Buffer{}
	for _, cmd := range commands {
		err := encode.Write(buf, cmd)
		if err != nil {
			return nil, fmt.Errorf("encode cmd: %w", err)
		}
	}

	return buf.Bytes(), nil
}
