package encode

import (
	"encoding/binary"
	"fmt"
	"io"

	"inmem-db/internal/domain/command"
)

var CmdType2Byte = map[string]byte{
	string(command.CommandSET): 2,
	string(command.CommandDEL): 3,
}

func Write(w io.Writer, cmd command.Command) error {
	if cmd.Type == command.CommandGET {
		return nil
	}

	err := writeType(w, cmd)
	if err != nil {
		return err
	}

	err = writeString(w, cmd.Name)
	if err != nil {
		return err
	}
	if cmd.Type != command.CommandSET {
		return nil
	}

	return writeString(w, cmd.Set.Value)
}

func writeType(w io.Writer, cmd command.Command) error {
	b, ok := CmdType2Byte[string(cmd.Type)]
	if !ok {
		return fmt.Errorf("unknown cmd type: %s", cmd.Type)
	}
	_, err := w.Write([]byte{b})
	if err != nil {
		return fmt.Errorf("write cmd type: %w", err)
	}
	return nil
}

func writeString(w io.Writer, s string) error {
	err := binary.Write(w, binary.BigEndian, uint16(len(s)))
	if err != nil {
		return fmt.Errorf("write string size: %w", err)
	}

	_, err = w.Write([]byte(s))
	if err != nil {
		return fmt.Errorf("write string: %w", err)
	}
	return nil
}
