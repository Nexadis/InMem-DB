package encode

import (
	"encoding/binary"
	"fmt"
	"io"

	"inmem-db/internal/domain/command"
)

var Byte2CmdType = map[byte]string{
	CmdType2Byte[string(command.CommandSET)]: string(command.CommandSET),
	CmdType2Byte[string(command.CommandDEL)]: string(command.CommandDEL),
}

func ReadID(r io.Reader) (int64, error) {
	id := int64(0)
	err := binary.Read(r, binary.BigEndian, &id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func ReadSize(r io.Reader) (uint32, error) {
	size := uint32(0)
	err := binary.Read(r, binary.BigEndian, &size)
	if err != nil {
		return 0, err
	}
	return size, nil
}

func Read(r io.Reader) (command.Command, error) {
	cmd := command.Command{}

	cmdType, err := readType(r)
	if err != nil {
		return command.Command{}, err
	}

	switch cmdType {
	case string(command.CommandDEL):
		cmd.Type = command.CommandDEL
	case string(command.CommandSET):
		cmd.Type = command.CommandSET
	}

	name, err := readString(r)
	if err != nil {
		return command.Command{}, err
	}
	cmd.Name = name

	if cmd.Type != command.CommandSET {
		return cmd, nil
	}

	value, err := readString(r)
	if err != nil {
		return command.Command{}, err
	}

	cmd.Set.Value = value

	return cmd, nil
}

func readType(r io.Reader) (string, error) {
	var typeByte byte
	err := binary.Read(r, binary.BigEndian, &typeByte)
	if err != nil {
		return "", fmt.Errorf("read cmd type: %w", err)
	}
	cmdType, ok := Byte2CmdType[typeByte]
	if !ok {
		return "", fmt.Errorf("unknown cmd type: %d", typeByte)
	}
	return cmdType, nil
}

func readString(r io.Reader) (string, error) {
	size := uint16(0)
	err := binary.Read(r, binary.BigEndian, &size)
	if err != nil {
		return "", fmt.Errorf("read string size: %w", err)
	}

	s := make([]byte, size)
	_, err = r.Read(s)
	if err != nil {
		return "", fmt.Errorf("read string: %w", err)
	}

	return string(s), nil
}
