package fstore

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"inmem-db/internal/domain/command"
)

var CmdType2Byte = map[string]byte{
	string(command.CommandSET): 2,
	string(command.CommandDEL): 3,
}

var Byte2CmdType = map[byte]string{
	CmdType2Byte[string(command.CommandSET)]: string(command.CommandSET),
	CmdType2Byte[string(command.CommandDEL)]: string(command.CommandDEL),
}

func encodeCmd(buf *bytes.Buffer, cmd command.Command) error {
	if cmd.Type == command.CommandGET {
		return nil
	}

	b, ok := CmdType2Byte[string(cmd.Type)]
	if !ok {
		return fmt.Errorf("unknown cmd type: %s", cmd.Type)
	}
	// записываем тип операции
	buf.WriteByte(b)

	// записываем имя
	size := [2]byte{}
	binary.BigEndian.PutUint16(size[:], uint16(len(cmd.Name)))
	buf.Write(size[:])
	buf.WriteString(cmd.Name)

	// записываем значение, если это SET
	switch cmd.Type {
	case command.CommandSET:
		binary.BigEndian.PutUint16(size[:], uint16(len(cmd.Set.Value)))
		buf.Write(size[:])
		buf.WriteString(cmd.Set.Value)
	}
	return nil
}

func decodeCmd(buf *bytes.Buffer) (command.Command, error) {
	typeByte, err := buf.ReadByte()
	if err != nil {
		return command.Command{}, fmt.Errorf("read type: %w", err)
	}
	cmdType, ok := Byte2CmdType[typeByte]
	if !ok {
		return command.Command{}, fmt.Errorf("unknown type: %d", typeByte)
	}

	size := [2]byte{}
	_, err = buf.Read(size[:])
	if err != nil {
		return command.Command{}, fmt.Errorf("read name size: %w", err)
	}

	nameLen := binary.BigEndian.Uint16(size[:])

	name := make([]byte, nameLen)
	_, err = buf.Read(name)
	if err != nil {
		return command.Command{}, fmt.Errorf("read name: %w", err)
	}

	cmd := command.Command{
		Name: string(name),
	}
	switch cmdType {
	case string(command.CommandDEL):
		cmd.Type = command.CommandDEL
		return cmd, nil
	}

	cmd.Type = command.CommandSET

	_, err = buf.Read(size[:])
	if err != nil {
		return command.Command{}, fmt.Errorf("read value size: %w", err)
	}

	valueLen := binary.BigEndian.Uint16(size[:])

	value := make([]byte, valueLen)
	_, err = buf.Read(value)
	if err != nil {
		return command.Command{}, fmt.Errorf("read value: %w", err)
	}

	cmd.Set.Value = string(value)

	return cmd, nil
}
