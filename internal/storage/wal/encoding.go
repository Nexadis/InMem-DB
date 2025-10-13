package wal

import (
	"fmt"
	"io"

	"inmem-db/internal/domain/command"
	"inmem-db/internal/storage/wal/decode"
	"inmem-db/internal/storage/wal/encode"
)

func EncodeSegment(w io.Writer, segment Segment) error {
	err := encode.WriteID(w, int64(segment.ID))
	if err != nil {
		return fmt.Errorf("encode segment id: %w", err)
	}

	err = encode.WriteSize(w, uint32(len(segment.commands)))
	if err != nil {
		return fmt.Errorf("encode segment size: %w", err)
	}

	for _, cmd := range segment.commands {
		err := encode.Write(w, cmd)
		if err != nil {
			return fmt.Errorf("encode command of segment '%d': %w", segment.ID, err)
		}
	}

	return nil
}

func DecodeSegment(r io.Reader) (Segment, error) {
	id, err := decode.ReadID(r)
	if err != nil {
		return Segment{}, fmt.Errorf("decode segment id: %w", err)
	}

	size, err := decode.ReadSize(r)
	if err != nil {
		return Segment{}, fmt.Errorf("decode segment size: %w", err)
	}
	segment := Segment{
		ID:       ID(id),
		commands: make([]command.Command, size),
	}

	for i := range size {
		cmd, err := decode.Read(r)
		if err != nil {
			return Segment{}, fmt.Errorf("decode command of segment '%d': %w", segment.ID, err)
		}
		segment.commands[i] = cmd
	}

	return segment, nil
}
