package wal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"

	"inmem-db/internal/domain/command"
	"inmem-db/internal/storage/wal/encode"
)

func decodeSegments(data []byte) ([]Segment, error) {
	buf := bytes.NewBuffer(data)

	segments := make([]Segment, 0, 100)
	for {
		segment, err := decodeSegment(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return segments, nil
			}
			return nil, fmt.Errorf("decode cmd: %w", err)
		}
		segments = append(segments, segment)
	}
}

func encodeSegments(segments []Segment) ([]byte, error) {
	buf := &bytes.Buffer{}
	for _, segment := range segments {
		err := encodeSegment(buf, segment)
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func encodeSegment(w io.Writer, segment Segment) error {
	segment.mu.RLock()
	defer segment.mu.RUnlock()

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

func decodeSegment(r io.Reader) (Segment, error) {
	id, err := encode.ReadID(r)
	if err != nil {
		return Segment{}, fmt.Errorf("decode segment id: %w", err)
	}

	size, err := encode.ReadSize(r)
	if err != nil {
		return Segment{}, fmt.Errorf("decode segment size: %w", err)
	}
	segment := Segment{
		mu:       &sync.RWMutex{},
		ID:       ID(id),
		commands: make([]command.Command, size),
	}

	for i := range size {
		cmd, err := encode.Read(r)
		if err != nil {
			return Segment{}, fmt.Errorf("decode command of segment '%d': %w", segment.ID, err)
		}
		segment.commands[i] = cmd
	}

	return segment, nil
}
