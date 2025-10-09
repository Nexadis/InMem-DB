package storage

import (
	"fmt"
	"io"

	"inmem-db/internal/storage/wal"
)

func encodeSegments(w io.Writer, segments []wal.Segment) error {
	for _, segment := range segments {
		err := wal.EncodeSegment(w, segment)
		if err != nil {
			return fmt.Errorf("encode segment: %w", err)
		}
	}
	return nil
}

func decodeSegments(r io.Reader, segments []wal.Segment) error {
	for i := range len(segments) {
		segment, err := wal.DecodeSegment(r)
		if err != nil {
			return fmt.Errorf("decode segment: %w", err)
		}
		segments[i] = segment
	}
	return nil
}
