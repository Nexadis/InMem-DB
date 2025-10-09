package wal

import (
	"testing"
	"time"

	"inmem-db/internal/config"
	"inmem-db/internal/domain/command"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAfterID(t *testing.T) {
	t.Parallel()
	const totalSegments = 100
	segments := make([]Segment, totalSegments)
	for i := range segments {
		segments[i] = newSegment(ID(i+1), []command.Command{})
	}

	cfg := config.WAL{
		BatchSize:      100,
		BatchTimeout:   time.Millisecond * 100,
		MaxSegmentSize: "10MB",
		DataDir:        t.TempDir(),
	}

	w, err := New(cfg)
	require.NoError(t, err)
	for _, s := range segments {
		err = w.SaveSegment(s)
		require.NoError(t, err)
	}

	for i := range segments {
		segmentsAfter := w.SegmentsAfter(int64(i))
		assert.Len(t, segmentsAfter, totalSegments-i)
		assert.Subset(t, segments, segmentsAfter)
	}
}

func TestLastSegmentID(t *testing.T) {
	t.Parallel()
	const totalSegments = 100
	segments := make([]Segment, totalSegments)
	for i := range segments {
		segments[i] = newSegment(ID(i+1), []command.Command{})
	}

	cfg := config.WAL{
		BatchSize:      100,
		BatchTimeout:   time.Millisecond * 100,
		MaxSegmentSize: "10MB",
		DataDir:        t.TempDir(),
	}

	w, err := New(cfg)
	require.NoError(t, err)
	for _, s := range segments {
		err = w.SaveSegment(s)
		require.NoError(t, err)
	}
	id := w.LastSegmentID()
	assert.EqualValues(t, totalSegments, id)
}
