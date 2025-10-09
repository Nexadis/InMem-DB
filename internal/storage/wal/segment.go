package wal

import (
	"log/slog"

	"inmem-db/internal/domain/command"
)

type ID int64

type Segment struct {
	ID       ID
	commands []command.Command
}

func newSegment(id ID, commands []command.Command) Segment {
	segmentCommands := make([]command.Command, len(commands))
	copy(segmentCommands, commands)

	return Segment{
		ID:       id,
		commands: segmentCommands,
	}
}

func (w *WAL) makeSegment(commands []command.Command) Segment {
	return newSegment(w.genSegmentID(), commands)
}

func (w *WAL) addSegment(s Segment) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.segments[s.ID] = s

	if s.ID > w.maxID {
		w.maxID = s.ID
	}
}

func (w *WAL) genSegmentID() ID {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.maxID == 0 {
		return 1
	}
	w.maxID++
	return w.maxID
}

func (w *WAL) SegmentsAfter(id int64) []Segment {
	slog.Debug("SegmentsAfter", slog.Int64("id", id))

	segments := []Segment{}
	w.mu.Lock()
	defer w.mu.Unlock()
	for sID, segment := range w.segments {
		if sID > ID(id) {
			segments = append(segments, segment)
		}
	}
	return segments
}

func SegmentCommands(segment Segment) []command.Command {
	commands := append([]command.Command{}, segment.commands...)
	return commands
}

func (w *WAL) SaveSegment(segment Segment) error {
	w.addSegment(segment)
	return EncodeSegment(w.store, segment)
}

func (w *WAL) LastSegmentID() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return int64(w.maxID)
}
