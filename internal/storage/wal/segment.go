package wal

import (
	"sync"

	"inmem-db/internal/domain/command"
)

type ID int64

type Segment struct {
	mu *sync.RWMutex

	ID       ID
	commands []command.Command
}

func (w *WAL) makeSegment(commands []command.Command) Segment {
	segmentCommands := make([]command.Command, len(commands))
	copy(segmentCommands, commands)

	segment := Segment{
		mu:       &sync.RWMutex{},
		ID:       w.genSegmentID(),
		commands: segmentCommands,
	}
	return segment
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
