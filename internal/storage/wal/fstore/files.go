package fstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"sync"

	"inmem-db/internal/config"
	"inmem-db/internal/domain/command"
)

const nameTmpl = "wal_[0-9]*.bin"

type FStore struct {
	mu       sync.Mutex
	opened   *os.File
	filesCnt uint

	dir          string
	maxFileSize  uint64
	maxBatchSize uint64
	written      uint64
}

func New(cfg config.WAL) (*FStore, error) {
	maxSize, err := parseSize(cfg.MaxSegmentSize)
	if err != nil {
		return nil, err
	}

	s := FStore{
		dir:          cfg.DataDir,
		maxFileSize:  maxSize,
		maxBatchSize: uint64(cfg.BatchSize),
	}

	return &s, nil
}

func (s *FStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.opened.Close()
}

// WriteCommands - записывает batch в открытый файл, используйте LoadFiles перед первым вызовом WriteCommands.
// Вы можете стереть существующие файлы, если не вызовете LoadFiles.
func (s *FStore) WriteCommands(batch []command.Command) error {
	defer s.opened.Sync()

	buf := &bytes.Buffer{}
	for _, cmd := range batch {
		err := encodeCmd(buf, cmd)
		if err != nil {
			return fmt.Errorf("encode cmd: %w", err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.opened == nil {
		err := s.newFile()
		if err != nil {
			return fmt.Errorf("new file: %w", err)
		}
	}

	written, err := s.opened.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("write '%v' : %w", buf.Bytes(), err)
	}
	s.written += uint64(written)

	if s.written > s.maxFileSize {
		err := s.newFile()
		if err != nil {
			return fmt.Errorf("new file: %w", err)
		}
	}
	return nil
}

// LoadFiles загружает все данные из файлов в папке
func (s *FStore) LoadFiles() ([]command.Command, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("read dir: %w", err)
	}
	cmds := make([]command.Command, 0, int(s.maxBatchSize)*len(entries))

	cnt := uint(0)
	slog.Debug("find entries", slog.Int("cnt", len(entries)))

	for _, e := range entries {
		slog.Debug("check file", slog.String("name", e.Name()))
		ok, err := path.Match(nameTmpl, e.Name())
		if err != nil {
			return nil, fmt.Errorf("match file: %w", err)
		}
		if !ok {
			continue
		}

		fileCmds, err := s.loadFile(e.Name())
		if err != nil {
			return nil, err
		}
		cnt++
		cmds = append(cmds, fileCmds...)
	}

	s.filesCnt = cnt

	return cmds, nil
}

func (s *FStore) loadFile(name string) ([]command.Command, error) {
	slog.Debug("load file", slog.String("name", name))

	name = path.Join(s.dir, name)
	data, err := os.ReadFile(name)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	buf := bytes.NewBuffer(data)

	cmds := make([]command.Command, 0, s.maxBatchSize)
	for {
		cmd, err := decodeCmd(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return cmds, nil
			}
			return nil, fmt.Errorf("decode cmd: %w", err)
		}
		cmds = append(cmds, cmd)
	}
}

// newFile следует вызывать внутри критической секции с mu.Lock()
func (s *FStore) newFile() error {
	s.filesCnt++
	name := fmt.Sprintf("wal_%04d.bin", s.filesCnt)
	f, err := os.Create(path.Join(s.dir, name))
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	s.opened.Close()

	s.written = 0
	s.opened = f
	return nil
}
