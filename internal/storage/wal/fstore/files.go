package fstore

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path"
	"sync"

	"inmem-db/internal/config"
)

const nameTmpl = "wal_[0-9]*.bin"

type FStore struct {
	mu       sync.Mutex
	opened   *os.File
	filesCnt uint

	dir         string
	maxFileSize uint64
	written     uint64
}

func New(cfg config.WAL) (*FStore, error) {
	maxSize, err := parseSize(cfg.MaxSegmentSize)
	if err != nil {
		return nil, err
	}

	s := FStore{
		dir:         cfg.DataDir,
		maxFileSize: maxSize,
	}

	return &s, nil
}

func (s *FStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.opened.Close()
}

// Write - записывает данные в открытый файл, используйте ReadAll перед первым вызовом Write.
// Вы можете стереть существующие файлы, если не вызовете ReadAll.
func (s *FStore) Write(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.opened == nil {
		err := s.newFile()
		if err != nil {
			return fmt.Errorf("new file: %w", err)
		}
	}

	defer s.opened.Sync()

	written, err := s.opened.Write(data)
	if err != nil {
		return fmt.Errorf("write '%v' : %w", data, err)
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

// ReadAll загружает все данные из файлов в папке
func (s *FStore) ReadAll() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	files, err := filesInDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("all files: %w", err)
	}

	return s.readFiles(files)
}

// loadFile следует вызывать внутри критической секции с mu.Lock()
func (s *FStore) loadFile(name string) ([]byte, error) {
	slog.Debug("load file", slog.String("name", name))

	data, err := os.ReadFile(name)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	return data, nil
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

func (s *FStore) readFiles(names []string) ([]byte, error) {
	cnt := uint(0)
	slog.Debug("find entries", slog.Int("cnt", len(names)))

	sumData := &bytes.Buffer{}

	var last string
	for _, name := range names {
		if skipFile(name) {
			continue
		}
		name = path.Join(s.dir, name)

		data, err := s.loadFile(name)
		if err != nil {
			return nil, err
		}
		cnt++
		sumData.Write(data)
	}

	if last != "" {
		err := s.openLast(last)
		if err != nil {
			return nil, err
		}
	}

	s.filesCnt = cnt
	return sumData.Bytes(), nil
}

func (s *FStore) openLast(name string) error {
	f, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0o766)
	if err != nil {
		return fmt.Errorf("open last: %w", err)
	}
	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat last: %w", err)
	}

	s.opened = f
	s.written = uint64(stat.Size())

	return nil
}

func filesInDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir: %w", err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		if e.IsDir() {
			continue
		}
		names[i] = e.Name()
	}
	return names, nil
}

func skipFile(name string) bool {
	ok, err := path.Match(nameTmpl, name)
	if err != nil || !ok {
		return true
	}

	return false
}
