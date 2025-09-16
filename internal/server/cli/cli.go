package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"

	"inmem-db/internal/domain/command"
)

const prompt = "-> "

type Parser interface {
	Parse(ctx context.Context, line string) (command.Command, error)
}
type Storage interface {
	Do(ctx context.Context, cmd command.Command) (string, error)
}

type Cli struct {
	s *bufio.Scanner
	w io.Writer

	p       Parser
	storage Storage
}

type Factory func(r io.Reader, w io.Writer) *Cli

func NewFactory(p Parser, storage Storage) Factory {
	return func(r io.Reader, w io.Writer) *Cli {
		return New(r, w, p, storage)
	}
}

func New(r io.Reader, w io.Writer, p Parser, storage Storage) *Cli {
	s := bufio.NewScanner(r)
	s.Split(bufio.ScanLines)

	return &Cli{
		s:       s,
		w:       w,
		p:       p,
		storage: storage,
	}
}

func (c *Cli) Start(ctx context.Context) error {
	for {
		fmt.Fprint(c.w, prompt)

		if !c.s.Scan() {
			break
		}

		line := c.s.Text()
		slog.DebugContext(ctx, "read line", slog.String("line", line))

		cmd, err := c.p.Parse(ctx, line)
		if err != nil {
			printErr(c.w, err)
			continue
		}

		out, err := c.storage.Do(ctx, cmd)
		if err != nil {
			printErr(c.w, err)
			continue
		}

		fmt.Fprint(c.w, out, "\n")
	}

	return c.s.Err()
}

func printErr(w io.Writer, err error) {
	msg := fmt.Sprintf("\nError: %s\n", err)
	fmt.Fprint(w, msg)
}
