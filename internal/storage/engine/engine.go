package engine

import (
	"context"
	"errors"
	"inmem-db/internal/domain/command"
	"log/slog"
)

var (
	ErrUnknownCmd = errors.New("unknown command")
	ErrInvalidCmd = errors.New("invalid command")
)

type Output struct {
	Msg   string
	Error error
}

type Engine struct {
	s *storage
}

func New() *Engine {
	return &Engine{
		s: newStorage(),
	}
}

func (e *Engine) Do(ctx context.Context, cmd command.Command) (string, error) {
	slog.DebugContext(ctx, "do command", slog.String("cmd", string(cmd.Type)))

	name := cmd.Name
	if len(name) == 0 {
		return "", ErrInvalidCmd
	}
	switch cmd.Type {
	case command.CommandGET:
		return e.s.Get(ctx, name)

	case command.CommandSET:
		value := cmd.Set.Value
		if len(value) == 0 {
			return "", ErrInvalidCmd
		}

		return "", e.s.Set(ctx, name, value)

	case command.CommandDEL:
		return "", e.s.Del(ctx, name)

	}
	return "", ErrUnknownCmd
}
