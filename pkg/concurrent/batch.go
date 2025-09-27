package concurrent

import (
	"context"
	"time"
)

type Batch[T any] struct {
	isClosed chan struct{}

	queue chan T
	errs  chan error

	handleBatch func([]T) error
	maxSize     int
	timeout     time.Duration
}

func NewBatch[T any](size int, timeout time.Duration, handleBatch func([]T) error) *Batch[T] {
	b := Batch[T]{
		isClosed: make(chan struct{}),
		queue:    make(chan T),
		errs:     make(chan error),

		handleBatch: handleBatch,
		maxSize:     size,
		timeout:     timeout,
	}
	b.serve()

	return &b
}

func (b *Batch[T]) Add(ctx context.Context, v T) *Future {
	select {
	case <-ctx.Done():
		return nil
	case b.queue <- v:
	}

	recvErr := func() error { return <-b.errs }
	f := NewFuture()
	f.Set(recvErr)

	return f
}

func (b *Batch[T]) serve() {
	values := make([]T, 0, b.maxSize)

	go func() {
		for {
			select {
			case <-b.isClosed:
				close(b.errs)
				return
			default:
			}

			values = b.waitBatch(values)
			err := b.handleBatch(values)
			b.sendErrs(len(values), err)

			values = values[:0]
		}
	}()
}

func (b *Batch[T]) sendErrs(count int, err error) {
	for range count {
		b.errs <- err
	}
}

func (b *Batch[T]) waitBatch(values []T) []T {
	t := time.NewTimer(b.timeout)
	defer t.Stop()

	for {
		select {
		case <-b.isClosed:
			return values
		case <-t.C:
			return values

		case v := <-b.queue:
			values = append(values, v)
			if len(values) == b.maxSize {
				return values
			}

		}
	}
}

func (b *Batch[T]) Close() {
	close(b.isClosed)
}
