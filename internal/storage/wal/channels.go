package wal

func fill(done <-chan struct{}, ch chan<- struct{}, size int) {
	for range size {
		select {
		case <-done:
			return

		case ch <- struct{}{}:
		}
	}
}

func wait(done <-chan struct{}, ch <-chan struct{}, size int) {
	var got int
	for got != size {
		select {
		case <-done:
			return

		case <-ch:
			got++
		}
	}
}

type semaphore struct {
	tokens chan struct{}
}

func newSema(size int) *semaphore {
	tokens := make(chan struct{}, size)
	for range size {
		tokens <- struct{}{}
	}

	return &semaphore{
		tokens: tokens,
	}
}

func (s *semaphore) Acquire() {
	<-s.tokens
}

func (s *semaphore) Release() {
	s.tokens <- struct{}{}
}
