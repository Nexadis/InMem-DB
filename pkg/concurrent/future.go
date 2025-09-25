package concurrent

type Future struct {
	out chan error
}

func NewFuture() *Future {
	f := Future{
		out: make(chan error),
	}

	return &f
}

func (f *Future) Set(action func() error) {
	if action == nil {
		action = func() error { return nil }
	}

	go func() {
		err := action()
		f.out <- err
	}()
}

func (f *Future) Get() error {
	if f == nil {
		return nil
	}

	defer close(f.out)
	return <-f.out
}
