package streams

type source[T any] struct {
	next    func() T
	hasNext func() bool
}

func newSource[T any](next func() T, hasNext func() bool) *source[T] {
	return &source[T]{next: next, hasNext: hasNext}
}
