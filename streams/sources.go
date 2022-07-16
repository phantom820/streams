package streams

import "github.com/phantom820/collections/errors"

type Source[T any] interface {
	Next() T
	HasNext() bool
}

// source a sequential stream source.
type source[T any] struct {
	next    func() T
	hasNext func() bool
}

// NewSource creates a new source. Where elements are generated by the next function and end of the source is checked by has next function. The source
// can be finite/infinite. When using an infinite source a limit operation can be used to avoid infinite loops in invoking a terminal operation.
func NewSource[T any](next func() T, hasNext func() bool) *source[T] {
	return &source[T]{next: next, hasNext: hasNext}
}

// newSource creates a new source.
func newSource[T any](next func() T, hasNext func() bool) *source[T] {
	return &source[T]{next: next, hasNext: hasNext}
}

// newSourceFromSlice creates a source from a slice.
func newSourceFromSlice[T any](f func() []T) *source[T] {
	var data []T
	initialized := false
	i := 0
	hasNext := func() bool {
		if !initialized {
			initialized = true
			data = f()
		}
		if data == nil || i >= len(data) {
			return false
		}
		return true
	}
	next := func() T {
		if !hasNext() {
			panic(errors.ErrNoNextElement())
		}
		element := data[i]
		i++
		return element
	}
	source := source[T]{next: next, hasNext: hasNext}
	return &source
}
