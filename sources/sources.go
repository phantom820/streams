// package source provides an implementation of a producer of elements for a stream , currently streams only support sources that are finite otherwise
// infinite sources will lead to an infinite loop.
package sources

import (
	"errors"
)

// Source a source of elements for a stream. Sources can come from a slice, collection etc and should be finite.
type Source[T any] interface {
	Next() T       // Returns the next element from the source.
	HasNext() bool // Checks if the Source has a next element to produce.
}

// source a  stream source.
type source[T any] struct {
	next    func() T
	hasNext func() bool
}

// HasNext checks if the source has a next element to produce.
func (source *source[T]) HasNext() bool {
	return source.hasNext()
}

// Next returns the next element from the source.
func (source *source[T]) Next() T {
	return source.next()
}

//  New creates a new source.
func New[T any](next func() T, hasNext func() bool) Source[T] {
	return &source[T]{next: next, hasNext: hasNext}
}

// FromSlice creates a sequential source from a slice.
func FromSlice[T any](f func() []T) Source[T] {
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
			panic(errors.New("ErrNoNextElement"))
		}
		element := data[i]
		i++
		return element
	}
	source := source[T]{next: next, hasNext: hasNext}
	return &source
}
