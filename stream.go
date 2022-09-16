// package streams provides a way of applying a pipeline of operations to a sequence of elements.
package streams

import (
	"github.com/phantom820/collections"
	"github.com/phantom820/collections/types"
	"github.com/phantom820/streams/sources"
)

// Stream a sequence of elements that can be operated on sequential / concurrently.
type Stream[T any] interface {

	// Intermediate operations.
	Filter(f func(x T) bool) Stream[T]                               // Returns a stream consisting of the elements of this stream that satisfy the given predicate.
	Map(f func(x T) T) Stream[T]                                     // Returns a stream consisting of the results of applying the given transformation to the elements of the stream.
	Limit(n int) Stream[T]                                           // Returns a stream consisting of the elements of the stream but only limited to processing n elements.
	Skip(n int) Stream[T]                                            // Returns a stream that skips the first n elements it encounters in processing.
	Distinct(equals func(x, y T) bool, hash func(x T) int) Stream[T] // Returns a stream consisting of distinct elements. Elements are distinguished using equality and hash code.
	Peek(f func(x T)) Stream[T]                                      // Returns a stream consisting of the elements of the given stream but additionaly the given function is invoked for each element.

	// Terminal operations.
	ForEach(f func(x T))               // Performs an action specified by the function f for each element of the stream.
	Count() int                        // Returns a count of elements in the stream.
	Reduce(f func(x, y T) T) (T, bool) // Returns the result of appying a reduction on the elements of the stream. If the stream has no elements then the result would
	// be invalid and the zero value for T along with false would be returned.
	Collect() []T // Returns a slice containing the elements from the stream.

	// State.
	Terminated() bool // Checks if a terminal operation has been invoked on the stream.
	Closed() bool     // Checks if a stream has been closed. A stream is closed either when a new stream is created from it using intermediate
	// operations, terminated streams are also closed.
	Concurrent() bool // Checks if the underlying stream is concurrent or sequential.
}

// FromCollection returns a stream that is sequential and uses the given collection as its source.
func FromCollection[T types.Equitable[T]](collection collections.Collection[T]) Stream[T] {
	it := collection.Iterator()
	return &sequentialStream[T]{
		source:   sources.New(it.Next, it.HasNext),
		pipeline: func(input T) (T, bool) { return input, true },
	}
}

// ConcurrentFromCollection returns a stream that is concurrent and uses the given collection as its source. Elements are processed
// in batches of partition size.
func ConcurrentFromCollection[T types.Equitable[T]](collection collections.Collection[T], concurrency, partitionSize int) Stream[T] {
	it := collection.Iterator()
	return &concurrentStream[T]{
		source:        sources.New(it.Next, it.HasNext),
		pipeline:      func(input T) (T, bool) { return input, true },
		concurrency:   concurrency,
		partitionSize: partitionSize,
	}
}

// FromSlice returns a sequential stream which will use the given callback to initialize its source when required.
func FromSlice[T any](f func() []T) Stream[T] {
	return &sequentialStream[T]{
		source:   sources.FromSlice(f),
		pipeline: func(input T) (T, bool) { return input, true },
	}
}

// ConcurrentFromSlice returns a concurrent stream which will use the given callback to initialize its source when required.
// Elements are processed in batches of partition size.
func ConcurrentFromSlice[T any](f func() []T, concurrency, partitionSize int) Stream[T] {
	return &concurrentStream[T]{
		source:        sources.FromSlice(f),
		pipeline:      func(input T) (T, bool) { return input, true },
		concurrency:   concurrency,
		partitionSize: partitionSize,
	}
}
