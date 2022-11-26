package streams

import "fmt"

// Stream a sequence of elements that can be operated on sequentially or in parallel. The underlying source for a stream should be finite, infinite sources
// are not supported and will lead to an infinite loop.
type Stream[T any] interface {
	Filter(f func(x T) bool) Stream[T]        // Returns a stream consisting of the elements of this stream that satisfy the given predicate.
	Map(f func(x T) T) Stream[T]              // Returns a stream consisting of the results of applying the given transformation to the elements of the stream.
	Limit(n int) Stream[T]                    // Returns a stream consisting of the elements of this stream, truncated to be no longer than given length.
	Skip(n int) Stream[T]                     // Returns a stream consisting of the remaining elements of this stream after discarding the first n elements of the stream.
	Distinct(hash func(x T) string) Stream[T] // Returns a stream consisting of the distinct elements (according to the given hash of elements) of this stream.
	Peek(f func(x T)) Stream[T]               // Returns a stream consisting of the elements of this stream.
	// additionally the provided action on each element as elements are consumed.	// Terminal operations.

	ForEach(f func(x T))       // Performs an action specified by the function f for each element of the stream.
	Count() int                // Returns a count of elements in the stream.
	Reduce(f func(x, y T) T) T // Returns result of performing reduction on the elements of the stream, using ssociative accumulation function, and returns the reduced value.
	// The zero value is returned if there are no elements.

	Collect() []T              // Returns a slice containing the elements from the stream.
	Parallel() bool            // Returns an indication of whether the stream is parallel.
	Parallelize(int) Stream[T] // Returns a parallel stream with the given level of parallelism.

	Terminated() bool // Checks if a terminal operation has been invoked on the stream.
	Closed() bool     // Checks if a stream has been closed. A stream is closed either when a new stream is created from it using intermediate
	// operations, terminated streams are also closed.

}

// stream underlying concrete type, keeps track of operations.
type stream[T any] struct {
	supplier    func() []T
	operations  []operator[T]
	parallel    bool
	maxRoutines int
	distinct    bool
	terminated  bool
	closed      bool
}

// New creates a new stream with the given supplier for elements.
func New[T any](supplier func() []T) Stream[T] {
	return &stream[T]{
		supplier:   supplier,
		operations: make([]operator[T], 0),
	}
}

// new creates a new stream which adds the given operation.
func new[T any](s *stream[T], operator operator[T]) *stream[T] {
	defer s.close()
	return &stream[T]{
		supplier:    s.supplier,
		operations:  append(s.operations, operator),
		parallel:    s.parallel,
		distinct:    s.distinct,
		maxRoutines: s.maxRoutines,
	}
}

// Closed returns an indication of whether the stream has been closed or not.
func (s *stream[T]) Closed() bool {
	return s.closed
}

// close closes the stream.
func (s *stream[T]) close() {
	s.closed = true
}

// Terminated returns an indication of whether the stream has been closed by invoking a terminal operation.
func (s *stream[T]) Terminated() bool {
	return s.terminated
}

// terminate terminate the stream.
func (s *stream[T]) terminate() {
	s.terminated = true
	s.closed = true
}

// valid checks if a stream is valid before performing any type of operation.
func (s *stream[T]) valid() (bool, *streamError) {
	if s.Terminated() {
		err := errStreamTerminated()
		return false, &err
	} else if s.Closed() {
		err := errStreamClosed()
		return false, &err
	}
	return true, nil
}

// Parallel returns an indication of whether the stream is parallel.
func (s stream[T]) Parallel() bool {
	return s.parallel
}

// Parallelize returns a parallel stream with the given level of parallelism
func (s *stream[T]) Parallelize(n int) Stream[T] {
	if n <= 1 {
		panic(errIllegalConfig("Parallelism", fmt.Sprint(n)))
	}
	return &stream[T]{
		supplier:    s.supplier,
		operations:  s.operations,
		parallel:    true,
		maxRoutines: n,
	}
}

// Collect returns a slice containing the elements from the stream.
func (s *stream[T]) Collect() []T {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	defer s.terminate()
	if s.parallel {
		return parallelCollect(s.supplier(), s.operations, s.maxRoutines)
	}
	return collect(s.supplier(), s.operations)
}

// Map returns a stream consisting of the results of applying the given uniform
// mapping function to the elements of this stream.
func (s *stream[T]) Map(f func(T) T) Stream[T] {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	return new(s, uniformMap(f))
}

// Filter returns a stream consisting of the elements of this stream that match the given predicate.
func (s *stream[T]) Filter(f func(T) bool) Stream[T] {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	return new(s, filter(f))
}

// Limit returns a stream consisting of the elements of this stream, truncated to be no longer than given length.
func (s *stream[T]) Limit(n int) Stream[T] {
	if ok, err := s.valid(); !ok {
		panic(err)
	} else if n < 0 {
		panic(errIllegalArgument("Limit", fmt.Sprint(n)))
	}
	return new(s, limit[T](s.parallel, n))
}

// Skip returns a stream consisting of the remaining elements of this stream after discarding the first n elements of the stream.
func (s *stream[T]) Skip(n int) Stream[T] {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	return new(s, skip[T](s.parallel, n))
}

// Count returns the count of elements in this stream.
func (s *stream[T]) Count() int {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	defer s.terminate()
	if s.parallel {
		return parallelCount(s.supplier(), s.operations, s.maxRoutines)
	}
	return count(s.supplier(), s.operations)

}

// Distinct returns a stream consisting of the distinct elements (according to the given hash of elements) of this stream.
func (s *stream[T]) Distinct(hash func(x T) string) Stream[T] {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	newStream := new(s, distinct(s.parallel, s.distinct, hash))
	newStream.distinct = true
	return newStream
}

// ForEach performs an action for each element of this stream.
func (s *stream[T]) ForEach(f func(T)) {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	defer s.terminate()
	data := s.supplier()
	operations := s.operations
	if s.parallel {
		parallelForEach(data, operations, f, s.maxRoutines)
		return
	}
	forEach(data, operations, f)
}

// Peek returns a stream consisting of the elements of this stream,
// additionally the provided action on each element as elements are consumed.
func (s *stream[T]) Peek(f func(T)) Stream[T] {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	return new(s, peek(f))
}

// Reduce performs a reduction on the elements of the stream, using ssociative accumulation function, and returns the reduced value.
// The zero value is returned if there are no elements.
func (s *stream[T]) Reduce(f func(x, y T) T) T {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	defer s.terminate()
	if s.parallel {
		val, _ := parallelReduce(s.supplier(), s.operations, f, s.maxRoutines)
		return val
	}
	val, _ := reduce(s.supplier(), s.operations, f)
	return val

}
