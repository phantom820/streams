package streams

import "fmt"

type GroupedStream[T any] interface {
	// Filter(f func(x Group[T]) bool) GroupedStream[T]  // Returns a stream consisting of the groups of this stream that satisfy the given predicate.
	// Map(f func(x Group[T]) Group[T]) GroupedStream[T] // Returns a stream consisting of the results of applying the given transformation to the elements of the stream.
	// Limit(n int) GroupedStream[T]                     // Returns a stream consisting of the elements of this stream, truncated to be no longer than given length.
	// Skip(n int) GroupedStream[T]                      // Returns a stream consisting of the remaining elements of this stream after discarding the first n elements of the stream.
	// Distinct(hash func(x T) string) Stream[T]         // Returns a stream consisting of the distinct elements (according to the given hash of elements) of this stream.
	// Peek(f func(x Group[T])) GroupedStream[T] // Returns a stream consisting of the elements of this stream.
	// additionally the provided action on each element as elements are consumed.	// Terminal operations.

	ForEach(f func(x Group[T])) // Performs an action specified by the function f for each group of the stream.
	Count() map[string]int      // Returns a count of elements in the stream.
	// Reduce(f func(x, y Group[T]) T) T // Returns result of performing reduction on the elements of the stream, using ssociative accumulation function, and returns the reduced value.
	// The zero value is returned if there are no elements.

	Collect() []Group[T]              // Returns a slice containing the elements from the stream.
	Parallel() bool                   // Returns an indication of whether the stream is parallel.
	Parallelize(int) GroupedStream[T] // Returns a parallel stream with the given level of parallelism.

	Terminated() bool // Checks if a terminal operation has been invoked on the stream.
	Closed() bool     // Checks if a stream has been closed. A stream is closed either when a new stream is created from it using intermediate
	// operations, terminated streams are also closed.

}

type groupedStream[T any] struct {
	supplier    func() []Group[T]
	operations  []operator[Group[T]]
	parallel    bool
	maxRoutines int
	distinct    bool
	terminated  bool
	closed      bool
}

type Group[T any] struct {
	name string
	data []T
}

func (g Group[T]) Name() string {
	return g.name
}

func (g Group[T]) Data() []T {
	return g.data
}

func (g Group[T]) Len() int {
	return len(g.data)
}

// Closed returns an indication of whether the stream has been closed or not.
func (s *groupedStream[T]) Closed() bool {
	return s.closed
}

// close closes the stream.
func (s *groupedStream[T]) close() {
	s.closed = true
}

// Terminated returns an indication of whether the stream has been closed by invoking a terminal operation.
func (s *groupedStream[T]) Terminated() bool {
	return s.terminated
}

// terminate terminate the stream.
func (s *groupedStream[T]) terminate() {
	s.terminated = true
	s.closed = true
}

// valid checks if a stream is valid before performing any type of operation.
func (s *groupedStream[T]) valid() (bool, *streamError) {
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
func (s groupedStream[T]) Parallel() bool {
	return s.parallel
}

// Parallelize returns a parallel stream with the given level of parallelism
func (s *groupedStream[T]) Parallelize(n int) GroupedStream[T] {
	if n <= 1 {
		panic(errIllegalConfig("Parallelism", fmt.Sprint(n)))
	}
	return &groupedStream[T]{
		supplier:    s.supplier,
		operations:  s.operations,
		parallel:    true,
		maxRoutines: n,
	}
}

// Collect returns a slice containing the elements from the stream.
func (s *groupedStream[T]) Collect() []Group[T] {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	defer s.terminate()
	if s.parallel {
		return parallelCollect(s.supplier(), s.operations, s.maxRoutines)
	}
	return collect(s.supplier(), s.operations)
}

// Count returns the count of elements in this stream.
func (s *groupedStream[T]) Count() map[string]int {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	defer s.terminate()
	if s.parallel {
		return groupParallelCount(s.supplier(), s.maxRoutines)
	}
	return groupCount(s.supplier())

}

// ForEach performs an action for each group of this stream.
func (s *groupedStream[T]) ForEach(f func(Group[T])) {
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
