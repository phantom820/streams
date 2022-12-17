package streams

import "fmt"

// PartitionedStream a stream in which source elements are slices.
type PartitionedStream[T any] interface {
	Filter(f func(x T) bool) PartitionedStream[T]        // Returns a stream consisting of the elements of this stream that satisfy the given predicate.
	Map(f func(x T) T) PartitionedStream[T]              // Returns a stream consisting of the results of applying the given transformation to the elements of the stream.
	Limit(n int) PartitionedStream[T]                    // Returns a stream consisting of the elements of this stream, truncated to be no longer than given length.
	Skip(n int) PartitionedStream[T]                     // Returns a stream consisting of the remaining elements of this stream after discarding the first n elements of the stream.
	Distinct(hash func(x T) string) PartitionedStream[T] // Returns a stream consisting of the distinct elements (according to the given hash of elements) of this stream.
	Peek(f func(x []T)) PartitionedStream[T]             // Returns a stream consisting of the elements of this stream.
	// additionally the provided action on each element as elements are consumed.	// Terminal operations.
	FlatMap() Stream[T] // Returns a stream in which the source elements have been flattened by one level.

	ForEach(f func(x []T))           // Performs an action specified by the function f for each element of the stream.
	Count() int                      // Returns a count of elements in the stream.
	Reduce(f func(x, y []T) []T) []T // Returns result of performing reduction on the elements of the stream, using ssociative accumulation function, and returns the reduced value.
	// The zero value is returned if there are no elements.

	Collect() [][]T                       // Returns a slice containing the elements from the stream.
	Parallel() bool                       // Returns an indication of whether the stream is parallel.
	Parallelize(int) PartitionedStream[T] // Returns a parallel stream with the given level of parallelism.

	Terminated() bool // Checks if a terminal operation has been invoked on the stream.
	Closed() bool     // Checks if a stream has been closed. A stream is closed either when a new stream is created from it using intermediate
	// operations, terminated streams are also closed.

}

// stream underlying concrete type, keeps track of operations.
type partitionedStream[T any] struct {
	supplier    func() [][]T
	operations  []operator[[]T]
	parallel    bool
	maxRoutines int
	distinct    bool
	terminated  bool
	closed      bool
}

// newPartitionedStream creates a new stream which adds the given operation.
func newPartitionedStream[T any](s *partitionedStream[T], operator operator[[]T]) *partitionedStream[T] {
	defer s.close()
	return &partitionedStream[T]{
		supplier:    s.supplier,
		operations:  append(s.operations, operator),
		parallel:    s.parallel,
		distinct:    s.distinct,
		maxRoutines: s.maxRoutines,
	}
}

// Closed returns an indication of whether the stream has been closed or not.
func (s *partitionedStream[T]) Closed() bool {
	return s.closed
}

// close closes the stream.
func (s *partitionedStream[T]) close() {
	s.closed = true
}

// Terminated returns an indication of whether the stream has been closed by invoking a terminal operation.
func (s *partitionedStream[T]) Terminated() bool {
	return s.terminated
}

// terminate terminate the stream.
func (s *partitionedStream[T]) terminate() {
	s.terminated = true
	s.closed = true
}

// valid checks if a stream is valid before performing any type of operation.
func (s *partitionedStream[T]) valid() (bool, *streamError) {
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
func (s partitionedStream[T]) Parallel() bool {
	return s.parallel
}

// Parallelize returns a parallel stream with the given level of parallelism
func (s *partitionedStream[T]) Parallelize(n int) PartitionedStream[T] {
	if n <= 1 {
		panic(errIllegalConfig("Parallelism", fmt.Sprint(n)))
	}
	return &partitionedStream[T]{
		supplier:    s.supplier,
		operations:  s.operations,
		distinct:    s.distinct,
		parallel:    true,
		maxRoutines: n,
	}
}

// Collect returns a slice containing the elements from the stream.
func (s *partitionedStream[T]) Collect() [][]T {
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
func (s *partitionedStream[T]) Map(f func(T) T) PartitionedStream[T] {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	return newPartitionedStream(s, extendOperator(uniformMap(f)))
}

// FlatMap converts the partitioned stream of elements [[]T,[]T,...] to a stream of elements []T.
func (s *partitionedStream[T]) FlatMap() Stream[T] {
	defer s.close()
	if ok, err := s.valid(); !ok {
		panic(err)
	} else if s.parallel {
		return &stream[T]{
			supplier:    parallelFlatMapSupplier(s.supplier, s.operations, s.maxRoutines),
			operations:  make([]operator[T], 0),
			parallel:    s.parallel,
			distinct:    s.distinct,
			maxRoutines: s.maxRoutines,
		}
	}
	return &stream[T]{
		supplier:    flatMapSupplier(s.supplier, s.operations),
		operations:  make([]operator[T], 0),
		distinct:    s.distinct,
		parallel:    s.parallel,
		maxRoutines: s.maxRoutines,
	}
}

// Filter returns a stream consisting of the elements of this stream that match the given predicate.
func (s *partitionedStream[T]) Filter(f func(T) bool) PartitionedStream[T] {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	return newPartitionedStream(s, extendOperator(filter(f)))
}

// Distinct returns a stream consisting of the distinct elements (according to the given hash of elements) of this stream.
func (s *partitionedStream[T]) Distinct(hash func(x T) string) PartitionedStream[T] {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	newPartitionedStream := newPartitionedStream(s, extendOperator(distinct(s.parallel, s.distinct, hash)))
	newPartitionedStream.distinct = true
	return newPartitionedStream
}

// Limit returns a stream consisting of the elements of this stream, truncated to be no longer than given length.
func (s *partitionedStream[T]) Limit(n int) PartitionedStream[T] {
	if ok, err := s.valid(); !ok {
		panic(err)
	} else if n < 0 {
		panic(errIllegalArgument("Limit", fmt.Sprint(n)))
	}
	return newPartitionedStream(s, limit[[]T](s.parallel, n))
}

// Skip returns a stream consisting of the remaining elements of this stream after discarding the first n elements of the stream.
func (s *partitionedStream[T]) Skip(n int) PartitionedStream[T] {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	return newPartitionedStream(s, skip[[]T](s.parallel, n))
}

// Count returns the count of elements in this stream.
func (s *partitionedStream[T]) Count() int {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	defer s.terminate()
	if s.parallel {
		return parallelCount(s.supplier(), s.operations, s.maxRoutines)
	}
	return count(s.supplier(), s.operations)

}

// ForEach performs an action for each element of this stream.
func (s *partitionedStream[T]) ForEach(f func([]T)) {
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
func (s *partitionedStream[T]) Peek(f func([]T)) PartitionedStream[T] {
	if ok, err := s.valid(); !ok {
		panic(err)
	}
	return newPartitionedStream(s, peek(f))
}

// Reduce performs a reduction on the elements of the stream, using ssociative accumulation function, and returns the reduced value.
// The zero value is returned if there are no elements.
func (s *partitionedStream[T]) Reduce(f func(x, y []T) []T) []T {
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
