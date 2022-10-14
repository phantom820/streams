package streams

import (
	"fmt"

	"github.com/phantom820/streams/operator"
)

// sequentialStream sequential stream implementation.
type sequentialStream[T any] struct {
	data                  func() []T                         // The callback for retrieving the data the stream will process
	intermediateOperators []operator.IntermediateOperator[T] // The sequence of operations that the stream will apply to elements.
	terminated            bool                               // Indicates if a terminal operation has been invoked on the stream.
	closed                bool                               // Indicates if a new stream has been derived from the stream or it has been terminated.
	distinct              bool                               // Keeps track of whether the stream has distinc elements or not.
}

// terminate terminates the stream when a terminal operation is invoked on it.
func (stream *sequentialStream[T]) terminate() {
	stream.terminated = true
	stream.closed = true
	stream.intermediateOperators = nil
}

// close closes the stream when a new stream is derived from it.
func (stream *sequentialStream[T]) close() {
	stream.closed = true
	stream.intermediateOperators = nil
}

// valid checks if a stream is valid for any type of operation.
func (stream *sequentialStream[T]) valid() (bool, *streamError) {
	if stream.Terminated() {
		err := errStreamTerminated()
		return false, &err
	} else if stream.Closed() {
		err := errStreamClosed()
		return false, &err
	}
	return true, nil
}

// Terminated returns termination status of the stream.
func (stream *sequentialStream[T]) Terminated() bool {
	return stream.terminated
}

// Closed returns closure status of the stream.
func (stream *sequentialStream[T]) Closed() bool {
	return stream.closed
}

// Concurrent returns false always since its a sequential stream..
func (stream *sequentialStream[T]) Concurrent() bool {
	return false
}

// Filter returns a stream consisting of the elements of this stream that match the given predicate function.
func (stream *sequentialStream[T]) Filter(f func(element T) bool) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.close()

	return &sequentialStream[T]{
		data:                  stream.data,
		intermediateOperators: append(stream.intermediateOperators, operator.Filter(f)),
		distinct:              stream.distinct,
	}
}

// Limit returns a stream consisting of the elements of this stream, truncated to be no longer than the given limit.
func (stream *sequentialStream[T]) Limit(limit int) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	} else if limit < 0 {
		panic(errIllegalArgument("Limit", fmt.Sprint(limit)))
	}
	defer stream.close()

	return &sequentialStream[T]{
		data:                  stream.data,
		intermediateOperators: append(stream.intermediateOperators, operator.Limit[T](limit)),
		distinct:              stream.distinct,
	}

}

// Skip returns a stream consisting of the remaining elements of this stream after skipping the first n elements of the stream.
// If this stream contains fewer than n elements then an empty stream will be returned.
func (stream *sequentialStream[T]) Skip(n int) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	} else if n < 0 {
		panic(errIllegalArgument("Skip", fmt.Sprint(n)))
	}
	defer stream.close()

	return &sequentialStream[T]{
		data:                  stream.data,
		intermediateOperators: append(stream.intermediateOperators, operator.Skip[T](n)),
		distinct:              stream.distinct,
	}
}

// Peek returns a stream consisting of the elements of this stream, additionally performing the provided action on each element as elements are processed.
func (stream *sequentialStream[T]) Peek(f func(element T)) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.close()

	return &sequentialStream[T]{
		data:                  stream.data,
		intermediateOperators: append(stream.intermediateOperators, operator.Peek(f)),
		distinct:              stream.distinct,
	}
}

// Map returns a stream consisting of the results of applying the given transformation function to the elements of this stream.
func (stream *sequentialStream[T]) Map(f func(element T) T) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.close()

	return &sequentialStream[T]{
		data:                  stream.data,
		intermediateOperators: append(stream.intermediateOperators, operator.Map(f)),
		distinct:              false,
	}
}

// Distinct returns a stream consisting of the distinct element of this stream using equals and hashCode for the underlying set.
func (stream *sequentialStream[T]) Distinct(equals func(x, y T) bool, hashCode func(x T) int) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.close()

	alreadyDistinct := stream.distinct

	return &sequentialStream[T]{
		data:                  stream.data,
		intermediateOperators: append(stream.intermediateOperators, operator.Distinct(alreadyDistinct, equals, hashCode)),
		distinct:              true,
	}
}

// ForEach performs an action for each element of this stream.
func (stream *sequentialStream[T]) ForEach(f func(element T)) {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()

	data := stream.data()
	operators := operator.Sort(stream.intermediateOperators)
	forEach(f, operators, data)

}

// Count returns the count of elements in this stream.
func (stream *sequentialStream[T]) Count() int {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()

	data := stream.data()
	operators := operator.Sort(stream.intermediateOperators)

	return count(operators, data)
}

// Reduce performs a reduction on the elements of this stream, using an associative function.
func (stream *sequentialStream[T]) Reduce(f func(x, y T) T) (T, bool) {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()

	data := stream.data()
	operators := operator.Sort(stream.intermediateOperators)

	return reduce(f, operators, data)
}

// Collect returns a slice containing the resulting elements from processing the stream.
func (stream *sequentialStream[T]) Collect() []T {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()

	data := stream.data()
	operators := operator.Sort(stream.intermediateOperators)

	return collect(operators, data)
}
