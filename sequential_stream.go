package streams

import (
	"fmt"
	"sync"

	"github.com/phantom820/collections/sets/hashset"
	"github.com/phantom820/streams/sources"
)

// sequentialStream sequential stream implementation.
type sequentialStream[T any] struct {
	source     sources.Source[T]       // source of elements for the stream.
	pipeline   func(input T) (T, bool) // pipeline with operations of the stream.
	terminated bool                    // terminated indicates if a terminal operation has been invoked on the stream.
	closed     bool                    //closed indicates if a new stream has been derived from the stream or it has been terminated.
}

// terminate terminates the stream when a terminal operation is invoked on it.
func (stream *sequentialStream[T]) terminate() {
	stream.terminated = true
	stream.closed = true
}

// close closes the stream when a new stream is derived from it.
func (stream *sequentialStream[T]) close() {
	stream.closed = true
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
		source: stream.source,
		pipeline: func(input T) (T, bool) {
			element, ok := stream.pipeline(input)
			if !ok {
				return element, ok
			}
			return element, f(element)
		},
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
	counter := 0
	return &sequentialStream[T]{
		source: stream.source,
		pipeline: func(input T) (T, bool) {
			element, ok := stream.pipeline(input)
			if !ok {
				return element, ok
			} else {
				if counter < limit {
					counter++
					return element, ok
				}
				return element, false
			}
		},
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
	skipped := atomicCounter{}
	var mutex sync.Mutex
	return &sequentialStream[T]{
		source: stream.source,
		pipeline: func(input T) (T, bool) {
			element, ok := stream.pipeline(input)
			if !ok {
				return element, ok
			} else {
				mutex.Lock()
				defer mutex.Unlock()
				if skipped.read() < n {
					skipped.add(1)
					return element, false
				}
				return element, true
			}
		},
	}
}

// Peek returns a stream consisting of the elements of this stream, additionally performing the provided action on each element as elements are processed.
func (stream *sequentialStream[T]) Peek(f func(element T)) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.close()
	return &sequentialStream[T]{
		source: stream.source,
		pipeline: func(input T) (T, bool) {
			element, ok := stream.pipeline(input)
			if !ok {
				return element, ok
			}
			f(element)
			return element, ok
		},
	}
}

// Map returns a stream consisting of the results of applying the given transformation function to the elements of this stream.
func (stream *sequentialStream[T]) Map(f func(element T) T) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.close()
	return &sequentialStream[T]{
		source: stream.source,
		pipeline: func(input T) (T, bool) {
			element, ok := stream.pipeline(input)
			if !ok {
				return element, false
			}
			return f(element), ok
		},
	}
}

// Distinct returns a stream consisting of the distinct element of this stream using equals and hashCode for the underlying set.
func (stream *sequentialStream[T]) Distinct(equals func(x, y T) bool, hashCode func(x T) int) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.close()
	set := hashset.New[entry[T]]()
	var mutex sync.Mutex
	return &sequentialStream[T]{
		source: stream.source,
		pipeline: func(input T) (T, bool) {
			element, ok := stream.pipeline(input)
			if !ok {
				return element, false
			} else {
				mutex.Lock()
				defer mutex.Unlock()
				if set.Contains(entry[T]{value: element, equals: equals, hashCode: hashCode}) {
					return element, false
				}
				set.Add(entry[T]{value: element, equals: equals, hashCode: hashCode})
				return element, true
			}
		},
	}
}

// ForEach performs an action for each element of this stream.
func (stream *sequentialStream[T]) ForEach(f func(element T)) {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()
	source := stream.source
	pipeline := stream.pipeline
	for source.HasNext() {
		element, ok := pipeline(source.Next())
		if ok {
			f(element)
		}
	}
}

// Count returns the count of elements in this stream.
func (stream *sequentialStream[T]) Count() int {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()
	source := stream.source
	pipeline := stream.pipeline
	counter := 0
	for source.HasNext() {
		_, ok := pipeline(source.Next())
		if ok {
			counter++
		}
	}
	return counter
}

// Reduce performs a reduction on the elements of this stream, using an associative function.
func (stream *sequentialStream[T]) Reduce(f func(x, y T) T) (T, bool) {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()
	source := stream.source
	pipeline := stream.pipeline
	count := 0
	var x, y T
	for source.HasNext() {
		element, ok := pipeline(source.Next())
		if ok {
			switch count {
			case 0:
				x = element
				break
			case 1:
				y = element
				x = f(x, y)
				break
			case 2:
				x = f(x, element)
				break
			default:
				x = f(x, element)
				break
			}
			count++
		}
	}
	if count < 1 {
		var zero T
		return zero, false
	}

	return x, true
}

// Collect returns a slice containing the resulting elements from processing the stream.
func (stream *sequentialStream[T]) Collect() []T {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()
	slice := make([]T, 0)
	source := stream.source
	pipeline := stream.pipeline
	for source.HasNext() {
		element, ok := pipeline(source.Next())
		if ok {
			slice = append(slice, element)
		}
	}
	return slice
}
