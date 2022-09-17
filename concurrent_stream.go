package streams

import (
	"fmt"
	"sync"

	"github.com/phantom820/collections/sets/hashset"
	"github.com/phantom820/streams/sources"
)

// concurrentStream sequential stream concrete type.
type concurrentStream[T any] struct {
	source        sources.Source[T]       // source of elements for the stream.
	pipeline      func(input T) (T, bool) // pipeline with operations of the stream.
	terminated    bool                    // terminated indicates if a terminal operation has been invoked on the stream.
	closed        bool                    // closed indicates if a new stream has been derived from the stream or it has been terminated.
	concurrency   int                     // concurrency indicates the concurrency level of the stream.
	partitionSize int                     // partitionSize the number of elements each go routine should process independently.
	distinct      bool                    // distinct keeps track of whether the stream has distinc elements or not.

}

// terminate terminates the stream when a terminal operation is invoked on it.
func (stream *concurrentStream[T]) terminate() {
	stream.terminated = true
	stream.closed = true
}

// close closes the stream when a new stream is derived from it.
func (stream *concurrentStream[T]) close() {
	stream.closed = true
}

// Terminated returns termination status of the stream.
func (stream *concurrentStream[T]) Terminated() bool {
	return stream.terminated
}

// Closed returns closure status of the stream.
func (stream *concurrentStream[T]) Closed() bool {
	return stream.closed
}

// valid checks if a stream is valid for any type of operation.
func (stream *concurrentStream[T]) valid() (bool, *streamError) {
	if stream.Terminated() {
		err := errStreamTerminated()
		return false, &err
	} else if stream.Closed() {
		err := errStreamClosed()
		return false, &err
	}
	return true, nil
}

// Concurrent returns true always.
func (stream *concurrentStream[T]) Concurrent() bool {
	return true
}

// Filter returns a stream consisting of the elements of this stream that match the given predicate function.
func (stream *concurrentStream[T]) Filter(f func(element T) bool) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.close()
	source := stream.source
	pipeline := stream.pipeline
	return &concurrentStream[T]{
		source: source,
		pipeline: func(input T) (T, bool) {
			element, ok := pipeline(input)
			if !ok {
				return element, ok
			}
			return element, f(element)
		},
		partitionSize: stream.partitionSize,
		concurrency:   stream.concurrency,
		distinct:      stream.distinct,
	}
}

// Limit returns a stream consisting of the elements of this stream, truncated to be no longer than the given limit.
func (stream *concurrentStream[T]) Limit(limit int) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	} else if limit < 0 {
		panic(errIllegalArgument("Limit", fmt.Sprint(limit)))
	}
	defer stream.close()
	source := stream.source
	pipeline := stream.pipeline
	counter := atomicCounter{}
	var mutex sync.Mutex
	return &concurrentStream[T]{
		source: source,
		pipeline: func(input T) (T, bool) {
			element, ok := pipeline(input)
			if !ok {
				return element, ok
			} else {
				mutex.Lock()
				defer mutex.Unlock()
				if counter.read() < limit {
					counter.add(1)
					return element, ok
				}
				return element, false
			}
		},
		partitionSize: stream.partitionSize,
		concurrency:   stream.concurrency,
		distinct:      stream.distinct,
	}
}

// Skip returns a stream consisting of the remaining elements of this stream after skipping the first n elements of the stream.
// If this stream contains fewer than n elements then an empty stream will be returned.
func (stream *concurrentStream[T]) Skip(n int) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	} else if n < 0 {
		panic(errIllegalArgument("Skip", fmt.Sprint(n)))
	}
	defer stream.close()
	source := stream.source
	pipeline := stream.pipeline
	skipped := atomicCounter{}
	var mutex sync.Mutex
	return &concurrentStream[T]{
		source: source,
		pipeline: func(input T) (T, bool) {
			element, ok := pipeline(input)
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
		partitionSize: stream.partitionSize,
		concurrency:   stream.concurrency,
		distinct:      stream.distinct,
	}
}

// Peek returns a stream consisting of the elements of this stream, additionally performing the provided action on each element as elements are processed.
func (stream *concurrentStream[T]) Peek(f func(element T)) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.close()
	source := stream.source
	pipeline := stream.pipeline
	return &concurrentStream[T]{
		source: source,
		pipeline: func(input T) (T, bool) {
			element, ok := pipeline(input)
			if !ok {
				return element, ok
			}
			f(element)
			return element, ok
		},
		partitionSize: stream.partitionSize,
		concurrency:   stream.concurrency,
		distinct:      stream.distinct,
	}
}

// Map returns a stream consisting of the results of applying the given transformation function to the elements of this stream.
func (stream *concurrentStream[T]) Map(f func(element T) T) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.close()
	source := stream.source
	pipeline := stream.pipeline
	return &concurrentStream[T]{
		source: source,
		pipeline: func(input T) (T, bool) {
			element, ok := pipeline(input)
			if !ok {
				return element, false
			}
			return f(element), ok
		},
		partitionSize: stream.partitionSize,
		concurrency:   stream.concurrency,
		distinct:      false,
	}
}

// Distinct returns a stream consisting of the distinct element of this stream using equals and hashCode for the underlying set.
func (stream *concurrentStream[T]) Distinct(equals func(x, y T) bool, hashCode func(x T) int) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.close()
	source := stream.source
	pipeline := stream.pipeline
	set := hashset.New[entry[T]]()
	alreadyDistinct := stream.distinct
	var mutex sync.Mutex
	return &concurrentStream[T]{
		source: source,
		pipeline: func(input T) (T, bool) {
			element, ok := pipeline(input)
			if !ok {
				return element, false
			} else {
				if alreadyDistinct { // parent stream was already has distinc elements.
					return element, ok
				}
				mutex.Lock()
				defer mutex.Unlock()
				if set.Contains(entry[T]{value: element, equals: equals, hashCode: hashCode}) {
					return element, false
				}
				set.Add(entry[T]{value: element, equals: equals, hashCode: hashCode})
				return element, true
			}
		},
		partitionSize: stream.partitionSize,
		concurrency:   stream.concurrency,
		distinct:      true,
	}
}

// ForEach performs an action for each element of this stream.
func (stream *concurrentStream[T]) ForEach(f func(element T)) {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()
	limiter := make(chan struct{}, stream.concurrency)
	scatterForEach(stream.source, stream.partitionSize, stream.pipeline, f, limiter)
}

// Count returns the count of elements in this stream.
func (stream *concurrentStream[T]) Count() int {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()
	limiter := make(chan struct{}, stream.concurrency)
	return scatterCount(stream.source, stream.partitionSize, stream.pipeline, limiter)
}

// Reduce performs a reduction on the elements of this stream, using an associative function.
func (stream *concurrentStream[T]) Reduce(f func(x, y T) T) (T, bool) {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()
	limiter := make(chan struct{}, stream.concurrency)
	return scatterReduce(stream.source, stream.partitionSize, stream.pipeline, f, limiter)
}

// Collect returns a slice containing the resulting elements from processing the stream.
func (stream *concurrentStream[T]) Collect() []T {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()
	limiter := make(chan struct{}, stream.concurrency)
	return scatterGather(stream.source, stream.partitionSize, stream.pipeline, limiter)

}
