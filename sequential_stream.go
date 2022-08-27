package streams

import (
	"fmt"

	"github.com/phantom820/collections"
	"github.com/phantom820/collections/sets/hashset"
	"github.com/phantom820/collections/types"
	"github.com/phantom820/streams/sources"
)

// stream a sequential stream implementation. The stream is lazy evaluated and any modification made to its source before a terminal operation is invoked
// on the stream will be visible to the stream. A stream can be finite/ infinite based on ots source, for a infinite stream a limit operation shoudl always
// be applied to avoid an infinite loop when trying to process the stream.
type stream[T any] struct {
	terminated bool             // indicates whether the stream has been closed.
	closed     bool             // indicates whether the stream has been closed , all streams are auto closed once a terminal operation is invoked.
	completed  func() bool      // checks if the stream has completed processing all elements.
	pipeline   func() (T, bool) // pipeline of the operations.
}

// terminate this terminates the stream and sets some properties to nil.
func (stream *stream[T]) terminate() {
	stream.terminated = true
	stream.closed = true
	stream.pipeline = nil
	stream.completed = nil
}

// Concurrent always returns false for a sequential stream.
func (stream *stream[T]) Concurrent() bool {
	return false
}

// closes the stream, i,e another stream has been derived from it.
func (stream *stream[T]) close() {
	stream.closed = true
}

// Closed chekcs if the stream is closed which means that another stream was derived from it.
func (stream *stream[T]) Closed() bool {
	return stream.closed
}

// Terminated checks if a terminal operation has been invoked on the stream.
func (stream *stream[T]) Terminated() bool {
	return stream.terminated
}

// valid if a stream is valid for any type of operation.
func (stream *stream[T]) valid() (bool, *Error) {
	if stream.Terminated() {
		err := ErrStreamTerminated()
		return false, &err
	} else if stream.Closed() {
		err := ErrStreamClosed()
		return false, &err
	}
	return true, nil
}

// getPipeline returns the pipeline of operations of the stream.
func (stream *stream[T]) getPipeline() func() (T, bool) {
	return stream.pipeline
}

// fromSource creates a stream from the given source. The source can be finite/ infinite.
func fromSource[T any](source sources.Source[T]) Stream[T] {
	stream := &stream[T]{
		pipeline:   emptyPipeline(source),
		completed:  func() bool { return !(source.HasNext()) },
		terminated: false,
	}
	return stream
}

// fromCollection creates a stream from the given collection. All changes made to the collection before the stream is terminated
// are visible to the stream. Creating from a specific collection is recommended i.e FromSet .
func fromCollection[T types.Equitable[T]](collection collections.Collection[T]) Stream[T] {
	it := collection.Iterator()
	source := sources.NewSource(it.Next, it.HasNext)

	stream := &stream[T]{
		pipeline:   emptyPipeline(source),
		completed:  func() bool { return !(source.HasNext()) },
		terminated: false,
	}
	return stream
}

// fromSlice creates a stream by using the callback to retrieve the underlying slice. All changes made to the slice before the stream is terminated
// are visible to the stream.
func fromSlice[T any](f func() []T) Stream[T] {
	source := sources.NewSourceFromSlice(f)
	stream := stream[T]{
		pipeline:   emptyPipeline(source),
		completed:  func() bool { return !(source.HasNext()) },
		terminated: false,
	}
	return &stream
}

// Map returns a stream containing the results of applying the given transformation function to the elements of the stream.
func (inputStream *stream[T]) Map(f func(x T) T) Stream[T] {
	if ok, err := inputStream.valid(); !ok {
		panic(err)
	}
	defer inputStream.close()
	newStream := stream[T]{
		pipeline: func() (T, bool) {
			element, ok := inputStream.getPipeline()()
			if !ok {
				var sentinel T
				return sentinel, ok
			}
			return f(element), ok
		},
		completed:  inputStream.completed,
		terminated: false,
	}
	return &newStream
}

// Filter returns a stream consisting of the elements of the stream that match the given predicate.
func (inputStream *stream[T]) Filter(f func(x T) bool) Stream[T] {
	if ok, err := inputStream.valid(); !ok {
		panic(err)
	}
	defer inputStream.close()
	newStream := stream[T]{
		pipeline: func() (T, bool) {
			element, ok := inputStream.pipeline()
			if !ok {
				var sentinel T
				return sentinel, ok
			} else if !f(element) {
				var sentinel T
				return sentinel, false
			}
			return element, true
		},
		completed:  inputStream.completed,
		terminated: false,
	}
	return &newStream
}

// Returns a stream that is limited to only producing n elements. Will panic if limit is negative.
func (inputStream *stream[T]) Limit(limit int) Stream[T] {
	if ok, err := inputStream.valid(); !ok {
		panic(err)
	} else if limit < 0 {
		panic(ErrIllegalArgument("Limit", fmt.Sprint(limit)))
	}
	defer inputStream.close()
	n := 0
	newStream := stream[T]{
		pipeline: func() (T, bool) {
			element, ok := inputStream.getPipeline()()
			if !ok {
				return element, ok
			} else {
				if n < limit {
					n++
					return element, true
				}
				return element, false
			}
		},
		completed: func() bool {
			if inputStream.completed() || n >= limit {
				return true
			}
			return false
		},
		terminated: false,
	}
	return &newStream
}

// Returns a stream that skips the first n elements in processing. Will panic if number of elements to skip is negative.
func (inputStream *stream[T]) Skip(skip int) Stream[T] {
	if ok, err := inputStream.valid(); !ok {
		panic(err)
	}
	defer inputStream.close()
	skipped := 0
	newStream := stream[T]{
		pipeline: func() (T, bool) {
			element, ok := inputStream.pipeline()
			if !ok {
				return element, ok
			} else {
				if skipped < skip {
					skipped++
					return element, false
				}
				return element, true
			}
		},
		completed:  inputStream.completed,
		terminated: false,
	}
	return &newStream
}

// element this type allows us to use sets for the Distinct operation.
type element[T any] struct {
	value    T
	equals   func(a, b T) bool
	hashCode func(a T) int
}

// Equals required by Hashable for using a set.
func (a element[T]) Equals(b element[T]) bool {
	return a.equals(a.value, b.value)
}

// HashCode produces the hash code of the element.
func (a element[T]) HashCode() int {
	return a.hashCode(a.value)
}

// Distinct returns a stream consisting of distinct elements. Elements are distinguished using equality and hash code.
func (inputStream *stream[T]) Distinct(equals func(x, y T) bool, hashCode func(x T) int) Stream[T] {
	if ok, err := inputStream.valid(); !ok {
		panic(err)
	}
	defer inputStream.close()
	set := hashset.New[element[T]]()
	newStream := stream[T]{
		pipeline: func() (T, bool) {
			item, ok := inputStream.pipeline()
			if !ok {
				return item, false
			} else if set.Contains(element[T]{value: item, equals: equals, hashCode: hashCode}) {
				var sentinel T
				return sentinel, false
			}
			set.Add(element[T]{value: item, equals: equals, hashCode: hashCode})
			return item, true
		},
		completed:  inputStream.completed,
		terminated: false,
	}
	return &newStream
}

// Peek Returns a stream consisting of the elements of the given stream but additionaly the given function is invoked for each element.
func (inputStream *stream[T]) Peek(f func(x T)) Stream[T] {
	if ok, err := inputStream.valid(); !ok {
		panic(err)
	}
	defer inputStream.close()
	newStream := stream[T]{
		pipeline: func() (T, bool) {
			element, ok := inputStream.pipeline()
			if !ok {
				return element, ok
			}
			f(element)
			return element, ok
		},
		completed:  inputStream.completed,
		terminated: false,
	}
	return &newStream
}

// ForEach performs the given task on each element of the stream.
func (stream *stream[T]) ForEach(f func(element T)) {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()
	pipeline := stream.getPipeline()
	for !stream.completed() {
		element, ok := pipeline()
		if ok {
			f(element)
		}
	}
}

// Count returns a count of how many elements are in the stream.
func (stream *stream[T]) Count() int {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()
	count := 0
	pipeline := stream.getPipeline()
	for !stream.completed() {
		_, ok := pipeline()
		if ok {
			count++
		}
	}
	return count
}

// Reduce returns the result of applying the associative binary function on elements of the stream. The binary operator is only applied if the are
// at least 2 elements in the stream, otherwise the returned result is invalid and will be indicated by the second returned value.
func (stream *stream[T]) Reduce(f func(x, y T) T) (T, bool) {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()
	pipeline := stream.getPipeline()
	count := 0
	var x, y T
	for !stream.completed() {
		element, ok := pipeline()
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

// Collect returns a slice containing the output elements of the stream.
func (stream *stream[T]) Collect() []T {
	slice := make([]T, 0)
	stream.ForEach(func(element T) {
		slice = append(slice, element)
	})
	return slice
}
