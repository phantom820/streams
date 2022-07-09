// package streams provides java motivated stream implementation.
package streams

import (
	"github.com/phantom820/collections"
	"github.com/phantom820/collections/lists/list"
	"github.com/phantom820/collections/sets/hashset"
	"github.com/phantom820/collections/sets/treeset"
	"github.com/phantom820/collections/types"
)

type Stream[T any] interface {
	getPipeline() func() (T, bool)

	// Intermediate operations.
	Filter(f func(x T) bool) Stream[T]               // Returns a stream consisting of the elements of this stream that match the given predicate.
	Map(f func(x T) interface{}) Stream[interface{}] // Returns a stream consisting of the results of applying the given function to the elements of the stream.
	Limit(n int) Stream[T]                           // Returns consisting of the elements of the stream but only limited to processing n elements.

	// Terminal operations.
	ForEach(f func(x T))       // Performs an action specified by the function f for each element of this stream.
	Count() int                // Returns a ount of how many are processed by the stream.
	Reduce(f func(x, y T) T) T // Reduces the stream to a single result using the given associative function.

}

// stream struct to represent a stream.
type stream[T any] struct {
	sized      bool
	distinct   bool
	sorted     bool
	ordered    bool
	terminated bool             // indicates whether a terminal operation was invoked on the stream.
	closed     bool             // indicates whether the stream has been closed , all streams are auto closed once a terminal operation is invoked.
	completed  func() bool      // checks if the stream has completed processing all elements.
	source     *source[T]       // the source that produces elements for the stream.
	pipeline   func() (T, bool) // pipeline of the operations.
}

// terminate
func (stream *stream[T]) terminate() {
	stream.terminated = true
}

// getPipeline returns the pipeline of operations of the stream.
func (stream *stream[T]) getPipeline() func() (T, bool) {
	return stream.pipeline
}

// FromCollection creates a stream from the given collection.
func FromCollection[T types.Equitable[T]](collection collections.Collection[T]) Stream[T] {
	it := collection.Iterator()
	source := newSource(it.Next, it.HasNext)
	stream := &stream[T]{source: source, pipeline: emptyPipeline(source), completed: func() bool { return !(source.hasNext()) }}
	return stream
}

// Map returns a stream containing the results of applying the given mapping function to the elements of the stream. Applying this operation results in
// the underlying type of the stream being an interface since receiver methods do not support generic types.
func (inputStream stream[T]) Map(f func(x T) interface{}) Stream[interface{}] {
	newStream := stream[interface{}]{ // we already have an operation.
		pipeline: func() (interface{}, bool) {
			element, ok := inputStream.getPipeline()()
			if !ok {
				var sentinel interface{}
				return sentinel, ok
			}
			return f(element), ok
		},
		completed: inputStream.completed,
	}
	return &newStream
}

// Filter returns a stream consisting of the elements of the stream that match the given predicate.
func (inputStream *stream[T]) Filter(f func(x T) bool) Stream[T] {
	newStream := stream[T]{
		pipeline: func() (T, bool) {
			element, ok := inputStream.getPipeline()()
			if !ok {
				var sentinel T
				return sentinel, ok
			} else if !f(element) {
				var sentinel T
				return sentinel, false
			}
			return element, true
		},
		completed: inputStream.completed,
	}
	return &newStream
}

// Returns consisting of the elements of the stream but only limited to processing n elements.
func (inputStream *stream[T]) Limit(limit int) Stream[T] {
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
		completed: inputStream.completed,
	}
	return &newStream
}

// ForEach performs the given task on each element of the stream.
func (stream *stream[T]) ForEach(f func(element T)) {
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

// Reduce reduces the stream using the given associative binary function.
func (stream *stream[T]) Reduce(f func(x, y T) T) T {
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
			case 2:
				x = f(x, y)
			default:
				x = f(x, element)
			}
			count++
		}
	}
	return x
}

// ToSlice returns a slice containing the elements of the stream.
func ToSlice[T any](stream Stream[T]) []T {
	slice := make([]T, 0)
	stream.ForEach(func(x T) {
		slice = append(slice, x)
	})
	return slice
}

// ToList returns a List containing the elements of the stream.
func ToList[T types.Equitable[T]](stream Stream[T]) *list.List[T] {
	list := list.New[T]()
	stream.ForEach(func(x T) {
		list.Add(x)
	})
	return list
}

// ToHashSet returns a HashSet containing the elements of the stream.
func ToHashSet[T types.Hashable[T]](stream Stream[T]) *hashset.HashSet[T] {
	set := hashset.New[T]()
	stream.ForEach(func(x T) {
		set.Add(x)
	})
	return set
}

// ToTreeSet returns a TreeSet containing the elements of the stream.
func ToTreeSet[T types.Comparable[T]](stream Stream[T]) *treeset.TreeSet[T] {
	set := treeset.New[T]()
	stream.ForEach(func(x T) {
		set.Add(x)
	})
	return set
}
