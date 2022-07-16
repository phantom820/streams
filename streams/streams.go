// package streams provides java motivated stream implementation.
package streams

import (
	"fmt"

	"github.com/phantom820/collections"
	"github.com/phantom820/collections/lists/list"
	"github.com/phantom820/collections/sets/hashset"
	"github.com/phantom820/collections/sets/treeset"
	"github.com/phantom820/collections/types"
)

// Stream interface. Any modifications made to the source before the stream is terminated will be visible
// to the stream. We lose the concrete type of stream elements once a map operation is invoked and it falls on the user to
// perform the relevant type casts :( .
type Stream[T any] interface {

	// Intermediate operations.
	Filter(f func(x T) bool) Stream[T]                               // Returns a stream consisting of the elements of this stream that match the given predicate.
	Map(f func(x T) interface{}) Stream[interface{}]                 // Returns a stream consisting of the results of applying the given function to the elements of the stream.
	Limit(n int) Stream[T]                                           // Returns a stream consisting of the elements of the stream but only limited to processing n elements.
	Skip(n int) Stream[T]                                            // Returns a stream that skips the first n elements it encounters in processing.
	Distinct(equals func(x, y T) bool, hash func(x T) int) Stream[T] // Returns a stream consisting of distinct elements. Elements are distinguished using equality and hash code.

	// Terminal operations.
	ForEach(f func(x T))                                   // Performs an action specified by the function f for each element of this stream.
	Count() int                                            // Returns a count of how many are processed by the stream.
	Reduce(f func(x, y T) interface{}) (interface{}, bool) // Returns the result of applying the associative binary function on elements of the stream. The binary operator is only applied if the are
	// at least 2 elements in the stream, otherwise the returned result is invalid and will be indicated by the second returned value.

	// Util.
	Terminated() bool // Checks if a terminal operation has been invoked on the stream.

}

// terinationStatus only exists as means to share the termination status across parent and child streams.
type terminationStatus struct {
	status bool
}

// stream struct to represent a stream. For a given source any modifications made to it before the stream is terminated will be visible
// to the stream.
type stream[T any] struct {
	distinct          bool               // indicates if the stream consist of distinct elements only , i.e say we constructed this from a set.
	terminationStatus *terminationStatus // indicates whether a terminal operation was invoked on the stream.
	// isTerminated      func() bool        // checks if the stream has been terminated or any stream it was derived from/ stream derived from it.
	closed    bool             // indicates whether the stream has been closed , all streams are auto closed once a terminal operation is invoked.
	completed func() bool      // checks if the stream has completed processing all elements.
	source    *source[T]       // the source that produces elements for the stream.
	pipeline  func() (T, bool) // pipeline of the operations.
}

// terminate this terinates the stream and sets its source to nil.
func (stream *stream[T]) terminate() {
	stream.terminationStatus.status = true
	stream.closed = true
	stream.source = nil
	stream.pipeline = nil
	stream.completed = nil
}

// isTerminated checks if the stream or its parent has been terminated.
func (stream *stream[T]) Terminated() bool {
	return stream.terminationStatus.status
}

// getPipeline returns the pipeline of operations of the stream.
func (stream *stream[T]) getPipeline() func() (T, bool) {
	return stream.pipeline
}

// FromSource creates a stream from the given source. The source can be finite/ infinite.
func FromSource[T any](source Source[T]) Stream[T] {
	_source := newSource(func() T { return source.Next() }, func() bool { return source.HasNext() })
	terminationStatus := terminationStatus{false}
	stream := &stream[T]{
		source:            _source,
		pipeline:          emptyPipeline(_source),
		completed:         func() bool { return !(_source.hasNext()) },
		terminationStatus: &terminationStatus,
	}
	return stream
}

// FromSet creates a stream from the given set. All changes made to the set before the stream is terminated
// are visible to the stream. We currently do not need this.
// func FromSet[T types.Equitable[T]](set sets.Set[T]) Stream[T] {
// 	it := set.Iterator()
// 	source := newSource(it.Next, it.HasNext)
// 	terminated := false
// 	stream := &stream[T]{
// 		source:           source,
// 		distinct:         true,
// 		pipeline:         emptyPipeline(source),
// 		completed:        func() bool { return !(source.hasNext()) },
// 		terminated:       &terminated,
// 		parentTerminated: &terminated,
// 	}
// 	return stream
// }

// FromCollection creates a stream from the given collection. All changes made to the collection before the stream is terminated
// are visible to the stream. Creating from a specific collection is recommended i.e FromSet .
func FromCollection[T types.Equitable[T]](collection collections.Collection[T]) Stream[T] {
	it := collection.Iterator()
	source := newSource(it.Next, it.HasNext)
	terminationStatus := terminationStatus{false}
	stream := &stream[T]{
		source:            source,
		pipeline:          emptyPipeline(source),
		completed:         func() bool { return !(source.hasNext()) },
		terminationStatus: &terminationStatus,
	}
	return stream
}

// FromSlice creates a stream by using the callback to retrieve the underlying slice. All changes made to the slice before the stream is terminated
// are visible to the stream.
func FromSlice[T any](f func() []T) Stream[T] {
	source := newSourceFromSlice(f)
	terminationStatus := terminationStatus{false}
	stream := stream[T]{
		source:            source,
		pipeline:          emptyPipeline(source),
		completed:         func() bool { return !(source.hasNext()) },
		terminationStatus: &terminationStatus,
	}
	return &stream
}

// Map returns a stream containing the results of applying the given mapping function to the elements of the stream. Applying this operation results in
// the underlying type of the stream being an interface since receiver methods do not support generic types.
func (inputStream *stream[T]) Map(f func(x T) interface{}) Stream[interface{}] {
	if inputStream.Terminated() {
		panic(ErrStreamTerminated())
	}
	newStream := stream[interface{}]{
		pipeline: func() (interface{}, bool) {
			element, ok := inputStream.getPipeline()()
			if !ok {
				var sentinel interface{}
				return sentinel, ok
			}
			return f(element), ok
		},
		distinct:          false,
		completed:         inputStream.completed,
		terminationStatus: inputStream.terminationStatus,
	}
	return &newStream
}

// Filter returns a stream consisting of the elements of the stream that match the given predicate.
func (inputStream *stream[T]) Filter(f func(x T) bool) Stream[T] {
	if inputStream.Terminated() {
		panic(ErrStreamTerminated())
	}
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
		distinct:          inputStream.distinct,
		completed:         inputStream.completed,
		terminationStatus: inputStream.terminationStatus,
	}
	return &newStream
}

// Returns a stream that is limited to only producing n elements. Will panic if limit is negative.
func (inputStream *stream[T]) Limit(limit int) Stream[T] {
	if inputStream.Terminated() {
		panic(ErrStreamTerminated())
	} else if limit < 0 {
		panic(ErrIllegalArgument("Limit", fmt.Sprint(limit)))
	}
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
		distinct:          inputStream.distinct,
		terminationStatus: inputStream.terminationStatus,
	}
	return &newStream
}

// Returns a stream that skips the first n elements in processing. Will panic if number of elements to skip is negative.
func (inputStream *stream[T]) Skip(skip int) Stream[T] {
	if inputStream.Terminated() {
		panic(ErrStreamTerminated())
	} else if skip < 0 {
		panic(ErrIllegalArgument("Skip", fmt.Sprint(skip)))
	}
	skipped := 0
	newStream := stream[T]{
		pipeline: func() (T, bool) {
			element, ok := inputStream.getPipeline()()
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
		distinct:          inputStream.distinct,
		completed:         inputStream.completed,
		terminationStatus: inputStream.terminationStatus,
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
	if inputStream.Terminated() {
		panic(ErrStreamTerminated())
	}
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
		distinct:          true,
		completed:         inputStream.completed,
		terminationStatus: inputStream.terminationStatus,
	}
	return &newStream
}

// ForEach performs the given task on each element of the stream.
func (stream *stream[T]) ForEach(f func(element T)) {
	if stream.Terminated() {
		panic(ErrStreamTerminated())
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
	if stream.Terminated() {
		panic(ErrStreamTerminated())
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
func (stream *stream[T]) Reduce(f func(x, y T) interface{}) (interface{}, bool) {
	if stream.Terminated() {
		panic(ErrStreamTerminated())
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
				x = f(x, y).(T)
				break
			case 2:
				x = f(x, element).(T)
				break
			default:
				x = f(x, element).(T)
				break
			}
			count++
		}
	}

	if count < 2 {
		var zero T
		return zero, false
	}

	return x, true
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
