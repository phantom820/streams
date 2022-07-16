// package streams provides java motivated stream implementation.
package streams

import (
	"fmt"
	"sync"

	"github.com/phantom820/collections"
	"github.com/phantom820/collections/sets/hashset"
	"github.com/phantom820/collections/types"
	"github.com/phantom820/streams/sources"
)

// ConcurrentStream interface. Any modifications made to the source before the stream is terminated will be visible
// to the stream. We lose the concrete type of stream elements once a map operation is invoked and it falls on the user to
// perform the relevant type casts :( .
// concurrentStream struct to represent a stream. For a given source any modifications made to it before the stream is terminated will be visible
// to the stream. The source has to be finite.
type concurrentStream[T any] struct {
	maxcConcurrency   int                   // maximum number of partitions that can be made.
	distinct          bool                  // indicates if the stream consist of distinct elements only , i.e say we constructed this from a set.
	terminationStatus *terminationStatus    // indicates whether a terminal operation was invoked on the stream.
	closed            bool                  // indicates whether the stream has been closed , all streams are auto closed once a terminal operation is invoked.
	completed         func(i int) bool      // checks if the stream has completed processing all elements.
	pipeline          func(i int) (T, bool) // pipeline of the operations.
	partition         func() int            // partitions the source and returns the number of partitions/
}

// terminate this terinates the stream and sets its source to nil.
func (stream *concurrentStream[T]) terminate() {
	stream.terminationStatus.status = true
	stream.closed = true
	stream.pipeline = nil
	stream.completed = nil
}

// isTerminated checks if the stream or its parent has been terminated.
func (stream *concurrentStream[T]) Terminated() bool {
	return stream.terminationStatus.status
}

// getPipeline returns the pipeline of operations of the stream.
func (stream *concurrentStream[T]) getPipeline() func(i int) (T, bool) {
	return stream.pipeline
}

// FromCollection creates a stream from the given collection. All changes made to the collection before the stream is terminated
// are visible to the stream. Creating from a specific collection is recommended i.e FromSet .
func ConcurrentFromCollection[T types.Equitable[T]](collection collections.Collection[T], maxConcurrecny int) Stream[T] {
	it := collection.Iterator()
	source := sources.NewSource(it.Next, it.HasNext)
	concurrentSource := sources.NewConcurrentSource(source)
	terminationStatus := terminationStatus{false}
	stream := &concurrentStream[T]{
		pipeline:          emptyConcurrentPipeline(concurrentSource),
		completed:         func(i int) bool { return !(concurrentSource.GetPartition(i).HasNext()) },
		terminationStatus: &terminationStatus,
		partition:         func() int { return concurrentSource.Partition(maxConcurrecny) },
	}
	return stream
}

// FromSlice creates a stream by using the callback to retrieve the underlying slice. All changes made to the slice before the stream is terminated
// are visible to the stream.
func ConcurrentFromSlice[T any](f func() []T, maxConcurrency int) Stream[T] {
	source := sources.NewSourceFromSlice(f)
	concurrentSource := sources.NewConcurrentSource(source)
	terminationStatus := terminationStatus{false}
	stream := concurrentStream[T]{
		pipeline:          emptyConcurrentPipeline(concurrentSource),
		completed:         func(i int) bool { return !(concurrentSource.GetPartition(i).HasNext()) },
		terminationStatus: &terminationStatus,
		partition:         func() int { return concurrentSource.Partition(maxConcurrency) },
	}
	return &stream
}

// Map returns a stream containing the results of applying the given mapping function to the elements of the stream. Applying this operation results in
// the underlying type of the stream being an interface since receiver methods do not support generic types.
func (inputStream *concurrentStream[T]) Map(f func(x T) interface{}) Stream[interface{}] {
	if inputStream.Terminated() {
		panic(ErrStreamTerminated())
	}
	newStream := concurrentStream[interface{}]{
		pipeline: func(i int) (interface{}, bool) {
			element, ok := inputStream.pipeline(i)
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
func (inputStream *concurrentStream[T]) Filter(f func(x T) bool) Stream[T] {
	if inputStream.Terminated() {
		panic(ErrStreamTerminated())
	}
	newStream := concurrentStream[T]{
		pipeline: func(i int) (T, bool) {
			element, ok := inputStream.pipeline(i)
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
func (inputStream *concurrentStream[T]) Limit(limit int) Stream[T] {
	if inputStream.Terminated() {
		panic(ErrStreamTerminated())
	} else if limit < 0 {
		panic(ErrIllegalArgument("Limit", fmt.Sprint(limit)))
	}
	n := 0
	newStream := concurrentStream[T]{
		pipeline: func(i int) (T, bool) {
			element, ok := inputStream.getPipeline()(i)
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
		completed: func(i int) bool {
			if inputStream.completed(i) || n >= limit {
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
func (inputStream *concurrentStream[T]) Skip(skip int) Stream[T] {
	if inputStream.Terminated() {
		panic(ErrStreamTerminated())
	} else if skip < 0 {
		panic(ErrIllegalArgument("Skip", fmt.Sprint(skip)))
	}
	skipped := 0
	newStream := concurrentStream[T]{
		pipeline: func(i int) (T, bool) {
			element, ok := inputStream.pipeline(i)
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

// Distinct returns a stream consisting of distinct elements. Elements are distinguished using equality and hash code.
func (inputStream *concurrentStream[T]) Distinct(equals func(x, y T) bool, hashCode func(x T) int) Stream[T] {
	if inputStream.Terminated() {
		panic(ErrStreamTerminated())
	}
	set := hashset.New[element[T]]()
	newStream := concurrentStream[T]{
		pipeline: func(i int) (T, bool) {
			item, ok := inputStream.pipeline(i)
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

// collect collects the result from a particular partition into a slice.
func collect[T any](wg *sync.WaitGroup, result chan []T, stream *concurrentStream[T], i int) {
	defer wg.Done()
	data := make([]T, 0)
	for !stream.completed(i) {
		element, ok := stream.pipeline(i)
		if ok {
			data = append(data, element)
		}
	}
	result <- data
}

// ForEach performs the given task on each element of the stream.
func (stream *concurrentStream[T]) ForEach(f func(element T)) {
	if stream.Terminated() {
		panic(ErrStreamTerminated())
	}
	defer stream.terminate()
	var wg sync.WaitGroup
	n := stream.partition()
	results := make(chan []T, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go collect(&wg, results, stream, i)
	}
	wg.Wait()
	close(results)
	for result := range results {
		for _, element := range result {
			f(element)
		}
	}
}

// count performs a count of the number of elements in a partition.
func count[T any](wg *sync.WaitGroup, result chan int, stream *concurrentStream[T], i int) {
	defer wg.Done()
	count := 0
	for !stream.completed(i) {
		_, ok := stream.pipeline(i)
		if ok {
			count++
		}
	}
	result <- count
}

// Count returns a count of how many elements are in the stream.
func (stream *concurrentStream[T]) Count() int {
	if stream.Terminated() {
		panic(ErrStreamTerminated())
	}
	defer stream.terminate()
	var wg sync.WaitGroup
	n := stream.partition()
	results := make(chan int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go count(&wg, results, stream, i)
	}
	wg.Wait()
	close(results)
	count := 0
	for result := range results {
		count = count + result
	}
	return count
}

// Reduce returns the result of applying the associative binary function on elements of the stream. The binary operator is only applied if the are
// at least 2 elements in the stream, otherwise the returned result is invalid and will be indicated by the second returned value.
func (stream *concurrentStream[T]) Reduce(f func(x, y T) interface{}) (interface{}, bool) {
	if stream.Terminated() {
		panic(ErrStreamTerminated())
	}
	defer stream.terminate()
	// pipeline := stream.getPipeline()
	// count := 0
	var x T
	// for !stream.completed() {
	// 	element, ok := pipeline()
	// 	if ok {
	// 		switch count {
	// 		case 0:
	// 			x = element
	// 			break
	// 		case 1:
	// 			y = element
	// 			x = f(x, y).(T)
	// 			break
	// 		case 2:
	// 			x = f(x, element).(T)
	// 			break
	// 		default:
	// 			x = f(x, element).(T)
	// 			break
	// 		}
	// 		count++
	// 	}
	// }

	// if count < 2 {
	// 	var zero T
	// 	return zero, false
	// }

	return x, true
}

// Collect returns a slice containing the output elements of the stream.
func (stream *concurrentStream[T]) Collect() []T {
	slice := make([]T, 0)
	stream.ForEach(func(element T) {
		slice = append(slice, element)
	})
	return slice
}
