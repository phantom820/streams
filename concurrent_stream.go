// package streams provides java motivated stream implementation.
package streams

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/phantom820/collections"
	"github.com/phantom820/collections/sets/hashset"
	"github.com/phantom820/collections/types"
	"github.com/phantom820/streams/sources"
)

// concurrentSTream represent a stream implementation in which elements can be operated on concurrently. The underlying source has to be finite in order
// to avoid an infinite loop when trying to split stream elements for concurrent processing.
type concurrentStream[T any] struct {
	maxcConcurrency int                   // maximum number of go routines to use when processing the stream
	distinct        bool                  // indicates if the stream consist of distinct elements only , i.e say we constructed this from a set.
	terminated      bool                  // indicates whether a terminal operation was invoked on the stream.
	closed          bool                  // indicates whether the stream has been closed , all streams are auto closed once a terminal operation is invoked.
	completed       func(i int) bool      // checks if the stream has completed processing all elements.
	pipeline        func(i int) (T, bool) // pipeline of the operations.
	partition       func() int            // partitions the source and returns the number of partitions.
}

// terminate this terinates the stream and sets its source to nil.
func (stream *concurrentStream[T]) terminate() {
	stream.terminated = true
	stream.closed = true
	stream.pipeline = nil
	stream.completed = nil
	stream.partition = nil
}

// closes the stream, i,e another stream has been derived from it.
func (stream *concurrentStream[T]) close() {
	stream.closed = true
}

// Concurrent always returns false for a sequential stream.
func (stream *concurrentStream[T]) Concurrent() bool {
	return true
}

// Terminated checks if a terminal operation has been invoked on the stream.
func (stream *concurrentStream[T]) Terminated() bool {
	return stream.terminated
}

// Closed check if the stream has been closed, a stream is closed when another stream was derived from it, a terminated stream is closed also.
func (stream *concurrentStream[T]) Closed() bool {
	return stream.closed
}

// valid if a stream is valid for any type of operation.
func (stream *concurrentStream[T]) valid() (bool, *Error) {
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
func (stream *concurrentStream[T]) getPipeline() func(i int) (T, bool) {
	return stream.pipeline
}

//  concurrentFromCollection creates a sconcurrent tream from the given collection. All changes made to the collection before the stream is terminated
// are visible to the stream
func concurrentFromCollection[T types.Equitable[T]](collection collections.Collection[T], maxConcurrency int) Stream[T] {
	it := collection.Iterator()
	source := sources.NewSource(it.Next, it.HasNext)
	concurrentSource := sources.NewPartitionedSource(source)
	stream := &concurrentStream[T]{
		pipeline:  emptyConcurrentPipeline(concurrentSource),
		completed: func(i int) bool { return !(concurrentSource.At(i).HasNext()) },
		partition: func() int { return concurrentSource.Partition(maxConcurrency) },
	}
	return stream
}

// concurrentFromSlice creates a concurrent stream by using the callback to retrieve the underlying slice. All changes made to the slice before the stream is terminated
// are visible to the stream.
func concurrentFromSlice[T any](f func() []T, maxConcurrency int) Stream[T] {
	source := sources.NewSourceFromSlice(f)
	concurrentSource := sources.NewPartitionedSource(source)
	stream := concurrentStream[T]{
		pipeline:  emptyConcurrentPipeline(concurrentSource),
		completed: func(i int) bool { return !(concurrentSource.At(i).HasNext()) },
		partition: func() int { return concurrentSource.Partition(maxConcurrency) },
	}
	return &stream
}

// concurrentFromSource creates a concurrent stream from the given source.
func concurrentFromSource[T any](source sources.Source[T], maxConcurrency int) Stream[T] {
	concurrentSource := sources.NewPartitionedSource(source)
	stream := &concurrentStream[T]{
		pipeline:  emptyConcurrentPipeline(concurrentSource),
		completed: func(i int) bool { return !(concurrentSource.At(i).HasNext()) },
		partition: func() int { return concurrentSource.Partition(maxConcurrency) },
	}
	return stream
}

// Map returns a stream containing the results of applying the given mapping function to the elements of the stream. Applying this operation results in
// the underlying type of the stream being an interface since receiver methods do not support generic types.
func (inputStream *concurrentStream[T]) Map(f func(x T) interface{}) Stream[interface{}] {
	if ok, err := inputStream.valid(); !ok {
		panic(err)
	}
	defer inputStream.close()
	newStream := concurrentStream[interface{}]{
		pipeline: func(i int) (interface{}, bool) {
			element, ok := inputStream.pipeline(i)
			if !ok {
				var sentinel interface{}
				return sentinel, ok
			}
			return f(element), ok
		},
		distinct:  false,
		completed: inputStream.completed,
		partition: inputStream.partition,
	}
	return &newStream
}

// Filter returns a stream consisting of the elements of the stream that match the given predicate.
func (inputStream *concurrentStream[T]) Filter(f func(x T) bool) Stream[T] {
	if ok, err := inputStream.valid(); !ok {
		panic(err)
	}
	defer inputStream.close()
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
		distinct:  inputStream.distinct,
		completed: inputStream.completed,
		partition: inputStream.partition,
	}
	return &newStream
}

// Returns a stream that is limited to only producing n elements. Will panic if limit is negative.
func (inputStream *concurrentStream[T]) Limit(limit int) Stream[T] {
	if ok, err := inputStream.valid(); !ok {
		panic(err)
	} else if limit < 0 {
		panic(ErrIllegalArgument("Limit", fmt.Sprint(limit)))
	}
	defer inputStream.close()
	var counter uint32
	var mutex sync.Mutex
	newStream := concurrentStream[T]{
		pipeline: func(i int) (T, bool) {
			mutex.Lock()
			defer mutex.Unlock()
			element, ok := inputStream.getPipeline()(i)
			if !ok {
				return element, ok
			} else {
				if int(atomic.LoadUint32(&counter)) < limit {
					atomic.AddUint32(&counter, 1)
					return element, true
				}
				return element, false
			}
		},
		completed: func(i int) bool {
			if inputStream.completed(i) || int(atomic.LoadUint32(&counter)) >= limit {
				return true
			}
			return false
		},
		distinct:  inputStream.distinct,
		partition: inputStream.partition,
	}
	return &newStream
}

// Returns a stream that skips the first n elements in processing. Will panic if number of elements to skip is negative.
func (inputStream *concurrentStream[T]) Skip(skip int) Stream[T] {
	if ok, err := inputStream.valid(); !ok {
		panic(err)
	} else if skip < 0 {
		panic(ErrIllegalArgument("Skip", fmt.Sprint(skip)))
	}
	defer inputStream.close()
	var counter uint32
	var mutex sync.Mutex
	newStream := concurrentStream[T]{
		pipeline: func(i int) (T, bool) {
			mutex.Lock()
			defer mutex.Unlock()
			element, ok := inputStream.pipeline(i)
			if !ok {
				return element, ok
			} else {
				if int(atomic.LoadUint32(&counter)) < skip {
					atomic.AddUint32(&counter, 1)
					return element, false
				}
				return element, true
			}
		},
		distinct:  inputStream.distinct,
		completed: inputStream.completed,
		partition: inputStream.partition,
	}
	return &newStream
}

// Distinct returns a stream consisting of distinct elements. Elements are distinguished using equality and hash code.
func (inputStream *concurrentStream[T]) Distinct(equals func(x, y T) bool, hashCode func(x T) int) Stream[T] {
	if ok, err := inputStream.valid(); !ok {
		panic(err)
	}
	defer inputStream.close()
	set := hashset.New[element[T]]()
	var mutex sync.Mutex
	newStream := concurrentStream[T]{
		pipeline: func(i int) (T, bool) {
			mutex.Lock()
			defer mutex.Unlock()
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
		distinct:  true,
		completed: inputStream.completed,
		partition: inputStream.partition,
	}
	return &newStream
}

// Peek Returns a stream consisting of the elements of the given stream but additionaly the given function is invoked for each element.
func (inputStream *concurrentStream[T]) Peek(f func(x T)) Stream[T] {
	if ok, err := inputStream.valid(); !ok {
		panic(err)
	}
	defer inputStream.close()
	newStream := concurrentStream[T]{
		pipeline: func(i int) (T, bool) {
			element, ok := inputStream.pipeline(i)
			if !ok {
				return element, ok
			}
			f(element)
			return element, ok
		},
		completed:  inputStream.completed,
		terminated: false,
		partition:  inputStream.partition,
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
	if ok, err := stream.valid(); !ok {
		panic(err)
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
	if ok, err := stream.valid(); !ok {
		panic(err)
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

// reduce performs a reduction on a partition.
func reduce[T any](wg *sync.WaitGroup, result chan T, stream *concurrentStream[T], f func(x, y T) T, i int) {
	defer wg.Done()
	pipeline := stream.pipeline
	count := 0
	var x, y T
	for !stream.completed(i) {
		element, ok := pipeline(i)
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
	if count > 0 {
		result <- x
	}
}

// Reduce returns the result of applying the associative binary function on elements of the stream. The binary operator is only applied if the are
// at least 2 elements in the stream, in the case of 1 element , the element is returned. Otherwise the returned result is invalid and will be indicated by the second returned value.
func (stream *concurrentStream[T]) Reduce(f func(x, y T) T) (T, bool) {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()
	var wg sync.WaitGroup
	n := stream.partition()
	results := make(chan T, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go reduce(&wg, results, stream, f, i)
	}
	wg.Wait()
	close(results)
	var x, y T
	count := 0
	for result := range results {
		if count == 0 {
			x = result
		} else {
			y = result
			x = f(x, y)
		}
		count++
	}
	if count == 0 {
		var zero T
		return zero, false
	}
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
