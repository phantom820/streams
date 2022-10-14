package streams

import (
	"fmt"
	"math"
	"sync"

	"github.com/phantom820/streams/operator"
)

// import (
// 	"fmt"

// 	"github.com/phantom820/streams/operations"
// 	"github.com/phantom820/streams/sources"
// )

// concurrentStream sequential stream concrete type.
type concurrentStream[T any] struct {
	data                  func() []T                         // The callback for retrieving the data the stream will process
	intermediateOperators []operator.IntermediateOperator[T] // The sequence of operations that the stream will apply to elements.
	terminated            bool                               // Indicates if a terminal operation has been invoked on the stream.
	closed                bool                               // Indicates if a new stream has been derived from the stream or it has been terminated.
	distinct              bool                               // Keeps track of whether the stream has distinc elements or not.
	concurrency           int                                // Indicates maximum go routines to use when processing stream

}

// terminate terminates the stream when a terminal operation is invoked on it.
func (stream *concurrentStream[T]) terminate() {
	stream.terminated = true
	stream.closed = true
	stream.intermediateOperators = nil
}

// close closes the stream when a new stream is derived from it.
func (stream *concurrentStream[T]) close() {
	stream.closed = true
	stream.intermediateOperators = nil
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

// Terminated returns termination status of the stream.
func (stream *concurrentStream[T]) Terminated() bool {
	return stream.terminated
}

// Closed returns closure status of the stream.
func (stream *concurrentStream[T]) Closed() bool {
	return stream.closed
}

// Concurrent returns false always since its a sequential stream..
func (stream *concurrentStream[T]) Concurrent() bool {
	return false
}

// Filter returns a stream consisting of the elements of this stream that match the given predicate function.
func (stream *concurrentStream[T]) Filter(f func(element T) bool) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.close()

	return &concurrentStream[T]{
		concurrency:           stream.concurrency,
		data:                  stream.data,
		intermediateOperators: append(stream.intermediateOperators, operator.Filter(f)),
		distinct:              stream.distinct,
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

	return &concurrentStream[T]{
		concurrency:           stream.concurrency,
		data:                  stream.data,
		intermediateOperators: append(stream.intermediateOperators, operator.ConcurrentLimit[T](limit)),
		distinct:              stream.distinct,
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

	return &concurrentStream[T]{
		concurrency:           stream.concurrency,
		data:                  stream.data,
		intermediateOperators: append(stream.intermediateOperators, operator.ConcurrentSkip[T](n)),
		distinct:              stream.distinct,
	}
}

// Peek returns a stream consisting of the elements of this stream, additionally performing the provided action on each element as elements are processed.
func (stream *concurrentStream[T]) Peek(f func(element T)) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.close()

	return &concurrentStream[T]{
		concurrency:           stream.concurrency,
		data:                  stream.data,
		intermediateOperators: append(stream.intermediateOperators, operator.Peek(f)),
		distinct:              stream.distinct,
	}
}

// Map returns a stream consisting of the results of applying the given transformation function to the elements of this stream.
func (stream *concurrentStream[T]) Map(f func(element T) T) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.close()

	return &concurrentStream[T]{
		concurrency:           stream.concurrency,
		data:                  stream.data,
		intermediateOperators: append(stream.intermediateOperators, operator.Map(f)),
		distinct:              false,
	}
}

// Distinct returns a stream consisting of the distinct element of this stream using equals and hashCode for the underlying set.
func (stream *concurrentStream[T]) Distinct(equals func(x, y T) bool, hashCode func(x T) int) Stream[T] {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.close()

	alreadyDistinct := stream.distinct

	return &concurrentStream[T]{
		concurrency:           stream.concurrency,
		data:                  stream.data,
		intermediateOperators: append(stream.intermediateOperators, operator.ConcurrentDistinct(alreadyDistinct, equals, hashCode)),
		distinct:              true,
	}
}

// ForEach performs an action for each element of this stream.
func (stream *concurrentStream[T]) ForEach(f func(element T)) {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()

	work := func(wg *sync.WaitGroup, operators []operator.IntermediateOperator[T], partition []T) {
		defer wg.Done()
		forEach(f, operators, partition)
	}

	data := stream.data()
	operators := operator.Sort(stream.intermediateOperators)
	partitionSize := len(data) / stream.concurrency
	numberOfPartions := int(math.Ceil(float64(len(data)) / float64(partitionSize)))
	intervals := partition(len(data), numberOfPartions)
	var wg sync.WaitGroup

	for i := 0; i < len(intervals)-1; i++ {
		wg.Add(1)
		go work(&wg, operators, data[intervals[i]:intervals[i+1]])
	}
	wg.Wait()

}

// Count returns the count of elements in this stream.
func (stream *concurrentStream[T]) Count() int {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()

	work := func(operators []operator.IntermediateOperator[T], partition []T, outputChannel chan int) {
		outputChannel <- count(operators, partition)
	}

	data := stream.data()
	operators := operator.Sort(stream.intermediateOperators)
	partitionSize := len(data) / stream.concurrency
	numberOfPartions := int(math.Ceil(float64(len(data)) / float64(partitionSize)))
	intervals := partition(len(data), numberOfPartions)
	outputChannel := make(chan int, numberOfPartions)

	for i := 0; i < len(intervals)-1; i++ {
		go work(operators, data[intervals[i]:intervals[i+1]], outputChannel)
	}

	count := 0
	for i := 0; i < numberOfPartions; i++ {
		count = count + <-outputChannel
	}

	return count
}

// Reduce performs a reduction on the elements of this stream, using an associative function.
func (stream *concurrentStream[T]) Reduce(f func(x, y T) T) (T, bool) {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()

	work := func(operators []operator.IntermediateOperator[T], partition []T, outputChannel chan []T) {
		result, ok := reduce(f, operators, partition)
		if !ok {
			outputChannel <- []T{}
			return
		}
		outputChannel <- []T{result}
	}

	data := stream.data()
	operators := operator.Sort(stream.intermediateOperators)
	partitionSize := len(data) / stream.concurrency
	numberOfPartions := int(math.Ceil(float64(len(data)) / float64(partitionSize)))
	intervals := partition(len(data), numberOfPartions)
	outputChannel := make(chan []T, numberOfPartions)

	for i := 0; i < len(intervals)-1; i++ {
		go work(operators, data[intervals[i]:intervals[i+1]], outputChannel)
	}
	partialResults := make([]T, 0)
	for i := 0; i < numberOfPartions; i++ {
		partialResults = append(partialResults, <-outputChannel...)
	}
	return reduce(f, []operator.IntermediateOperator[T]{}, partialResults)
}

// Collect returns a slice containing the resulting elements from processing the stream.
func (stream *concurrentStream[T]) Collect() []T {
	if ok, err := stream.valid(); !ok {
		panic(err)
	}
	defer stream.terminate()

	work := func(operators []operator.IntermediateOperator[T], partition []T, ouputChannel chan []T) {
		ouputChannel <- collect(operators, partition)
	}

	data := stream.data()
	operators := operator.Sort(stream.intermediateOperators)
	partitionSize := len(data) / stream.concurrency
	numberOfPartions := int(math.Ceil(float64(len(data)) / float64(partitionSize)))
	intervals := partition(len(data), numberOfPartions)
	outputChannel := make(chan []T, numberOfPartions)

	for i := 0; i < len(intervals)-1; i++ {
		go work(operators, data[intervals[i]:intervals[i+1]], outputChannel)
	}

	results := make([]T, 0)
	for i := 0; i < numberOfPartions; i++ {
		results = append(results, <-outputChannel...)
	}

	return results
}
