// package streams provides a way of applying a pipeline of operations to a sequence of elements.
package streams

import (
	"fmt"

	"github.com/phantom820/streams/operator"
)

// Stream a sequence of elements that can be operated on sequential / concurrently. The underlying source for a stream should be finite, infinite sources
// are not supported and will lead to an infinite loop.
type Stream[T any] interface {

	// Intermediate operations.
	Filter(f func(x T) bool) Stream[T]                               // Returns a stream consisting of the elements of this stream that satisfy the given predicate.
	Map(f func(x T) T) Stream[T]                                     // Returns a stream consisting of the results of applying the given transformation to the elements of the stream.
	Limit(n int) Stream[T]                                           // Returns a stream consisting of the elements of the stream but only limited to processing n elements.
	Skip(n int) Stream[T]                                            // Returns a stream that skips the first n elements it encounters in processing.
	Distinct(equals func(x, y T) bool, hash func(x T) int) Stream[T] // Returns a stream consisting of distinct elements. Elements are distinguished using equality and hash code.
	Peek(f func(x T)) Stream[T]                                      // Returns a stream consisting of the elements of the given stream but additionaly the given function is invoked for each element.

	// Terminal operations.
	ForEach(f func(x T))               // Performs an action specified by the function f for each element of the stream.
	Count() int                        // Returns a count of elements in the stream.
	Reduce(f func(x, y T) T) (T, bool) // Returns the result of appying a reduction on the elements of the stream. If the stream has no elements then the result would
	// be invalid and the zero value for T along with false would be returned.
	Collect() []T // Returns a slice containing the elements from the stream.

	// State.
	Terminated() bool // Checks if a terminal operation has been invoked on the stream.
	Closed() bool     // Checks if a stream has been closed. A stream is closed either when a new stream is created from it using intermediate
	// operations, terminated streams are also closed.
	Concurrent() bool // Checks if the underlying stream is concurrent or sequential.

}

// FromSlice returns a sequential stream which will use the given callback to obtain elements when terminal operation is invoked.
func FromSlice[T any](f func() []T, concurrency int) Stream[T] {
	if concurrency < 1 {
		panic(errIllegalConfig(fmt.Sprintf("concurrency=%v", concurrency), "FromSlice"))
	} else if concurrency == 1 {
		return &sequentialStream[T]{
			data:                  f,
			intermediateOperators: make([]operator.IntermediateOperator[T], 0),
		}
	}

	return &concurrentStream[T]{
		concurrency:           concurrency,
		data:                  f,
		intermediateOperators: make([]operator.IntermediateOperator[T], 0),
	}

}

// partition creates a number of sub intervals in the range [0,n].
func partition(n int, numberOfPartitions int) []int {
	if n == 0 {
		return []int{}
	}
	intervals := []int{}
	partitionSize := n / numberOfPartitions

	for i := 0; i < numberOfPartitions; i++ {
		intervals = append(intervals, i*partitionSize)
	}

	intervals = append(intervals, n)
	return intervals
}

// apply applies the give list of operations to givene element and returns a result and whether the result is valid or not.
func apply[T any](operators []operator.IntermediateOperator[T], x T) (T, bool) {
	for _, operator := range operators {
		y, ok := operator.Apply(x)
		if !ok {
			return y, false
		}
		x = y
	}
	return x, true
}

// count returns a count of elements that operations were succesfully applied to. This is for implementing Count on a stream.
func count[T any](operators []operator.IntermediateOperator[T], data []T) int {
	count := 0
	for _, x := range data {
		if _, ok := apply(operators, x); ok {
			count++
		}
	}
	return count
}

// collect returns a slice containing elements in which operations were applied succesfully on.
func collect[T any](operators []operator.IntermediateOperator[T], data []T) []T {
	results := make([]T, 0)
	for _, x := range data {
		if y, ok := apply(operators, x); ok {
			results = append(results, y)
		}
	}
	return results
}

// forEach applies the given function on elements that yield succesfully when operators are applied. For implementing ForEach.
func forEach[T any](f func(x T), operators []operator.IntermediateOperator[T], data []T) {
	for _, x := range data {
		if y, ok := apply(operators, x); ok {
			f(y)
		}
	}
}

// reduce applies associative binary function f on elements that yield succesful results after operators are applied. For implementing Reduce.
func reduce[T any](f func(x, y T) T, operators []operator.IntermediateOperator[T], data []T) (T, bool) {
	var z T
	valid := false
	for _, x := range data {
		y, ok := apply(operators, x)
		if ok && valid {
			z = f(z, y)
		} else if ok {
			z = y
			valid = true
		}
	}
	return z, valid
}
