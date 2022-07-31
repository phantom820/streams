// package streams provides java motivated stream API for go. It provides both sequential streams and concurrent streams. The type of stream
// is specified by a max concurrency parameter that is available in the various stream constructors. The underlying source for a stream can be finit/infinite
// in the case of an infinite source a sequential stream will process the source correctly if a limit operation is applied before invoking a terminal operation,
// while for a concurrent stream (max concurrency > 1) an infinite source will lead to an infinite loop even if we apply a limit operation.
package streams

import (
	"fmt"

	"github.com/phantom820/collections"
	"github.com/phantom820/collections/types"
	"github.com/phantom820/streams/sources"
)

// Stream a sequence of elements that can be operated on sequential / concurrently.
type Stream[T any] interface {

	// Intermediate operations.
	Filter(f func(x T) bool) Stream[T]                               // Returns a stream consisting of the elements of this stream that satisfy the given predicate.
	Map(f func(x T) interface{}) Stream[interface{}]                 // Returns a stream consisting of the results of applying the given function to the elements of the stream.
	Limit(n int) Stream[T]                                           // Returns a stream consisting of the elements of the stream but only limited to processing n elements.
	Skip(n int) Stream[T]                                            // Returns a stream that skips the first n elements it encounters in processing.
	Distinct(equals func(x, y T) bool, hash func(x T) int) Stream[T] // Returns a stream consisting of distinct elements. Elements are distinguished using equality and hash code.
	Peek(f func(x T)) Stream[T]                                      // Returns a stream consisting of the elements of the given stream but additionaly the given function is invoked for each element.
	// Terminal operations.
	ForEach(f func(x T))               // Performs an action specified by the function f for each element of this stream.
	Count() int                        // Returns a count of elements in the stream.
	Reduce(f func(x, y T) T) (T, bool) // Returns the result of appying a reduction on the elements of the stream. If the stream has no elements then the result would
	// be invalid and the zero value for T along with false would be returned.
	Collect() []T // Returns a slice containing the elements from the stream.

	// Util.
	Terminated() bool // Checks if a terminal operation has been invoked on the stream.
	Closed() bool     // Checks if a stream has been closed. In stream is closed either when a new stream is created from it using intermediate
	// operations, terminated streams are also closed.
	Concurrent() bool // Checks if the stream has a max concurrency > 1 or not.
}

// NewFromCollection creates a stream from the given collection with the specified levele of concurrency. If the concurrency is set to 1
// then the resulting stream is sequential otherwise for values > 1 it is concurrent stream that use no more than the specified number of go routines
// when processing the stream.
func NewFromCollection[T types.Equitable[T]](collection collections.Collection[T], maxConcurrency int) Stream[T] {
	if maxConcurrency < 1 {
		panic(ErrIllegalConfig(fmt.Sprintf("maxConcurrency=%v", maxConcurrency), "FromCollection"))
	}
	if maxConcurrency == 1 {
		return fromCollection(collection)
	}
	return concurrentFromCollection(collection, maxConcurrency)
}

// NewFromSlice creates a stream which will use the slice obtained from the callback. The callback is invoked only when a terminal operation is used on the stream.
//  If the concurrency is set to 1 then the resulting stream is sequential otherwise for values > 1 it is concurrent stream that use no more than the specified number of go routines
// when processing the stream.
func NewFromSlice[T any](f func() []T, maxConcurrency int) Stream[T] {
	if maxConcurrency < 1 {
		panic(ErrIllegalConfig(fmt.Sprintf("maxConcurrency=%v", maxConcurrency), "FromSlice"))
	}
	if maxConcurrency == 1 {
		return fromSlice(f)
	}
	return concurrentFromSlice(f, maxConcurrency)
}

// NewFromSource creates a stream from the given collection with the specified level of concurrency. If the concurrency is set to 1
// then the resulting stream is sequential otherwise for values > 1 it is concurrent stream that use no more than the specified number of go routines
// when processing the stream. An infinite source should only be used when the stream is sequential and a limit operation will be applied on it,
// otherwise we will run into an infinite loop in trying to use a concurrent stream with an infinite source even if we apply limit.
func NewFromSource[T any](source sources.Source[T], maxConcurrency int) Stream[T] {
	if maxConcurrency < 1 {
		panic(ErrIllegalConfig(fmt.Sprintf("maxConcurrency=%v", maxConcurrency), "FromSource"))
	}
	if maxConcurrency == 1 {
		return fromSource(source)
	}
	return concurrentFromSource(source, maxConcurrency)
}
