// package source provides an implementation of a producer of elements for a stream , a normal source will be used with a sequential stream while
// a partitioned source is to be used with a concurrent stream. A source can be finite/infinite while a partitioned source should always be finite.
package sources

import (
	"errors"
	"fmt"
	"math"
)

// Source a source of elements for a stream. Sources can come from a slice, collection etc and can be finite/infinite/
type Source[T any] interface {
	Next() T       // Returns the next element from the source.
	HasNext() bool // Checks if the Source has a next element to produce.
}

// PartitionedSource a source that is made up of disjoint sources that can be processed independently.
type PartionedSource[T any] interface {
	At(i int) Source[T]  // Returns the i'th partition of the source.
	Len() int            // Returns the number of partitions the source has.
	Partition(n int) int // Partitions the source into n parttions
}

// collectSource returns a slice containing all the elements from the source.
func collectSource[T any](source Source[T]) []T {
	data := make([]T, 0)
	for source.HasNext() {
		data = append(data, source.Next())
	}
	return data
}

// NewPartitionedSource creates a new partitioned source from the given source.
func NewPartitionedSource[T any](source Source[T]) PartionedSource[T] {
	return &partitionedSource[T]{source: source, sources: make([]Source[T], 0)}
}

// partitionedSource a source that is made up of disjoint sources that can be processed independently.
type partitionedSource[T any] struct {
	source  Source[T]
	sources []Source[T]
}

// Partition returns a partioned source with na partitions. The input source must be finite otherwise will run into an infinite loop when trying
// to collect it into a slice.
func (partitionedSource *partitionedSource[T]) Partition(n int) int {
	data := collectSource(partitionedSource.source)
	if len(data) == 0 {
		partitionedSource.sources = make([]Source[T], 0)
		return 0
	}
	partitionSize := int(math.Ceil(float64(len(data)) / float64(n))) // Do we need smarter ways of picking the partition size here.
	numberOfPartitons := int(math.Ceil(float64(len(data)) / float64(partitionSize)))
	sources := make([]Source[T], numberOfPartitons)
	j := 0
	for i := 0; i < len(data); i = i + partitionSize {
		var partition []T
		if i+partitionSize >= (len(data)) {
			partition = data[i:]
		} else {
			partition = data[i : i+partitionSize]
		}
		source := NewSourceFromSlice(func() []T { return partition })
		sources[j] = source
		j++
	}
	partitionedSource.sources = sources
	return len(sources)
}

// At returns the partition at the i'th index and will panic if the index is out of bounds.
func (partitionedSource *partitionedSource[T]) At(i int) Source[T] {
	if i >= len(partitionedSource.sources) {
		err := fmt.Sprintf("ErrIndexOutOfBounds: Index: %v, Size: %v", i, len(partitionedSource.sources))
		panic(errors.New(err))
	}
	return partitionedSource.sources[i]
}

// Len returns the number of partitions the source has.
func (partitionedSource *partitionedSource[T]) Len() int {
	return len(partitionedSource.sources)
}

// source a sequential stream source.
type source[T any] struct {
	next    func() T
	hasNext func() bool
}

// HasNext checks if the source has a next element to produce.
func (source *source[T]) HasNext() bool {
	return source.hasNext()
}

// Next returns the next element from the source.
func (source *source[T]) Next() T {
	return source.next()
}

// NewSource creates a new sequential source.
func NewSource[T any](next func() T, hasNext func() bool) Source[T] {
	return &source[T]{next: next, hasNext: hasNext}
}

// NewSourceFromSlice creates a sequential source from a slice.
func NewSourceFromSlice[T any](f func() []T) Source[T] {
	var data []T
	initialized := false
	i := 0
	hasNext := func() bool {
		if !initialized {
			initialized = true
			data = f()
		}
		if data == nil || i >= len(data) {
			return false
		}
		return true
	}
	next := func() T {
		if !hasNext() {
			panic(errors.New("ErrNoNextElement"))
		}
		element := data[i]
		i++
		return element
	}
	source := source[T]{next: next, hasNext: hasNext}
	return &source
}
