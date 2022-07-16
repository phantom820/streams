package sources

import "github.com/phantom820/collections/errors"

// Source a source for a sequential stream. This can be finite/infinite. An infinite source may lead to infinite loops if not handled properly.
// A limit operation should be applied on a stream derived from an infinite source.
type Source[T any] interface {
	Next() T       // Returns the next element from the source.
	HasNext() bool // Checks if the Source has a next element to produce.
}

// ConcurrentSource a source for a concurrent stream. This should always be finite.
type ConcurrentSource[T any] interface {
	GetPartition(i int) Source[T]
	Partition(n int) int
}

// concurrentSource a concurrent stream source to be used by concurrent streams.
type concurrentSource[T any] struct {
	source  Source[T]   // The source to be partitioned.
	n       int         // The number of partitions to be made from the source.
	sources []Source[T] // n partitions of the source.
}

func (source *concurrentSource[T]) GetPartition(i int) Source[T] {
	return source.sources[i]
}

// partitionSource partitions a source into n partitions. For internal use to make concurrent streams.
func partitionSource[T any](source Source[T], n int) []Source[T] {
	data := make([]T, 0)
	for source.HasNext() {
		data = append(data, source.Next())
	}
	partitionSize := len(data) / n // Do we need smarter ways of picking the partition size here.
	sources := make([]Source[T], 0)
	for i := 0; i < len(data); i = i + partitionSize {
		partition := data[i : i+partitionSize]
		if i+partitionSize >= len(data) {
			partition = data[i:]
		}
		source := NewSourceFromSlice(func() []T { return partition })
		sources = append(sources, source)
	}
	return sources
}

// Partition partitions the source into n partitions.
func (source *concurrentSource[T]) Partition(n int) int {
	sources := partitionSource[T](source.source, n)
	source.sources = sources
	return len(sources)
}

// source a sequential stream source.
type source[T any] struct {
	next    func() T
	hasNext func() bool
}

// HasNext checks if the source has a next element to yield.
func (source *source[T]) HasNext() bool {
	return source.hasNext()
}

// Next returns the next element from the source.
func (source *source[T]) Next() T {
	return source.next()
}

// NewConcurrentSource creates a concurrent source by partitioning the given source into n partitions.
func NewConcurrentSource[T any](source Source[T]) ConcurrentSource[T] {
	return &concurrentSource[T]{source: source}
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
			panic(errors.ErrNoNextElement())
		}
		element := data[i]
		i++
		return element
	}
	source := source[T]{next: next, hasNext: hasNext}
	return &source
}
