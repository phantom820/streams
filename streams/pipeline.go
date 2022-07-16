package streams

import "github.com/phantom820/streams/sources"

// emptyPipeline returns the initial pipeline of the stream.
func emptyPipeline[T any](source sources.Source[T]) func() (T, bool) {
	return func() (T, bool) {
		return source.Next(), true
	}

}

// emptyPipeline returns the initial pipeline of the stream.
func emptyConcurrentPipeline[T any](source sources.ConcurrentSource[T]) func(i int) (T, bool) {
	return func(i int) (T, bool) {
		return source.GetPartition(i).Next(), true
	}
}
