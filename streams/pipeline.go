package streams

import "github.com/phantom820/streams/sources"

// emptyPipeline returns the initial pipeline for a stream.
func emptyPipeline[T any](source sources.Source[T]) func() (T, bool) {
	return func() (T, bool) {
		return source.Next(), true
	}

}

// emptyConcurrentPipeline returns the initial pipeline for a concurrent stream.
func emptyConcurrentPipeline[T any](source sources.PartionedSource[T]) func(i int) (T, bool) {
	return func(i int) (T, bool) {
		return source.At(i).Next(), true
	}
}
