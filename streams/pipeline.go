package streams

// emptyPipeline returns the initial pipeline of the stream.
func emptyPipeline[T any](source *source[T]) func() (T, bool) {
	return func() (T, bool) {
		return source.next(), true
	}
}

type operation[T any, U any] struct {
	operator func(x T) U
	next     *operation[U, interface{}]
}
