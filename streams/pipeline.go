package streams

// emptyPipeline returns the initial pipeline of the stream.
func emptyPipeline[T any](source *source[T]) func() (T, bool) {
	return func() (T, bool) {
		return source.next(), true
	}
}

// newPipeline returns a new pipeline which is of the form g(f), f is the current set of operations and g is a new operation to be added..
func newPipeline[T any](f func() (T, bool), g func() (T, bool)) func() (T, bool) {
	return nil
}
