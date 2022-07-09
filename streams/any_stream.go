package streams

// anyStream struct to represent a stream in which elements have no constraint.
type anyStream[T any] struct {
	sized       bool
	distinct    bool
	sorted      bool
	ordered     bool
	_exhausted  func() bool
	source      source[T]
	_operations func() (T, bool)
}
