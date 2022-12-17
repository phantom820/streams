package streams

import (
	"sync"
)

const (
	filterOperatorName   = "FILTER"
	peekOperatorName     = "PEEK"
	mapOperatorName      = "MAP"
	skipOperatorName     = "SKIP"
	limitOperatorName    = "LIMIT"
	distinctOperatorName = "DISTINCT"
)

// operator type to represent an intermediate stream operation.
type operator[T any] struct {
	apply    func(x T) (T, bool)
	name     string
	stateful bool
}

// extendOperator extends an operator from acting on a single element to a slice of elements.
func extendOperator[T any](f operator[T]) operator[[]T] {
	return operator[[]T]{
		name:     f.name,
		stateful: f.stateful,
		apply: func(values []T) ([]T, bool) {
			results := make([]T, 0)
			for _, val := range values {
				if result, ok := f.apply(val); ok {
					results = append(results, result)
				}
			}
			return results, len(results) != 0
		},
	}

}

// filter returnf filter operator with the given predicate.
func filter[T any](f func(T) bool) operator[T] {
	return operator[T]{
		apply: func(x T) (T, bool) { return x, f(x) },
		name:  filterOperatorName,
	}
}

// peek returns peek operator with the given action.
func peek[T any](f func(T)) operator[T] {
	return operator[T]{
		apply: func(x T) (T, bool) {
			f(x)
			return x, true
		},
		name: peekOperatorName,
	}
}

// uniformMap returns map operator with given uniformMap function.
func uniformMap[T any](f func(T) T) operator[T] {
	return operator[T]{
		apply: func(x T) (T, bool) {
			return f(x), true
		},
		name: mapOperatorName,
	}
}

// limit returns limit operator with given limit.
func limit[T any](multipleRoutineAccess bool, n int) operator[T] {
	// If its a parallel stream we use atomic to avoid race conditions.
	if multipleRoutineAccess {
		var mux sync.Mutex
		counter := 0
		return operator[T]{
			apply: func(x T) (T, bool) {
				mux.Lock()
				defer mux.Unlock()
				if counter >= n {
					var ref T
					return ref, false
				}
				counter++
				return x, true
			},
			name:     limitOperatorName,
			stateful: true,
		}
	}
	// Sequential stream no need for atomic.
	counter := 0
	return operator[T]{
		apply: func(x T) (T, bool) {
			if counter >= n {
				var ref T
				return ref, false
			}
			counter++
			return x, true
		},
		name:     limitOperatorName,
		stateful: true,
	}

}

// skip returns skip operator with given skip number.
func skip[T any](multipleRoutineAccess bool, n int) operator[T] {
	// If its a parallel stream we use atomic to avoid race conditions.
	if multipleRoutineAccess {
		var mux sync.Mutex
		counter := 0
		return operator[T]{
			apply: func(x T) (T, bool) {
				mux.Lock()
				defer mux.Unlock()
				if counter < n {
					counter++
					var ref T
					return ref, false
				}
				return x, true
			},
			name:     skipOperatorName,
			stateful: true,
		}
	}
	// Sequential stream no need for atomic.
	counter := 0
	return operator[T]{
		apply: func(x T) (T, bool) {
			if counter < n {
				counter++
				var ref T
				return ref, false
			}
			return x, true
		},
		name:     skipOperatorName,
		stateful: true,
	}

}

// distinct returns distinct operator with hiven hash functions for map keys.
func distinct[T any](multipleRoutineAccess bool, alreadyDistinct bool, hash func(T) string) operator[T] {
	if alreadyDistinct { // if the stream is already distinct then just use an identity func.
		return operator[T]{
			apply: func(x T) (T, bool) {
				return x, true
			},
			name:     distinctOperatorName,
			stateful: true,
		}
	} else if multipleRoutineAccess { // If its a parallel stream we use mutex lock to synchronize things.
		elements := make(map[string]struct{})
		var mutex sync.Mutex
		return operator[T]{
			apply: func(x T) (T, bool) {
				mutex.Lock()
				defer mutex.Unlock()
				if _, ok := elements[hash(x)]; ok {
					var zero T
					return zero, false
				}
				elements[hash(x)] = struct{}{}
				return x, true
			},
			name:     distinctOperatorName,
			stateful: true,
		}
	}
	// If its a sequential stream no need for mutex.
	elements := make(map[string]struct{})
	return operator[T]{
		apply: func(x T) (T, bool) {
			if _, ok := elements[hash(x)]; ok {
				var zero T
				return zero, false
			}
			elements[hash(x)] = struct{}{}
			return x, true
		},
		name:     distinctOperatorName,
		stateful: true,
	}
}
