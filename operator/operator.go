// package operator provides an implementation of operators that can be applied to a stream before it is evaluated.
package operator

import (
	"sort"
	"sync"

	"github.com/phantom820/collections/sets/hashset"
)

const (
	filter        = "filter"
	filterCost    = 1
	limit         = "limit"
	skip          = "skip"
	distinct      = "distinct"
	distinct_cost = 2
	peek          = "peek"
	_map          = "map"
)

// intermediateOperator an operator that can be applied to an element. This serves as the building block of building up pipelines that combine different operations
// to process elements , the boolean indicates whether subsequent operators should consider the element.
type IntermediateOperator[T any] struct {
	apply    func(x T) (T, bool) // Actual operation to be applied , process flag indicates whether the operator should act on the element or ignore it.
	name     string
	cost     int  // Indicate the how expensive the operation is compared to other opertaions. Useful when sorting a sequence of operations.
	stateful bool // Indicates whether the operation is stateful or not i.e stateful operations must store previously encounted elements.
}

// Name returns the name of the intermediate operation.
func (operator IntermediateOperator[T]) Name() string {
	return operator.name
}

// Stateful returns an indication of whether the intermediate operation is stateful or not.
func (operator IntermediateOperator[T]) Stateful() bool {
	return operator.stateful
}

// Apply applies the underlying operation on the given element.
func (operator IntermediateOperator[T]) Apply(x T) (T, bool) {
	return operator.apply(x)
}

// Filter returns a filtering operator constructed from the given predicate.
func Filter[T any](f func(x T) bool) IntermediateOperator[T] {
	return IntermediateOperator[T]{
		name: filter,
		cost: filterCost,
		apply: func(x T) (T, bool) {
			return x, f(x)
		},
	}
}

// Distinct returns an operator that yields distinct elements when applied to a group using given equals and hashCode.
func Distinct[T any](alreadyDistinct bool, equals func(x, y T) bool, hashCode func(x T) int) IntermediateOperator[T] {

	set := hashset.New[entry[T]]()

	return IntermediateOperator[T]{
		name: distinct,
		cost: distinct_cost,
		apply: func(x T) (T, bool) {
			if alreadyDistinct {
				return x, true
			} else if set.Contains(entry[T]{value: x, equals: equals, hashCode: hashCode}) {
				return x, false
			}
			set.Add(entry[T]{value: x, equals: equals, hashCode: hashCode})
			return x, true
		},
		stateful: true,
	}
}

// ConcurrentDistinct returns an operator that yields distinct elements when applied to a group using given equals and hashCode.
func ConcurrentDistinct[T any](alreadyDistinct bool, equals func(x, y T) bool, hashCode func(x T) int) IntermediateOperator[T] {

	var mutex sync.Mutex
	set := hashset.New[entry[T]]()

	return IntermediateOperator[T]{
		name: distinct,
		cost: distinct_cost,
		apply: func(x T) (T, bool) {
			if alreadyDistinct {
				return x, true
			} else {
				mutex.Lock()
				defer mutex.Unlock()
				if set.Contains(entry[T]{value: x, equals: equals, hashCode: hashCode}) {
					return x, false
				}
				set.Add(entry[T]{value: x, equals: equals, hashCode: hashCode})
				return x, true
			}
		},
		stateful: true,
	}
}

// Limit returns a limiting operation that is constructed from the given limit.
func Limit[T any](n int) IntermediateOperator[T] {
	counter := 0
	return IntermediateOperator[T]{
		name: limit,
		apply: func(x T) (T, bool) {
			if counter < n {
				counter++
				return x, true
			}
			return x, false

		},
		stateful: true,
	}
}

// ConcurrentLimit returns a thread limiting operation that is constructed from the given limit.
func ConcurrentLimit[T any](n int) IntermediateOperator[T] {
	counter := atomicCounter{}
	var mutex sync.Mutex
	return IntermediateOperator[T]{
		name: limit,
		apply: func(x T) (T, bool) {
			mutex.Lock()
			defer mutex.Unlock()
			if counter.read() < n {
				counter.add(1)
				return x, true
			}
			return x, false

		},
		stateful: true,
	}
}

// Skip returns an operation that skips the first n given elements.
func Skip[T any](n int) IntermediateOperator[T] {
	skipped := 0
	return IntermediateOperator[T]{
		name: skip,
		apply: func(x T) (T, bool) {
			if skipped < n {
				skipped++
				return x, false
			}
			return x, true
		},
		stateful: true,
	}
}

// ConcurrentSkip returns an operation that skips the first n given elements.
func ConcurrentSkip[T any](n int) IntermediateOperator[T] {
	var mutex sync.Mutex
	skipped := atomicCounter{}

	return IntermediateOperator[T]{
		name: skip,
		apply: func(x T) (T, bool) {
			mutex.Lock()
			defer mutex.Unlock()
			if skipped.read() < n {
				skipped.add(1)
				return x, false
			}
			return x, true
		},
		stateful: true,
	}
}

// Peek return an operation that applies the given function on an observed element.
func Peek[T any](f func(x T)) IntermediateOperator[T] {
	return IntermediateOperator[T]{
		name: peek,
		apply: func(x T) (T, bool) {
			f(x)
			return x, true
		},
	}
}

// Map return an operation that applies the given transformation to an observed element.
func Map[T any](f func(x T) T) IntermediateOperator[T] {
	return IntermediateOperator[T]{
		name: _map,
		apply: func(x T) (T, bool) {
			return f(x), true
		},
	}
}

// commutative returns true if operators a and b are commutative, i.e (ab)*x = (ba)*x.
func commutative[T any](a, b IntermediateOperator[T]) bool {
	switch a.name {
	case filter:
		return b.name == distinct
	case distinct:
		return b.name == filter
	default:
		return false
	}
}

// Sort returns an optimal ordering of operators that would same results as given input but at a lower cost by changing the ordering of operators
// that commute based on their cost.
func Sort[T any](operators []IntermediateOperator[T]) []IntermediateOperator[T] {

	sortedOperators := make([]IntermediateOperator[T], len(operators))
	copy(sortedOperators, operators)

	start := -1
	end := -1

	for i := 0; i < len(operators)-1; i++ {
		if commutative(operators[i], operators[i+1]) {
			if start == -1 {
				start = i
			}
			end = i + 1
		} else if end > start {
			sort.SliceStable(sortedOperators[start:end+1], func(i, j int) bool {
				return sortedOperators[i+start].cost < sortedOperators[j+start].cost
			})
			start = -1
			end = -1
		}
	}

	if end > start && start >= 0 {
		sort.SliceStable(sortedOperators[start:end+1], func(i, j int) bool {
			return sortedOperators[i+start].cost < sortedOperators[j+start].cost
		})
	}

	return sortedOperators
}
