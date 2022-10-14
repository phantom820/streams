package operator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func sameOperations[T any](x, y []IntermediateOperator[T]) bool {
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if x[i].name != y[i].name {
			return false
		}
	}
	return true
}

func TestCommutes(t *testing.T) {

	a := Filter(func(x int) bool { return x == 2 })
	b := Distinct(false, func(x, y int) bool { return x == y }, func(x int) int { return x })

	assert.Equal(t, true, commutative(a, b))
	assert.Equal(t, true, commutative(b, a))

}
func TestSort(t *testing.T) {

	// Case 1 : Empty operations should return empty.
	assert.ElementsMatch(t, []IntermediateOperator[int]{}, Sort([]IntermediateOperator[int]{}))

	// Case 2 : No commuting operations.
	operations := []IntermediateOperator[int]{Filter(func(x int) bool { return x > 2 }), Limit[int](10), Skip[int](2), Peek(func(x int) {})}
	assert.Equal(t, true, sameOperations(operations, Sort(operations)))

	// Case 3 : Non consective commuting operations.
	operations = []IntermediateOperator[int]{Filter(func(x int) bool { return x > 2 }), Limit[int](10), Distinct(false,
		func(x, y int) bool { return x == y }, func(x int) int { return x }), Peek(func(x int) {})}
	assert.Equal(t, true, sameOperations(operations, Sort(operations)))

	// Case 4 : 2 consective commuting operations.
	operations = []IntermediateOperator[int]{Limit[int](10), Distinct(false,
		func(x, y int) bool { return x == y }, func(x int) int { return x }), Filter(func(x int) bool { return x > 2 })}
	assert.Equal(t, false, sameOperations(operations, Sort(operations)))

	// Case 5 : A number of commuting operations.
	operations = []IntermediateOperator[int]{Limit[int](10), Distinct(false,
		func(x, y int) bool { return x == y }, func(x int) int { return x }), Filter(func(x int) bool { return x > 2 }), Peek(func(x int) {}),
		Distinct(false, func(x, y int) bool { return x == y }, func(x int) int { return x }), Filter(func(x int) bool { return x > 2 })}

	// assert.Equal(t, []string{LIMIT, FILTER, DISTINCT, PEEK, FILTER, DISTINCT}, Sequence(Sort(operations)))

}
