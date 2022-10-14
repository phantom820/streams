package operator

import (
	"fmt"
	"strings"
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

func TestFilter(t *testing.T) {

	operator := Filter(func(x int) bool { return x > 2 })

	assert.Equal(t, false, operator.stateful)
	val, ok := operator.apply(3)
	assert.Equal(t, val, 3)
	assert.Equal(t, true, ok)

}

func TestDistinct(t *testing.T) {

	operator := Distinct(false, func(x, y int) bool { return x == y }, func(x int) int { return x })

	assert.Equal(t, true, operator.stateful)
	_, ok := operator.apply(2)
	assert.Equal(t, true, ok)

	_, ok = operator.apply(2)
	assert.Equal(t, false, ok)

	_, ok = operator.apply(3)
	assert.Equal(t, true, ok)

	operator = Distinct(true, func(x, y int) bool { return x == y }, func(x int) int { return x })

	_, ok = operator.apply(2)
	assert.Equal(t, true, ok)

	_, ok = operator.apply(2)
	assert.Equal(t, true, ok)

}

func TestConcurrentDistinct(t *testing.T) {

	operator := ConcurrentDistinct(false, func(x, y int) bool { return x == y }, func(x int) int { return x })

	assert.Equal(t, true, operator.stateful)
	_, ok := operator.apply(2)
	assert.Equal(t, true, ok)

	_, ok = operator.apply(2)
	assert.Equal(t, false, ok)

	_, ok = operator.apply(3)
	assert.Equal(t, true, ok)

	operator = ConcurrentDistinct(true, func(x, y int) bool { return x == y }, func(x int) int { return x })

	_, ok = operator.apply(2)
	assert.Equal(t, true, ok)

	_, ok = operator.apply(2)
	assert.Equal(t, true, ok)

}

func TestLimit(t *testing.T) {

	operator := Limit[int](3)

	assert.Equal(t, true, operator.stateful)
	_, ok := operator.apply(1)
	assert.Equal(t, true, ok)
	_, ok = operator.apply(2)
	assert.Equal(t, true, ok)
	_, ok = operator.apply(3)
	assert.Equal(t, true, ok)
	_, ok = operator.apply(3)
	assert.Equal(t, false, ok)

}

func TestSkip(t *testing.T) {

	operator := Skip[int](3)

	assert.Equal(t, true, operator.stateful)
	_, ok := operator.apply(1)
	assert.Equal(t, false, ok)
	_, ok = operator.apply(2)
	assert.Equal(t, false, ok)
	_, ok = operator.apply(3)
	assert.Equal(t, false, ok)
	_, ok = operator.apply(3)
	assert.Equal(t, true, ok)

}

func TestConcurrentSkip(t *testing.T) {

	operator := ConcurrentSkip[int](3)

	assert.Equal(t, true, operator.stateful)
	_, ok := operator.apply(1)
	assert.Equal(t, false, ok)
	_, ok = operator.apply(2)
	assert.Equal(t, false, ok)
	_, ok = operator.apply(3)
	assert.Equal(t, false, ok)
	_, ok = operator.apply(3)
	assert.Equal(t, true, ok)

}

func TestPeek(t *testing.T) {

	var sb strings.Builder
	operator := Peek(func(x int) { sb.WriteString(fmt.Sprint(x)) })

	assert.Equal(t, false, operator.stateful)
	_, ok := operator.apply(1)
	assert.Equal(t, true, ok)
	_, ok = operator.apply(2)
	assert.Equal(t, true, ok)
	_, ok = operator.apply(3)
	assert.Equal(t, true, ok)
	assert.Equal(t, "123", sb.String())

}

func TestConcurrentLimit(t *testing.T) {

	operator := ConcurrentLimit[int](3)

	assert.Equal(t, true, operator.stateful)
	_, ok := operator.apply(1)
	assert.Equal(t, true, ok)
	_, ok = operator.apply(2)
	assert.Equal(t, true, ok)
	_, ok = operator.apply(3)
	assert.Equal(t, true, ok)
	_, ok = operator.apply(3)
	assert.Equal(t, false, ok)

}

func TestMap(t *testing.T) {

	operator := Map(func(x int) int { return x + 1 })

	assert.Equal(t, false, operator.stateful)
	val, ok := operator.apply(1)
	assert.Equal(t, 2, val)
	assert.Equal(t, true, ok)

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

	expectedOrdering := []string{"limit", "filter", "distinct", "peek", "filter", "distinct"}
	actualOrdering := []string{}

	for _, operator := range Sort(operations) {
		actualOrdering = append(actualOrdering, operator.name)
	}

	assert.Equal(t, expectedOrdering, actualOrdering)

}
