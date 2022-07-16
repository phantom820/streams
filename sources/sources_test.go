package sources

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartitionSource(t *testing.T) {

	source := finiteSourceMock{maxSize: 10}

	// Case 1 : Perfect fit partition.
	sources := partitionSource[int](&source, 2)
	a := make([]int, 0)
	b := make([]int, 0)

	for i, s := range sources {
		for s.HasNext() {
			if i == 0 {
				a = append(a, s.Next())
			} else {
				b = append(b, s.Next())
			}
		}
	}

	assert.ElementsMatch(t, []int{1, 2, 3, 4, 5}, a)
	assert.ElementsMatch(t, []int{6, 7, 8, 9, 10}, b)

	// Case 2 : Imperfect partition.

	source = finiteSourceMock{maxSize: 10}
	sources = partitionSource[int](&source, 3)
	a = make([]int, 0)
	b = make([]int, 0)
	c := make([]int, 0)
	d := make([]int, 0)

	for i, s := range sources {
		for s.HasNext() {
			if i == 0 {
				a = append(a, s.Next())
			} else if i == 1 {
				b = append(b, s.Next())
			} else if i == 2 {
				c = append(c, s.Next())
			} else {
				d = append(d, s.Next())
			}
		}
	}

	assert.ElementsMatch(t, []int{1, 2, 3}, a)
	assert.ElementsMatch(t, []int{4, 5, 6}, b)
	assert.ElementsMatch(t, []int{7, 8, 9}, c)
	assert.ElementsMatch(t, []int{10}, d)

}
