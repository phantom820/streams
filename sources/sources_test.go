package sources

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type finiteSourceMock struct {
	value    int
	maxValue int
}

func (source *finiteSourceMock) Next() int {
	source.value++
	value := source.value
	return value
}

func (source *finiteSourceMock) HasNext() bool {
	if source.value < source.maxValue {
		return true
	}
	return false
}

func TestPartition(t *testing.T) {

	source := finiteSourceMock{maxValue: 1000}

	// Case 1 : Partition into a perfect fit.
	partitionedSource := NewPartitionedSource[int](&source)
	partitionedSource.Partition(4)
	assert.Equal(t, 4, partitionedSource.Len())

	// Case 2 : Partition into an imperfect fit.
	source = finiteSourceMock{maxValue: 1000}
	partitionedSource.Partition(6)
	assert.Equal(t, 6, partitionedSource.Len())

	// Case 3 : Elements of individual partitions.
	source = finiteSourceMock{maxValue: 10}
	partitionedSource.Partition(3)

	assert.Equal(t, []int{1, 2, 3, 4}, collectSource(partitionedSource.At(0)))
	assert.Equal(t, []int{5, 6, 7, 8}, collectSource(partitionedSource.At(1)))
	assert.Equal(t, []int{9, 10}, collectSource(partitionedSource.At(2)))

	t.Run("Access out of bounds.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.NotNil(t, r.(error))
			}
		}()
		partitionedSource.At(3)
	})

}
