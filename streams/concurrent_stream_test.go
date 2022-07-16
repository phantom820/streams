package streams

import (
	"testing"

	"github.com/phantom820/collections/lists/list"
	"github.com/phantom820/collections/types"
	"github.com/stretchr/testify/assert"
)

func TestConcurrentForEach(t *testing.T) {

	l := list.New[types.Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	stream := ConcurrentFromCollection[types.Int](l, 3)
	slice := make([]types.Int, 0)

	// Case 1: ConcurrentForEach on a stream with elements.
	assert.Equal(t, false, stream.Terminated())
	stream.ForEach(func(x types.Int) {
		slice = append(slice, x)
	})
	assert.Equal(t, true, stream.Terminated())
	assert.ElementsMatch(t, []types.Int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, slice)

	// Case 2 : ConcurrentForEach on a terminated stream.

}

func TestConcurrentCount(t *testing.T) {

	l := list.New[types.Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	stream := ConcurrentFromCollection[types.Int](l, 3)

	// Case 1: ConcurrentCount on a stream with elements.
	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, 10, stream.Count())
	assert.Equal(t, true, stream.Terminated())

}
