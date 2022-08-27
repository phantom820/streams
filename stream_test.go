package streams

import (
	"fmt"
	"testing"

	"github.com/phantom820/collections/lists/list"
	"github.com/phantom820/collections/types"
	"github.com/stretchr/testify/assert"
)

func TestNewFromSource(t *testing.T) {

	// Case 1 : Sequential stream.
	stream := NewFromSource[int](&finiteSourceMock{maxSize: 10}, 1)
	assert.NotNil(t, stream)
	assert.Equal(t, false, stream.Concurrent())

	// Case 2 : Concurrent stream.
	stream = NewFromSource[int](&finiteSourceMock{maxSize: 10}, 2)
	assert.Equal(t, true, stream.Concurrent())

	// Case 3 : Invalid concurrency.
	t.Run("FromSource with invalid concurrency", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, IllegalConfig, r.(Error).Code())
			}
		}()
		NewFromSource[int](&finiteSourceMock{maxSize: 10}, 0)
	})
}

func TestNewFromCollection(t *testing.T) {

	collection := list.New[types.Int](1, 2, 3, 4, 5, 6, 7, 8, 9)

	// Case 1 : Sequential stream.
	stream := NewFromCollection[types.Int](collection, 1)
	assert.NotNil(t, stream)
	assert.Equal(t, false, stream.Concurrent())

	// Case 2 : Concurrent stream.
	stream = NewFromCollection[types.Int](collection, 2)
	assert.Equal(t, true, stream.Concurrent())

	// Case 3 : Invalid concurrency.
	t.Run("FromSource with invalid concurrency", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, IllegalConfig, r.(Error).Code())
			}
		}()
		NewFromCollection[types.Int](collection, 0)
	})
}

func TestNewFromSlice(t *testing.T) {

	slice := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	// Case 1 : Sequential stream.
	stream := NewFromSlice(func() []int { return slice }, 1)
	assert.NotNil(t, stream)
	assert.Equal(t, false, stream.Concurrent())

	// Case 2 : Concurrent stream.
	stream = NewFromSlice(func() []int { return slice }, 2)
	assert.Equal(t, true, stream.Concurrent())

	// Case 3 : Invalid concurrency.
	t.Run("FromSource with invalid concurrency", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, IllegalConfig, r.(Error).Code())
			}
		}()
		NewFromSlice(func() []int { return slice }, 0)
	})
}

func TestTopLevelMap(t *testing.T) {

	stream := fromSource[int](&finiteSourceMock{maxSize: 10})
	mappedStream := Map(stream, func(e int) string { return fmt.Sprint(e) })
	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, false, mappedStream.Terminated())
	assert.ElementsMatch(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, mappedStream.Collect())
	assert.Equal(t, true, stream.Closed())
	assert.Equal(t, true, mappedStream.Closed())
	assert.Equal(t, true, mappedStream.Terminated())

	// Case 2 : Mapping a terminated stream.
	t.Run("Mapping a terminated stream.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(*Error).Code())
			}
		}()
		mappedStream.Map(func(x string) string { return x })
	})

	// Case 3 : Mapping on a closed stream.
	t.Run("Mapping a closed stream.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamClosed, r.(*Error).Code())
			}
		}()
		stream.Map(func(x int) int { return x })
	})
}

func TestTopLevelConcurrentMap(t *testing.T) {

	stream := concurrentFromSource[int](&finiteSourceMock{maxSize: 10}, 2)
	mappedStream := Map(stream, func(e int) string { return fmt.Sprint(e) })
	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, false, mappedStream.Terminated())
	assert.ElementsMatch(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, mappedStream.Collect())
	assert.Equal(t, true, stream.Closed())
	assert.Equal(t, true, mappedStream.Closed())
	assert.Equal(t, true, mappedStream.Terminated())

	// Case 2 : Mapping a terminated stream.
	t.Run("Mapping a terminated stream.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(*Error).Code())
			}
		}()
		mappedStream.Map(func(x string) string { return x })
	})

	// Case 3 : Mapping on a closed stream.
	t.Run("Mapping a closed stream.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamClosed, r.(*Error).Code())
			}
		}()
		stream.Map(func(x int) int { return x })
	})
}
