// package stream brings a java streams like API to go which can be used to do more functional programming and avoid materialization consists
// when manipulating collections / slices.
package streams

import (
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/phantom820/collections/lists/list"
	"github.com/phantom820/collections/types"
	"github.com/stretchr/testify/assert"
)

func TestConcurrentForEach(t *testing.T) {

	l := list.New[types.Int]()

	stream := concurrentFromCollection[types.Int](l, 3)
	slice := make([]types.Int, 0)

	// Case 1 : ForEach on a stream with no elements.
	assert.Equal(t, false, stream.Terminated())
	stream.ForEach(func(x types.Int) {
		slice = append(slice, x)
	})
	assert.Equal(t, true, stream.Terminated())
	assert.ElementsMatch(t, []types.Int{}, slice)

	// Case 2: ForEach on a stream with elements.
	l.Add(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	stream = concurrentFromCollection[types.Int](l, 3)
	assert.Equal(t, false, stream.Terminated())
	stream.ForEach(func(x types.Int) {
		slice = append(slice, x)
	})
	assert.Equal(t, true, stream.Terminated())
	assert.ElementsMatch(t, []types.Int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, slice)

	// Case 3 : ForEach on a terminated stream.
	t.Run("ForEach a terminated stream.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(*Error).Code())
			}
		}()
		stream.ForEach(func(x types.Int) {})
	})

}

func TestConcurrentCount(t *testing.T) {

	l := list.New[types.Int]()

	stream := concurrentFromCollection[types.Int](l, 3)

	// Case 1: Count on a stream with no elements.
	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, 0, stream.Count())
	assert.Equal(t, true, stream.Terminated())

	// Case 2 : Count on a stream with elements.
	l.Add(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	stream = concurrentFromCollection[types.Int](l, 3)
	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, 10, stream.Count())
	assert.Equal(t, true, stream.Terminated())

	// Case 3 : Count on a terminated stream.
	t.Run("Count a terminated stream.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(*Error).Code())
			}
		}()
		stream.Count()
	})

}

func TestConcurrentCollect(t *testing.T) {

	// Case 1 : Collect an empty stream.
	slice := []int{}
	stream := concurrentFromSlice(func() []int { return slice }, 3)
	assert.Equal(t, false, stream.Terminated())
	assert.ElementsMatch(t, []types.Int{}, stream.Collect())
	assert.Equal(t, true, stream.Terminated())
	slice = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	// Case 2 : Collect a stream with elements.
	stream = concurrentFromSlice(func() []int { return slice }, 3)
	assert.Equal(t, false, stream.Terminated())
	assert.ElementsMatch(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, stream.Collect())
	assert.Equal(t, true, stream.Terminated())

	// Case 3 : Collect a terminated stream.
	t.Run("Collect a terminated stream.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(*Error).Code())
			}
		}()
		stream.Collect()
	})

}

func TestConcurrentFilter(t *testing.T) {

	// Case 1 : Filter some items out.
	stream := concurrentFromSource[int](&finiteSourceMock{maxSize: 10}, 2)
	filteredStream := stream.Filter(func(x int) bool {
		return x%2 == 0
	})

	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, true, stream.Closed())
	assert.ElementsMatch(t, []int{2, 4, 6, 8, 10}, filteredStream.Collect())
	assert.Equal(t, true, stream.Closed())
	assert.Equal(t, true, filteredStream.Closed())
	assert.Equal(t, true, filteredStream.Terminated())

	// Case 2 : Filter everything out.
	stream = concurrentFromSource[int](&finiteSourceMock{maxSize: 10}, 2)
	filteredStream = stream.Filter(func(x int) bool {
		return x > 100
	})
	assert.Equal(t, false, stream.Terminated())
	assert.ElementsMatch(t, []int{}, filteredStream.Collect())
	assert.Equal(t, true, stream.Closed())
	assert.Equal(t, true, filteredStream.Closed())
	assert.Equal(t, true, filteredStream.Terminated())

	// Case 3 : Filter a terminated stream.
	t.Run("Filtering a terminated stream", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(*Error).Code())
			}
		}()
		filteredStream.Filter(func(x int) bool { return true })
	})

}

func TestConcurrentMap(t *testing.T) {

	stream := concurrentFromSource[int](&finiteSourceMock{maxSize: 10}, 2)
	mappedStream := stream.Map(func(x int) int {
		return x + 1
	})

	// Case : Map elements.
	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, false, mappedStream.Terminated())
	assert.ElementsMatch(t, []int{2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, mappedStream.Collect())
	assert.Equal(t, true, stream.Closed())
	assert.Equal(t, true, mappedStream.Terminated())

	// Case 2 : Mapping a terminated stream.
	t.Run("Mapping a terminated stream.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(*Error).Code())
			}
		}()
		mappedStream.Map(func(x int) int { return x })
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

func TestConcurrentLimit(t *testing.T) {

	stream := concurrentFromSource[int](&finiteSourceMock{maxSize: 20}, 3)
	runs := 1000

	// Case 1 : Limit with a valid number on a finite source. This should not fail.
	for i := 0; i < runs; i++ {
		stream = concurrentFromSource[int](&finiteSourceMock{maxSize: 200}, 4)
		limit := 4 + rand.Intn((100))
		limitedStream := stream.Limit(limit)
		assert.Equal(t, true, stream.Closed())
		assert.Equal(t, false, limitedStream.Terminated())
		slice := limitedStream.Collect()
		assert.Equal(t, true, limitedStream.Terminated())
		assert.Equal(t, limit, len(slice))
	}

	stream = concurrentFromSource[int](&finiteSourceMock{maxSize: 20}, 3)
	limitedStream := stream.Limit(4)
	limitedStream.Collect()

	// Case 3 : Limiting a terminated stream.
	t.Run("Limiting a terminated stream.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(*Error).Code())
			}
		}()
		limitedStream.Limit(1)
	})

	// Case 4 : Limit with an illegal number.
	stream = concurrentFromSource[int](&finiteSourceMock{maxSize: 10}, 3)
	t.Run("Limiting with an illegal argument.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, IllegalArgument, r.(Error).Code())
			}
		}()
		stream.Limit(-2)
	})

}

func TestConcurrentPeek(t *testing.T) {

	stream := concurrentFromSource[int](&finiteSourceMock{maxSize: 10}, 2)
	slice := make([]int, 0)
	mu := sync.Mutex{}
	otherStream := stream.Peek(func(x int) {
		mu.Lock()
		defer mu.Unlock()
		slice = append(slice, x)
	})

	// Case 1 : Test peeking at elements;
	otherSlice := otherStream.Collect()
	assert.ElementsMatch(t, slice, otherSlice)

	// Case 2: Test peeking for a terminated stream.
	t.Run("Peek on  a terminated stream", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(*Error).Code())
			}
		}()
		otherStream.Peek(func(x int) {})
	})

}

func TestConcurrentSkip(t *testing.T) {

	stream := concurrentFromSource[int](&finiteSourceMock{maxSize: 10}, 3)

	// Case 1 : Skip with a valid number on a finite source.
	skippedStream := stream.Skip(2)

	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, false, skippedStream.Terminated())
	slice := skippedStream.Collect()
	assert.Equal(t, true, stream.Closed())
	assert.Equal(t, true, skippedStream.Terminated())
	assert.Equal(t, 8, len(slice))

	// Case 2 : Skip a terminated stream.
	t.Run("Skipping a terminated stream", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(*Error).Code())
			}
		}()
		skippedStream.Skip(1)
	})

	// Case 3 : Skip with an illegal number.
	stream = concurrentFromSource[int](&finiteSourceMock{maxSize: 10}, 3)
	t.Run("Skip with an illegal argument.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, IllegalArgument, r.(Error).Code())
			}
		}()
		stream.Skip(-2)
	})
}

func TestConcurrentDistinct(t *testing.T) {

	l := list.New[types.Int](1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6)
	stream := concurrentFromCollection[types.Int](l, 4)

	// Case 1 : Apply Distinct on a stream that came from non distinct source.
	distinctStream := stream.Distinct(func(x, y types.Int) bool { return x == y }, func(x types.Int) int { return int(x) })
	slice := distinctStream.Collect()
	assert.Equal(t, true, stream.Closed())
	assert.Equal(t, true, distinctStream.Terminated())
	assert.ElementsMatch(t, []types.Int{1, 2, 3, 4, 5, 6}, slice)

	// Case 2 : Try Distinct on a terminated stream.
	t.Run("Distinct on  a terminated stream", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(*Error).Code())
			}
		}()
		distinctStream.Distinct(func(x, y types.Int) bool { return x == y }, func(x types.Int) int { return int(x) })
	})
}

func TestConcurrentReduce(t *testing.T) {

	source := finiteSourceMock{maxSize: 100}
	stream := concurrentFromSource[int](&source, 3)

	// Case 1 : Reduce on a stream with at least 2 elements.
	assert.Equal(t, false, stream.Terminated())
	sum, ok := stream.Reduce(func(x, y int) int {
		return x + y
	})
	assert.Equal(t, true, stream.Terminated())
	assert.Equal(t, 5050, sum)
	assert.Equal(t, true, ok)

	// Case 2 : Reduce on a stream with 1 element.
	source = finiteSourceMock{maxSize: 1}
	stream = fromSource[int](&source)

	sum, ok = stream.Reduce(func(x, y int) int {
		return x + y
	})

	assert.Equal(t, 1, sum)
	assert.Equal(t, true, ok)

	// Case 2 : Reduce on a stream with 0 elements.
	source = finiteSourceMock{maxSize: 0}
	stream = concurrentFromSource[int](&source, 3)

	sum, ok = stream.Reduce(func(x, y int) int {
		return x + y
	})

	assert.Equal(t, 0, sum)
	assert.Equal(t, false, ok)

	// Case 3 : Try Reduce on a terminated stream.
	t.Run("Reduce on  a terminated stream", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(*Error).Code())
			}
		}()
		sum, ok = stream.Reduce(func(x, y int) int {
			return x + y
		})
	})
}

func TestConcurrentIntegration(t *testing.T) {

	// Case 1 : Filter then map.
	vowels := concurrentFromSlice(func() []string { return []string{"A", "B", "C", "D", "E", "F", "G", "H", "I"} }, 1).
		Filter(func(x string) bool {
			return strings.ContainsAny(x, "AEIOU")
		}).Map(func(x string) string {
		return strings.ToLower(x)
	}).Collect()
	assert.ElementsMatch(t, []string{"a", "e", "i"}, vowels)

	// Case 2 : Filter limit.
	vowels = concurrentFromSlice(func() []string { return []string{"A", "B", "C", "D", "E", "F", "G", "H", "I"} }, 1).
		Filter(func(x string) bool {
			return strings.ContainsAny(x, "AEIOU")
		}).Map(func(x string) string {
		return strings.ToLower(x)
	}).Limit(2).Collect()
	assert.ElementsMatch(t, []string{"a", "e"}, vowels)

	// Case 3 : Finite stream -> distinct and count.
	fruitStream := concurrentFromCollection[types.String](
		list.New[types.String]("Apple", "Banana", "Orange", "Apple", "Kiwi", "Kiwi", "Orange", "Apple", "Watermelon"), 4)

	count := fruitStream.Distinct(func(x, y types.String) bool { return x == y }, func(x types.String) int { return x.HashCode() }).Count()
	assert.Equal(t, 5, count)

	// Case 4 : Finite stream -> filter, distinct and count.
	fruitStream = concurrentFromCollection[types.String](
		list.New[types.String]("Apple", "Banana", "Orange", "Apple", "Kiwi", "Kiwi", "Orange", "Apple", "Watermelon"), 4)

	count = fruitStream.Filter(func(x types.String) bool {
		return x != "Banana"
	}).Distinct(func(x, y types.String) bool {
		return x == y
	}, func(x types.String) int {
		return x.HashCode()
	}).Count()

	assert.Equal(t, 4, count)

	// Case 5 : Finite stream -> filter , skip .
	fruitStream = concurrentFromCollection[types.String](
		list.New[types.String]("Apple", "Banana", "Orange", "Apple", "Kiwi", "Kiwi", "Orange", "Apple", "Watermelon"), 3)

	count = fruitStream.Filter(func(x types.String) bool {
		return x != "Banana"
	}).Skip(2).Count()

	assert.Equal(t, 6, count)

	// Case 6 : Finite stream -> filter , limit .
	fruitStream = fromCollection[types.String](
		list.New[types.String]("Apple", "Banana", "Orange", "Apple", "Kiwi", "Kiwi", "Orange", "Apple", "Watermelon"))

	count = fruitStream.Filter(func(x types.String) bool {
		return x != "Banana"
	}).Limit(4).Count()

	assert.Equal(t, 4, count)

}
