package streams

import (
	"fmt"
	"strings"
	"testing"

	"github.com/phantom820/collections/lists/list"
	"github.com/phantom820/collections/types"
	"github.com/stretchr/testify/assert"
)

// go test ./... -race -covermode=atomic -coverprofile=coverage.out
// go tool cover -html coverage.out -o coverage.html

type finiteSourceMock struct {
	size    int
	maxSize int
}

func (source *finiteSourceMock) Next() int {
	source.size++
	value := source.size
	return value
}

func (source *finiteSourceMock) HasNext() bool {
	if source.size < source.maxSize {
		return true
	}
	return false
}

type infiniteSourceMock struct {
	size int
}

func (source *infiniteSourceMock) Next() int {
	source.size++
	value := source.size
	return value
}

func (source *infiniteSourceMock) HasNext() bool {
	return true
}

func TestFromSource(t *testing.T) {

	slice := []int{1, 2, 3, 4, 5, 6}
	source := finiteSourceMock{maxSize: 6}
	stream := fromSource[int](&source)

	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, false, stream.Closed())
	assert.ElementsMatch(t, slice, stream.Collect())
	assert.Equal(t, true, stream.Terminated())
	assert.Equal(t, true, stream.Closed())

}
func TestFromSlice(t *testing.T) {

	slice := []int{1, 2, 3, 4, 5, 6}
	stream := fromSlice(func() []int { return slice })

	// Case 1 : Default just collect back to a slice.
	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, false, stream.Closed())
	assert.ElementsMatch(t, slice, stream.Collect())
	assert.Equal(t, true, stream.Terminated())
	assert.Equal(t, true, stream.Closed())

	// Case 2 : Slice changes before we invoke terminal operation.
	stream = fromSlice(func() []int { return slice })
	slice = append(slice, 23)
	assert.ElementsMatch(t, slice, stream.Collect())

	// Case 3 : Slice becomes nil before we invoke terminal operation.
	stream = fromSlice(func() []int { return slice })
	slice = nil
	assert.ElementsMatch(t, []int{}, stream.Collect())

}

func TestFromCollection(t *testing.T) {

	l := list.New[types.Int](1, 2, 3, 4, 5, 6)
	stream := fromCollection[types.Int](l)

	// Case 1 : Default just collect back to a slice.
	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, false, stream.Closed())
	assert.ElementsMatch(t, l.Collect(), stream.Collect())
	assert.Equal(t, true, stream.Terminated())
	assert.Equal(t, true, stream.Closed())

	// Case 2 : Collection changes before we invoke terminal operation.
	stream = fromCollection[types.Int](l)
	l.Add(23)
	assert.ElementsMatch(t, l.Collect(), stream.Collect())

	// Case 3 : Collection becomes nil before we invoke terminal operation.
	stream = fromCollection[types.Int](l)
	l = nil
	assert.ElementsMatch(t, list.New[types.Int](1, 2, 3, 4, 5, 6, 23).Collect(), stream.Collect())

}

func TestFilter(t *testing.T) {

	// Case 1 : Filter some items out.
	stream := fromSource[int](&finiteSourceMock{maxSize: 10})
	filteredStream := stream.Filter(func(x int) bool {
		return x%2 == 0
	})

	assert.Equal(t, false, stream.Terminated())
	assert.ElementsMatch(t, []int{2, 4, 6, 8, 10}, filteredStream.Collect())
	assert.Equal(t, true, stream.Closed())
	assert.Equal(t, true, filteredStream.Terminated())

	// Case 2 : Filter everything out.
	stream = fromSource[int](&finiteSourceMock{maxSize: 10})
	filteredStream = stream.Filter(func(x int) bool {
		return x > 100
	})
	assert.Equal(t, false, stream.Terminated())
	assert.ElementsMatch(t, []int{}, filteredStream.Collect())
	assert.Equal(t, true, stream.Closed())
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

func TestMap(t *testing.T) {

	stream := fromSource[int](&finiteSourceMock{maxSize: 10})
	mappedStream := stream.Map(func(x int) int {
		return x + 1
	})

	// Case : Map elements.
	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, false, mappedStream.Terminated())
	assert.ElementsMatch(t, []int{2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, mappedStream.Collect())
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

func TestLimit(t *testing.T) {

	stream := fromSource[int](&finiteSourceMock{maxSize: 10})

	// Case 1 : Limit with a valid number on a finite source.
	limitedStream := stream.Limit(2)

	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, true, stream.Closed())
	assert.Equal(t, false, limitedStream.Terminated())
	assert.Equal(t, false, limitedStream.Closed())

	slice := limitedStream.Collect()
	assert.Equal(t, true, limitedStream.Terminated())
	assert.Equal(t, true, limitedStream.Closed())

	assert.ElementsMatch(t, []int{1, 2}, slice)

	// Case 2: Limit when limit exceeds number of elements.
	stream = fromSource[int](&finiteSourceMock{maxSize: 3})
	limitedStream = stream.Limit(5)
	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, true, stream.Closed())
	assert.Equal(t, false, limitedStream.Terminated())
	assert.Equal(t, false, limitedStream.Closed())

	slice = limitedStream.Collect()
	assert.Equal(t, true, limitedStream.Terminated())
	assert.Equal(t, true, limitedStream.Closed())

	assert.ElementsMatch(t, []int{1, 2, 3}, slice)

	// Case 3 : Limit with a valid number on an infinite source.
	stream = fromSource[int](&infiniteSourceMock{})
	limitedStream = stream.Limit(10)

	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, false, limitedStream.Terminated())
	slice = limitedStream.Collect()
	assert.Equal(t, true, stream.Closed())
	assert.Equal(t, true, limitedStream.Terminated())
	assert.ElementsMatch(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, slice)

	// Case 4 : Limiting a terminated stream.
	t.Run("Limiting a terminated stream.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(*Error).Code())
			}
		}()
		limitedStream.Limit(1)
	})

	// Case 5 : Limit with an illegal number.
	stream = fromSource[int](&finiteSourceMock{maxSize: 10})
	t.Run("Limiting with an illegal argument.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, IllegalArgument, r.(Error).Code())
			}
		}()
		stream.Limit(-2)
	})
}

func TestSkip(t *testing.T) {

	stream := fromSource[int](&finiteSourceMock{maxSize: 10})

	// Case 1 : Skip with a valid number on a finite source.
	skippedStream := stream.Skip(2)

	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, false, skippedStream.Terminated())
	slice := skippedStream.Collect()
	assert.Equal(t, true, stream.Closed())
	assert.Equal(t, true, skippedStream.Terminated())
	assert.ElementsMatch(t, []int{3, 4, 5, 6, 7, 8, 9, 10}, slice)

	// Case 2 : Skip with fewer elements than skip value.
	stream = fromSource[int](&finiteSourceMock{maxSize: 10})
	skippedStream = stream.Skip(11)
	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, false, skippedStream.Terminated())
	slice = skippedStream.Collect()
	assert.Equal(t, true, stream.Closed())
	assert.Equal(t, true, skippedStream.Terminated())
	assert.ElementsMatch(t, []int{}, slice)

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
	stream = fromSource[int](&finiteSourceMock{maxSize: 10})
	t.Run("Skip with an illegal argument.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, IllegalArgument, r.(*Error).Code())
			}
		}()
		stream.Skip(-2)
	})
}

func TestDistinct(t *testing.T) {

	l := list.New[types.Int](1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6)
	stream := fromCollection[types.Int](l)

	// Case 1 : Apply Distinct on a stream that came from non distinct source.
	distinctStream := stream.Distinct(func(x, y types.Int) bool { return x == y }, func(x types.Int) int { return int(x) })
	slice := distinctStream.Collect()
	assert.Equal(t, true, stream.Closed())
	assert.Equal(t, true, distinctStream.Terminated())
	assert.Equal(t, []types.Int{1, 2, 3, 4, 5, 6}, slice)

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

func TestPeek(t *testing.T) {

	stream := fromSource[int](&finiteSourceMock{maxSize: 10})
	slice := make([]int, 0)
	otherStream := stream.Peek(func(x int) {
		slice = append(slice, x)
	})

	// Case 1 : Test peeking at elements;
	otherSlice := otherStream.Collect()
	assert.Equal(t, slice, otherSlice)

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

func TestForEach(t *testing.T) {

	source := finiteSourceMock{maxSize: 6}
	stream := fromSource[int](&source)

	// Case 1 : ForEach number of items in stream.
	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, false, stream.Closed())
	sb := make([]string, 0)
	stream.ForEach(func(x int) {
		sb = append(sb, fmt.Sprint(x))
	})
	assert.Equal(t, "1, 2, 3, 4, 5, 6", strings.Join(sb, ", "))
	assert.Equal(t, true, stream.Terminated())
	assert.Equal(t, true, stream.Closed())

	// Case 2 : ForEach on a terminated stream.
	t.Run("ForEach on  a terminated stream", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(*Error).Code())
			}
		}()
		stream.ForEach(func(x int) {})
	})

}
func TestCount(t *testing.T) {

	source := finiteSourceMock{maxSize: 6}
	stream := fromSource[int](&source)

	// Case 1 : Count number of items in stream.
	assert.Equal(t, false, stream.Terminated())
	assert.Equal(t, false, stream.Closed())
	assert.Equal(t, 6, stream.Count())
	assert.Equal(t, true, stream.Terminated())
	assert.Equal(t, true, stream.Closed())

	// Case 2 : Count on a terminated stream.
	t.Run("Count on  a terminated stream", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(*Error).Code())
			}
		}()
		stream.Count()
	})
}

func TestReduce(t *testing.T) {

	source := finiteSourceMock{maxSize: 10}
	stream := fromSource[int](&source)

	// Case 1 : Reduce on a stream with at least 2 elements.
	assert.Equal(t, false, stream.Terminated())
	sum, ok := stream.Reduce(func(x, y int) int {
		return x + y
	})
	assert.Equal(t, true, stream.Terminated())
	assert.Equal(t, 55, sum)
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
	stream = fromSource[int](&source)

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

func TestCollect(t *testing.T) {

	source := finiteSourceMock{maxSize: 6}

	stream := fromSource[int](&source)

	// Case 1 : Just collect the stream to a slice.
	assert.ElementsMatch(t, []int{1, 2, 3, 4, 5, 6}, stream.Collect())

}

func TestIntegration(t *testing.T) {

	// Case 1 : Infinite stream -> filter out odd numbers and then get sum of first n even squares.
	stream := fromSource[int](&infiniteSourceMock{})

	sum, ok := stream.Filter(func(x int) bool {
		return x%2 == 0
	}).Map(func(x int) int {
		return x * x
	}).Limit(10).Reduce(func(x, y int) int {
		return x + y
	})

	assert.Equal(t, true, ok)
	assert.Equal(t, 1540, sum)

	// Case 2 : Infinite stream -> skip first 10 elements , keep only multiples of 3 limited to the first n.
	res, ok := fromSource[int](&infiniteSourceMock{}).Skip(10).Filter(func(x int) bool {
		return x%3 == 0
	}).Map(func(x int) int {
		return x
	}).Limit(4).Reduce(func(x, y int) int {
		return x + y
	})

	assert.Equal(t, true, ok)
	assert.Equal(t, 66, res)

	// Case 3 : Finite stream -> distinct and count.
	fruitStream := fromCollection[types.String](
		list.New[types.String]("Apple", "Banana", "Orange", "Apple", "Kiwi", "Kiwi", "Orange", "Apple", "Watermelon"))

	count := fruitStream.Distinct(func(x, y types.String) bool { return x == y }, func(x types.String) int { return x.HashCode() }).Count()
	assert.Equal(t, 5, count)

	// Case 4 : Finite stream -> filter, distinct and count.
	fruitStream = fromCollection[types.String](
		list.New[types.String]("Apple", "Banana", "Orange", "Apple", "Kiwi", "Kiwi", "Orange", "Apple", "Watermelon"))

	count = fruitStream.Filter(func(x types.String) bool {
		return x != "Banana"
	}).Distinct(func(x, y types.String) bool {
		return x == y
	}, func(x types.String) int {
		return x.HashCode()
	}).Count()

	assert.Equal(t, 4, count)

	// Case 5 : Finite stream -> filter , skip .
	fruitStream = fromCollection[types.String](
		list.New[types.String]("Apple", "Banana", "Orange", "Apple", "Kiwi", "Kiwi", "Orange", "Apple", "Watermelon"))

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
