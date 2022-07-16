package streams

import (
	"fmt"
	"strings"
	"testing"

	"github.com/phantom820/collections/lists/list"
	"github.com/phantom820/collections/sets/hashset"
	"github.com/phantom820/collections/sets/treeset"
	"github.com/phantom820/collections/types"
	"github.com/stretchr/testify/assert"
)

// go test ./... -race -covermode=atomic -coverprofile=coverage.out
// go tool cover -html coverage.out -o coverage.html

func TestFromSource(t *testing.T) {

	slice := []int{1, 2, 3, 4, 5, 6}
	source := finiteSourceMock{maxSize: 6}
	s := FromSource[int](&source)

	assert.Equal(t, false, s.Terminated())
	assert.Equal(t, false, s.(*stream[int]).closed)
	assert.ElementsMatch(t, slice, ToSlice(s))
	assert.Equal(t, true, s.Terminated())
	assert.Equal(t, true, s.(*stream[int]).closed)

}
func TestFromSlice(t *testing.T) {

	slice := []int{1, 2, 3, 4, 5, 6}
	s := FromSlice(func() []int { return slice })

	// Case 1 : Default just collect back to a slice.
	assert.Equal(t, false, s.Terminated())
	assert.Equal(t, false, s.(*stream[int]).distinct)
	assert.Equal(t, false, s.(*stream[int]).closed)
	assert.ElementsMatch(t, slice, ToSlice(s))
	assert.Equal(t, true, s.Terminated())
	assert.Equal(t, true, s.(*stream[int]).closed)

	// Case 2 : Slice changes before we invoke terminal operation.
	s = FromSlice(func() []int { return slice })
	slice = append(slice, 23)
	assert.ElementsMatch(t, slice, ToSlice(s))

	// Case 3 : Slice becomes nil before we invoke terminal operation.
	s = FromSlice(func() []int { return slice })
	slice = nil
	assert.ElementsMatch(t, []int{}, ToSlice(s))

}

// func TestFromSet(t *testing.T) {

// 	set := hashset.New[types.Int](1, 2, 3, 4, 5, 6)
// 	s := FromSet[types.Int](set)

// 	// Case 1 : Default just collect back to a slice.
// 	assert.Equal(t, false, s.Terminated())
// 	assert.Equal(t, true, s.(*stream[types.Int]).distinct)
// 	assert.Equal(t, false, s.(*stream[types.Int]).closed)
// 	assert.Equal(t, true, set.Equals(ToHashSet(s)))
// 	assert.Equal(t, true, s.Terminated())
// 	assert.Equal(t, true, s.(*stream[types.Int]).closed)

// 	// Case 2 : Set changes before we invoke terminal operation.
// 	s = FromSet[types.Int](set)
// 	set.Add(23)
// 	assert.Equal(t, true, set.Equals(ToHashSet(s)))

// 	// Case 3 : Set becomes nil before we invoke terminal operation.
// 	s = FromSet[types.Int](set)
// 	set = nil
// 	assert.Equal(t, true, list.New[types.Int](1, 2, 3, 4, 5, 6, 23).Equals(ToList(s)))

// }

func TestFromCollection(t *testing.T) {

	l := list.New[types.Int](1, 2, 3, 4, 5, 6)
	s := FromCollection[types.Int](l)

	// Case 1 : Default just collect back to a slice.
	assert.Equal(t, false, s.Terminated())
	assert.Equal(t, false, s.(*stream[types.Int]).distinct)
	assert.Equal(t, false, s.(*stream[types.Int]).closed)
	assert.Equal(t, true, l.Equals(ToList(s)))
	assert.Equal(t, true, s.Terminated())
	assert.Equal(t, true, s.(*stream[types.Int]).closed)

	// Case 2 : Collection changes before we invoke terminal operation.
	s = FromCollection[types.Int](l)
	l.Add(23)
	assert.Equal(t, true, l.Equals(ToList(s)))

	// Case 3 : Collection becomes nil before we invoke terminal operation.
	s = FromCollection[types.Int](l)
	l = nil
	assert.Equal(t, true, list.New[types.Int](1, 2, 3, 4, 5, 6, 23).Equals(ToList(s)))

}

func TestToSet(t *testing.T) {

	set := hashset.New[types.Int](1, 2, 3, 4, 5, 6)
	s := FromCollection[types.Int](set)

	assert.Equal(t, false, s.Terminated())
	assert.Equal(t, false, s.(*stream[types.Int]).closed)
	assert.Equal(t, true, set.Equals(ToHashSet(s)))
	assert.Equal(t, true, s.Terminated())
	assert.Equal(t, true, s.(*stream[types.Int]).closed)
}

func TestToTreeSet(t *testing.T) {

	set := treeset.New[types.Int](1, 2, 3, 4, 5, 6)
	s := FromCollection[types.Int](set)

	assert.Equal(t, false, s.Terminated())
	assert.Equal(t, false, s.(*stream[types.Int]).closed)
	assert.Equal(t, true, set.Equals(ToTreeSet(s)))
	assert.Equal(t, true, s.Terminated())
	assert.Equal(t, true, s.(*stream[types.Int]).closed)
}

func TestToList(t *testing.T) {

	l := list.New[types.Int](1, 2, 3, 4, 5, 6)
	s := FromCollection[types.Int](l)

	assert.Equal(t, false, s.Terminated())
	assert.Equal(t, false, s.(*stream[types.Int]).closed)
	assert.Equal(t, true, l.Equals(ToList(s)))
	assert.Equal(t, true, s.Terminated())
	assert.Equal(t, true, s.(*stream[types.Int]).closed)
}

func TestFilter(t *testing.T) {

	l := list.New[types.Int](1, 2, 3, 4, 5, 6)
	rawStream := FromCollection[types.Int](l)

	// Case 1 : Filter some items out.
	filteredStream := rawStream.Filter(func(x types.Int) bool {
		return x > 2
	})
	slice := ToSlice(filteredStream)
	assert.Equal(t, true, rawStream.Terminated())
	assert.Equal(t, true, filteredStream.Terminated())
	assert.Equal(t, []types.Int{3, 4, 5, 6}, slice)

	// Case 2 : Try filtering a terminated stream.
	t.Run("Filtering a terminated stream", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(Error).Code())
			}
		}()
		rawStream.Filter(func(x types.Int) bool { return true })
	})

}

func TestMap(t *testing.T) {

	l := list.New[types.Int](1, 2, 3, 4, 5, 6)
	rawStream := FromCollection[types.Int](l)

	// Case 1 : Filter some items out.
	mappedStream := rawStream.Map(func(x types.Int) interface{} {
		return x + 2
	})

	assert.Equal(t, false, rawStream.Terminated())
	assert.Equal(t, false, mappedStream.Terminated())
	slice := ToSlice(mappedStream)
	assert.Equal(t, true, rawStream.Terminated())
	assert.Equal(t, true, mappedStream.Terminated())
	assert.ElementsMatch(t, []types.Int{3, 4, 5, 6, 7, 8}, slice)

	// Case 2 : Try filtering a terminated stream.
	t.Run("Mapping a terminated stream.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(Error).Code())
			}
		}()
		rawStream.Map(func(x types.Int) interface{} { return x + 1 })
	})

}

func TestLimit(t *testing.T) {

	rawStream := FromSource[int](&finiteSourceMock{maxSize: 10})

	// Case 1 : Limit with a valid number on a finite source.
	limitedStream := rawStream.Limit(2)

	assert.Equal(t, false, rawStream.Terminated())
	assert.Equal(t, false, limitedStream.Terminated())
	slice := ToSlice(limitedStream)
	assert.Equal(t, true, rawStream.Terminated())
	assert.Equal(t, true, limitedStream.Terminated())
	assert.ElementsMatch(t, []int{1, 2}, slice)

	// Case 2 : Limit with a valid number on an infinite source.
	rawStream = FromSource[int](&infiniteSourceMock{})
	limitedStream = rawStream.Limit(10)

	assert.Equal(t, false, rawStream.Terminated())
	assert.Equal(t, false, limitedStream.Terminated())
	slice = ToSlice(limitedStream)
	assert.Equal(t, true, rawStream.Terminated())
	assert.Equal(t, true, limitedStream.Terminated())
	assert.ElementsMatch(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, slice)

	// Case 3 : Limiting a terminated stream.
	t.Run("Limiting a terminated stream.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(Error).Code())
			}
		}()
		rawStream.Limit(1)
	})

	// Case 4 : Limit with an illegal number.
	rawStream = FromSource[int](&finiteSourceMock{maxSize: 10})
	t.Run("Limiting with an illegal argument.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, IllegalArgument, r.(Error).Code())
			}
		}()
		rawStream.Limit(-2)
	})
}

func TestSkip(t *testing.T) {

	rawStream := FromSource[int](&finiteSourceMock{maxSize: 10})

	// Case 1 : Skip with a valid number on a finite source.
	skippedStream := rawStream.Skip(2)

	assert.Equal(t, false, rawStream.Terminated())
	assert.Equal(t, false, skippedStream.Terminated())
	slice := ToSlice(skippedStream)
	assert.Equal(t, true, rawStream.Terminated())
	assert.Equal(t, true, skippedStream.Terminated())
	assert.ElementsMatch(t, []int{3, 4, 5, 6, 7, 8, 9, 10}, slice)

	// Case 2 : Skip a terminated stream.
	t.Run("Skipping a terminated stream", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(Error).Code())
			}
		}()
		rawStream.Skip(1)
	})

	// Case 3 : Skip with an illegal number.
	rawStream = FromSource[int](&finiteSourceMock{maxSize: 10})
	t.Run("Skip with an illegal argument.", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, IllegalArgument, r.(Error).Code())
			}
		}()
		rawStream.Skip(-2)
	})
}

func TestDistinct(t *testing.T) {

	l := list.New[types.Int](1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6)
	rawStream := FromCollection[types.Int](l)

	// Case 1 : Apply Distinct on a stream that came from non distinct source.
	distinctStream := rawStream.Distinct(func(x, y types.Int) bool { return x == y }, func(x types.Int) int { return int(x) })
	slice := ToSlice(distinctStream)
	assert.Equal(t, true, rawStream.Terminated())
	assert.Equal(t, true, distinctStream.Terminated())
	assert.Equal(t, []types.Int{1, 2, 3, 4, 5, 6}, slice)

	// Case 2 : Try Distinct on a terminated stream.
	t.Run("Distinct on  a terminated stream", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(Error).Code())
			}
		}()
		rawStream.Distinct(func(x, y types.Int) bool { return x == y }, func(x types.Int) int { return int(x) })
	})

}

func TestForEach(t *testing.T) {

	source := finiteSourceMock{maxSize: 6}
	rawStream := FromSource[int](&source)

	// Case 1 : ForEach number of items in stream.
	assert.Equal(t, false, rawStream.Terminated())
	assert.Equal(t, false, rawStream.(*stream[int]).closed)
	sb := make([]string, 0)
	rawStream.ForEach(func(x int) {
		sb = append(sb, fmt.Sprint(x))
	})
	assert.Equal(t, "1, 2, 3, 4, 5, 6", strings.Join(sb, ", "))
	assert.Equal(t, true, rawStream.Terminated())
	assert.Equal(t, true, rawStream.(*stream[int]).closed)

	// Case 2 : ForEach on a terminated stream.
	t.Run("ForEach on  a terminated stream", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(Error).Code())
			}
		}()
		rawStream.ForEach(func(x int) {})
	})

}
func TestCount(t *testing.T) {

	source := finiteSourceMock{maxSize: 6}
	rawStream := FromSource[int](&source)

	// Case 1 : Count number of items in stream.
	assert.Equal(t, false, rawStream.Terminated())
	assert.Equal(t, false, rawStream.(*stream[int]).distinct)
	assert.Equal(t, false, rawStream.(*stream[int]).closed)
	assert.Equal(t, 6, rawStream.Count())
	assert.Equal(t, true, rawStream.Terminated())
	assert.Equal(t, true, rawStream.(*stream[int]).closed)

	// Case 2 : Count on a terminated stream.
	t.Run("Count on  a terminated stream", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(Error).Code())
			}
		}()
		rawStream.Count()
	})
}

func TestReduce(t *testing.T) {

	source := finiteSourceMock{maxSize: 10}
	rawStream := FromSource[int](&source)

	// Case 1 : Reduce on a stream with at least 2 elements.
	assert.Equal(t, false, rawStream.Terminated())
	sum, ok := rawStream.Reduce(func(x, y int) interface{} {
		return x + y
	})
	assert.Equal(t, true, rawStream.Terminated())
	assert.Equal(t, 55, sum)
	assert.Equal(t, true, ok)

	// Case 2 : Reduce on a stream with at least 2 elements.
	source = finiteSourceMock{maxSize: 1}
	rawStream = FromSource[int](&source)

	sum, ok = rawStream.Reduce(func(x, y int) interface{} {
		return x + y
	})

	assert.Equal(t, 0, sum.(int))
	assert.Equal(t, false, ok)

	// Case 3 : Try Reduce on a terminated stream.
	t.Run("Reduce on  a terminated stream", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, StreamTerminated, r.(Error).Code())
			}
		}()

		sum, ok = rawStream.Reduce(func(x, y int) interface{} {
			return x + y
		})
	})

}

func TestCollect(t *testing.T) {

	source := finiteSourceMock{maxSize: 6}

	stream := FromSource[int](&source)

	// Case 1 : Just collect the stream to a slice.
	assert.ElementsMatch(t, []int{1, 2, 3, 4, 5, 6}, stream.Collect())

}

func TestIntegration(t *testing.T) {

	// Case 1 : Infinite stream -> filter out odd numbers and then get sum of first n even squares.
	stream := FromSource[int](&infiniteSourceMock{})

	sum, ok := stream.Filter(func(x int) bool {
		return x%2 == 0
	}).Map(func(x int) interface{} {
		return x * x
	}).Limit(10).Reduce(func(x, y interface{}) interface{} {
		return x.(int) + y.(int)
	})

	assert.Equal(t, true, ok)
	assert.Equal(t, 1540, sum.(int))

	// Case 2 : Infinite stream -> skip first 10 elements , keep only multiples of 3 limited to the first n.
	str, ok := FromSource[int](&infiniteSourceMock{}).Skip(10).Filter(func(x int) bool {
		return x%3 == 0
	}).Map(func(x int) interface{} {
		return fmt.Sprint(x)
	}).Limit(4).Reduce(func(x, y interface{}) interface{} {
		return x.(string) + ", " + y.(string)
	})

	assert.Equal(t, true, ok)
	assert.Equal(t, "12, 15, 18, 21", str.(string))

	// Case 3 : Finite stream -> distinct and count.
	fruitStream := FromCollection[types.String](
		list.New[types.String]("Apple", "Banana", "Orange", "Apple", "Kiwi", "Kiwi", "Orange", "Apple", "Watermelon"))

	count := fruitStream.Distinct(func(x, y types.String) bool { return x == y }, func(x types.String) int { return x.HashCode() }).Count()
	assert.Equal(t, 5, count)

	// Case 4 : Finite stream -> filter, distinct and count.
	fruitStream = FromCollection[types.String](
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
	fruitStream = FromCollection[types.String](
		list.New[types.String]("Apple", "Banana", "Orange", "Apple", "Kiwi", "Kiwi", "Orange", "Apple", "Watermelon"))

	count = fruitStream.Filter(func(x types.String) bool {
		return x != "Banana"
	}).Skip(2).Count()

	assert.Equal(t, 6, count)

	// Case 6 : Finite stream -> filter , limit .
	fruitStream = FromCollection[types.String](
		list.New[types.String]("Apple", "Banana", "Orange", "Apple", "Kiwi", "Kiwi", "Orange", "Apple", "Watermelon"))

	count = fruitStream.Filter(func(x types.String) bool {
		return x != "Banana"
	}).Limit(4).Count()

	assert.Equal(t, 4, count)

}
