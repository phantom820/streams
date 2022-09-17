package streams

import (
	"testing"

	"github.com/phantom820/collections/lists/list"
	"github.com/phantom820/collections/types"
	"github.com/stretchr/testify/assert"
)

func newSlice(size int) []int {
	slice := make([]int, size)
	for i := 0; i < size; i++ {
		slice[i] = i + 1
	}
	return slice
}

func TestFromSlice(t *testing.T) {

	f := func() []int {
		return newSlice(10)
	}
	seqStream := FromSlice(f)
	concStream := ConcurrentFromSlice(f, 2, 2)

	assert.NotNil(t, seqStream.(*sequentialStream[int]))
	assert.NotNil(t, concStream.(*concurrentStream[int]))

	t.Run("", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, IllegalConfig, r.(streamError).Code())
			}
		}()
		ConcurrentFromSlice(f, -2, 2)
	})

	t.Run("", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, IllegalConfig, r.(streamError).Code())
			}
		}()
		ConcurrentFromSlice(f, 2, 0)
	})

}

func TestFromCollection(t *testing.T) {
	l := list.New[types.Int](1, 2, 3, 4, 5)

	seqStream := FromCollection[types.Int](l)
	concStream := ConcurrentFromCollection[types.Int](l, 2, 2)

	assert.NotNil(t, seqStream.(*sequentialStream[types.Int]))
	assert.NotNil(t, concStream.(*concurrentStream[types.Int]))

	t.Run("", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, IllegalConfig, r.(streamError).Code())
			}
		}()
		ConcurrentFromCollection[types.Int](l, -2, 2)
	})

	t.Run("", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, IllegalConfig, r.(streamError).Code())
			}
		}()
		ConcurrentFromCollection[types.Int](l, 2, 0)
	})

}

func TestFromSource(t *testing.T) {
	l := list.New[types.Int](1, 2, 3, 4, 5)

	seqStream := FromSource[types.Int](l.Iterator())
	concStream := ConcurrentFromSource[types.Int](l.Iterator(), 2, 2)

	assert.NotNil(t, seqStream.(*sequentialStream[types.Int]))
	assert.NotNil(t, concStream.(*concurrentStream[types.Int]))

	t.Run("", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, IllegalConfig, r.(streamError).Code())
			}
		}()
		ConcurrentFromSource[types.Int](l.Iterator(), -2, 2)
	})

	t.Run("", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, IllegalConfig, r.(streamError).Code())
			}
		}()
		ConcurrentFromSource[types.Int](l.Iterator(), 2, 0)
	})

}

func TestCollect(t *testing.T) {

	f := func(size int) func() []int {
		return func() []int { return newSlice(size) }
	}

	var tests = []struct {
		name   string
		stream Stream[int]
		want   []int
	}{
		{name: "Sequential Collect size : 100", stream: FromSlice(f(100)), want: newSlice(100)},
		{name: "Concurrent Collect size : 100", stream: ConcurrentFromSlice(f(100), 2, 50), want: newSlice(100)},
		{name: "Concurrent Collect size : 1000", stream: ConcurrentFromSlice(f(1000), 2, 8), want: newSlice(1000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.stream.Closed(), false)
			assert.Equal(t, tt.stream.Terminated(), false)
			assert.ElementsMatch(t, tt.want, tt.stream.Collect())
			assert.Equal(t, tt.stream.Closed(), true)
			assert.Equal(t, tt.stream.Terminated(), true)
		})

		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					assert.Equal(t, StreamTerminated, r.(*streamError).Code())
				}
			}()
			tt.stream.Collect()
		})
	}

}

func TestReduce(t *testing.T) {

	f := func(size int) func() []int {
		return func() []int { return newSlice(size) }
	}

	var tests = []struct {
		name   string
		stream Stream[int]
		want   int
	}{
		{name: "Sequential Reduce size : 100", stream: FromSlice(f(100)), want: 5050},
		{name: "Sequential Reduce size : 1000", stream: FromSlice(f(1000)), want: 500500},
		{name: "Concurrent Reduce size : 100", stream: ConcurrentFromSlice(f(100), 2, 50), want: 5050},
		{name: "Concurrent Reduce size : 1000", stream: ConcurrentFromSlice(f(1000), 2, 8), want: 500500},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			assert.Equal(t, tt.stream.Closed(), false)
			assert.Equal(t, tt.stream.Terminated(), false)
			sum, _ := tt.stream.Reduce(func(x, y int) int { return x + y })
			assert.Equal(t, tt.want, sum)
			assert.Equal(t, tt.stream.Closed(), true)
			assert.Equal(t, tt.stream.Terminated(), true)

		})

		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					assert.Equal(t, StreamTerminated, r.(*streamError).Code())
				}
			}()
			tt.stream.Reduce(func(x, y int) int { return x + y })
		})
	}
}

func TestCount(t *testing.T) {

	f := func(size int) func() []int {
		return func() []int { return newSlice(size) }
	}

	var tests = []struct {
		name   string
		stream Stream[int]
		want   int
	}{
		{name: "Sequential Count size : 100", stream: FromSlice(f(100)), want: 100},
		{name: "Sequential Count size : 1000", stream: FromSlice(f(1000)), want: 1000},
		{name: "Concurrent Count size : 100 Concurrency : 2 Partition size : 50", stream: ConcurrentFromSlice(f(100), 2, 50), want: 100},
		{name: "Concurrent Count size : 1000 Concurrency : 2 Partition size : 8", stream: ConcurrentFromSlice(f(1000), 2, 8), want: 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.stream.Closed(), false)
			assert.Equal(t, tt.stream.Terminated(), false)
			assert.Equal(t, tt.want, tt.stream.Count())
			assert.Equal(t, tt.stream.Closed(), true)
			assert.Equal(t, tt.stream.Terminated(), true)
		})

		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					assert.Equal(t, StreamTerminated, r.(*streamError).Code())
				}
			}()
			tt.stream.Count()
		})
	}
}

func TestForEach(t *testing.T) {

	f := func(size int) func() []int {
		return func() []int { return newSlice(size) }
	}

	var tests = []struct {
		name   string
		stream Stream[int]
		want   int
	}{
		{name: "Sequential ForEach size : 100", stream: FromSlice(f(100)), want: 100},
		{name: "Sequential ForEach size : 1000", stream: FromSlice(f(1000)), want: 1000},
		{name: "Concurrent ForEach size : 100 Concurrency : 2 Partition size : 50", stream: ConcurrentFromSlice(f(100), 2, 50), want: 100},
		{name: "Concurrent ForEach size : 1000 Concurrency : 2 Partition size : 8", stream: ConcurrentFromSlice(f(1000), 2, 8), want: 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.stream.Closed(), false)
			assert.Equal(t, tt.stream.Terminated(), false)
			count := 0
			tt.stream.ForEach(func(x int) {
				count = count + 1
			})
			assert.Equal(t, tt.want, count)
			assert.Equal(t, tt.stream.Closed(), true)
			assert.Equal(t, tt.stream.Terminated(), true)

		})

		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					assert.Equal(t, StreamTerminated, r.(*streamError).Code())
				}
			}()
			tt.stream.ForEach(func(x int) {})
		})
	}
}

func TestFilter(t *testing.T) {

	f := func(size int) func() []int {
		return func() []int { return newSlice(size) }
	}

	var tests = []struct {
		name   string
		stream Stream[int]
		want   int
	}{
		{name: "Sequential Filter size : 100", stream: FromSlice(f(100)), want: 33},
		{name: "Sequential Filter size : 1000", stream: FromSlice(f(1000)), want: 333},
		{name: "Concurrent Filter size : 100 Concurrency : 2 Partition size : 50", stream: ConcurrentFromSlice(f(100), 2, 50), want: 33},
		{name: "Concurrent Filter size : 1000 Concurrency : 2 Partition size : 8", stream: ConcurrentFromSlice(f(1000), 2, 8), want: 333},
	}

	for _, tt := range tests {
		t.Run(t.Name(), func(t *testing.T) {
			assert.Equal(t, tt.stream.Closed(), false)
			assert.Equal(t, tt.stream.Terminated(), false)
			filteredStream := tt.stream.Filter(func(x int) bool { return x%3 == 0 })
			assert.Equal(t, tt.want, filteredStream.Count())
			assert.Equal(t, tt.stream.Closed(), true)
			assert.Equal(t, filteredStream.Terminated(), true)
		})

		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					assert.Equal(t, StreamClosed, r.(*streamError).Code())
				}
			}()
			tt.stream.Filter(func(x int) bool { return x%3 == 0 })
		})

	}

}

func TestMap(t *testing.T) {

	f := func(size int) func() []int {
		return func() []int { return newSlice(size) }
	}

	a := newSlice(100)
	b := newSlice(1000)

	for i := range a {
		a[i] = a[i] + 10
	}

	for i := range b {
		b[i] = b[i] + 10
	}

	var tests = []struct {
		name   string
		stream Stream[int]
		want   []int
	}{
		{name: "Sequential Map size : 100", stream: FromSlice(f(100)), want: a},
		{name: "Sequential Map size : 1000", stream: FromSlice(f(1000)), want: b},
		{name: "Concurrent Map size : 100 Concurrency : 2 Partition size : 50", stream: ConcurrentFromSlice(f(100), 2, 50), want: a},
		{name: "Concurrent Map size : 1000 Concurrency : 2 Partition size : 8", stream: ConcurrentFromSlice(f(1000), 2, 8), want: b},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.stream.Closed(), false)
			assert.Equal(t, tt.stream.Terminated(), false)
			mappedStream := tt.stream.Map(func(x int) int { return x + 10 })
			assert.ElementsMatch(t, tt.want, mappedStream.Collect())
			assert.Equal(t, tt.stream.Closed(), true)
			assert.Equal(t, mappedStream.Terminated(), true)
		})

		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					assert.Equal(t, StreamClosed, r.(*streamError).Code())
				}
			}()
			tt.stream.Map(func(x int) int { return x })
		})
	}

}

func TestLimit(t *testing.T) {

	f := func(size int) func() []int {
		return func() []int { return newSlice(size) }
	}

	var tests = []struct {
		name   string
		stream Stream[int]
		want   int
		limit  int
	}{
		{name: "Sequential Limit size : 100", stream: FromSlice(f(100)), want: 0, limit: 0},
		{name: "Sequential Limit size : 1000", stream: FromSlice(f(1000)), want: 10, limit: 10},
		{name: "Concurrent Limit size : 100 Concurrency : 2 Partition size : 50", stream: ConcurrentFromSlice(f(100), 2, 50), want: 5, limit: 5},
		{name: "Concurrent Limit size : 1000 Concurrency : 2 Partition size : 8", stream: ConcurrentFromSlice(f(1000), 2, 8), want: 1000, limit: 2000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.stream.Closed(), false)
			assert.Equal(t, tt.stream.Terminated(), false)
			assert.Equal(t, tt.want, tt.stream.Limit(tt.limit).Count())
			assert.Equal(t, tt.stream.Closed(), true)
		})

		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					assert.Equal(t, StreamClosed, r.(*streamError).Code())
				}
			}()
			tt.stream.Limit(1)
		})

	}
}

func TestSkip(t *testing.T) {

	f := func(size int) func() []int {
		return func() []int { return newSlice(size) }
	}

	var tests = []struct {
		name   string
		stream Stream[int]
		want   int
		skip   int
	}{
		{name: "Sequential Skip size : 100", stream: FromSlice(f(100)), want: 100, skip: 0},
		{name: "Sequential Skip size : 1000", stream: FromSlice(f(1000)), want: 990, skip: 10},
		{name: "Concurrent Skip size : 100 Concurrency : 2 Partition size : 50", stream: ConcurrentFromSlice(f(100), 2, 50), want: 50, skip: 50},
		{name: "Concurrent Skip size : 1000 Concurrency : 2 Partition size : 8", stream: ConcurrentFromSlice(f(1000), 2, 8), want: 0, skip: 2000},
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.stream.Closed(), false)
			assert.Equal(t, tt.stream.Terminated(), false)
			assert.Equal(t, tt.want, tt.stream.Skip(tt.skip).Count())
			assert.Equal(t, tt.stream.Closed(), true)
		})

		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					assert.Equal(t, StreamClosed, r.(*streamError).Code())
				}
			}()
			tt.stream.Skip(1)
		})

	}
}

func TestDistinct(t *testing.T) {

	f := func(size int) func() []int {
		return func() []int {
			data := make([]int, size)
			for i := 0; i < size; i++ {
				data[i] = i % 10
			}
			return data
		}
	}

	distinceElements := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	var tests = []struct {
		name   string
		stream Stream[int]
		want   []int
	}{
		{name: "Sequential Distinct size : 100", stream: FromSlice(f(100)), want: distinceElements},
		{name: "Sequential Distinct size : 1000", stream: FromSlice(f(1000)), want: distinceElements},
		{name: "Concurrent Distinct size : 100 Concurrency : 2 Partition size : 50", stream: ConcurrentFromSlice(f(100), 2, 50), want: distinceElements},
		{name: "Concurrent Distinct size : 1000 Concurrency : 2 Partition size : 8", stream: ConcurrentFromSlice(f(1000), 2, 8), want: distinceElements},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.stream.Closed(), false)
			assert.Equal(t, tt.stream.Terminated(), false)
			assert.ElementsMatch(t, tt.want, tt.stream.Distinct(func(x, y int) bool { return x == y }, func(x int) int { return x }).Collect())
			assert.Equal(t, tt.stream.Closed(), true)
		})

		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					assert.Equal(t, StreamClosed, r.(*streamError).Code())
				}
			}()
			tt.stream.Distinct(func(x, y int) bool { return x == y }, func(x int) int { return x })
		})

	}
}

func TestPeek(t *testing.T) {

	f := func(size int) func() []int {
		return func() []int { return newSlice(size) }
	}

	var tests = []struct {
		name   string
		stream Stream[int]
		want   int
	}{
		{name: "Sequential Peek size : 100", stream: FromSlice(f(100)), want: 100},
		{name: "Sequential Peek size : 1000", stream: FromSlice(f(1000)), want: 1000},
		{name: "Concurrent Peek size : 100 Concurrency : 2 Partition size : 50", stream: ConcurrentFromSlice(f(100), 2, 50), want: 100},
		{name: "Concurrent Peek size : 1000 Concurrency : 2 Partition size : 8", stream: ConcurrentFromSlice(f(1000), 2, 8), want: 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.stream.Closed(), false)
			assert.Equal(t, tt.stream.Terminated(), false)
			peeked := false
			count := tt.stream.Peek(func(x int) {
				if x == 1 {
					peeked = true
				}
			}).Count()
			assert.Equal(t, true, peeked)
			assert.Equal(t, tt.want, count)
			assert.Equal(t, tt.stream.Closed(), true)
		})

		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					assert.Equal(t, StreamClosed, r.(*streamError).Code())
				}
			}()
			tt.stream.Peek(func(x int) {})
		})

	}
}
func TestIntegration(t *testing.T) {

	f := func(size int) func() []int {
		return func() []int { return newSlice(size) }
	}

	// Case 1 : Filter + Map + Collect.
	a := []int{4, 6, 8, 10, 12}
	b := []int{4, 6, 8, 10, 12, 14, 16, 18, 20, 22}
	var tests1 = []struct {
		name   string
		stream Stream[int]
		want   []int
	}{
		{name: "Sequential Integ 1 size : 10", stream: FromSlice(f(10)), want: a},
		{name: "Sequential Integ 1 size : 20", stream: FromSlice(f(20)), want: b},
		{name: "Concurrent Integ 1 size : 10 Concurrency : 2 Partition size : 50", stream: ConcurrentFromSlice(f(10), 2, 50), want: a},
		{name: "Concurrent Integ 1 size : 20 Concurrency : 2 Partition size : 8", stream: ConcurrentFromSlice(f(20), 2, 8), want: b},
	}

	for _, tt := range tests1 {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.stream.Closed(), false)
			assert.Equal(t, tt.stream.Terminated(), false)
			results := tt.stream.Filter(func(x int) bool { return x%2 == 0 }).Map(func(x int) int { return x + 2 }).Collect()
			assert.ElementsMatch(t, tt.want, results)
			assert.Equal(t, tt.stream.Closed(), true)
		})
	}

	// Case 2 : Skip + Filter + Map + Limit + Collect.

	var tests2 = []struct {
		name   string
		stream Stream[int]
		limit  int
		want   int
	}{
		{name: "Sequential Integ 2 size : 10", stream: FromSlice(f(10)), want: 2, limit: 2},
		{name: "Sequential Integ 2 size : 20", stream: FromSlice(f(20)), want: 4, limit: 4},
		{name: "Concurrent Integ 2 size : 10 Concurrency : 2 Partition size : 50", stream: ConcurrentFromSlice(f(10), 2, 50), want: 2, limit: 2},
		{name: "Concurrent Integ 2 size : 20 Concurrency : 2 Partition size : 8", stream: ConcurrentFromSlice(f(20), 2, 8), want: 4, limit: 4},
	}

	for _, tt := range tests2 {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.stream.Closed(), false)
			assert.Equal(t, tt.stream.Terminated(), false)
			results := tt.stream.Filter(func(x int) bool { return x%2 == 0 }).Limit(tt.limit).Count()
			assert.Equal(t, tt.want, results)
			assert.Equal(t, tt.stream.Closed(), true)
		})
	}

	// Case 3 : Filter + Filter + Reduce.
	var tests3 = []struct {
		name   string
		stream Stream[int]
		want   int
	}{
		{name: "Sequential Integ 3 size : 10", stream: FromSlice(f(10)), want: 28},
		{name: "Sequential Integ 3 size : 20", stream: FromSlice(f(20)), want: 108},
		{name: "Concurrent Integ 3 size : 10 Concurrency : 2 Partition size : 50", stream: ConcurrentFromSlice(f(10), 2, 50), want: 28},
		{name: "Concurrent Integ 3 size : 20 Concurrency : 2 Partition size : 8", stream: ConcurrentFromSlice(f(20), 2, 8), want: 108},
	}

	for _, tt := range tests3 {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.stream.Closed(), false)
			assert.Equal(t, tt.stream.Terminated(), false)
			results, _ := tt.stream.Filter(func(x int) bool { return x%2 == 0 }).
				Filter(func(x int) bool { return x > 2 }).
				Reduce(func(x, y int) int { return x + y })
			assert.Equal(t, tt.want, results)
			assert.Equal(t, tt.stream.Closed(), true)
		})
	}

	// Case 4 : Map + Distinct + Filter + Reduce.
	var tests4 = []struct {
		name   string
		stream Stream[int]
		want   int
		n      int
	}{
		{name: "Sequential Integ 4 size : 100", stream: FromSlice(f(100)), want: 7, n: 5},
		{name: "Sequential Integ 4 size : 1000", stream: FromSlice(f(1000)), want: 42, n: 10},
		{name: "Concurrent Integ 4 size : 100 Concurrency : 2 Partition size : 50", stream: ConcurrentFromSlice(f(100), 2, 50), want: 7, n: 5},
		{name: "Concurrent Integ 4 size : 1000 Concurrency : 2 Partition size : 8", stream: ConcurrentFromSlice(f(1000), 2, 8), want: 42, n: 10},
	}

	for _, tt := range tests4 {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.stream.Closed(), false)
			assert.Equal(t, tt.stream.Terminated(), false)
			results, _ := tt.stream.Map(func(x int) int { return x % tt.n }).
				Distinct(func(x, y int) bool { return x == y }, func(x int) int { return x }).
				Filter(func(x int) bool { return x > 2 }).
				Distinct(func(x, y int) bool { return x == y }, func(x int) int { return x }).
				Reduce(func(x, y int) int { return x + y })
			assert.Equal(t, tt.want, results)
			assert.Equal(t, tt.stream.Closed(), true)
		})
	}

}
