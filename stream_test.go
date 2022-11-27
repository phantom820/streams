package streams

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {

	s1 := New(func() []int { return []int{} })
	s2 := New(func() []int { return []int{} }).Parallelize(2)

	assert.False(t, s1.Closed())
	assert.False(t, s1.Terminated())
	assert.False(t, s2.Closed())
	assert.False(t, s2.Terminated())
	assert.True(t, s2.Parallel())

}

func TestCollect(t *testing.T) {

	type collectTest struct {
		data     []int
		expected []int
	}

	var collectTests = []collectTest{
		{data: []int{}, expected: []int{}},
		{data: []int{1, 2, 3, 4, 5, 6, 9, 10}, expected: []int{1, 2, 3, 4, 5, 6, 9, 10}},
	}

	for _, test := range collectTests {
		s1, s2 := New(func() []int { return test.data }), New(func() []int { return test.data }).Parallelize(2)
		assert.ElementsMatch(t, test.expected, s1.Collect())
		assert.ElementsMatch(t, test.expected, s2.Collect())
		assert.True(t, s1.Closed())
		assert.True(t, s1.Terminated())
		assert.True(t, s2.Closed())
		assert.True(t, s2.Terminated())

	}

}

func TestFilter(t *testing.T) {

	type filterTest struct {
		data     []int
		filter   func(int) bool
		expected []int
	}

	var filterTests = []filterTest{
		{data: []int{}, filter: func(i int) bool { return true }, expected: []int{}},
		{data: []int{1, 2, 3, 4, 5, 6, 9, 10}, filter: func(i int) bool { return i%2 == 0 }, expected: []int{2, 4, 6, 10}},
	}

	for _, test := range filterTests {
		s1, s2 := New(func() []int { return test.data }).Filter(test.filter),
			New(func() []int { return test.data }).Parallelize(2).Filter(test.filter)
		assert.ElementsMatch(t, test.expected, s1.Collect())
		assert.ElementsMatch(t, test.expected, s2.Collect())
		assert.True(t, s1.Closed())
		assert.True(t, s1.Terminated())
		assert.True(t, s2.Closed())
		assert.True(t, s2.Terminated())
	}

}

func TestMap(t *testing.T) {

	type mapTest struct {
		data       []int
		uniformMap func(int) int
		expected   []int
	}

	var mapTests = []mapTest{
		{data: []int{}, uniformMap: func(i int) int { return i }, expected: []int{}},
		{data: []int{1, 2, 3, 4, 5}, uniformMap: func(i int) int { return i * 2 }, expected: []int{2, 4, 6, 8, 10}},
	}

	for _, test := range mapTests {
		s1, s2 := New(func() []int { return test.data }).Map(test.uniformMap),
			New(func() []int { return test.data }).Parallelize(2).Map(test.uniformMap)
		assert.ElementsMatch(t, test.expected, s1.Collect())
		assert.ElementsMatch(t, test.expected, s2.Collect())
		assert.True(t, s1.Closed())
		assert.True(t, s1.Terminated())
		assert.True(t, s2.Closed())
		assert.True(t, s2.Terminated())
	}

}

func TestCount(t *testing.T) {

	type countTest struct {
		data     []int
		expected int
	}

	var countTests = []countTest{
		{data: []int{}, expected: 0},
		{data: []int{1, 2, 3, 4, 5}, expected: 5},
	}

	for _, test := range countTests {
		s1, s2 := New(func() []int { return test.data }),
			New(func() []int { return test.data }).Parallelize(2)
		assert.Equal(t, test.expected, s1.Count())
		assert.Equal(t, test.expected, s2.Count())
		assert.True(t, s1.Closed())
		assert.True(t, s1.Terminated())
		assert.True(t, s2.Closed())
		assert.True(t, s2.Terminated())
	}

}

func TestReduce(t *testing.T) {

	type reduceTest struct {
		data     []int
		reduce   func(x, y int) int
		expected int
	}

	var reduceTests = []reduceTest{
		{data: []int{}, reduce: func(x, y int) int { return x + y }, expected: 0},
		{data: []int{1, 2, 3, 4, 5}, reduce: func(x, y int) int { return x + y }, expected: 15},
		{data: []int{1, 2, 3, 4, 5}, reduce: func(x, y int) int { return x * y }, expected: 120},
	}

	for _, test := range reduceTests {
		s1, s2 := New(func() []int { return test.data }),
			New(func() []int { return test.data }).Parallelize(2)
		assert.Equal(t, test.expected, s1.Reduce(test.reduce))
		assert.Equal(t, test.expected, s2.Reduce(test.reduce))
		assert.True(t, s1.Closed())
		assert.True(t, s1.Terminated())
		assert.True(t, s2.Closed())
		assert.True(t, s2.Terminated())
	}
}

func TestLimit(t *testing.T) {

	type limitTest struct {
		data     []int
		limit    int
		expected int
	}

	var limitTests = []limitTest{
		{data: []int{}, limit: 2, expected: 0},
		{data: []int{1, 2, 3, 4, 5}, limit: 3, expected: 3},
		{data: []int{1, 2, 3, 4, 5}, limit: 4, expected: 4},
		{data: []int{1, 2, 3, 4, 5}, limit: 0, expected: 0},
	}

	// Repeat here to check for any potential race conditions.
	for i := 0; i < 10; i++ {
		for _, test := range limitTests {
			s1, s2 := New(func() []int { return test.data }).Limit(test.limit),
				New(func() []int { return test.data }).Parallelize(2).Limit(test.limit)
			assert.Equal(t, test.expected, s1.Count())
			assert.Equal(t, test.expected, s2.Count())
			assert.True(t, s1.Closed())
			assert.True(t, s1.Terminated())
			assert.True(t, s2.Closed())
			assert.True(t, s2.Terminated())
		}
	}
}

func TestSkip(t *testing.T) {

	type skipTest struct {
		data     []int
		skip     int
		expected int
	}

	var skipTests = []skipTest{
		{data: []int{}, skip: 2, expected: 0},
		{data: []int{1, 2, 3, 4, 5}, skip: 2, expected: 3},
		{data: []int{1, 2, 3, 4, 5}, skip: 4, expected: 1},
		{data: []int{1, 2, 3, 4, 5}, skip: 5, expected: 0},
	}

	for _, test := range skipTests {
		s1, s2 := New(func() []int { return test.data }).Skip(test.skip),
			New(func() []int { return test.data }).Parallelize(2).Skip(test.skip)
		assert.Equal(t, test.expected, s1.Count())
		assert.Equal(t, test.expected, s2.Count())
		assert.True(t, s1.Closed())
		assert.True(t, s1.Terminated())
		assert.True(t, s2.Closed())
		assert.True(t, s2.Terminated())
	}
}

func TestDistinct(t *testing.T) {

	type distinctTest struct {
		data     []int
		expected []int
	}

	var distinctTests = []distinctTest{
		{data: []int{}, expected: []int{}},
		{data: []int{1, 2, 3, 4, 5, 1, 2, 2, 3, 4, 5, 6}, expected: []int{1, 2, 3, 4, 5, 6}},
		{data: []int{1, 1, 1, 1, 2, 3, 4, 5}, expected: []int{1, 2, 3, 4, 5}},
	}

	distinct := func(i int) string { return fmt.Sprint(i) }
	// Repeat here to check for any potential race conditions.
	for i := 0; i < 10; i++ {
		for _, test := range distinctTests {
			s1, s2, s3 := New(func() []int { return test.data }).Distinct(distinct),
				New(func() []int { return test.data }).Distinct(distinct).Distinct(distinct),
				New(func() []int { return test.data }).Parallelize(2).Distinct(distinct)

			assert.ElementsMatch(t, test.expected, s1.Collect())
			assert.ElementsMatch(t, test.expected, s2.Collect())
			assert.ElementsMatch(t, test.expected, s3.Collect())
			assert.True(t, s1.Closed())
			assert.True(t, s1.Terminated())
			assert.True(t, s2.Closed())
			assert.True(t, s2.Terminated())
		}
	}

}

func TestPeek(t *testing.T) {

	type peekTest struct {
		data     []int
		expected int
	}

	var peekTests = []peekTest{
		{data: []int{}, expected: 0},
		{data: []int{1, 2, 3, 4, 5}, expected: 5},
	}

	counter := 0
	var mux sync.Mutex
	peek := func(i int) {
		mux.Lock()
		defer mux.Unlock()
		counter++
	}

	for _, test := range peekTests {

		s1, s2 := New(func() []int { return test.data }).Peek(peek),
			New(func() []int { return test.data }).Parallelize(2).Peek(peek)

		s1.Collect()
		assert.Equal(t, test.expected, counter)
		counter = 0
		s2.Collect()
		assert.Equal(t, test.expected, counter)
		assert.True(t, s1.Closed())
		assert.True(t, s1.Terminated())
		assert.True(t, s2.Closed())
		assert.True(t, s2.Terminated())

	}

}

func TestForEach(t *testing.T) {

	type forEachTest struct {
		data     []int
		expected int
	}

	var forEachTests = []forEachTest{
		{data: []int{}, expected: 0},
		{data: []int{1, 2, 3, 4, 5}, expected: 5},
	}

	counter := 0
	var mux sync.Mutex
	forEach := func(i int) {
		mux.Lock()
		defer mux.Unlock()
		counter++
	}

	for _, test := range forEachTests {

		s1, s2 := New(func() []int { return test.data }),
			New(func() []int { return test.data }).Parallelize(2)

		s1.ForEach(forEach)
		assert.Equal(t, test.expected, counter)
		counter = 0
		s2.ForEach(forEach)
		assert.Equal(t, test.expected, counter)
		assert.True(t, s1.Closed())
		assert.True(t, s1.Terminated())
		assert.True(t, s2.Closed())
		assert.True(t, s2.Terminated())

	}

}

func TestErr(t *testing.T) {

	type errTest struct {
		f               func()
		expectedErrCode int
	}

	supplier := func() []int { return []int{} }
	var errTests = []errTest{
		{f: func() {
			a := New(supplier)
			_ = a.Filter(nil)
			a.Filter(nil)
		},
			expectedErrCode: StreamClosed,
		},
		{f: func() {
			a := New(supplier)
			_ = a.Filter(nil)
			a.Map(nil)
		},
			expectedErrCode: StreamClosed,
		},
		{
			f: func() {
				a := New(supplier)
				_ = a.Collect()
				a.Filter(nil)
			},
			expectedErrCode: StreamTerminated,
		},
		{
			f: func() {
				_ = New(supplier).Parallelize(1)
			},
			expectedErrCode: IllegalConfig,
		},
		{
			f: func() {
				_ = New(supplier).Limit(-1)
			},
			expectedErrCode: IllegalArgument,
		},
		{
			f: func() {
				_ = New(supplier).Skip(-1)
			},
			expectedErrCode: IllegalArgument,
		},
		{
			f: func() {
				a := New(supplier)
				_ = a.Filter(nil)
				a.Collect()
			},
			expectedErrCode: StreamClosed,
		},
		{
			f: func() {
				a := New(supplier)
				_ = a.Filter(nil)
				a.Limit(1)
			},
			expectedErrCode: StreamClosed,
		},
		{
			f: func() {
				a := New(supplier)
				_ = a.Filter(nil)
				a.Skip(1)
			},
			expectedErrCode: StreamClosed,
		},
	}

	for _, test := range errTests {
		t.Run("", func(t *testing.T) {
			defer func(expectedErrCode int) {
				if r := recover(); r != nil {
					assert.Equal(t, expectedErrCode, r.(*streamError).Code())
				}
			}(test.expectedErrCode)
			test.f()
		})
	}
}

func TestE2E(t *testing.T) {

	type e2eTest[T any] struct {
		data     []int
		sequence func(s Stream[int]) Stream[int]
		expected T
	}

	var e2eTestsA = []e2eTest[[]int]{
		{data: []int{}, expected: []int{}, sequence: func(s Stream[int]) Stream[int] {
			return s
		}},
		{data: []int{1, 2, 3, 4, 5, 6}, expected: []int{4, 8, 12}, sequence: func(s Stream[int]) Stream[int] {
			return s.Map(func(x int) int { return x * 2 }).
				Filter(func(x int) bool { return x%4 == 0 })
		}},
		{data: []int{1, 2, 3, 4, 5, 6}, expected: []int{4, 8, 12}, sequence: func(s Stream[int]) Stream[int] {
			return s.Map(func(x int) int { return x * 2 }).
				Filter(func(x int) bool { return x%4 == 0 })
		}},
	}

	for _, test := range e2eTestsA {

		s1, s2 := New(func() []int { return test.data }),
			New(func() []int { return test.data }).Parallelize(2)

		assert.ElementsMatch(t, test.expected, test.sequence(s1).Collect())
		assert.ElementsMatch(t, test.expected, test.sequence(s2).Collect())
		assert.True(t, s1.Closed())
		assert.True(t, s2.Closed())

	}

	var e2eTestsB = []e2eTest[int]{
		{data: []int{}, expected: 0, sequence: func(s Stream[int]) Stream[int] {
			return s
		}},
		{data: []int{1, 2, 3, 4, 5, 6}, expected: 2, sequence: func(s Stream[int]) Stream[int] {
			return s.Map(func(x int) int { return x * 2 }).
				Filter(func(x int) bool { return x%4 == 0 }).Limit(2)
		}},
		{data: []int{1, 2, 3, 4, 5, 6}, expected: 3, sequence: func(s Stream[int]) Stream[int] {
			return s.Map(func(x int) int { return x * 2 }).
				Filter(func(x int) bool { return x%4 == 0 }).Limit(10)
		}},
	}

	for _, test := range e2eTestsB {

		s1, s2 := New(func() []int { return test.data }),
			New(func() []int { return test.data }).Parallelize(2)

		assert.Equal(t, test.expected, test.sequence(s1).Count())
		assert.Equal(t, test.expected, test.sequence(s2).Count())
		assert.True(t, s1.Closed())
		assert.True(t, s2.Closed())

	}

}
