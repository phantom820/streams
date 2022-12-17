package streams

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGroupByCount(t *testing.T) {

	type groupByTest struct {
		data     []string
		expected map[string]int
	}

	groupByTests := []groupByTest{
		{data: []string{}, expected: map[string]int{}},
		{data: []string{"1", "2", "3", "4", "5"}, expected: map[string]int{"1": 1, "2": 1, "3": 1, "4": 1, "5": 1}},
	}

	for _, test := range groupByTests {
		a := New(func() []string { return test.data }).GroupBy(func(x string) string { return x }).Count()
		b := New(func() []string { return test.data }).GroupBy(func(x string) string { return x }).Parallelize(2).Count()

		assert.Equal(t, test.expected, a)
		assert.Equal(t, test.expected, b)

	}
}

func TestGroupByCollect(t *testing.T) {

	type groupByTest struct {
		data     []string
		expected []Group[string]
	}

	groupByTests := []groupByTest{
		{data: []string{}, expected: []Group[string]{}},
		{data: []string{"1", "2", "3", "4"}, expected: []Group[string]{
			{name: "1", data: []string{"1"}}, {name: "2", data: []string{"2"}},
			{name: "3", data: []string{"3"}}, {name: "4", data: []string{"4"}}}},
	}

	for _, test := range groupByTests {
		a := New(func() []string { return test.data }).GroupBy(func(x string) string { return x }).Collect()
		b := New(func() []string { return test.data }).GroupBy(func(x string) string { return x }).Parallelize(2).Collect()

		assert.ElementsMatch(t, test.expected, a)
		assert.ElementsMatch(t, test.expected, b)

	}
}

func TestGroupByForEach(t *testing.T) {

	type forEachTest struct {
		data     []string
		expected int
	}

	var forEachTests = []forEachTest{
		{data: []string{}, expected: 0},
		{data: []string{"1", "2", "3", "4", "5"}, expected: 5},
	}

	counter := 0
	var mux sync.Mutex
	forEach := func(group Group[string]) {
		mux.Lock()
		defer mux.Unlock()
		counter++
	}

	for _, test := range forEachTests {

		s1, s2 := New(func() []string { return test.data }).GroupBy(func(x string) string { return x }),
			New(func() []string { return test.data }).Parallelize(2).GroupBy(func(x string) string { return x })

		counter = 0
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

func TestGroupByReduce(t *testing.T) {

	type groupByReduceTest struct {
		data     []string
		expected map[string]string
	}

	groupByReduceTests := []groupByReduceTest{
		{data: []string{}, expected: make(map[string]string)},
		{data: []string{"1", "2", "3", "4", "1"}, expected: map[string]string{"1": "11", "2": "2", "3": "3", "4": "4"}}}

	reduce := func(x, y string) string {
		return x + y
	}

	for _, test := range groupByReduceTests {
		a := New(func() []string { return test.data }).GroupBy(func(x string) string { return x }).Reduce(reduce)
		b := New(func() []string { return test.data }).GroupBy(func(x string) string { return x }).Parallelize(2).Reduce(reduce)

		assert.Equal(t, test.expected, a)
		assert.Equal(t, test.expected, b)

	}
}

func TestGroupByAggregate(t *testing.T) {

	type groupByAggregateTest struct {
		data     []string
		expected map[string]string
	}

	groupByAggregateTests := []groupByAggregateTest{
		{data: []string{}, expected: make(map[string]string)},
		{data: []string{"1", "2", "3", "4", "1"}, expected: map[string]string{"1": "11", "2": "2", "3": "3", "4": "4"}}}

	agg := func(g Group[string]) string {
		return strings.Join(g.Data(), "")
	}

	for _, test := range groupByAggregateTests {
		a := New(func() []string { return test.data }).GroupBy(func(x string) string { return x }).Aggregate(agg)
		b := New(func() []string { return test.data }).GroupBy(func(x string) string { return x }).Parallelize(2).Aggregate(agg)

		assert.Equal(t, test.expected, a)
		assert.Equal(t, test.expected, b)

	}
}

func TestGroupyFilter(t *testing.T) {

	type groupByFilterTest struct {
		data     []string
		expected map[string]string
	}

	groupByFilterTests := []groupByFilterTest{
		{data: []string{}, expected: make(map[string]string)},
		{data: []string{"1", "2", "3", "4", "1"}, expected: map[string]string{"2": "2", "3": "3", "4": "4"}}}

	agg := func(g Group[string]) string {
		return strings.Join(g.Data(), "")
	}

	filter := func(x Group[string]) bool {
		return x.Name() != "1"
	}

	for _, test := range groupByFilterTests {
		a := New(func() []string { return test.data }).GroupBy(func(x string) string { return x }).Filter(filter).Aggregate(agg)
		b := New(func() []string { return test.data }).GroupBy(func(x string) string { return x }).Parallelize(2).Filter(filter).Aggregate(agg)

		assert.Equal(t, test.expected, a)
		assert.Equal(t, test.expected, b)

	}
}
