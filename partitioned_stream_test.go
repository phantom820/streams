package streams

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartitionedtCollect(t *testing.T) {

	type collectTest struct {
		data     []string
		expected []string
	}

	var collectTests = []collectTest{
		{data: []string{}, expected: []string{}},
		{data: []string{"Hello world", "This is awesome"}, expected: []string{"Hello", "world", "This", "is", "awesome"}},
	}

	split := func(x string) []string {
		return strings.Split(x, " ")
	}

	collect := func(data [][]string) []string {
		result := make([]string, 0)
		for _, partition := range data {
			result = append(result, partition...)
		}
		return result
	}

	for _, test := range collectTests {
		s1, s2 := New(func() []string { return test.data }).Partition(split), New(func() []string { return test.data }).Partition(split).Parallelize(2)
		assert.ElementsMatch(t, test.expected, collect(s1.Collect()))
		assert.ElementsMatch(t, test.expected, collect(s2.Collect()))
		assert.True(t, s1.Closed())
		assert.True(t, s1.Terminated())
		assert.True(t, s2.Closed())
		assert.True(t, s2.Terminated())

	}

}

func TestFlatMap(t *testing.T) {

	type flatMapTest struct {
		data     []string
		expected []string
	}

	var flatMapTests = []flatMapTest{
		{data: []string{}, expected: []string{}},
		{data: []string{"Hello world", "This is awesome"}, expected: []string{"Hello", "world", "This", "is", "awesome"}},
	}

	split := func(x string) []string {
		return strings.Split(x, " ")
	}

	for _, test := range flatMapTests {
		s1, s2 := New(func() []string { return test.data }).Partition(split).FlatMap(), New(func() []string { return test.data }).Partition(split).Parallelize(2).FlatMap()
		assert.ElementsMatch(t, test.expected, s1.Collect())
		assert.ElementsMatch(t, test.expected, s2.Collect())
		assert.True(t, s1.Closed())
		assert.True(t, s1.Terminated())
		assert.True(t, s2.Closed())
		assert.True(t, s2.Terminated())

	}

}

func TestPartitionedReduce(t *testing.T) {

	type reduceTest struct {
		data     []string
		expected []string
	}

	var flatMapTests = []reduceTest{
		{data: []string{}, expected: []string{}},
		{data: []string{"Hello world", "This is awesome"}, expected: []string{"Hello", "world", "This", "is", "awesome"}},
	}

	split := func(x string) []string {
		return strings.Split(x, " ")
	}

	reduce := func(x, y []string) []string {
		return append(x, y...)
	}

	for _, test := range flatMapTests {
		s1, s2 := New(func() []string { return test.data }).Partition(split),
			New(func() []string { return test.data }).Partition(split).Parallelize(2)
		assert.ElementsMatch(t, test.expected, s1.Reduce(reduce))
		assert.ElementsMatch(t, test.expected, s2.Reduce(reduce))
		assert.True(t, s1.Closed())
		assert.True(t, s1.Terminated())
		assert.True(t, s2.Closed())
		assert.True(t, s2.Terminated())

	}

}

func TestPartitionedMap(t *testing.T) {

	type mapTest struct {
		data     []string
		expected []string
	}

	var mapTests = []mapTest{
		{data: []string{}, expected: []string{}},
		{data: []string{"Hello world", "This is awesome"}, expected: []string{"HELLO", "WORLD", "THIS", "IS", "AWESOME"}},
	}

	split := func(x string) []string {
		return strings.Split(x, " ")
	}

	uniformMap := func(x string) string {
		return strings.ToUpper(x)
	}

	for _, test := range mapTests {
		s1, s2 := New(func() []string { return test.data }).Partition(split).Map(uniformMap).FlatMap(),
			New(func() []string { return test.data }).Partition(split).Parallelize(2).Map(uniformMap).FlatMap()
		assert.ElementsMatch(t, test.expected, s1.Collect())
		assert.ElementsMatch(t, test.expected, s2.Collect())
		assert.True(t, s1.Closed())
		assert.True(t, s1.Terminated())
		assert.True(t, s2.Closed())
		assert.True(t, s2.Terminated())

	}

}

func TestPartitionedFilter(t *testing.T) {

	type filterTest struct {
		data     []string
		expected []string
	}

	var filterTests = []filterTest{
		{data: []string{}, expected: []string{}},
		{data: []string{"Hello world", "This is awesome"}, expected: []string{"This"}},
	}

	split := func(x string) []string {
		return strings.Split(x, " ")
	}

	filter := func(x string) bool {
		return x == "This"
	}

	for _, test := range filterTests {
		s1, s2 := New(func() []string { return test.data }).Partition(split).Filter(filter).FlatMap(),
			New(func() []string { return test.data }).Partition(split).Parallelize(2).Filter(filter).FlatMap()
		assert.ElementsMatch(t, test.expected, s1.Collect())
		assert.ElementsMatch(t, test.expected, s2.Collect())
		assert.True(t, s1.Closed())
		assert.True(t, s1.Terminated())
		assert.True(t, s2.Closed())
		assert.True(t, s2.Terminated())

	}

}

func TestPartitionedDistinct(t *testing.T) {

	type distinctTest struct {
		data     []string
		expected []string
	}

	var distinctTests = []distinctTest{
		{data: []string{}, expected: []string{}},
		{data: []string{"Hello world", "This is awesome", "This is me"}, expected: []string{"Hello", "world", "This", "is", "awesome", "me"}},
	}

	split := func(x string) []string {
		return strings.Split(x, " ")
	}

	hash := func(x string) string {
		return x
	}

	for _, test := range distinctTests {
		s1, s2 := New(func() []string { return test.data }).Partition(split).Distinct(hash).FlatMap(),
			New(func() []string { return test.data }).Partition(split).Parallelize(2).Distinct(hash).FlatMap()
		assert.ElementsMatch(t, test.expected, s1.Collect())
		assert.ElementsMatch(t, test.expected, s2.Collect())
		assert.True(t, s1.Closed())
		assert.True(t, s1.Terminated())
		assert.True(t, s2.Closed())
		assert.True(t, s2.Terminated())

	}

}

func TestPartitionedCount(t *testing.T) {

	type countTest struct {
		data     []string
		expected int
	}

	var countTests = []countTest{
		{data: []string{}, expected: 0},
		{data: []string{"Hello world", "This is awesome"}, expected: 2},
	}

	split := func(x string) []string {
		return strings.Split(x, " ")
	}

	for _, test := range countTests {
		s1, s2 := New(func() []string { return test.data }).Partition(split), New(func() []string { return test.data }).Partition(split).Parallelize(2)
		assert.Equal(t, test.expected, s1.Count())
		assert.Equal(t, test.expected, s2.Count())
		assert.True(t, s1.Closed())
		assert.True(t, s1.Terminated())
		assert.True(t, s2.Closed())
		assert.True(t, s2.Terminated())

	}

}

func TestPartitionedLimit(t *testing.T) {

	type limitTest struct {
		data     []string
		limit    int
		expected int
	}

	var limitTests = []limitTest{
		{data: []string{}, limit: 2, expected: 0},
		{data: []string{"Hello world", "This is awesome", "This is me", "At it again"}, limit: 3, expected: 3},
		{data: []string{"Hello world", "This is awesome", "This is me", "At it again"}, limit: 0, expected: 0},
	}

	split := func(x string) []string {
		return strings.Split(x, " ")
	}
	// Repeat here to check for any potential race conditions.
	for i := 0; i < 10; i++ {
		for _, test := range limitTests {
			s1, s2 := New(func() []string { return test.data }).Partition(split).Limit(test.limit),
				New(func() []string { return test.data }).Partition(split).Parallelize(2).Limit(test.limit)
			assert.Equal(t, test.expected, s1.Count())
			assert.Equal(t, test.expected, s2.Count())
			assert.True(t, s1.Closed())
			assert.True(t, s1.Terminated())
			assert.True(t, s2.Closed())
			assert.True(t, s2.Terminated())
		}
	}
}

func TestPartitionedSkip(t *testing.T) {

	type skipTest struct {
		data     []string
		skip     int
		expected int
	}

	var skipTests = []skipTest{
		{data: []string{}, skip: 2, expected: 0},
		{data: []string{"Hello world", "This is awesome", "This is me", "At it again"}, skip: 3, expected: 1},
		{data: []string{"Hello world", "This is awesome", "This is me", "At it again"}, skip: 0, expected: 4},
		{data: []string{"Hello world", "This is awesome", "This is me", "At it again"}, skip: 10, expected: 0},
	}

	split := func(x string) []string {
		return strings.Split(x, " ")
	}
	// Repeat here to check for any potential race conditions.
	for i := 0; i < 10; i++ {
		for _, test := range skipTests {
			s1, s2 := New(func() []string { return test.data }).Partition(split).Skip(test.skip),
				New(func() []string { return test.data }).Partition(split).Parallelize(2).Skip(test.skip)
			assert.Equal(t, test.expected, s1.Count())
			assert.Equal(t, test.expected, s2.Count())
			assert.True(t, s1.Closed())
			assert.True(t, s1.Terminated())
			assert.True(t, s2.Closed())
			assert.True(t, s2.Terminated())
		}
	}
}

func TestPartitionedForEach(t *testing.T) {

	type forEachTest struct {
		data     []string
		expected int
	}

	var forEachTests = []forEachTest{
		{data: []string{}, expected: 0},
		{data: []string{"Hello world", "This is awesome", "This is me", "At it again"}, expected: 4},
		{data: []string{"Hello world", "This is awesome", "This is me"}, expected: 3},
		{data: []string{"Hello world", "This is awesome"}, expected: 2},
	}

	counter := 0
	var mux sync.Mutex
	forEach := func(i []string) {
		mux.Lock()
		defer mux.Unlock()
		counter++
	}

	split := func(x string) []string {
		return strings.Split(x, " ")
	}

	for _, test := range forEachTests {

		s1, s2 := New(func() []string { return test.data }).Partition(split),
			New(func() []string { return test.data }).Partition(split).Parallelize(2)

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

func TestPartitionedPeek(t *testing.T) {

	type peekTest struct {
		data     []string
		expected int
	}

	var peekTests = []peekTest{
		{data: []string{}, expected: 0},
		{data: []string{"Hello world", "This is awesome", "This is me", "At it again"}, expected: 4},
	}

	counter := 0
	var mux sync.Mutex
	peek := func(i []string) {
		mux.Lock()
		defer mux.Unlock()
		counter++
	}

	split := func(x string) []string {
		return strings.Split(x, " ")
	}

	for _, test := range peekTests {

		s1, s2 := New(func() []string { return test.data }).Partition(split).Peek(peek),
			New(func() []string { return test.data }).Partition(split).Parallelize(2).Peek(peek)

		counter = 0
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
