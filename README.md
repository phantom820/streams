# streams
[![Build Status](https://app.travis-ci.com/phantom820/streams.svg?branch=master)](https://app.travis-ci.com/phantom820/streams)
[![codecov](https://codecov.io/gh/phantom820/streams/branch/master/graph/badge.svg?token=I19NMI3C7U)](https://codecov.io/gh/phantom820/streams)

streams is a libray that aims to bring a java streams motivated library to go, this is intended to work with slices, collections and with a custom source that the user can create. 

### Stream
A stream has two type of operations.
 - Intermediate operations - These are operations that produce another stream , and do not terminate the stream (i.e process it).
 - Terminal operations - These are operations that process the stream, these may / may not give a result depending on the type of operation see below.

Streams are also lazily evaluated and any modifications made to the source before a stream is evaluated will be visible to the stream.


```go
// Stream a sequence of elements that can be operated on sequential / concurrently.
type Stream[T any] interface {

	// Intermediate operations.
	Filter(f func(x T) bool) Stream[T]                               // Returns a stream consisting of the elements of this stream that satisfy the given predicate.
	Map(f func(x T) T) Stream[T]                                     // Returns a stream consisting of the results of applying the given transformation to the elements of the stream.
	Limit(n int) Stream[T]                                           // Returns a stream consisting of the elements of the stream but only limited to processing n elements.
	Skip(n int) Stream[T]                                            // Returns a stream that skips the first n elements it encounters in processing.
	Distinct(equals func(x, y T) bool, hash func(x T) int) Stream[T] // Returns a stream consisting of distinct elements. Elements are distinguished using equality and hash code.
	Peek(f func(x T)) Stream[T]                                      // Returns a stream consisting of the elements of the given stream but additionaly the given function is invoked for each element.

	

	// Terminal operations.
	ForEach(f func(x T))               // Performs an action specified by the function f for each element of this stream.
	Count() int                        // Returns a count of elements in the stream.
	Reduce(f func(x, y T) T) (T, bool) // Returns the result of appying a reduction on the elements of the stream. If the stream has no elements then the result would
	// be invalid and the zero value for T along with false would be returned.
	Collect() []T // Returns a slice containing the elements from the stream.

	Terminated() bool // Checks if a terminal operation has been invoked on the stream.
	Closed() bool     // Checks if a stream has been closed. In stream is closed either when a new stream is created from it using intermediate
	// operations, terminated streams are also closed.
	Concurrent() bool // Checks if the stream has a max concurrency > 1 or not.
}
```

#### Sequential vs Concurrent streams
| Sequential      | Concurrent |
| ----------- | ----------- |
| Processes its elements sequentially .    | Processes its elements concurrently using no more than a specified number of go routines and elements are processed in batches of the specified partition size by a routine.     |
| Performs well when cost of processing an element low | Performs well when cost of processing a single element is high and performance may be improved by changin number of routines used and partition size , generally number of go routines and partion sized should somewhat be considered with inverse relationship i.e high number of routines may be better utilized by smaller partitions|
| Limit, Skip & Distinct operations are cheap | Limit,Skip & Distinct operations are expensive due to locks |
| Preserves encounter order from the source  | Does not preserve encounter order from the source.      |
| Infinite source not supported. | Infinite source not supported |
| Reduce operation does not require function to be commutative. | Reduce results may not make sense if given function is not commutative.|
 
  
```go
slice := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}

// A sequential stream .
sequentialStream := streams.FromSlice[int](func() []int { return slice })
// A concurrent stream specifies a level of concurrency and partition size. (concurrency level 2)
concurrentStream := streams.FromSlice[int](func() []int { return slice }, 2)

```

#### Examples
##### Filter
```go
slice := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	newSlice := streams.FromSlice(func() []int { return slice }, 1).Filter(func(x int) bool { return x > 10 }).Collect()

// [11 12 13 14 15 16 17 18 19 20]
```
##### Map
```go
slice := []int{1, 2, 3, 4, 5}
newSlice := streams.FromSlice(func() []int { return slice },1 ).Map(func(x int) interface{} { return x + 1 }).Collect()
// [2 3 4 5 6]
```
##### Limit
```go
slice := []int{1, 2, 3, 4, 5}
newSlice := streams.FromSlice(func() []int { return slice }, 1).Limit(2).Collect()
// [1 2]
```
##### Distinct
Requires an equals function and hashcode function for internal hashset.
```go
slice := []int{1, 1, 0, 2, 2, 3, 4, 5, 5}
newSlice := streams.FromSlice(func() []int { return slice }, 1).
Distinct(func(x, y int) bool { return x == y }, func(x int) int { return x }).
		Collect()
// [1 0 2 3 4 5]
```

#### Complex manipulations

Sum of the even numbers in the range [1,10].
```go
slice := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
sum, ok := streams.FromSlice(func() []int { return slice }, 1).
		Filter(func(x int) bool { return x%2 == 0 }).Reduce(func(x, y int) int { return x + y })
// 30, true
```
Reduce to lower case and filter out consonants.
```go

slice := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}

newSlice := streams.FromSlice(func() []string { return slice }, 2).
	Map(func(x string) string { return strings.ToLower(x) }).
	Filter(func(x string) bool { return strings.ContainsAny(x, "aeiou") }).
	Collect()

// [a e i]
```





