package streams

import (
	"github.com/phantom820/streams/sources"
)

// scatterGather returns the resulting elements after processing the source in parallel.
func scatterGather[T any](source sources.Source[T], partitionSize int, f func(element T) (T, bool), limiter chan struct{}) []T {
	// Wrap f and g in a routine.
	h := func(partition []T, channel chan []T) {
		limiter <- struct{}{}
		partitionResults := make([]T, 0)
		for _, input := range partition {
			element, ok := f(input)
			if ok {
				partitionResults = append(partitionResults, element)
			}
		}
		channel <- partitionResults
		<-limiter
	}

	// Build up a partition and send it over for processing.
	channel := make(chan []T)
	partition := make([]T, 0)
	numberOfPartions := 0
	for source.HasNext() {
		if len(partition) < partitionSize {
			partition = append(partition, source.Next())
		} else {
			go h(partition, channel)
			partition = make([]T, 0)
			numberOfPartions++

		}
	}

	// Last partition if present needs to be processed.
	if len(partition) > 0 {
		go h(partition, channel)
		numberOfPartions++
	}

	// Gather results to send back.
	results := make([]T, 0)
	for i := 0; i < numberOfPartions; i++ {
		results = append(results, <-channel...)
	}
	return results
}

// scatterForEach processes the given source concurrently, limiter limits the number of routines and each routine processes at most
// partitionSize elements.
func scatterForEach[T any](source sources.Source[T], partitionSize int, f func(element T) (T, bool), g func(element T), limiter chan struct{}) {
	// Wrap f and g in a routine.
	h := func(partition []T, channel chan []T) {
		limiter <- struct{}{}
		partitionResults := make([]T, 0)
		for _, input := range partition {
			element, ok := f(input)
			if ok {
				partitionResults = append(partitionResults, element)
			}
		}
		channel <- partitionResults
		<-limiter
	}

	// Build up a partition and send it over for processing.
	channel := make(chan []T)
	partition := make([]T, 0)
	numberOfPartions := 0
	for source.HasNext() {
		if len(partition) < partitionSize {
			partition = append(partition, source.Next())
		} else {
			go h(partition, channel)
			partition = make([]T, 0)
			numberOfPartions++

		}
	}

	// Last partition if present needs to be processed.
	if len(partition) > 0 {
		go h(partition, channel)
		numberOfPartions++
	}

	// Apply on each element of a partition.
	for i := 0; i < numberOfPartions; i++ {
		for _, element := range <-channel {
			g(element)
		}
	}
}

// scatterReduce returns the results of applying the binary associative function g on the elements that pass the filter function f. Elements are processed
// concurrently.
func scatterReduce[T any](source sources.Source[T], partitionSize int, f func(element T) (T, bool), g func(x, y T) T, limiter chan struct{}) (T, bool) {

	// Wrap f and g to allow it to be a worker.
	h := func(partition []T, channel chan []T) {
		limiter <- struct{}{}
		partitionResults := make([]T, 0)
		for _, input := range partition {
			element, ok := f(input)
			if ok {
				if len(partitionResults) > 0 {
					partitionResults[0] = g(partitionResults[0], element)
				} else {
					partitionResults = append(partitionResults, element)
				}
			}
		}
		channel <- partitionResults
		<-limiter
	}

	// Build up a partition and send it over for processing.
	channel := make(chan []T)
	partition := make([]T, 0)
	numberOfPartions := 0
	for source.HasNext() {
		if len(partition) < partitionSize {
			partition = append(partition, source.Next())
		} else {
			go h(partition, channel)
			partition = make([]T, 0)
			numberOfPartions++
		}
	}

	// Last partition if present needs to be processed.
	if len(partition) > 0 {
		go h(partition, channel)
		numberOfPartions++
	}

	// Gather results from channel.
	results := make([]T, 0)
	for i := 0; i < numberOfPartions; i++ {
		partitionResults := <-channel
		if len(results) > 0 && len(partitionResults) > 0 {
			results[0] = g(results[0], partitionResults[0])
		} else if len(partitionResults) > 0 {
			results = append(results, partitionResults...)
		}
	}

	// If no element was encountered.
	if len(results) == 0 {
		var zero T
		return zero, false
	}
	return results[0], true
}

// scatterCount returns a count of the number of elements that are pass through the filter function f.
func scatterCount[T any](source sources.Source[T], partitionSize int, f func(element T) (T, bool), limiter chan struct{}) int {

	// Wrap f and g to allow it to be a worker.
	h := func(partition []T, channel chan int) {
		limiter <- struct{}{}
		count := 0
		for _, input := range partition {
			_, ok := f(input)
			if ok {
				count = count + 1
			}
		}
		channel <- count
		<-limiter
	}

	// Build up a partition and send it over for processing.
	channel := make(chan int)
	partition := make([]T, 0)
	numberOfPartions := 0
	for source.HasNext() {
		if len(partition) < partitionSize {
			partition = append(partition, source.Next())
		} else {
			go h(partition, channel)
			partition = make([]T, 0)
			numberOfPartions++
		}
	}

	// Last partition if present needs to be processed.
	if len(partition) > 0 {
		go h(partition, channel)
		numberOfPartions++
	}

	// Gather results from channel.
	count := 0
	for i := 0; i < numberOfPartions; i++ {
		count = count + <-channel
	}

	return count
}
