package streams

import (
	"sync"
)

// applyOpeartions applies the given operations on the element.
func applyOperations[T any](val T, operations []operator[T]) (T, bool) {

	if len(operations) == 0 {
		return val, true
	}
	result, ok := operations[0].apply(val)
	for i := 1; i < len(operations) && ok; i++ {
		result, ok = operations[i].apply(result)
	}
	return result, ok
}

// subIntervals returns sub intervals by splitting the rane [0,n).]
func subIntervals(n int, numberOfSubIntervals int) []int {
	if n == 0 {
		return []int{}
	}
	subIntervals := []int{}
	subIntervalSize := n / numberOfSubIntervals

	for i := 0; i < numberOfSubIntervals; i++ {
		subIntervals = append(subIntervals, i*subIntervalSize)
	}

	subIntervals = append(subIntervals, n)
	return subIntervals
}

// forEach performs given action on each resulting element.
func forEach[T any](data []T, operations []operator[T], f func(T)) {
	for _, val := range data {
		if result, ok := applyOperations(val, operations); ok {
			f(result)
		}
	}
}

// parallelForEach performs given action on each resulting element.
func parallelForEach[T any](data []T, operations []operator[T], f func(T), maxRoutines int) {

	subIntervals := subIntervals(len(data), maxRoutines)
	var wg sync.WaitGroup
	for i := 0; i < len(subIntervals)-1; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, partition []T) {
			defer wg.Done()
			forEach(partition, operations, f)
		}(&wg, data[subIntervals[i]:subIntervals[i+1]])
	}
	wg.Wait()
}

// reduce returns result of reduction on the resulting elements after applying given operations.
func reduce[T any](data []T, operations []operator[T], f func(x, y T) T) (T, bool) {
	var x T
	valid := false
	for i := range data {
		y, ok := applyOperations(data[i], operations)
		if i == 0 && ok {
			x = y
			valid = true
		} else if ok {
			x = f(x, y)
		}
	}
	return x, valid
}

// parallelReduce returns result of reduction on the resulting elements after applying given operations.
func parallelReduce[T any](data []T, operations []operator[T], f func(x, y T) T, maxRoutines int) (T, bool) {
	subIntervals := subIntervals(len(data), maxRoutines)
	channel := make(chan []T)
	for i := 0; i < len(subIntervals)-1; i++ {
		go func(partition []T) {
			if val, ok := reduce(partition, operations, f); ok {
				channel <- []T{val}
				return
			}
			channel <- []T{}
		}(data[subIntervals[i]:subIntervals[i+1]])
	}

	results := make([]T, 0)
	for i := 0; i < len(subIntervals)-1; i++ {
		results = append(results, <-channel...)
	}

	return reduce(data, operations, f)
}

// count returns a count of  resulting elements from applying given operations on each input element of the data.
func count[T any](data []T, operations []operator[T]) int {
	var counter int
	for _, val := range data {
		_, ok := applyOperations(val, operations)
		if ok {
			counter++
		}
	}
	return counter
}

// groupCount returns a count of each group.
func groupCount[T any](groups []Group[T]) map[string]int {
	result := make(map[string]int)
	for _, group := range groups {
		result[group.name] = group.Len()
	}
	return result
}

// parallelCount returns a count of  resulting elements from applying given operations on each input element of the data.
func parallelCount[T any](data []T, operations []operator[T], maxRoutines int) int {

	subIntervals := subIntervals(len(data), maxRoutines)
	channel := make(chan int)

	for i := 0; i < len(subIntervals)-1; i++ {
		go func(partition []T) {
			channel <- count(partition, operations)
		}(data[subIntervals[i]:subIntervals[i+1]])
	}

	count := 0
	for i := 0; i < len(subIntervals)-1; i++ {
		count = count + <-channel
	}
	return count

}

// groupParallelCount returns a count of each group.
func groupParallelCount[T any](groups []Group[T], maxRoutines int) map[string]int {

	channel := make(chan map[string]int)
	subIntervals := subIntervals(len(groups), maxRoutines)
	for i := 0; i < len(subIntervals)-1; i++ {
		go func(partition []Group[T]) {
			channel <- groupCount(partition)
		}(groups[subIntervals[i]:subIntervals[i+1]])
	}

	results := make(map[string]int)
	for i := 0; i < len(subIntervals)-1; i++ {
		for key, val := range <-channel {
			results[key] = results[key] + val
		}
	}

	return results

}

// collect returns a slice of resulting elements from applying given operations on each input element of the data.
func collect[T any](data []T, operations []operator[T]) []T {
	result := make([]T, 0)
	for i := range data {
		if val, ok := applyOperations(data[i], operations); ok {
			result = append(result, val)
		}
	}
	return result
}

// parallelCollect returns a slice of resulting elements from applying given operations on each input element of the data.
func parallelCollect[T any](data []T, operations []operator[T], maxRoutines int) []T {

	subIntervals := subIntervals(len(data), maxRoutines)
	channel := make(chan []T)

	for i := 0; i < len(subIntervals)-1; i++ {
		go func(partition []T) {
			channel <- collect(partition, operations)
		}(data[subIntervals[i]:subIntervals[i+1]])
	}

	results := make([]T, 0)

	for i := 0; i < len(subIntervals)-1; i++ {
		results = append(results, <-channel...)
	}
	return results
}

func groupBy[T any](data []T, f func(x T) string) []Group[T] {
	m := make(map[string][]T)
	for _, val := range data {
		m[f(val)] = append(m[f(val)], val)
	}
	groups := []Group[T]{}
	for key := range m {
		groups = append(groups, Group[T]{name: key, data: m[key]})
	}
	return groups
}
