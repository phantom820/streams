package streams

// transformSupplier transforms a supplier from one type to another, the prior operations on previous supplier must be invoked once we evaluate new supplier.
func transformSupplier[T any, U any](supplier func() []T, operations []operator[T], f func(data []T) []U) func() []U {
	transformedSupplier := func() []U {
		data := collect(supplier(), operations)
		return f(data)
	}
	return transformedSupplier
}

// parallelTransformSupplier transforms a supplier from one type to another in parallel, the prior operations on previous supplier must be invoked once we evaluate new supplier.
func parallelTransformSupplier[T any, U any](supplier func() []T, operations []operator[T], f func(data []T) []U, maxRoutines int) func() []U {
	transformedSupplier := func() []U {
		data := parallelCollect(supplier(), operations, maxRoutines)
		return f(data)
	}
	return transformedSupplier
}

// partitionSupplierElements converts each element of the supplier to a slice using the given function.
func partitionSupplierElements[T any](data []T, operations []operator[T], f func(x T) []T) [][]T {
	partitions := make([][]T, 0)
	for i := 0; i < len(data); i++ {
		if val, ok := applyOperations(data[i], operations); ok {
			partitions = append(partitions, f(val))
		}
	}
	return partitions
}

// parallelPartitionSupplierElements converts each element of the supplier to a slice using the given function. Performed in parallel fashion.
func parallelPartitionSupplierElements[T any](supplier func() []T, operations []operator[T], f func(x T) []T, maxRoutines int) func() [][]T {

	partitionedSupplier := func() [][]T {
		data := supplier()
		subIntervals := subIntervals(len(data), maxRoutines)
		channel := make(chan [][]T)
		for i := 0; i < len(subIntervals)-1; i++ {
			go func(_partition []T) {
				channel <- partitionSupplierElements(_partition, operations, f)
			}(data[subIntervals[i]:subIntervals[i+1]])
		}
		partitions := make([][]T, 0)
		for i := 0; i < len(subIntervals)-1; i++ {
			partitions = append(partitions, <-channel...)
		}
		return partitions
	}

	return partitionedSupplier
}

// flatMapSupplier converts a supplier of the form [[], [], ...] to a supplier of the form [.......], by joining given slices.
func flatMapSupplier[T any](supplier func() [][]T, operations []operator[[]T]) func() []T {
	flatMappedSupplier := func() []T {
		data := collect(supplier(), operations)
		result, _ := reduce(data, []operator[[]T]{}, func(x, y []T) []T { return append(x, y...) })
		return result
	}
	return flatMappedSupplier
}

// parallelFlatMapSupplier converts a supplier of the form [[], [], ...] to a supplier of the form [.......], by joining given slices, does this in parallel.
func parallelFlatMapSupplier[T any](supplier func() [][]T, operations []operator[[]T], maxRoutines int) func() []T {
	flatMappedSupplier := func() []T {
		data := parallelCollect(supplier(), operations, maxRoutines)
		result, _ := parallelReduce(data, []operator[[]T]{}, func(x, y []T) []T { return append(x, y...) }, maxRoutines)
		return result
	}
	return flatMappedSupplier
}
