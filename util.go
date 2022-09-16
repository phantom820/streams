package streams

import "sync/atomic"

// atomicCountera counter.
type atomicCounter struct {
	number uint64
}

// add increments counter by specified value.
func (c *atomicCounter) add(num uint64) {
	atomic.AddUint64(&c.number, num)
}

// read returns value of the counter.
func (c *atomicCounter) read() int {
	return int(atomic.LoadUint64(&c.number))
}

// entry this type allows us to use sets for the Distinct operation.
type entry[T any] struct {
	value    T
	equals   func(a, b T) bool
	hashCode func(a T) int
}

// Equals required by Hashable for using a set.
func (a entry[T]) Equals(b entry[T]) bool {
	return a.equals(a.value, b.value)
}

// HashCode produces the hash code of the element.
func (a entry[T]) HashCode() int {
	return a.hashCode(a.value)
}
