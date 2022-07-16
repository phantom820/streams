package main

import (
	"fmt"

	"github.com/phantom820/streams/streams"
)

func main() {

	slice := []int{1, 2, 3, 4, 5, 6, 7}
	stream := streams.FromSlice(func() []int { return slice })
	newStream := stream.Map(
		func(x int) interface{} {
			return x + 2
		}).Filter(func(x interface{}) bool {
		y := x.(int)
		return y > 3
	}).Map(
		func(x interface{}) interface{} {
			y := x.(int)
			return y
		}).Distinct(
		func(a, b interface{}) bool {
			return a.(int) == b.(int)
		},
		func(x interface{}) int {
			return x.(int)
		})
	slice = slice[:4]
	fmt.Println(streams.ToSlice(newStream))

	sk := streams.ConcurrentFromSlice[int](func() []int { return slice }, 2)
	sk.Count()

}
