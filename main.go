package main

import (
	"fmt"

	"github.com/phantom820/collections/lists/forwardlist"
	"github.com/phantom820/collections/types"
	"github.com/phantom820/streams/streams"
)

func main() {

	list := forwardlist.New[types.Int](1, 1, 2, 2, 2, 3, 4, 5, 6)
	stream := streams.FromCollection[types.Int](list)
	newStream := stream.Map(func(x types.Int) interface{} { return x + 0 }).Filter(func(x interface{}) bool {
		y := x.(types.Int)
		return y > 0
	}).Map(func(x interface{}) interface{} {
		y := x.(types.Int)
		return y
	}).Distinct(func(a, b interface{}) bool { return a.(types.Int) == b.(types.Int) }, func(x interface{}) int { return int(x.(types.Int)) })

	// sum := newStream.Reduce(func(x, y interface{}) interface{} {
	// 	return x.(types.Int) + y.(types.Int)
	// })
	// newStream.Collect(forwardlist.New[]())
	fmt.Println(streams.ToSlice(newStream))
	// stream.ForEach(func(x types.Int) {
	// 	fmt.Println(x)
	// })

}
