package main

import (
	"fmt"

	"github.com/phantom820/collections/lists/forwardlist"
	"github.com/phantom820/collections/types"
	"github.com/phantom820/streams/streams"
)

func main() {

	list := forwardlist.New[types.Int](1, 2, 3, 4, 5, 6)
	stream := streams.FromCollection[types.Int](list)
	newStream := stream.Map(func(x types.Int) interface{} { return x + 1 }).Filter(func(x interface{}) bool {
		y := x.(types.Int)
		return y > 1
	}).Map(func(x interface{}) interface{} {
		y := x.(types.Int)
		return y
	}).Limit(3)

	sum := newStream.Reduce(func(x, y interface{}) interface{} {
		return x.(types.Int) + y.(types.Int)
	})
	fmt.Println(sum)
	// stream.ForEach(func(x types.Int) {
	// 	fmt.Println(x)
	// })

}
