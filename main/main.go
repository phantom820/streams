package main

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/phantom820/streams"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// sequence generates slice of integer from 1-> n
func sequence(n int) []int {
	slice := make([]int, n)
	for i := 0; i < n; i++ {
		slice[i] = i + 1
	}
	return slice
}

// randomInts generate a slice of random integers in the range [0,n).
func randomInts(n int) []int {
	slice := make([]int, n)
	for i := 0; i < n; i++ {
		slice[i] = rand.Intn(n)
	}
	return slice
}

// randomString generates a random string of the given length.
func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// randomStrings generates a slice of N random strings of length n.
func randomStrings(n int, N int) []string {
	slice := make([]string, N)
	for i := 0; i < N; i++ {
		slice[i] = randomString(n)
	}
	return slice
}

func main() {

	words := randomStrings(10, 10)
	s := streams.New(func() []string { return words })
	// s.GroupBy(func(x string) string { return x }).Parallelize(3).ForEach(func(x streams.Group[string]) {
	// 	fmt.Printf("Name : %v Count : %v\n", x.Name(), x.Len())
	// })
	r := s.Partition(func(x string) []string { return strings.Split(x, "a") }).Collect()
	fmt.Println(r)
}
