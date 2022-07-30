package main

import (
	"math/rand"
	"strings"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

type Student struct {
	studentNumber int
	name          string
	surname       string
	age           int
	degreeAverage float32
}

func newStudent() Student {

	studentNumber := 16118281 + rand.Intn(10000)
	b := make([]rune, 500+rand.Intn(100))
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	name := string(b)
	surname := strings.Repeat(name, 2)
	age := 18 + rand.Intn(6)
	degreeAverage := rand.Float32()

	student := Student{
		studentNumber: studentNumber,
		name:          name,
		surname:       surname,
		age:           age,
		degreeAverage: degreeAverage,
	}

	return student

}

func modifyStudent(student Student) Student {
	runes := []rune(student.name)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	name := string(runes)
	surname := student.surname[:200]
	degreeAverage := (student.degreeAverage + 0.5*student.degreeAverage) / 2

	newStudent := Student{
		studentNumber: student.studentNumber + 1,
		name:          name,
		surname:       surname,
		age:           student.age + 1,
		degreeAverage: degreeAverage,
	}

	return newStudent
}

func createSlice(n int) []Student {
	slice := make([]Student, n)
	for i := 0; i < n; i++ {
		slice[i] = newStudent()
	}
	return slice
}

func main() {

	// n := int(1e3)
	// slice := createSlice(n)
	// sequentialStream := streams.NewFromSlice(func() []Student { return slice }, 1)
	// concurrentStream := streams.NewFromSlice(func() []Student { return slice }, 16)

	// startTime := time.Now()
	// _ = sequentialStream.Filter(func(x Student) bool { return x.age > 20 }).Map(func(x Student) interface{} { return modifyStudent(x) }).Collect()
	// fmt.Printf("Seq Duration: %v\n", time.Since(startTime))

	// startTime = time.Now()
	// _ = concurrentStream.Filter(func(x Student) bool { return x.age > 20 }).Map(func(x Student) interface{} { return modifyStudent(x) }).Collect()
	// fmt.Printf("Conc Duration: %v\n", time.Since(startTime))

}
