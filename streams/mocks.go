package streams

type finiteSourceMock struct {
	size    int
	maxSize int
}

func (source *finiteSourceMock) Next() int {
	source.size++
	value := source.size
	return value
}

func (source *finiteSourceMock) HasNext() bool {
	if source.size < source.maxSize {
		return true
	}
	return false
}

type infiniteSourceMock struct {
	size int
}

func (source *infiniteSourceMock) Next() int {
	source.size++
	value := source.size
	return value
}

func (source *infiniteSourceMock) HasNext() bool {
	return true
}
