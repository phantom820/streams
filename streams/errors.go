package streams

import (
	"bytes"
	"text/template"
)

// error codes.
const (
	StreamTerminated = 1
	IllegalArgument  = 2
)

// error templates.
var (
	StreamTerminatedTemplate, _ = template.New("StreamTerminated").Parse("ErrStreamTerminated: A terminal operation has been invoked on the stream or it has been closed.")
	IllegalArgumentTemplate, _  = template.New("IllegalArgument").Parse("ErrIllegalArgument: Illegal argument: {{.argument}} for operation: {{.operation}}.")
)

// Error a custom error type for stream.
type Error struct {
	code int
	msg  string
	Err  error
}

// Code returns the error code for the error.
func (err Error) Code() int {
	return err.code
}

// Error returns the error message.
func (err *Error) Error() string {
	return err.msg
}

// ErrStreamTerminated returns an error for a  stream that has already been terminated.
func ErrStreamTerminated() Error {
	var buffer bytes.Buffer
	StreamTerminatedTemplate.Execute(&buffer, map[string]int{})
	return Error{code: StreamTerminated, msg: buffer.String()}
}

// ErrIllegalArgument returns an error for a  stream operation that has been given an illegal argument.
func ErrIllegalArgument(argument, operation string) Error {
	var buffer bytes.Buffer
	IllegalArgumentTemplate.Execute(&buffer, map[string]string{"argument": argument, "operation": operation})
	return Error{code: IllegalArgument, msg: buffer.String()}
}
