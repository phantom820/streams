package streams

import (
	"bytes"
	"text/template"
)

// error codes.
const (
	StreamTerminated     = 1
	IllegalArgument      = 2
	StreamClosed         = 3
	IllegalConfig        = 4
	IllegalStreamMapping = 5
)

// error templates.
var (
	StreamTerminatedTemplate, _     = template.New("StreamTerminated").Parse("ErrStreamTerminated: A terminal operation has been invoked on the stream.")
	IllegalArgumentTemplate, _      = template.New("IllegalArgument").Parse("ErrIllegalArgument: Illegal argument: {{.argument}} for operation: {{.operation}}.")
	StreamClosedTemplate, _         = template.New("StreamClosed").Parse("ErrStreamClosed: The stream has been closed.")
	IllegalConfigTemplate, _        = template.New("IllegalConfig").Parse("ErrIllegalStreamConfig: Illegal configuration {{.argument}} when trying to create a stream using {{.function}}.")
	IllegalStreamMappingTemplate, _ = template.New("IllegalMapping").Parse("ErrIllegalStreamMapping: The given stream cannot be mapped to {{.type}}.")
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

// ErrStreamClosed returns an error for a  stream that has been closed.
func ErrStreamClosed() Error {
	var buffer bytes.Buffer
	StreamClosedTemplate.Execute(&buffer, map[string]int{})
	return Error{code: StreamClosed, msg: buffer.String()}
}

// ErrIllegalArgument returns an error for a  stream operation that has been given an illegal argument.
func ErrIllegalArgument(argument, operation string) Error {
	var buffer bytes.Buffer
	IllegalArgumentTemplate.Execute(&buffer, map[string]string{"argument": argument, "operation": operation})
	return Error{code: IllegalArgument, msg: buffer.String()}
}

// ErrIllegalConfig returns an error for trying to construct a stream with an illegal config.
func ErrIllegalConfig(config, function string) Error {
	var buffer bytes.Buffer
	IllegalConfigTemplate.Execute(&buffer, map[string]string{"config": config, "function": function})
	return Error{code: IllegalConfig, msg: buffer.String()}
}

// ErrIllegalStreamMapping returns an error when top level map function for  streams cannot identify underlying type as *stream or *concurrentStream.
func ErrIllegalStreamMapping(toType string) Error {
	var buffer bytes.Buffer
	IllegalStreamMappingTemplate.Execute(&buffer, map[string]string{"type": toType})
	return Error{code: IllegalStreamMapping, msg: buffer.String()}
}
