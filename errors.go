package streams

import (
	"bytes"
	"text/template"
)

// stream error codes.
const (
	StreamTerminated     = 1
	IllegalArgument      = 2
	StreamClosed         = 3
	IllegalConfig        = 4
	IllegalStreamMapping = 5
)

// stream error templates.
var (
	streamTerminatedTemplate, _     = template.New("StreamTerminated").Parse("ErrStreamTerminated: A terminal operation has been invoked on the stream.")
	illegalArgumentTemplate, _      = template.New("IllegalArgument").Parse("ErrIllegalArgument: Illegal argument: {{.argument}} for operation: {{.operation}}.")
	streamClosedTemplate, _         = template.New("StreamClosed").Parse("ErrStreamClosed: The stream has been closed.")
	illegalConfigTemplate, _        = template.New("IllegalConfig").Parse("ErrIllegalStreamConfig: Illegal configuration {{.argument}} when trying to create a stream using {{.function}}.")
	illegalStreamMappingTemplate, _ = template.New("IllegalMapping").Parse("ErrIllegalStreamMapping: The given stream cannot be mapped to {{.type}}.")
)

// streamError a custom error type for streams.
type streamError struct {
	code int
	msg  string
	Err  error
}

// Code returns the error code for the error.
func (err streamError) Code() int {
	return err.code
}

// streamError returns the error message.
func (err streamError) streamError() string {
	return err.msg
}

// errStreamTerminated returns an error for a  stream that has already been terminated.
func errStreamTerminated() streamError {
	var buffer bytes.Buffer
	streamTerminatedTemplate.Execute(&buffer, map[string]int{})
	return streamError{code: StreamTerminated, msg: buffer.String()}
}

// errStreamClosed returns an error for a  stream that has been closed.
func errStreamClosed() streamError {
	var buffer bytes.Buffer
	streamClosedTemplate.Execute(&buffer, map[string]int{})
	return streamError{code: StreamClosed, msg: buffer.String()}
}

// errIllegalArgument returns an error for a  stream operation that has been given an illegal argument.
func errIllegalArgument(argument, operation string) streamError {
	var buffer bytes.Buffer
	illegalArgumentTemplate.Execute(&buffer, map[string]string{"argument": argument, "operation": operation})
	return streamError{code: IllegalArgument, msg: buffer.String()}
}

// errIllegalConfig returns an error for trying to construct a stream with an illegal config.
func errIllegalConfig(config, function string) streamError {
	var buffer bytes.Buffer
	illegalConfigTemplate.Execute(&buffer, map[string]string{"config": config, "function": function})
	return streamError{code: IllegalConfig, msg: buffer.String()}
}

// errIllegalStreamMapping returns an error when top level map function for  streams cannot identify underlying type as *stream or *concurrentStream.
func errIllegalStreamMapping(toType string) streamError {
	var buffer bytes.Buffer
	illegalStreamMappingTemplate.Execute(&buffer, map[string]string{"type": toType})
	return streamError{code: IllegalStreamMapping, msg: buffer.String()}
}
