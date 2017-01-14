package common

import "errors"



var (
	ResponseOK   = "OK"
)

var (
	ErrEmptyCommand          = errors.New("empty command")
	ErrNotFound              = errors.New("command not found")
	ErrNotAuthenticated      = errors.New("not authenticated")
	ErrAuthenticationFailure = errors.New("authentication failure")
	ErrCmdParams             = errors.New("invalid command param")
	ErrValue                 = errors.New("value is not an integer or out of range")
	ErrSyntax                = errors.New("syntax error")
	ErrOffset                = errors.New("offset bit is not an natural number")
	ErrBool                  = errors.New("value is not 0 or 1")
	ErrInvalidArgument       = errors.New("invalid argument")
)
