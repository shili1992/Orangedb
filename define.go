package Orangedb

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
	ErrAlreadyInTransaction   = errors.New("already in transaction ")
	ErrNotInTransaction   = errors.New("not in transaction ")


	// Queue is Empty.
	ErrEmptyQueue = errors.New("queue is empty")
	// Queue is Full.
	ErrFullQueue = errors.New("queue is full")
	ErrOutOfRange = errors.New("out of range")

	ErrNotLeader = errors.New("not leader")
	ErrNotFindPeer = errors.New("can't find perr")
	ErrNotLocalKey  = errors.New("not local key")
	ErrNotFindKey  = errors.New("not find key")
)
