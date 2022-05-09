package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type RegisterArgs struct{}

type RegisterReply struct {
	Id      WorkerId
	NMap    int
	NReduce int
}

type GetTaskRes int

const (
	Ok GetTaskRes = iota
	Wait
	None
)

type GetTaskArgs struct {
	Id   WorkerId
	Prev Task
}

type GetTaskReply struct {
	Result GetTaskRes
	Task   Task
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
