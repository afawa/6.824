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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type REPLYTYPE int

const (
	DOWORK REPLYTYPE = 0
	IDLE   REPLYTYPE = 1
	DONE   REPLYTYPE = 2
)

type RegisterArgs struct {
}

type RegisterReply struct {
	Avaliable bool
	Num       int
	NReduce   int
}

type MapTriggerArgs struct {
	Worker_id int
}

type MapTriggerReply struct {
	Reply_type REPLYTYPE
	Job_id     int
	Filename   string
}

type MapDoneArgs struct {
	Job_id    int
	Worker_id int
	Files     []string
}

type MapDoneReply struct {
	Reply_type REPLYTYPE
}

type ReduceTriggerArgs struct {
	Worker_id int
}

type ReduceTriggerReply struct {
	Reply_type REPLYTYPE
	Job_id     int
	Files      []string
	Worker_id  int
}

type ReduceDoneArgs struct {
	Job_id    int
	Worker_id int
}

type ReduceDoneReply struct {
	Reply_type REPLYTYPE
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
