package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type STATUS int
type JOBSTATUS int

const (
	START          STATUS = 0
	WAITING_MAP    STATUS = 1
	MAP_DONE       STATUS = 2
	WAITING_REDUCE STATUS = 3
	FINISH         STATUS = 4
)

const (
	NOWORKER   JOBSTATUS = 0
	PROCESSING JOBSTATUS = 1
	JOBDONE    JOBSTATUS = 2
)

type Job struct {
	job_id      int
	worker_num  int
	job_status  JOBSTATUS
	input_file  string
	output_file string
}

type Coordinator struct {
	// Your definitions here.
	status             STATUS
	max_workers        int
	use_workers        int
	use_jobs           int
	input_files        []string
	intermediate_files []string
	output_files       []string
	worker_list        []int
	job_list           []Job
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Worker_Register(args *RegisterArgs, reply *RegisterReply) error {
	// TODO: add lock
	if c.status == FINISH {
		reply.avaliable = false
		reply.num = 0
		return nil
	}
	c.use_workers += 1
	if len(c.worker_list) > c.max_workers {
		fmt.Println("Too many workers")
		reply.avaliable = false
		reply.num = -1
		return nil
	}
	c.worker_list = append(c.worker_list, c.use_workers)
	reply.avaliable = true
	reply.num = c.use_workers
	return nil
}

func (c *Coordinator) Map_Trigger(args *MapTriggerArgs, reply *MapTriggerReply) {

}

func (c *Coordinator) Map_Done(args *MapDoneArgs, reply *MapDoneReply) {

}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.status == FINISH {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.input_files = files
	c.status = START
	c.max_workers = nReduce
	c.use_workers = 0
	c.use_jobs = 0

	for _, file := range files {
		tmp := Job{}
		tmp.job_id = c.use_jobs
		c.use_jobs += 1
		tmp.job_status = NOWORKER
		tmp.input_file = file
		c.job_list = append(c.job_list, tmp)
	}
	c.status = WAITING_MAP

	c.server()
	return &c
}
