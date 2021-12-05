package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type STATUS int
type JOBSTATUS int

const (
	START          STATUS = 0
	WAITING_MAP    STATUS = 1
	WAITING_REDUCE STATUS = 2
	FINISH         STATUS = 3
)

const (
	NOWORKER   JOBSTATUS = 0
	PROCESSING JOBSTATUS = 1
	JOBDONE    JOBSTATUS = 2
)

type Job struct {
	Job_id      int
	Worker_id   int
	Job_status  JOBSTATUS
	Input_files []string
}

type Coordinator struct {
	// Your definitions here.
	status             STATUS
	max_workers        int
	use_workers        int
	use_jobs           int
	input_files        []string
	intermediate_files []string
	worker_list        []int
	job_list           []Job
	mu                 sync.Mutex
	//output_files       []string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Map_Job_Monitor(idx int) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	if c.job_list[idx].Job_status != JOBDONE {
		// fmt.Println("map fail", c.job_list[idx].Worker_id)
		c.job_list[idx].Job_status = NOWORKER
	}
	c.mu.Unlock()
}

func (c *Coordinator) Reduce_Job_Monitor(idx int) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	if c.job_list[idx].Job_status != JOBDONE {
		// fmt.Println("reduce fail", c.job_list[idx].Worker_id)
		c.job_list[idx].Job_status = NOWORKER
	}
	c.mu.Unlock()
}

func (c *Coordinator) Worker_Register(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	if c.status == FINISH {
		c.mu.Unlock()
		reply.Avaliable = false
		reply.Num = 0
		return nil
	}
	if len(c.worker_list) > c.max_workers {
		c.mu.Unlock()
		fmt.Println("Too many workers")
		reply.Avaliable = false
		reply.Num = -1
		return nil
	}
	c.use_workers += 1
	c.worker_list = append(c.worker_list, c.use_workers)
	reply.Num = c.use_workers
	reply.NReduce = c.max_workers
	reply.Avaliable = true
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) Map_Trigger(args *MapTriggerArgs, reply *MapTriggerReply) error {
	c.mu.Lock()
	if c.status != WAITING_MAP {
		c.mu.Unlock()
		reply.Reply_type = DONE
		return nil
	}
	for idx := range c.job_list {
		job := &c.job_list[idx]
		if job.Job_status == NOWORKER {
			reply.Filename = job.Input_files[0]
			reply.Reply_type = DOWORK
			reply.Job_id = job.Job_id
			job.Job_status = PROCESSING
			job.Worker_id = args.Worker_id
			// fmt.Printf("Job id %v worker %v reply %v\n", reply.Job_id, args.Worker_id, reply.Reply_type)
			c.mu.Unlock()
			go c.Map_Job_Monitor(idx)
			return nil
		}
	}
	reply.Reply_type = IDLE
	c.mu.Unlock()
	return nil
}

func unique(strings []string) []string {
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range strings {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

func (c *Coordinator) Map_Done(args *MapDoneArgs, reply *MapDoneReply) error {
	c.mu.Lock()
	for idx := range c.job_list {
		job := &c.job_list[idx]
		if job.Job_id == args.Job_id {
			job.Job_status = JOBDONE
			reply.Reply_type = DONE
			c.intermediate_files = append(c.intermediate_files, args.Files...)
			c.mu.Unlock()
			return nil
		}
	}
	c.mu.Unlock()
	reply.Reply_type = IDLE
	fmt.Println("Error, map work done but not find in job list")
	return nil
}

func (c *Coordinator) Reduce_Trigger(args *ReduceTriggerArgs, reply *ReduceTriggerReply) error {
	c.mu.Lock()
	if c.status == WAITING_MAP {
		fmt.Println("Error, map not done")
		c.mu.Unlock()
		return nil
	}
	if c.status == FINISH {
		reply.Reply_type = DONE
		c.mu.Unlock()
		return nil
	}
	for idx := range c.job_list {
		job := &c.job_list[idx]
		if job.Job_status == NOWORKER {
			reply.Reply_type = DOWORK
			reply.Files = job.Input_files
			reply.Job_id = job.Job_id
			reply.Worker_id = job.Worker_id
			job.Job_status = PROCESSING
			c.mu.Unlock()
			go c.Reduce_Job_Monitor(idx)
			return nil
		}
	}
	reply.Reply_type = IDLE
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) Reduce_Done(args *ReduceDoneArgs, reply *ReduceDoneReply) error {
	c.mu.Lock()
	for idx := range c.job_list {
		job := &c.job_list[idx]
		if job.Job_id == args.Job_id {
			job.Job_status = JOBDONE
			reply.Reply_type = DONE
			c.mu.Unlock()
			return nil
		}
	}
	c.mu.Unlock()
	reply.Reply_type = IDLE
	fmt.Println("Error, reduce work done but not find in job list")
	return nil
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
	c.mu.Lock()
	if c.status == WAITING_MAP {
		flag := true
		for _, job := range c.job_list {
			if job.Job_status != JOBDONE {
				// fmt.Println(job.Worker_id, job.Input_files)
				flag = false
				break
			}
		}
		c.intermediate_files = unique(c.intermediate_files)
		if flag {
			for id := 1; id <= c.max_workers; id++ {
				tmp := Job{}
				tmp.Job_id = c.use_jobs
				c.use_jobs += 1
				tmp.Job_status = NOWORKER
				tmp.Worker_id = id
				for _, intermediate_file := range c.intermediate_files {
					t := strings.Split(intermediate_file, "-") // X-X
					s, _ := strconv.Atoi(t[len(t)-1])
					if s == id {
						tmp.Input_files = append(tmp.Input_files, intermediate_file)
					}
				}
				c.job_list = append(c.job_list, tmp)
			}
			c.status = WAITING_REDUCE
		}
	}
	if c.status == WAITING_REDUCE {
		flag := true
		for _, job := range c.job_list {
			if job.Job_status != JOBDONE {
				// fmt.Println(job.Worker_id, job.Input_files)
				flag = false
				break
			}
		}
		if flag {
			c.status = FINISH
		}
	}
	if c.status == FINISH {
		ret = true
	}
	c.mu.Unlock()

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
		tmp.Job_id = c.use_jobs
		c.use_jobs += 1
		tmp.Job_status = NOWORKER
		tmp.Input_files = append(tmp.Input_files, file)
		c.job_list = append(c.job_list, tmp)
	}
	c.status = WAITING_MAP

	c.server()
	return &c
}
