package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

func DoMap(mapf func(string, string) []KeyValue, filename string, nReduce int, worker_id int) []string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	var output_files []string
	kva := mapf(filename, string(content))
	for i := 1; i <= nReduce; i++ {
		tmpfile, _ := ioutil.TempFile(".", fmt.Sprintf("map-temp-%v-%v", worker_id, i))
		enc := json.NewEncoder(tmpfile)
		output_file := fmt.Sprintf("mr-%v-%v", worker_id, i)
		output_files = append(output_files, output_file)
		if _, err := os.Stat(output_file); !errors.Is(err, os.ErrNotExist) {
			before_file, _ := os.Open(output_file)
			dec := json.NewDecoder(before_file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}
		for _, kv := range kva {
			if ihash(kv.Key)%nReduce != i-1 {
				continue
			}
			enc.Encode(&kv)
		}
		defer os.Rename(tmpfile.Name(), output_file)
	}
	return output_files
}

func DoReduce(reducef func(string, []string) string, filenames []string, worker_id int) {
	tmpfile, _ := ioutil.TempFile(".", fmt.Sprintf("mr-out-tmp-%v", worker_id))
	defer os.Rename(tmpfile.Name(), fmt.Sprintf("mr-out-%v", worker_id))
	var kva []KeyValue
	for _, filename := range filenames {
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)

		i = j
	}
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker_id := 0
	worker_args := RegisterArgs{}
	worker_replys := RegisterReply{}
	call("Coordinator.Worker_Register", &worker_args, &worker_replys)
	if !worker_replys.Avaliable {
		fmt.Println("This Worker register fail")
		return
	}
	worker_id = worker_replys.Num
	NReduce := 10
	// fmt.Printf("This worker %v, all worker %v\n", worker_id, NReduce)

	map_trigger_args := MapTriggerArgs{}
	map_trigger_replys := MapTriggerReply{}
	map_trigger_args.Worker_id = worker_id

	call("Coordinator.Map_Trigger", &map_trigger_args, &map_trigger_replys)
	for map_trigger_replys.Reply_type != DONE {
		// fmt.Printf("Worker %v map, reply %v, job id %v\n", worker_id, map_trigger_replys.Reply_type, map_trigger_replys.Job_id)
		if map_trigger_replys.Reply_type == DOWORK {
			// do map
			job_id := map_trigger_replys.Job_id
			intermediate_files := DoMap(mapf, map_trigger_replys.Filename, NReduce, worker_id)
			map_done_args := MapDoneArgs{}
			map_done_args.Files = intermediate_files
			map_done_args.Worker_id = worker_id
			map_done_args.Job_id = job_id
			map_done_replys := MapDoneReply{}
			call("Coordinator.Map_Done", &map_done_args, &map_done_replys)
		} else {
			time.Sleep(time.Second)
		}
		map_trigger_replys = MapTriggerReply{}
		call("Coordinator.Map_Trigger", &map_trigger_args, &map_trigger_replys)
	}

	reduce_trigger_args := ReduceTriggerArgs{}
	reduce_trigger_replys := ReduceTriggerReply{}
	reduce_trigger_args.Worker_id = worker_id
	call("Coordinator.Reduce_Trigger", &reduce_trigger_args, &reduce_trigger_replys)
	for reduce_trigger_replys.Reply_type != DONE {
		if reduce_trigger_replys.Reply_type == DOWORK {
			// do reduce
			job_id := reduce_trigger_replys.Job_id
			// fmt.Printf("This worker id %v, do reduce worker id %v\n", worker_id, reduce_trigger_replys.Worker_id)
			DoReduce(reducef, reduce_trigger_replys.Files, reduce_trigger_replys.Worker_id)
			reduce_done_args := ReduceDoneArgs{}
			reduce_done_replys := ReduceDoneReply{}
			reduce_done_args.Job_id = job_id
			reduce_done_args.Worker_id = worker_id
			call("Coordinator.Reduce_Done", &reduce_done_args, &reduce_done_replys)
		} else {
			time.Sleep(time.Second)
		}
		reduce_trigger_replys = ReduceTriggerReply{}
		call("Coordinator.Reduce_Trigger", &reduce_trigger_args, &reduce_trigger_replys)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
