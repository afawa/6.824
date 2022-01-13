package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType string

const (
	GET    OpType = "Get"
	PUT    OpType = "Put"
	APPEND OpType = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    OpType
	Key     string
	Value   string
	ClerkID int64
	OpIndex int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data           map[string]string
	lastApplyIndex int
	opIndexmap     map[int64]int
	pendingChannel map[int]chan raft.ApplyMsg
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{}
	command.Type = GET
	command.Key = args.Key
	command.ClerkID = args.ClerkID
	command.OpIndex = args.OperationIndex
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
	}
	// waiting for apply
	ch := make(chan raft.ApplyMsg)
	kv.mu.Lock()
	kv.pendingChannel[index] = ch
	kv.mu.Unlock()
	msg := <-ch
	op := msg.Command.(Op)
	if op.Type == GET && op.ClerkID == args.ClerkID && op.OpIndex == args.OperationIndex && op.Key == args.Key {
		kv.mu.Lock()
		value, ok := kv.data[op.Key]
		kv.mu.Unlock()
		if ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		// lost when leader doesn't commit
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op{}
	if args.Op == "Put" {
		command.Type = PUT
	} else if args.Op == "Append" {
		command.Type = APPEND
	} else {
		fmt.Println("[Fatal Error] Unknown op type")
	}
	command.Key = args.Key
	command.ClerkID = args.ClerkID
	command.OpIndex = args.OperationIndex
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
	}
	ch := make(chan raft.ApplyMsg)
	kv.mu.Lock()
	kv.pendingChannel[index] = ch
	kv.mu.Unlock()
	msg := <-ch
	op := msg.Command.(Op)
	if op.Type == command.Type && op.ClerkID == args.ClerkID && op.OpIndex == args.OperationIndex && op.Key == args.Key && op.Value == args.Value {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) Receiver() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			if msg.CommandIndex-1 != kv.lastApplyIndex {
				fmt.Println("[Fatal Error] raft apply msg out of order")
				continue
			}
			op := msg.Command.(Op)
			if op.Type == PUT || op.Type == APPEND {
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					opidx, ok := kv.opIndexmap[op.ClerkID]
					if ok && opidx >= op.OpIndex {
						DPrintf("Recv old put|append op from clerk %v, now opidx %v, this %v", op.ClerkID, opidx, op.OpIndex)
						return
					}
					if op.Type == PUT {
						kv.data[op.Key] = op.Value
					} else {
						value, ok := kv.data[op.Key]
						if !ok {
							kv.data[op.Key] = op.Value
						} else {
							kv.data[op.Key] = value + op.Value
						}
					}
					kv.opIndexmap[op.ClerkID] = op.OpIndex
				}()
			}
			kv.mu.Lock()
			kv.lastApplyIndex = msg.CommandIndex
			ch, ok := kv.pendingChannel[msg.CommandIndex]
			if ok {
				ch <- msg
			}
			kv.mu.Unlock()
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.lastApplyIndex = 0
	kv.opIndexmap = make(map[int64]int)
	kv.pendingChannel = make(map[int]chan raft.ApplyMsg)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
