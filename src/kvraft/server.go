package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

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
	GET     OpType = "Get"
	PUT     OpType = "Put"
	APPEND  OpType = "Append"
	INVALID OpType = "Invalid"
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

type SnapShot struct {
	LastIndex int
	LastTerm  int
	Data      map[string]string
	OpIndex   map[int64]int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Data           map[string]string
	LastApplyIndex int
	LastTerm       int
	OpIndexmap     map[int64]int
	pendingChannel map[int][]chan raft.ApplyMsg
	lastSnapshot   SnapShot
}

func (kv *KVServer) decodeSnapshot(data []byte) SnapShot {
	var snapshot SnapShot
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var LastIndex int
	var LastTerm int
	var Data map[string]string
	var OpIndex map[int64]int
	if d.Decode(&LastIndex) != nil || d.Decode(&LastTerm) != nil || d.Decode(&Data) != nil || d.Decode(&OpIndex) != nil {
		fmt.Print("[Read Snapshot] error in decode\n")
	} else {
		snapshot.LastIndex = LastIndex
		snapshot.LastTerm = LastTerm
		snapshot.Data = Data
		snapshot.OpIndex = OpIndex
	}
	return snapshot
}

func (kv *KVServer) readSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	snapshot := kv.rf.GetSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	ret := kv.decodeSnapshot(snapshot)
	kv.LastApplyIndex = ret.LastIndex
	kv.LastTerm = ret.LastTerm
	kv.Data = ret.Data
	kv.OpIndexmap = ret.OpIndex
	DPrintf("[Read Snapshot] Server %v Snapshot %v", kv.me, ret)
}

func (kv *KVServer) termChecker() {
	for !kv.killed() {
		time.Sleep(10 * time.Millisecond)
		term, _ := kv.rf.GetState()
		kv.mu.Lock()
		if term > kv.LastTerm {
			msg := raft.ApplyMsg{}
			var op Op
			op.Type = INVALID
			msg.Command = op
			for _, v := range kv.pendingChannel {
				for i := range v {
					v[i] <- msg
				}
			}
			kv.LastTerm = term
			kv.pendingChannel = make(map[int][]chan raft.ApplyMsg)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{}
	command.Type = GET
	command.Key = args.Key
	command.ClerkID = args.ClerkID
	command.OpIndex = args.OperationIndex
	DPrintf("[Server %v Get] Clerk %v, Key %v, Opidx %v", kv.me, args.ClerkID, args.Key, args.OperationIndex)

	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := make(chan raft.ApplyMsg)
	kv.pendingChannel[index] = append(kv.pendingChannel[index], ch)
	kv.mu.Unlock()

	msg := <-ch
	op := msg.Command.(Op)
	if op == command {
		kv.mu.Lock()
		value, ok := kv.Data[op.Key]
		kv.mu.Unlock()
		if ok {
			DPrintf("[Server %v] Get, key %v, value %v", kv.me, op.Key, value)
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		DPrintf("[Server %v] recv op: %v, expect op: %v", kv.me, op, command)
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
	command.Value = args.Value
	DPrintf("[Server %v P&A] Type %v, Clerk %v, Key %v, Value %v, Opidx %v", kv.me, command.Type, args.ClerkID, args.Key, args.Value, args.OperationIndex)

	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := make(chan raft.ApplyMsg)
	kv.pendingChannel[index] = append(kv.pendingChannel[index], ch)
	kv.mu.Unlock()

	msg := <-ch
	op := msg.Command.(Op)
	if op == command {
		reply.Err = OK
	} else {
		DPrintf("[Server %v] recv op: %v, expect op: %v", kv.me, op, command)
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) Receiver() {
	for !kv.killed() {
		// kv.mu.Lock()
		msg := <-kv.applyCh
		// kv.mu.Unlock()
		if msg.CommandValid {
			// if msg.CommandIndex-1 != kv.LastApplyIndex {
			// 	fmt.Printf("[Fatal Error] raft apply msg out of order, last index %v, this index %v\n", kv.LastApplyIndex, msg.CommandIndex)
			// }
			op := msg.Command.(Op)
			DPrintf("[Server %v] Recv, type %v, key %v, value %v, clerk %v, opidx %v", kv.me, op.Type, op.Key, op.Value, op.ClerkID, op.OpIndex)
			if op.Type == PUT || op.Type == APPEND {
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					opidx, ok := kv.OpIndexmap[op.ClerkID]
					if ok && opidx >= op.OpIndex {
						DPrintf("[Server %v] Recv old put|append op from clerk %v, this %v, op %v", kv.me, op.ClerkID, opidx, op)
						return
					}
					if op.Type == PUT {
						kv.Data[op.Key] = op.Value
					} else {
						value, ok := kv.Data[op.Key]
						if !ok {
							kv.Data[op.Key] = op.Value
						} else {
							kv.Data[op.Key] = value + op.Value
						}
					}
					DPrintf("[Server %v] Data %v", kv.me, kv.Data)
					kv.OpIndexmap[op.ClerkID] = op.OpIndex
				}()
			}
			kv.mu.Lock()
			kv.LastApplyIndex = msg.CommandIndex
			ch_list, ok := kv.pendingChannel[msg.CommandIndex]
			if ok {
				for i := range ch_list {
					DPrintf("[Server %v] upload recv, opidx: %v", kv.me, op.OpIndex)
					ch_list[i] <- msg
				}
				delete(kv.pendingChannel, msg.CommandIndex)
			} else {
				DPrintf("[Server %v] out recv, opidx: %v", kv.me, op.OpIndex)
			}
			if kv.maxraftstate != -1 {
				nowsize := kv.rf.GetStateSize()
				if kv.maxraftstate-nowsize <= 100 {
					snapshot := SnapShot{}
					snapshot.LastIndex = kv.LastApplyIndex
					snapshot.LastTerm = kv.LastTerm
					snapshot.Data = kv.Data
					snapshot.OpIndex = kv.OpIndexmap
					kv.lastSnapshot = snapshot
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.LastApplyIndex)
					e.Encode(kv.LastTerm)
					e.Encode(kv.Data)
					e.Encode(kv.OpIndexmap)
					data := w.Bytes()
					kv.rf.Snapshot(kv.LastApplyIndex, data)
					DPrintf("[Server %v] make snapshot %v", kv.me, kv.lastSnapshot)
				}
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			snapshot := kv.decodeSnapshot(msg.Snapshot)
			DPrintf("[Server %v] Install Snapshot %v", kv.me, snapshot)
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				if kv.LastApplyIndex < snapshot.LastIndex {
					kv.LastApplyIndex = snapshot.LastIndex
					kv.LastTerm = snapshot.LastTerm
					kv.Data = snapshot.Data
					kv.OpIndexmap = snapshot.OpIndex
				}
				msg1 := raft.ApplyMsg{}
				var op Op
				op.Type = INVALID
				msg1.Command = op
				for idx, v := range kv.pendingChannel {
					if idx <= msg.SnapshotIndex {
						for i := range v {
							v[i] <- msg1
						}
					}
					delete(kv.pendingChannel, idx)
				}
				kv.lastSnapshot = snapshot
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
	kv.Data = make(map[string]string)
	kv.LastApplyIndex = 0
	kv.OpIndexmap = make(map[int64]int)
	kv.pendingChannel = make(map[int][]chan raft.ApplyMsg)
	kv.LastTerm = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.readSnapshot()
	go kv.Receiver()
	go kv.termChecker()

	return kv
}
