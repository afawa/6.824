package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
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
	GET            OpType = "Get"
	PUT            OpType = "Put"
	APPEND         OpType = "Append"
	CONFIG         OpType = "Config"
	RECVSHARD      OpType = "RecvShard"
	SENDSHARD      OpType = "SendShard"
	SHARDMIGRATION OpType = "ShardMigration"
	INVALID        OpType = "Invalid"
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
	// re-config
	ConfigNum    int
	ConfigShards []int
	Groups       map[int][]string
	// move shard
	ShardID      int
	From         int
	To           int
	ShardMoveIdx int
	ShardData    map[string]string
}

type SnapShot struct {
	LastTerm       int
	LastApplyIndex int
	ShardData      map[int]map[string]string
	OpIndex        map[int64]int
	//
	ShardJobs       map[int][]raft.ApplyMsg
	LastConfig      shardctrler.Config
	ShardIndexmap   map[int]int // gid -> index
	ShardMoveIdx    int
	ShardMigrations map[int]map[string]string
}

type PendingListen struct {
	err Err
	ret interface{}
	msg raft.ApplyMsg
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	sm           *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	ShardData      map[int]map[string]string
	LastApplyIndex int
	LastTerm       int
	OpIndexmap     map[int64]int
	pendingChannel map[int][]chan PendingListen

	ShardJobs       map[int][]raft.ApplyMsg
	LastConfig      shardctrler.Config
	ShardIndexmap   map[int]int // gid -> index
	ShardMigrations map[int]map[string]string
	ShardMoveOpIdx  int
}

func (kv *ShardKV) configReader() {
	for !kv.killed() {
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
		lastConfigNum := kv.LastConfig.Num
		kv.mu.Unlock()
		config := kv.sm.Query(lastConfigNum + 1)
		if config.Num > lastConfigNum {
			command := Op{}
			command.Type = CONFIG
			command.ConfigNum = config.Num
			copy(command.ConfigShards, config.Shards[:])
			command.Groups = make(map[int][]string)
			for k, v := range config.Groups {
				command.Groups[k] = v
			}
			kv.mu.Lock()
			index, _, isLeader := kv.rf.Start(command)
			if !isLeader {
				kv.mu.Unlock()
				continue
			}
			ch := make(chan PendingListen)
			kv.pendingChannel[index] = append(kv.pendingChannel[index], ch)
			kv.mu.Unlock()
			<-ch
		}
	}
}

func (kv *ShardKV) decodeSnapshot(data []byte) SnapShot {
	var snapshot SnapShot
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var LastApplyIndex int
	var LastTerm int
	var ShardData map[int]map[string]string
	var OpIndex map[int64]int
	var ShardJobs map[int][]raft.ApplyMsg
	var LastConfig shardctrler.Config
	var ShardIndexmap map[int]int
	var ShardMoveIdx int
	var ShardMigrations map[int]map[string]string
	if d.Decode(&LastApplyIndex) != nil ||
		d.Decode(&LastTerm) != nil ||
		d.Decode(&ShardData) != nil ||
		d.Decode(&OpIndex) != nil ||
		d.Decode(&ShardJobs) != nil ||
		d.Decode(&LastConfig) != nil ||
		d.Decode(&ShardIndexmap) != nil ||
		d.Decode(&ShardMoveIdx) != nil ||
		d.Decode(&ShardMigrations) != nil {
		fmt.Print("[Read Snapshot] error in decode\n")
	} else {
		snapshot.LastApplyIndex = LastApplyIndex
		snapshot.LastTerm = LastTerm
		snapshot.ShardData = ShardData
		snapshot.OpIndex = OpIndex
		snapshot.ShardJobs = ShardJobs
		snapshot.LastConfig = LastConfig
		snapshot.ShardIndexmap = ShardIndexmap
		snapshot.ShardMoveIdx = ShardMoveIdx
		snapshot.ShardMigrations = ShardMigrations
	}
	return snapshot
}

func (kv *ShardKV) readSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	snapshot := kv.rf.GetSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	ret := kv.decodeSnapshot(snapshot)
	kv.LastApplyIndex = ret.LastApplyIndex
	kv.LastTerm = ret.LastTerm
	kv.ShardData = ret.ShardData
	kv.OpIndexmap = ret.OpIndex
	kv.ShardJobs = ret.ShardJobs
	kv.LastConfig = ret.LastConfig
	kv.ShardIndexmap = ret.ShardIndexmap
	kv.ShardMoveOpIdx = ret.ShardMoveIdx
	kv.ShardMigrations = ret.ShardMigrations
	DPrintf("[Read Snapshot] Server %v Snapshot %v", kv.me, ret)
}

func (kv *ShardKV) termChecker() {
	for !kv.killed() {
		time.Sleep(10 * time.Millisecond)
		term, _ := kv.rf.GetState()
		kv.mu.Lock()
		if term > kv.LastTerm {
			msg := PendingListen{}
			var op Op
			op.Type = INVALID
			msg.msg.Command = op
			for _, v := range kv.pendingChannel {
				for i := range v {
					v[i] <- msg
				}
			}
			kv.LastTerm = term
			kv.pendingChannel = make(map[int][]chan PendingListen)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
	ch := make(chan PendingListen)
	kv.pendingChannel[index] = append(kv.pendingChannel[index], ch)
	kv.mu.Unlock()

	msg := <-ch
	op := msg.msg.Command.(Op)
	if reflect.DeepEqual(op, command) {
		reply.Err = msg.err
		reply.Value = msg.ret.(string)
	} else {
		DPrintf("[Server %v] recv op: %v, expect op: %v", kv.me, op, command)
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
	ch := make(chan PendingListen)
	kv.pendingChannel[index] = append(kv.pendingChannel[index], ch)
	kv.mu.Unlock()

	msg := <-ch
	op := msg.msg.Command.(Op)
	if reflect.DeepEqual(op, command) {
		reply.Err = msg.err
	} else {
		DPrintf("[Server %v] recv op: %v, expect op: %v", kv.me, op, command)
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) ShardMigration(args *ShardMigrationArgs, reply *ShardMigrationReply) {
	command := Op{}
	command.Type = SHARDMIGRATION
	command.ShardID = args.ShardID
	command.From = args.From
	command.To = args.To
	command.ShardData = make(map[string]string)
	for k, v := range args.ShardData {
		command.ShardData[k] = v
	}
	command.OpIndex = args.OperationIndex

	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := make(chan PendingListen)
	kv.pendingChannel[index] = append(kv.pendingChannel[index], ch)
	kv.mu.Unlock()

	msg := <-ch
	op := msg.msg.Command.(Op)
	if reflect.DeepEqual(op, command) {
		reply.Err = msg.err
	} else {
		DPrintf("[Server %v] recv op: %v, expect op: %v", kv.me, op, command)
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) shardWorker(shardID int) {
	for !kv.killed() {
		time.Sleep(10 * time.Millisecond)
		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			if len(kv.ShardJobs[shardID]) == 0 {
				return
			}
			msg := kv.ShardJobs[shardID][0]
			op := msg.Command.(Op)
			ret_msg := PendingListen{}
			ret_msg.msg = msg
			if op.Type == PUT || op.Type == APPEND {
				ret_msg.err = OK
				opidx, ok := kv.OpIndexmap[op.ClerkID]
				if ok && opidx >= op.OpIndex {
					DPrintf("[Server %v] Recv old put|append op from clerk %v, this %v, op %v", kv.me, op.ClerkID, opidx, op)
				}
				if op.Type == PUT {
					kv.ShardData[shardID][op.Key] = op.Value
				} else {
					value, ok := kv.ShardData[shardID][op.Key]
					if !ok {
						kv.ShardData[shardID][op.Key] = op.Value
					} else {
						kv.ShardData[shardID][op.Key] = value + op.Value
					}
				}
				DPrintf("[Server %v] Data %v", kv.me, kv.ShardData)
				kv.OpIndexmap[op.ClerkID] = op.OpIndex
			} else if op.Type == GET {
				value, ok := kv.ShardData[shardID][op.Key]
				if ok {
					DPrintf("[Server %v] Get, key %v, value %v", kv.me, op.Key, value)
					ret_msg.err = OK
					ret_msg.ret = value
				} else {
					ret_msg.err = ErrNoKey
				}
			} else if op.Type == SENDSHARD {
				// make rpc and move data
				kv.mu.Unlock()
				func() {
					args := ShardMigrationArgs{}
					args.ShardID = op.ShardID
					args.From = op.From
					args.To = op.To
					args.OperationIndex = op.ShardMoveIdx
					args.ShardData = make(map[string]string)
					for k, v := range op.ShardData {
						args.ShardData[k] = v
					}
					if servers, ok := op.Groups[op.To]; ok {
						for si := range servers {
							srv := kv.make_end(servers[si])
							var reply ShardMigrationReply
							ok := srv.Call("ShardKV.ShardMigration", &args, &reply)
							if ok && reply.Err == OK {
								kv.mu.Lock()
								delete(kv.ShardData, shardID)
								kv.mu.Unlock()
								return
							}
						}
					} else {
						fmt.Printf("[Fatal Error] migration to gid %v, but can't find in groups\n", args.To)
					}
				}()
				kv.mu.Lock()
			} else if op.Type == RECVSHARD {
				// waiting for a shard migration
				shard_value, ok := kv.ShardMigrations[shardID]
				if ok {
					kv.ShardData[shardID] = make(map[string]string)
					for k, v := range shard_value {
						kv.ShardData[shardID][k] = v
					}
					delete(kv.ShardMigrations, shardID)
				} else {
					if op.From == 0 {
						kv.ShardData[shardID] = make(map[string]string)
					} else {
						return
					}
				}
			}
			if op.Type == GET || op.Type == PUT || op.Type == APPEND {
				ch_list, ok := kv.pendingChannel[msg.CommandIndex]
				if ok {
					for i := range ch_list {
						DPrintf("[Server %v] upload recv, opidx: %v", kv.me, op.OpIndex)
						ch_list[i] <- ret_msg
					}
					delete(kv.pendingChannel, msg.CommandIndex)
				} else {
					DPrintf("[Server %v] out recv, opidx: %v", kv.me, op.OpIndex)
				}
			}
			kv.ShardJobs[shardID] = kv.ShardJobs[shardID][1:]
		}()
	}
}

func (kv *ShardKV) receiver() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			DPrintf("[Server %v] Recv, type %v, key %v, value %v, clerk %v, opidx %v", kv.me, op.Type, op.Key, op.Value, op.ClerkID, op.OpIndex)
			ret_msg := PendingListen{}
			ret_msg.msg = msg
			if op.Type == PUT || op.Type == APPEND || op.Type == GET {
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					shard := key2shard(op.Key)
					gid := kv.LastConfig.Shards[shard]
					if gid != kv.gid {
						DPrintf("[Server %v] wrong group, key %v.", kv.me, op.Key)
						ret_msg.ret = ErrWrongGroup
						ch_list, ok := kv.pendingChannel[msg.CommandIndex]
						if ok {
							for i := range ch_list {
								DPrintf("[Server %v] upload recv, opidx: %v", kv.me, op.OpIndex)
								ch_list[i] <- ret_msg
							}
							delete(kv.pendingChannel, msg.CommandIndex)
						} else {
							DPrintf("[Server %v] out recv, opidx: %v", kv.me, op.OpIndex)
						}
						return
					}
					kv.ShardJobs[shard] = append(kv.ShardJobs[shard], msg)
				}()
			} else if op.Type == CONFIG {
				// re-config
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					ret_msg.err = OK
					if op.ConfigNum < kv.LastConfig.Num {
						fmt.Printf("[Fatal Error] Try to install a old config, old num %v, now num %v\n", op.ConfigNum, kv.LastConfig.Num)
						return
					}
					kv.LastConfig.Num = op.ConfigNum
					for k, v := range op.Groups {
						kv.LastConfig.Groups[k] = v
					}
					for i := range op.ConfigShards {
						if kv.LastConfig.Shards[i] == kv.gid && op.ConfigShards[i] != kv.gid {
							// move out
							out_msg := raft.ApplyMsg{}
							out_msg.CommandIndex = msg.CommandIndex
							out_op := Op{}
							out_op.Type = SENDSHARD
							out_op.From = kv.LastConfig.Shards[i]
							out_op.To = op.ConfigShards[i]
							kv.ShardMoveOpIdx += 1
							out_op.ShardMoveIdx = kv.ShardMoveOpIdx
							out_op.Groups = make(map[int][]string)
							for k, v := range op.Groups {
								out_op.Groups[k] = v
							}
							out_op.ShardData = make(map[string]string)
							for k, v := range kv.ShardData[i] {
								out_op.ShardData[k] = v
							}
							out_msg.Command = out_op
							kv.ShardJobs[i] = append(kv.ShardJobs[i], out_msg)
						} else if kv.LastConfig.Shards[i] != kv.gid && op.ConfigShards[i] == kv.gid {
							// recv in
							in_msg := raft.ApplyMsg{}
							in_msg.CommandIndex = msg.CommandIndex
							in_op := Op{}
							in_op.Type = RECVSHARD
							in_op.From = kv.LastConfig.Shards[i]
							in_op.To = op.ConfigShards[i]
							kv.ShardJobs[i] = append(kv.ShardJobs[i], in_msg)
						}
					}
					copy(kv.LastConfig.Shards[:], op.ConfigShards)
					ch_list, ok := kv.pendingChannel[msg.CommandIndex]
					if ok {
						for i := range ch_list {
							DPrintf("[Server %v] upload recv, opidx: %v", kv.me, op.OpIndex)
							ch_list[i] <- ret_msg
						}
						delete(kv.pendingChannel, msg.CommandIndex)
					} else {
						DPrintf("[Server %v] out recv, opidx: %v", kv.me, op.OpIndex)
					}
				}()
			} else if op.Type == SHARDMIGRATION {
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					opidx, ok := kv.ShardIndexmap[op.From]
					if ok && opidx >= op.OpIndex {
						DPrintf("[Server %v] Recv old migration op from clerk %v, this %v, op %v", kv.me, op.ClerkID, opidx, op)
						return
					}
					kv.ShardMigrations[op.ShardID] = make(map[string]string)
					for k, v := range op.ShardData {
						kv.ShardMigrations[op.ShardID][k] = v
					}
					kv.ShardIndexmap[op.From] = op.OpIndex
					ch_list, ok := kv.pendingChannel[msg.CommandIndex]
					if ok {
						for i := range ch_list {
							DPrintf("[Server %v] upload recv, opidx: %v", kv.me, op.OpIndex)
							ch_list[i] <- ret_msg
						}
						delete(kv.pendingChannel, msg.CommandIndex)
					} else {
						DPrintf("[Server %v] out recv, opidx: %v", kv.me, op.OpIndex)
					}
				}()
			}
			kv.mu.Lock()
			kv.LastApplyIndex = msg.CommandIndex
			if kv.maxraftstate != -1 {
				nowsize := kv.rf.GetStateSize()
				if kv.maxraftstate-nowsize <= 100 {
					snapshot := SnapShot{}
					snapshot.LastApplyIndex = kv.LastApplyIndex
					snapshot.LastTerm = kv.LastTerm
					snapshot.ShardData = kv.ShardData
					snapshot.OpIndex = kv.OpIndexmap
					snapshot.ShardJobs = kv.ShardJobs
					snapshot.LastConfig = kv.LastConfig
					snapshot.ShardIndexmap = kv.ShardIndexmap
					snapshot.ShardMoveIdx = kv.ShardMoveOpIdx
					snapshot.ShardMigrations = kv.ShardMigrations
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.LastApplyIndex)
					e.Encode(kv.LastTerm)
					e.Encode(kv.ShardData)
					e.Encode(kv.OpIndexmap)
					e.Encode(kv.ShardJobs)
					e.Encode(kv.LastConfig)
					e.Encode(kv.ShardIndexmap)
					e.Encode(kv.ShardMoveOpIdx)
					e.Encode(kv.ShardMigrations)
					data := w.Bytes()
					kv.rf.Snapshot(kv.LastApplyIndex, data)
					DPrintf("[Server %v] make snapshot %v", kv.me, snapshot)
				}
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			snapshot := kv.decodeSnapshot(msg.Snapshot)
			DPrintf("[Server %v] Install Snapshot %v", kv.me, snapshot)
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				if kv.LastApplyIndex < snapshot.LastApplyIndex {
					kv.LastApplyIndex = snapshot.LastApplyIndex
					kv.LastTerm = snapshot.LastTerm
					kv.ShardData = snapshot.ShardData
					kv.OpIndexmap = snapshot.OpIndex
					kv.ShardJobs = snapshot.ShardJobs
					kv.LastConfig = snapshot.LastConfig
					kv.ShardIndexmap = snapshot.ShardIndexmap
					kv.ShardMoveOpIdx = snapshot.ShardMoveIdx
					kv.ShardMigrations = snapshot.ShardMigrations
				}
				ret_msg := PendingListen{}
				msg1 := raft.ApplyMsg{}
				var op Op
				op.Type = INVALID
				msg1.Command = op
				ret_msg.msg = msg1
				for idx, v := range kv.pendingChannel {
					if idx <= msg.SnapshotIndex {
						for i := range v {
							v[i] <- ret_msg
						}
					}
					delete(kv.pendingChannel, idx)
				}
			}
			kv.mu.Unlock()
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.ShardData = make(map[int]map[string]string)
	kv.OpIndexmap = make(map[int64]int)
	kv.pendingChannel = make(map[int][]chan PendingListen)
	kv.LastConfig.Groups = make(map[int][]string)
	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.ShardJobs = make(map[int][]raft.ApplyMsg)
	kv.ShardIndexmap = make(map[int]int)
	kv.ShardMigrations = make(map[int]map[string]string)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readSnapshot()
	go kv.receiver()
	go kv.termChecker()
	go kv.configReader()
	for i := range kv.LastConfig.Shards {
		go kv.shardWorker(i)
	}
	return kv
}
