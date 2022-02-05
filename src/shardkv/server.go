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
	ShardID int
	From    int
	To      int
	Shard   MigrationData
}

type MigrationData struct {
	ShardData map[string]string
	OpIdxMap  map[int64]int
}

type SnapShot struct {
	LastTerm        int
	LastApplyIndex  int
	ShardData       map[int]map[string]string
	OpIndex         map[int]map[int64]int
	ShardJobs       map[int][]raft.ApplyMsg
	LastConfig      shardctrler.Config
	ShardMigrations map[int]map[int]MigrationData
	ShardConfigNum  map[int]int
	// ShardMoveIdx int
	// ShardIndexmap   map[int]int // gid -> index
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
	OpIndexmap     map[int]map[int64]int
	pendingChannel map[int][]chan PendingListen

	ShardJobs       map[int][]raft.ApplyMsg
	LastConfig      shardctrler.Config
	ShardMigrations map[int]map[int]MigrationData // ShardID -> ConfigNum -> Data
	ShardConfigNum  map[int]int
}

func (kv *ShardKV) configReader() {
	for !kv.killed() {
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
		lastConfigNum := kv.LastConfig.Num
		kv.mu.Unlock()
		config := kv.sm.Query(lastConfigNum + 1)
		if config.Num > lastConfigNum {
			DPrintf("[Group %v Server %v] Found re-config, prev config %v, new config %v", kv.gid, kv.me, lastConfigNum, config)
			command := Op{}
			command.Type = CONFIG
			command.ConfigNum = config.Num
			for i := range config.Shards {
				command.ConfigShards = append(command.ConfigShards, config.Shards[i])
			}
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
	var OpIndex map[int]map[int64]int
	var ShardJobs map[int][]raft.ApplyMsg
	var LastConfig shardctrler.Config
	var ShardMigrations map[int]map[int]MigrationData
	var ShardConfigNum map[int]int
	if d.Decode(&LastApplyIndex) != nil ||
		d.Decode(&LastTerm) != nil ||
		d.Decode(&ShardData) != nil ||
		d.Decode(&OpIndex) != nil ||
		d.Decode(&ShardJobs) != nil ||
		d.Decode(&LastConfig) != nil ||
		d.Decode(&ShardMigrations) != nil ||
		d.Decode(&ShardConfigNum) != nil {
		fmt.Print("[Read Snapshot] error in decode\n")
	} else {
		snapshot.LastApplyIndex = LastApplyIndex
		snapshot.LastTerm = LastTerm
		snapshot.ShardData = ShardData
		snapshot.OpIndex = OpIndex
		snapshot.ShardJobs = ShardJobs
		snapshot.LastConfig = LastConfig
		snapshot.ShardMigrations = ShardMigrations
		snapshot.ShardConfigNum = ShardConfigNum
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
	kv.ShardMigrations = ret.ShardMigrations
	kv.ShardConfigNum = ret.ShardConfigNum
	DPrintf("[Read Snapshot] Group %v Server %v Snapshot %v", kv.gid, kv.me, ret)
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
	// DPrintf("[Group %v Server %v Get] Clerk %v, Key %v, Opidx %v", kv.gid, kv.me, args.ClerkID, args.Key, args.OperationIndex)

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
		// DPrintf("[Group %v Server %v] recv op: %v, expect op: %v", kv.gid, kv.me, op, command)
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
	// DPrintf("[Group %v Server %v P&A] Type %v, Clerk %v, Key %v, Value %v, Opidx %v", kv.gid, kv.me, command.Type, args.ClerkID, args.Key, args.Value, args.OperationIndex)

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
		// DPrintf("[Group %v Server %v] recv op: %v, expect op: %v", kv.gid, kv.me, op, command)
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) ShardMigration(args *ShardMigrationArgs, reply *ShardMigrationReply) {
	command := Op{}
	command.Type = SHARDMIGRATION
	command.ShardID = args.ShardID
	command.From = args.From
	command.To = args.To
	command.ConfigNum = args.ConfigID
	command.Shard = MigrationData{}
	command.Shard.ShardData = make(map[string]string)
	for k, v := range args.ShardData {
		command.Shard.ShardData[k] = v
	}
	command.Shard.OpIdxMap = make(map[int64]int)
	for k, v := range args.OpIdxMap {
		command.Shard.OpIdxMap[k] = v
	}
	DPrintf("[Group %v Server %v Migration] ShardID %v, From %v, To %v, data %v", kv.gid, kv.me, command.ShardID, command.From, command.To, command.Shard)

	kv.mu.Lock()
	if kv.ShardConfigNum[args.ShardID] >= command.ConfigNum {
		// fmt.Println("recv early rpc")
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
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
		DPrintf("[Group %v Server %v] recv op: %v, expect op: %v", kv.gid, kv.me, op, command)
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
			DPrintf("[Group %v Server %v Shard %v] recv job %v", kv.gid, kv.me, shardID, op)
			ret_msg := PendingListen{}
			ret_msg.msg = msg
			if op.Type == PUT || op.Type == APPEND {
				ret_msg.err = OK
				opidx, ok := kv.OpIndexmap[shardID][op.ClerkID]
				if ok && opidx >= op.OpIndex {
					// DPrintf("[Group %v Server %v] Recv old put|append op from clerk %v, this %v, op %v", kv.gid, kv.me, op.ClerkID, opidx, op)
				} else {
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
					// DPrintf("[Group %v Server %v Shard %v] Data %v", kv.gid, kv.me, shardID, kv.ShardData[shardID])
					kv.OpIndexmap[shardID][op.ClerkID] = op.OpIndex
				}
			} else if op.Type == GET {
				value, ok := kv.ShardData[shardID][op.Key]
				if ok {
					// DPrintf("[Group %v Server %v Shard %v] Get, key %v, value %v", kv.gid, kv.me, shardID, op.Key, value)
					ret_msg.err = OK
					ret_msg.ret = value
				} else {
					ret_msg.err = ErrNoKey
					ret_msg.ret = ""
				}
			} else if op.Type == SENDSHARD {
				// make rpc and move data
				kv.mu.Unlock()
				func() {
					args := ShardMigrationArgs{}
					args.ShardID = shardID
					args.From = op.From
					args.To = op.To
					args.ConfigID = op.ConfigNum
					args.ShardData = make(map[string]string)
					args.OpIdxMap = make(map[int64]int)
					kv.mu.Lock()
					for k, v := range kv.ShardData[shardID] {
						args.ShardData[k] = v
					}
					for k, v := range kv.OpIndexmap[shardID] {
						args.OpIdxMap[k] = v
					}
					DPrintf("[Group %v Server %v] Send Shard %v to group %v config %v", kv.gid, kv.me, shardID, args.To, args.ConfigID)
					kv.mu.Unlock()
					for {
						if servers, ok := op.Groups[op.To]; ok {
							for si := range servers {
								DPrintf("[Group %v Server %v] Send Shard %v to group %v server %v", kv.gid, kv.me, shardID, args.To, si)
								srv := kv.make_end(servers[si])
								var reply ShardMigrationReply
								ok := srv.Call("ShardKV.ShardMigration", &args, &reply)
								if ok && reply.Err == OK {
									kv.mu.Lock()
									DPrintf("[Group %v Server %v] Send Shard %v to group %v done", kv.gid, kv.me, shardID, args.To)
									delete(kv.ShardData, shardID)
									kv.mu.Unlock()
									return
								}
							}
						} else {
							fmt.Printf("[Fatal Error] migration to gid %v, but can't find in groups\n", args.To)
						}
					}
				}()
				kv.mu.Lock()
				if len(kv.ShardJobs[shardID]) == 0 || !reflect.DeepEqual(msg, kv.ShardJobs[shardID][0]) {
					return
				}
			} else if op.Type == RECVSHARD {
				// waiting for a shard migration
				shard_value, ok := kv.ShardMigrations[shardID][op.ConfigNum]
				for k := range kv.ShardMigrations[shardID] {
					if k < op.ConfigNum {
						delete(kv.ShardMigrations[shardID], k)
					}
				}
				if ok {
					DPrintf("[Group %v Server %v] Recv Shard %v config %v", kv.gid, kv.me, shardID, op.ConfigNum)
					kv.ShardData[shardID] = make(map[string]string)
					for k, v := range shard_value.ShardData {
						kv.ShardData[shardID][k] = v
					}
					for k, v := range shard_value.OpIdxMap {
						kv.OpIndexmap[shardID][k] = v
					}
					kv.ShardConfigNum[shardID] = op.ConfigNum
					delete(kv.ShardMigrations[shardID], op.ConfigNum)
				} else {
					if op.From == 0 {
						DPrintf("[Group %v Server %v] Create Shard %v", kv.gid, kv.me, shardID)
						kv.ShardData[shardID] = make(map[string]string)
						kv.ShardConfigNum[shardID] = op.ConfigNum
					} else {
						return
					}
				}
			}
			if op.Type == GET || op.Type == PUT || op.Type == APPEND {
				ch_list, ok := kv.pendingChannel[msg.CommandIndex]
				if ok {
					for i := range ch_list {
						// DPrintf("[Group %v Server %v] upload recv, opidx: %v", kv.gid, kv.me, op.OpIndex)
						ch_list[i] <- ret_msg
					}
					delete(kv.pendingChannel, msg.CommandIndex)
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
			DPrintf("[Group %v Server %v] Recv type %v, %v", kv.gid, kv.me, op.Type, op)
			ret_msg := PendingListen{}
			ret_msg.msg = msg
			if op.Type == PUT || op.Type == APPEND || op.Type == GET {
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					shard := key2shard(op.Key)
					gid := kv.LastConfig.Shards[shard]
					if gid != kv.gid {
						// DPrintf("[Group %v Server %v] wrong group, key %v.", kv.gid, kv.me, op.Key)
						ret_msg.ret = ErrWrongGroup
						ch_list, ok := kv.pendingChannel[msg.CommandIndex]
						if ok {
							for i := range ch_list {
								ch_list[i] <- ret_msg
							}
							delete(kv.pendingChannel, msg.CommandIndex)
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
						DPrintf("[Group %v Server %v] Try to install a old config, old num %v, now num %v", kv.gid, kv.me, op.ConfigNum, kv.LastConfig.Num)
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
							out_op.ConfigNum = op.ConfigNum
							out_op.Type = SENDSHARD
							out_op.From = kv.LastConfig.Shards[i]
							out_op.To = op.ConfigShards[i]
							out_op.Groups = make(map[int][]string)
							for k, v := range op.Groups {
								out_op.Groups[k] = v
							}
							out_msg.Command = out_op
							kv.ShardJobs[i] = append(kv.ShardJobs[i], out_msg)
						} else if kv.LastConfig.Shards[i] != kv.gid && op.ConfigShards[i] == kv.gid {
							// recv in
							in_msg := raft.ApplyMsg{}
							in_msg.CommandIndex = msg.CommandIndex
							in_op := Op{}
							in_op.ConfigNum = op.ConfigNum
							in_op.Type = RECVSHARD
							in_op.From = kv.LastConfig.Shards[i]
							in_op.To = op.ConfigShards[i]
							in_msg.Command = in_op
							kv.ShardJobs[i] = append(kv.ShardJobs[i], in_msg)
						}
					}
					copy(kv.LastConfig.Shards[:], op.ConfigShards)
					ch_list, ok := kv.pendingChannel[msg.CommandIndex]
					if ok {
						for i := range ch_list {
							ch_list[i] <- ret_msg
						}
						delete(kv.pendingChannel, msg.CommandIndex)
					}
				}()
			} else if op.Type == SHARDMIGRATION {
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					ret_msg.err = OK
					if kv.ShardConfigNum[op.ShardID] < op.ConfigNum {
						_, ok := kv.ShardMigrations[op.ShardID][op.ConfigNum]
						if ok {
							DPrintf("[Group %v Server %v] Recv old migration op for shard %v config %v", kv.gid, kv.me, op.ShardID, op.ConfigNum)
						} else {
							DPrintf("[Attention] [Group %v Server %v] make shard migration data for shard %v config %v", kv.gid, kv.me, op.ShardID, op.ConfigNum)
							_, ok1 := kv.ShardMigrations[op.ShardID]
							if !ok1 {
								kv.ShardMigrations[op.ShardID] = make(map[int]MigrationData)
							}
							kv.ShardMigrations[op.ShardID][op.ConfigNum] = MigrationData{ShardData: make(map[string]string), OpIdxMap: make(map[int64]int)}
							for k, v := range op.Shard.ShardData {
								kv.ShardMigrations[op.ShardID][op.ConfigNum].ShardData[k] = v
							}
							for k, v := range op.Shard.OpIdxMap {
								kv.ShardMigrations[op.ShardID][op.ConfigNum].OpIdxMap[k] = v
							}
						}
					}
					ch_list, ok := kv.pendingChannel[msg.CommandIndex]
					if ok {
						for i := range ch_list {
							ch_list[i] <- ret_msg
						}
						delete(kv.pendingChannel, msg.CommandIndex)
					}
				}()
			}
			kv.mu.Lock()
			kv.LastApplyIndex = msg.CommandIndex
			if kv.maxraftstate != -1 {
				nowsize := kv.rf.GetStateSize()
				if kv.maxraftstate-nowsize <= 100 {
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.LastApplyIndex)
					e.Encode(kv.LastTerm)
					e.Encode(kv.ShardData)
					e.Encode(kv.OpIndexmap)
					e.Encode(kv.ShardJobs)
					e.Encode(kv.LastConfig)
					e.Encode(kv.ShardMigrations)
					e.Encode(kv.ShardConfigNum)
					// fmt.Printf("group %v server %v config %v migrations %v\n", kv.gid, kv.me, kv.ShardConfigNum, kv.ShardMigrations)
					data := w.Bytes()
					kv.rf.Snapshot(kv.LastApplyIndex, data)
				}
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			snapshot := kv.decodeSnapshot(msg.Snapshot)
			DPrintf("[Group %v Server %v] Install Snapshot", kv.gid, kv.me)
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				if kv.LastApplyIndex < snapshot.LastApplyIndex {
					kv.LastApplyIndex = snapshot.LastApplyIndex
					kv.LastTerm = snapshot.LastTerm
					kv.ShardData = snapshot.ShardData
					kv.OpIndexmap = snapshot.OpIndex
					kv.ShardJobs = snapshot.ShardJobs
					kv.LastConfig = snapshot.LastConfig
					kv.ShardMigrations = snapshot.ShardMigrations
					kv.ShardConfigNum = snapshot.ShardConfigNum
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
	kv.OpIndexmap = make(map[int]map[int64]int)
	kv.pendingChannel = make(map[int][]chan PendingListen)
	kv.LastConfig.Groups = make(map[int][]string)
	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.ShardJobs = make(map[int][]raft.ApplyMsg)
	kv.ShardMigrations = make(map[int]map[int]MigrationData)
	kv.ShardConfigNum = make(map[int]int)
	for i := range kv.LastConfig.Shards {
		kv.OpIndexmap[i] = make(map[int64]int)
	}

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
