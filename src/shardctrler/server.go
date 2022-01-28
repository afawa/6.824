package shardctrler

import (
	"container/heap"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Item struct {
	Gid   int
	Count int
	Index int
}

type MinHeap []*Item

func (h MinHeap) Len() int { return len(h) }
func (h MinHeap) Less(i, j int) bool {
	if h[i].Count != h[j].Count {
		return h[i].Count < h[j].Count
	}
	return h[i].Gid < h[j].Gid
}
func (h MinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].Index = i
	h[j].Index = j
}
func (h *MinHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*Item)
	item.Index = n
	*h = append(*h, item)
}
func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	item.Index = -1
	*h = old[0 : n-1]
	return item
}
func (h *MinHeap) update(item *Item, gid int, count int) {
	item.Count = count
	item.Gid = gid
	heap.Fix(h, item.Index)
}

type MaxHeap []*Item

func (h MaxHeap) Len() int { return len(h) }
func (h MaxHeap) Less(i, j int) bool {
	if h[i].Count != h[j].Count {
		return h[i].Count > h[j].Count
	}
	return h[i].Gid < h[j].Gid
}
func (h MaxHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].Index = i
	h[j].Index = j
}
func (h *MaxHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*Item)
	item.Index = n
	*h = append(*h, item)
}
func (h *MaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	item.Index = -1
	*h = old[0 : n-1]
	return item
}
func (h *MaxHeap) update(item *Item, gid int, count int) {
	item.Count = count
	item.Gid = gid
	heap.Fix(h, item.Index)
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.
	LastApplyIndex int
	LastTerm       int
	OpIndexmap     map[int64]int
	pendingChannel map[int][]chan raft.ApplyMsg

	configs []Config // indexed by config num
}

type OpType string

const (
	JOIN    OpType = "Join"
	LEAVE   OpType = "Leave"
	MOVE    OpType = "Move"
	QUERY   OpType = "Query"
	INVALID OpType = "Invalid"
)

type Op struct {
	// Your data here.
	Type OpType
	Args interface{}
}

func (sc *ShardCtrler) termChecker() {
	for !sc.killed() {
		time.Sleep(10 * time.Millisecond)
		term, _ := sc.rf.GetState()
		sc.mu.Lock()
		if term > sc.LastTerm {
			msg := raft.ApplyMsg{}
			var op Op
			op.Type = INVALID
			msg.Command = op
			for _, v := range sc.pendingChannel {
				for i := range v {
					v[i] <- msg
				}
			}
			sc.LastTerm = term
			sc.pendingChannel = make(map[int][]chan raft.ApplyMsg)
		}
		sc.mu.Unlock()
	}
}

func contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func (sc *ShardCtrler) receiver() {
	for !sc.killed() {
		msg := <-sc.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			if msg.CommandIndex-1 != sc.LastApplyIndex {
				fmt.Printf("[Fatal Error] raft apply msg out of order, last index %v, this index %v\n", sc.LastApplyIndex, msg.CommandIndex)
			}
			if op.Type == JOIN {
				func() {
					sc.mu.Lock()
					defer sc.mu.Unlock()
					args := op.Args.(JoinArgs)
					opidx, ok := sc.OpIndexmap[args.ClerkID]
					if ok && opidx >= args.OperationIndex {
						return
					}
					config := Config{}
					config.Num = sc.configs[len(sc.configs)-1].Num + 1
					config.Groups = make(map[int][]string)
					for k, v := range sc.configs[len(sc.configs)-1].Groups {
						config.Groups[k] = v
					}
					for k, v := range args.Servers {
						config.Groups[k] = v
					}
					if len(sc.configs[len(sc.configs)-1].Groups) == 0 {
						new_gids := make([]int, 0)
						for k := range args.Servers {
							new_gids = append(new_gids, k)
						}
						sort.Ints(new_gids)
						i := 0
						for idx := range sc.configs[len(sc.configs)-1].Shards {
							config.Shards[idx] = new_gids[i]
							i = (i + 1) % len(new_gids)
						}
					} else {
						for i, v := range sc.configs[len(sc.configs)-1].Shards {
							config.Shards[i] = v
						}
						tmp := NShards / len(config.Groups)
						tmpMap := make(map[int][]int)
						for i, v := range config.Shards {
							tmpMap[v] = append(tmpMap[v], i)
						}
						hmin := make(MinHeap, len(config.Groups))
						hmax := make(MaxHeap, len(config.Groups))
						i := 0
						for gid := range config.Groups {
							hmin[i] = &Item{Gid: gid, Count: len(tmpMap[gid]), Index: i}
							hmax[i] = &Item{Gid: gid, Count: len(tmpMap[gid]), Index: i}
							i++
						}
						heap.Init(&hmin)
						heap.Init(&hmax)
						for {
							item_max := heap.Pop(&hmax).(*Item)
							item_min := heap.Pop(&hmin).(*Item)
							if (item_max.Count == tmp || item_max.Count == tmp+1) && item_min.Count == tmp {
								break
							}
							pos := tmpMap[item_max.Gid][0]
							config.Shards[pos] = item_min.Gid
							tmpMap[item_max.Gid] = tmpMap[item_max.Gid][1:]
							item_max.Count -= 1
							item_min.Count += 1
							heap.Push(&hmax, item_max)
							heap.Push(&hmin, item_min)
						}
					}
					sc.configs = append(sc.configs, config)
					sc.OpIndexmap[args.ClerkID] = args.OperationIndex
				}()
			} else if op.Type == LEAVE {
				func() {
					sc.mu.Lock()
					defer sc.mu.Unlock()
					args := op.Args.(LeaveArgs)
					opidx, ok := sc.OpIndexmap[args.ClerkID]
					if ok && opidx >= args.OperationIndex {
						return
					}
					config := Config{}
					config.Num = sc.configs[len(sc.configs)-1].Num + 1
					config.Groups = make(map[int][]string)
					for k, v := range sc.configs[len(sc.configs)-1].Groups {
						config.Groups[k] = v
					}
					for _, gid := range args.GIDs {
						delete(config.Groups, gid)
					}
					if len(config.Groups) == 0 {
						for i := range sc.configs[len(sc.configs)-1].Shards {
							config.Shards[i] = 0
						}
					} else {
						for i, v := range sc.configs[len(sc.configs)-1].Shards {
							config.Shards[i] = v
						}
						tmpMap := make(map[int]int)
						for _, v := range config.Shards {
							if !contains(args.GIDs, v) {
								tmpMap[v] += 1
							}
						}
						hmin := make(MinHeap, len(config.Groups))
						i := 0
						for gid := range config.Groups {
							hmin[i] = &Item{Gid: gid, Count: tmpMap[gid], Index: i}
							i++
						}
						heap.Init(&hmin)
						for _, gid := range args.GIDs {
							for i, v := range config.Shards {
								if v == gid {
									item := heap.Pop(&hmin).(*Item)
									config.Shards[i] = item.Gid
									item.Count += 1
									heap.Push(&hmin, item)
								}
							}
						}
					}
					sc.configs = append(sc.configs, config)
					sc.OpIndexmap[args.ClerkID] = args.OperationIndex
				}()
			} else if op.Type == MOVE {
				func() {
					sc.mu.Lock()
					defer sc.mu.Unlock()
					args := op.Args.(MoveArgs)
					opidx, ok := sc.OpIndexmap[args.ClerkID]
					if ok && opidx >= args.OperationIndex {
						return
					}
					config := Config{}
					config.Num = sc.configs[len(sc.configs)-1].Num + 1
					config.Groups = make(map[int][]string)
					for k, v := range sc.configs[len(sc.configs)-1].Groups {
						config.Groups[k] = v
					}
					for i, v := range sc.configs[len(sc.configs)-1].Shards {
						config.Shards[i] = v
					}
					config.Shards[args.Shard] = args.GID
					sc.configs = append(sc.configs, config)
					sc.OpIndexmap[args.ClerkID] = args.OperationIndex
				}()
			}
			sc.mu.Lock()
			sc.LastApplyIndex = msg.CommandIndex
			ch_list, ok := sc.pendingChannel[msg.CommandIndex]
			if ok {
				for i := range ch_list {
					ch_list[i] <- msg
				}
				delete(sc.pendingChannel, msg.CommandIndex)
			}
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	command := Op{}
	command.Type = JOIN
	command.Args = *args

	sc.mu.Lock()
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.Err = OK
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	ch := make(chan raft.ApplyMsg)
	sc.pendingChannel[index] = append(sc.pendingChannel[index], ch)
	sc.mu.Unlock()

	msg := <-ch
	op := msg.Command.(Op)
	reply.Err = OK
	if reflect.DeepEqual(command, op) {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	command := Op{}
	command.Type = LEAVE
	command.Args = *args

	sc.mu.Lock()
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.Err = OK
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	ch := make(chan raft.ApplyMsg)
	sc.pendingChannel[index] = append(sc.pendingChannel[index], ch)
	sc.mu.Unlock()

	msg := <-ch
	op := msg.Command.(Op)
	reply.Err = OK
	if reflect.DeepEqual(op, command) {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	command := Op{}
	command.Type = MOVE
	command.Args = *args

	sc.mu.Lock()
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.Err = OK
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	ch := make(chan raft.ApplyMsg)
	sc.pendingChannel[index] = append(sc.pendingChannel[index], ch)
	sc.mu.Unlock()

	msg := <-ch
	op := msg.Command.(Op)
	reply.Err = OK
	if reflect.DeepEqual(op, command) {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	command := Op{}
	command.Type = QUERY
	command.Args = *args

	sc.mu.Lock()
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.Err = OK
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	ch := make(chan raft.ApplyMsg)
	sc.pendingChannel[index] = append(sc.pendingChannel[index], ch)
	sc.mu.Unlock()

	msg := <-ch
	op := msg.Command.(Op)
	reply.Err = OK
	if reflect.DeepEqual(op, command) {
		reply.WrongLeader = false
		// process query
		sc.mu.Lock()
		if args.Num == -1 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(QueryArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.LastApplyIndex = 0
	sc.OpIndexmap = make(map[int64]int)
	sc.pendingChannel = make(map[int][]chan raft.ApplyMsg)
	sc.LastTerm = 0

	go sc.receiver()
	go sc.termChecker()

	return sc
}
