package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	states         []bool
	lastTry        int
	me             int64
	processedIndex int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.states = make([]bool, len(servers))
	for i := range ck.servers {
		ck.states[i] = false
	}
	ck.lastTry = 0
	ck.me = nrand()
	ck.processedIndex = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{}
	reply := GetReply{}
	args.Key = key
	args.ClerkID = ck.me
	ck.processedIndex += 1
	args.OperationIndex = ck.processedIndex
	target := ck.SendTo()
	for {
		DPrintf("Clerk %v, op GET, key %v, opidx %v, target %v, start", ck.me, key, ck.processedIndex, target)
		ok := ck.servers[target].Call("KVServer.Get", &args, &reply)
		if !ok {
			continue
		}
		if reply.Err == OK {
			ck.UpdateState(target)
			DPrintf("Clerk %v, opidx %v, done, value %v", ck.me, ck.processedIndex, reply.Value)
			return reply.Value
		} else if reply.Err == ErrNoKey {
			ck.UpdateState(target)
			break
		} else {
			for i := range ck.states {
				ck.states[i] = false
			}
			target = ck.SendTo()
		}
	}
	DPrintf("Clerk %v, opidx %v, done, NoKey", ck.me, ck.processedIndex)
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	reply := PutAppendReply{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClerkID = ck.me
	ck.processedIndex += 1
	args.OperationIndex = ck.processedIndex
	target := ck.SendTo()
	for {
		DPrintf("Clerk %v, op %v, key %v, value %v, opidx %v, target %v, start", ck.me, op, key, value, ck.processedIndex, target)
		ok := ck.servers[target].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			continue
		}
		if reply.Err == OK {
			ck.UpdateState(target)
			DPrintf("Clerk %v, opidx %v, done", ck.me, ck.processedIndex)
			return
		} else if reply.Err == ErrWrongLeader {
			for i := range ck.states {
				ck.states[i] = false
			}
			target = ck.SendTo()
		} else {
			fmt.Println("[Fatal Error] Put Append RPC recv unknown ret")
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) SendTo() int {
	for i := range ck.states {
		if ck.states[i] {
			return i
		}
	}
	ck.lastTry = (ck.lastTry + 1) % len(ck.servers)
	return ck.lastTry
}

func (ck *Clerk) UpdateState(id int) {
	for i := range ck.states {
		if i == id {
			ck.states[i] = true
		} else {
			ck.states[i] = false
		}
	}
}
