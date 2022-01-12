package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type VoteMsg struct {
	id    int
	ok    bool
	reply RequestVoteReply
}

type LeaderMsg struct {
	id         int
	ok         bool
	prevIndex  int
	entriesLen int
	reply      AppendEntriesReply
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

//
// A Go object implementing a single Raft peer.
//
type STATE string

const (
	LEADER    STATE = "leader"
	CANDIDATE STATE = "candidate"
	FOLLOWER  STATE = "follower"
)

type Log struct {
	Command interface{}
	Term    int
	Index   int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh       chan ApplyMsg
	hearHeartBeat bool
	state         STATE
	CurrentTerm   int
	VotedFor      int
	Logs          []Log
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int

	// SnapShot
	LastIncludedIndex int
	LastIncludedTerm  int
	CurrentSnapShot   []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.CurrentSnapShot)
	// DPrintf("[Save State] server %v term %v votefor %v logslen %v commit %v", rf.me, rf.CurrentTerm, rf.VotedFor, len(rf.Logs), rf.commitIndex)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Logs []Log
	var LastIncludedIndex int
	var LastIncludedTerm int
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VotedFor) != nil || d.Decode(&Logs) != nil || d.Decode(&LastIncludedIndex) != nil || d.Decode(&LastIncludedTerm) != nil {
		fmt.Printf("[Read State] error in decode\n")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Logs = Logs
		rf.LastIncludedIndex = LastIncludedIndex
		rf.LastIncludedTerm = LastIncludedTerm
		DPrintf("[Read State] server %v term %v votefor %v logslen %v", rf.me, rf.CurrentTerm, rf.VotedFor, len(rf.Logs))
	}
	rf.CurrentSnapShot = snapshot
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	DPrintf("[CondInstall] server %v lastIncludedIndex %v lastIncludedTerm %v", rf.me, lastIncludedIndex, lastIncludedTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.LastIncludedIndex >= lastIncludedIndex {
		DPrintf("[CondInstall] server %v refuse, curr LastIncludedIndex %v", rf.me, rf.LastIncludedIndex)
		return false
	}
	rf.CurrentSnapShot = snapshot
	if len(rf.Logs)+rf.LastIncludedIndex <= lastIncludedIndex {
		rf.Logs = []Log(nil)
	} else {
		rf.Logs = rf.Logs[lastIncludedIndex-rf.LastIncludedIndex-1:]
	}
	rf.LastIncludedIndex = lastIncludedIndex
	rf.LastIncludedTerm = lastIncludedTerm

	rf.persist()

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	DPrintf("[Install] server %v index %v", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.LastIncludedIndex >= index {
		return
	}
	rf.CurrentSnapShot = snapshot
	rf.LastIncludedIndex = index
	for idx := range rf.Logs {
		if rf.Logs[idx].Index == index {
			rf.LastIncludedTerm = rf.Logs[idx].Term
			rf.Logs = rf.Logs[idx+1:]
			DPrintf("[Install] sucess")
			break
		}
	}
	rf.persist()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludedTerm int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[VoteRequest] %v Receive from %v term %v lastterm %v lastlog %v\n", rf.me, args.CandidateID, args.Term, args.LastLogTerm, args.LastLogIndex)

	var lastterm int
	var lastindex int
	if len(rf.Logs) == 0 {
		lastterm = rf.LastIncludedTerm
		lastindex = rf.LastIncludedIndex
	} else {
		lastterm = rf.Logs[len(rf.Logs)-1].Term
		lastindex = rf.Logs[len(rf.Logs)-1].Index
	}

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	} else if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.state = FOLLOWER
		rf.VotedFor = args.CandidateID
		rf.hearHeartBeat = false
		reply.Term = rf.CurrentTerm
		if lastterm != args.LastLogTerm {
			reply.VoteGranted = args.LastLogTerm > lastterm
		} else {
			reply.VoteGranted = args.LastLogIndex >= lastindex
		}
	} else {
		reply.Term = rf.CurrentTerm
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
			if lastterm != args.LastLogTerm {
				reply.VoteGranted = args.LastLogTerm > lastterm
			} else {
				reply.VoteGranted = args.LastLogIndex >= lastindex
			}
		}
	}
	if reply.VoteGranted {
		rf.hearHeartBeat = true
	}
	rf.persist()
	DPrintf("[VoteRequest] %v reply to %v term %v success %v\n", rf.me, args.CandidateID, reply.Term, reply.VoteGranted)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("[AppendEntries] %v receive from %v term %v previndex %v len %v\n", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, len(args.Entries))
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		reply.Success = false
	} else {
		rf.hearHeartBeat = true
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.state = FOLLOWER
			reply.Term = rf.CurrentTerm
			DPrintf("[AppendEntries] %v change to follower\n", rf.me)
		} else {
			if rf.state == LEADER {
				fmt.Printf("[Fatal Error] In the same term have 2 different leader, %v and %v\n", rf.me, args.LeaderId)
			} else if rf.state == CANDIDATE {
				DPrintf("[AppendEntries] %v change from candidate to follower\n", rf.me)
				rf.state = FOLLOWER
				rf.VotedFor = args.LeaderId
			}
		}
		if rf.LastIncludedIndex > args.PrevLogIndex {
			fmt.Printf("[Fatal Error] log{Index %v Term %v} send by leader %v but is commited in follower %v\n", args.Entries[0].Index, args.Entries[0].Term, args.LeaderId, rf.me)
		}
		// success
		if args.PrevLogIndex == rf.LastIncludedIndex { // Term must be the same
			if args.PrevLogTerm != rf.LastIncludedTerm {
				fmt.Printf("[Fatal Error] Log in Snapshot inconsist with leader, term %v != %v\n", args.PrevLogTerm, rf.LastIncludedTerm)
			}
			reply.Success = true
		} else {
			if len(rf.Logs)+rf.LastIncludedIndex >= args.PrevLogIndex && rf.Logs[args.PrevLogIndex-rf.LastIncludedIndex-1].Term == args.PrevLogTerm {
				reply.Success = true
			} else {
				reply.Success = false
			}
		}
		if reply.Success {
			idx := args.PrevLogIndex - rf.LastIncludedIndex - 1
			for i := range args.Entries {
				idx++
				if idx == len(rf.Logs) {
					rf.Logs = append(rf.Logs, args.Entries[i:]...)
					break
				}
				if rf.Logs[idx].Term != args.Entries[i].Term {
					rf.Logs = rf.Logs[:idx]
					rf.Logs = append(rf.Logs, args.Entries[i:]...)
					break
				}
			}
		}
		if reply.Success {
			if args.LeaderCommit > rf.commitIndex {
				if len(args.Entries) != 0 {
					rf.commitIndex = min(args.LeaderCommit, args.Entries[len(args.Entries)-1].Index)
				} else {
					if args.PrevLogIndex != 0 {
						if args.PrevLogIndex > rf.LastIncludedIndex {
							rf.commitIndex = min(args.LeaderCommit, rf.Logs[args.PrevLogIndex-rf.LastIncludedIndex-1].Index)
						} else {
							rf.commitIndex = min(args.LeaderCommit, rf.LastIncludedIndex)
						}
					}
				}
			}
		} else {
			if len(rf.Logs)+rf.LastIncludedIndex < args.PrevLogIndex {
				reply.ConflictIndex = len(rf.Logs) + rf.LastIncludedIndex
				reply.ConflictTerm = 0
			} else {
				reply.ConflictTerm = rf.Logs[args.PrevLogIndex-rf.LastIncludedIndex-1].Term
				reply.ConflictIndex = 0
				for i := range rf.Logs {
					if rf.Logs[i].Term == reply.ConflictTerm {
						reply.ConflictIndex = rf.Logs[i].Index
						break
					}
				}
			}
		}
	}
	rf.persist()
	DPrintf("[AppendEntries] %v reply to %v term %v success %v\n", rf.me, args.LeaderId, reply.Term, reply.Success)
	rf.mu.Unlock()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("[Install Snapshot] server %v leader %v lastIncludeIndex %v lastIncludeTerm %v", rf.me, args.LeaderId, args.LastIncludeIndex, args.LastIncludedTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	}
	rf.hearHeartBeat = true
	if args.LastIncludeIndex <= rf.LastIncludedIndex {
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.state = FOLLOWER
		reply.Term = rf.CurrentTerm
		DPrintf("[AppendEntries] %v change to follower\n", rf.me)
	} else {
		if rf.state == LEADER {
			fmt.Printf("[Fatal Error] In the same term have 2 different leader, %v and %v\n", rf.me, args.LeaderId)
		} else if rf.state == CANDIDATE {
			DPrintf("[AppendEntries] %v change from candidate to follower\n", rf.me)
			rf.state = FOLLOWER
			rf.VotedFor = args.LeaderId
		}
	}
	msg := ApplyMsg{}
	msg.SnapshotValid = true
	msg.Snapshot = args.Data
	msg.SnapshotIndex = args.LastIncludeIndex
	msg.SnapshotTerm = args.LastIncludedTerm
	go func() {
		rf.applyCh <- msg
	}()
	rf.persist()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.state == LEADER
	if isLeader {
		term = rf.CurrentTerm
		index = rf.LastIncludedIndex + len(rf.Logs) + 1
		log := Log{}
		log.Command = command
		log.Term = term
		log.Index = index
		DPrintf("[Start] server %v append command term %v index %v", rf.me, log.Term, log.Index)
		rf.Logs = append(rf.Logs, log)
		rf.nextIndex[rf.me] = rf.LastIncludedIndex + len(rf.Logs) + 1
		rf.matchIndex[rf.me] = len(rf.Logs) + rf.LastIncludedIndex
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) leaderTask(target int, term int, leaderID int, prevLogIndex int, prevLogTerm int, entries []Log, leaderCommit int, ch chan<- LeaderMsg) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}
	args.Term = term
	args.LeaderId = leaderID
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = prevLogTerm
	args.Entries = entries
	args.LeaderCommit = leaderCommit
	DPrintf("[Leader Task] leader %v send to %v. term %v, prevlogindex %v, prevlogterm %v, entrylen %v, leadercommit %v.", rf.me, target, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
	ok := rf.sendAppendEntries(target, &args, &reply)
	msg := LeaderMsg{}
	msg.id = target
	msg.ok = ok
	msg.prevIndex = prevLogIndex
	msg.entriesLen = len(entries)
	msg.reply = reply
	ch <- msg
}

func (rf *Raft) doSendLog(term int, target int, ch chan LeaderMsg) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term < rf.CurrentTerm {
		return false
	}
	if rf.nextIndex[target] <= rf.LastIncludedIndex {
		return false
	}
	// send start from next index
	prevlogindex := rf.nextIndex[target] - 1
	var prevlogterm int
	if rf.LastIncludedIndex == prevlogindex {
		prevlogterm = rf.LastIncludedTerm
	} else {
		prevlogterm = rf.Logs[prevlogindex-rf.LastIncludedIndex-1].Term
	}
	var logs []Log
	for i := rf.nextIndex[target] - rf.LastIncludedIndex - 1; i < len(rf.Logs); i++ {
		logs = append(logs, rf.Logs[i])
	}
	go rf.leaderTask(target, term, rf.me, prevlogindex, prevlogterm, logs, rf.commitIndex, ch)
	return true
}

func (rf *Raft) leaderSend(term int, target int, ch chan LeaderMsg) {
	if rf.doSendLog(term, target, ch) {
		msg := <-ch
		if !msg.ok {
			return
		}
		if msg.reply.Term < term {
			fmt.Printf("[entriy] server %v find reply term %v less than ch term %v, impossible\n", rf.me, msg.reply.Term, term)
			return
		}
		if msg.reply.Term > term {
			rf.leader2follower(&msg)
			return
		}
		rf.leaderRecvMsg(term, &msg)
	}
}

func (rf *Raft) leaderInstallSnapshot(term int, target int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{}
	reply := InstallSnapshotReply{}
	args.Term = term
	args.LeaderId = rf.me
	args.LastIncludeIndex = rf.LastIncludedIndex
	args.LastIncludedTerm = rf.LastIncludedTerm
	args.Data = rf.CurrentSnapShot
	rf.mu.Unlock()
	ok := rf.sendInstallSnapshot(target, &args, &reply)
	if ok {
		if reply.Term < term {
			fmt.Printf("[entriy] server %v find reply term %v less than ch term %v, impossible\n", rf.me, reply.Term, term)
			return
		}
		if reply.Term > term {
			rf.mu.Lock()
			if rf.CurrentTerm < reply.Term {
				DPrintf("server %v change from leader to follower", rf.me)
				rf.CurrentTerm = reply.Term
				rf.state = FOLLOWER
				rf.VotedFor = -1
				rf.hearHeartBeat = true
				rf.persist()
			}
			rf.mu.Unlock()
			return
		}
		rf.mu.Lock()
		rf.matchIndex[target] = max(rf.matchIndex[target], args.LastIncludeIndex)
		rf.nextIndex[target] = rf.matchIndex[target] + 1
		rf.mu.Unlock()
	}
}

func (rf *Raft) leaderTaskTrigger(term int, target int, ch chan LeaderMsg) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.nextIndex[target] > rf.LastIncludedIndex {
		go rf.leaderSend(term, target, ch)
	} else {
		go rf.leaderInstallSnapshot(term, target)
	}
}

func (rf *Raft) leaderTaskLoop(term int, target int) {
	ch := make(chan LeaderMsg)
	go rf.leaderSend(term, target, ch)
	for !rf.killed() {
		time.Sleep(100 * time.Millisecond)
		if !rf.leaderCheck(term) {
			break
		}
		rf.leaderTaskTrigger(term, target, ch)
	}
}

func (rf *Raft) leaderCheck(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return term == rf.CurrentTerm && rf.state == LEADER
}

func (rf *Raft) leader2follower(msg *LeaderMsg) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm < msg.reply.Term {
		DPrintf("server %v change from leader to follower", rf.me)
		rf.CurrentTerm = msg.reply.Term
		rf.state = FOLLOWER
		rf.VotedFor = -1
		rf.hearHeartBeat = true
		rf.persist()
	}
}

func (rf *Raft) leaderRecvMsg(term int, msg *LeaderMsg) {
	rf.mu.Lock()
	DPrintf("[Recv Msg] server %v receive msg from %v ok %v", rf.me, msg.id, msg.ok)
	if rf.CurrentTerm == term && rf.state == LEADER {
		if !msg.reply.Success {
			if msg.reply.ConflictTerm != 0 {
				conflictTermIdx := 0
				for idx := msg.prevIndex - rf.LastIncludedIndex; idx >= 1; idx-- {
					if rf.Logs[idx-1].Term == msg.reply.ConflictTerm {
						conflictTermIdx = idx + rf.LastIncludedIndex
						break
					}
				}
				if conflictTermIdx != 0 {
					rf.nextIndex[msg.id] = conflictTermIdx + 1
				} else {
					rf.nextIndex[msg.id] = msg.reply.ConflictIndex
				}
			} else {
				rf.nextIndex[msg.id] = msg.reply.ConflictIndex + 1
			}
			DPrintf("[Recv Msg] leader %v target %v nextIndex set to %v", rf.me, msg.id, rf.nextIndex[msg.id])
		} else {
			rf.matchIndex[msg.id] = max(rf.matchIndex[msg.id], msg.prevIndex+msg.entriesLen)
			rf.nextIndex[msg.id] = rf.matchIndex[msg.id] + 1
			rf.matchIndex[rf.me] = len(rf.Logs) + rf.LastIncludedIndex
			tmpMatch := append([]int(nil), rf.matchIndex...)
			// fmt.Println(rf.me, rf.matchIndex)
			sort.Ints(tmpMatch)
			N := tmpMatch[len(rf.peers)-(len(rf.peers)/2)-1]
			if rf.commitIndex < N && N > rf.LastIncludedIndex {
				if rf.Logs[N-rf.LastIncludedIndex-1].Term == rf.CurrentTerm {
					rf.commitIndex = N
				}
			}
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) leaderStart(term int) {
	for i := range rf.peers {
		if i != rf.me {
			go rf.leaderTaskLoop(term, i)
		}
	}
}

func (rf *Raft) candidateTask(target int, term int, lastlogindex int, lastlogterm int, ch chan<- VoteMsg) {
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	args.CandidateID = rf.me
	args.Term = term
	args.LastLogIndex = lastlogindex
	args.LastLogTerm = lastlogterm
	ok := rf.sendRequestVote(target, &args, &reply)
	msg := VoteMsg{}
	msg.ok = ok
	msg.id = target
	msg.reply = reply
	ch <- msg
}

func (rf *Raft) candidate2follower(msg *VoteMsg) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm < msg.reply.Term {
		rf.CurrentTerm = msg.reply.Term
		rf.state = FOLLOWER
		rf.VotedFor = -1
		rf.hearHeartBeat = false
		rf.persist()
	}
}

func (rf *Raft) win(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm == term && rf.state == CANDIDATE {
		rf.state = LEADER
		DPrintf("Server %v become leader\n", rf.me)
		rf.nextIndex = []int(nil)
		rf.matchIndex = []int(nil)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex = append(rf.nextIndex, rf.LastIncludedIndex+len(rf.Logs)+1)
			rf.matchIndex = append(rf.matchIndex, 0)
		}
		// start leader task
		rf.leaderStart(rf.CurrentTerm)
	}
}

func (rf *Raft) candidateWait(term int, ch <-chan VoteMsg) {
	counter := 1
	voteforcounter := 1
	for counter < len(rf.peers) {
		msg := <-ch
		DPrintf("[Recv Vote] server %v target %v ok %v", rf.me, msg.id, msg.ok)
		counter++
		if !msg.ok {
			continue
		}
		if msg.reply.Term < term {
			fmt.Printf("[Vote] server %v find reply term %v less than ch term %v, impossible\n", rf.me, msg.reply.Term, term)
			continue
		}
		if msg.reply.Term > term {
			// find some one have bigger term
			rf.candidate2follower(&msg)
			break
		}
		if msg.reply.VoteGranted {
			voteforcounter++
			if voteforcounter > (len(rf.peers) / 2) {
				// success
				rf.win(term)
				break
			}
		}
	}
}

func (rf *Raft) doElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER && rf.hearHeartBeat {
		rf.hearHeartBeat = false
	} else if rf.state != LEADER && !rf.hearHeartBeat {
		rf.state = CANDIDATE
		rf.CurrentTerm++
		rf.VotedFor = rf.me

		rf.persist()

		DPrintf("Server %v become candidate term %v\n", rf.me, rf.CurrentTerm)

		ch := make(chan VoteMsg, len(rf.peers))
		for i := range rf.peers {
			if i != rf.me {
				var lastlogindex int
				var lastlogterm int
				if len(rf.Logs) == 0 {
					lastlogindex = rf.LastIncludedIndex
					lastlogterm = rf.LastIncludedTerm
				} else {
					lastlogindex = rf.Logs[len(rf.Logs)-1].Index
					lastlogterm = rf.Logs[len(rf.Logs)-1].Term
				}
				go rf.candidateTask(i, rf.CurrentTerm, lastlogindex, lastlogterm, ch)
			}
		}
		go rf.candidateWait(rf.CurrentTerm, ch)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		r := rand.Intn(100) + 300 // [300, 400)
		time.Sleep(time.Duration(r) * time.Millisecond)

		rf.doElection()
	}
}

func (rf *Raft) applyer() {
	last_update_idx := 0
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		now_commit := rf.commitIndex
		last_update_idx = max(last_update_idx, rf.LastIncludedIndex)
		rf.mu.Unlock()
		if now_commit > last_update_idx {
			rf.mu.Lock()
			for i := 0; i < now_commit-last_update_idx; i++ {
				msg := ApplyMsg{}
				msg.CommandValid = true
				msg.Command = rf.Logs[last_update_idx-rf.LastIncludedIndex+i].Command
				msg.CommandIndex = rf.Logs[last_update_idx-rf.LastIncludedIndex+i].Index
				msg.SnapshotValid = false
				DPrintf("[Apply] server %v apply msg. index %v, command %v", rf.me, msg.CommandIndex, msg.Command)
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
			}
			last_update_idx = now_commit
			rf.mu.Unlock()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(int64(me))
	rf.applyCh = applyCh
	rf.state = FOLLOWER
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.hearHeartBeat = false

	rf.LastIncludedIndex = 0
	rf.LastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyer()

	return rf
}
