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
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry
	state       int
	leaderId    int

	// Timer
	elcSignalChan   chan bool
	hbSignalChan    chan bool
	lastRestElcTime int64
	lastRestHbTime  int64
	elcTimeout      int64
	hbTimeout       int64
}

// State
const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

// Log entry
type LogEntry struct {
	term int // recorded the term in which it was created
}

// Election timer (used by Follower and Candidate)
func (rf *Raft) elcTimer() {
	// use goroutine to keep running
	for {
		rf.mu.RLock()
		// whenever find the cur state is not leader
		if rf.state != LEADER {
			elapse := time.Now().UnixMilli() - rf.lastRestElcTime
			if elapse > rf.elcTimeout { // notify the server to initialize election
				DPrintf("[elcTimer] | raft %d election timeout %d | current term: %d | current state: %d\n",
					rf.me, rf.elcTimeout, rf.currentTerm, rf.state)
				rf.elcSignalChan <- true
			}
		}
		rf.mu.RUnlock()
		time.Sleep(time.Millisecond * 10)
	}
}

// Election timer reset
func (rf *Raft) elcTimerReset() {
	rf.lastRestElcTime = time.Now().UnixMilli()
	// create new random timeout after reset
	//rand.Seed(time.Now().UnixNano())
	rf.elcTimeout = rf.hbTimeout*3 + rand.Int63n(150)
}

// Heartbeat timer (used by Leader)
func (rf *Raft) hbTimer() {
	// use goroutine to keep running
	for {
		rf.mu.RLock()
		// whenever find the cur state is leader
		if rf.state == LEADER {
			elapse := time.Now().UnixMilli() - rf.lastRestHbTime
			if elapse > rf.hbTimeout { // notify the server to broadcast heartbeat
				DPrintf("[hbTimer] | leader raft %d  heartbeat timeout | current term: %d | current state: %d\n",
					rf.me, rf.currentTerm, rf.state)
				rf.hbSignalChan <- true
			}
		}
		rf.mu.RUnlock()
		time.Sleep(time.Millisecond * 10)
	}
}

// Heartbeat timer reset
func (rf *Raft) hbTimerReset() {
	rf.lastRestHbTime = time.Now().UnixMilli()
}

// The main loop of the raft server
func (rf *Raft) mainLoop() {
	for !rf.killed() {
		select {
		case <-rf.hbSignalChan:
			rf.broadcastHb()
		case <-rf.elcSignalChan:
			rf.startElc()
		}
	}
}

// candidate start election
func (rf *Raft) startElc() {

	// If rf is FOLLOWER, change it to CANDIDATE
	rf.mu.Lock()

	// convertTo CANDIDATE including reset timeout and make currentTerm+1
	rf.convertTo(CANDIDATE)
	voteCnt := 1

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(id int) {
			rf.mu.RLock()
			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			rf.mu.RUnlock()

			var reply RequestVoteReply
			if rf.sendRequestVote(id, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != CANDIDATE {
					// check state whether changed during broadcasting
					DPrintf("[startElc| changed state] raft %d state changed | current term: %d | current state: %d\n",
						rf.me, rf.currentTerm, rf.state)
					return
				}

				if reply.VoteGranted == true {
					voteCnt++
					DPrintf("[startElc | reply true] raft %d get accept vote from %d | current term: %d | current state: %d | reply term: %d | voteCnt: %d\n",
						rf.me, id, rf.currentTerm, rf.state, reply.Term, voteCnt)

					if voteCnt > len(rf.peers)/2 && rf.state == CANDIDATE {
						rf.convertTo(LEADER)
						DPrintf("[startElc | become leader] raft %d convert to leader | current term: %d | current state: %d\n",
							rf.me, rf.currentTerm, rf.state)
						rf.hbSignalChan <- true //broadcast heartbeat immediately
					}
				} else {
					DPrintf("[startElc | reply false] raft %d get reject vote from %d | current term: %d | current state: %d | reply term: %d | VoteCnt: %d\n",
						rf.me, id, rf.currentTerm, rf.state, reply.Term, voteCnt)

					if rf.currentTerm < reply.Term {
						rf.convertTo(FOLLOWER)
						rf.currentTerm = reply.Term
					}
				}

			} else { // no reply
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// no reply
				DPrintf("[startElc | no reply] raft %d RPC to %d failed | current term: %d | current state: %d | reply term: %d\n",
					rf.me, id, rf.currentTerm, rf.state, reply.Term)
			}
		}(i)
	}

}

// leader broadcast heartbeat
func (rf *Raft) broadcastHb() {

	// check leadership
	rf.mu.Lock()
	if rf.state != LEADER {
		DPrintf("[broadcastHb | not leader] raft %d lost leadership | current term: %d | current state: %d\n",
			rf.me, rf.currentTerm, rf.state)
		return
	}
	rf.hbTimerReset()
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(id int) {
			rf.mu.RLock()
			args := AppendEntriesArgs{
				Term: rf.currentTerm,
			}
			rf.mu.RUnlock()

			var reply AppendEntriesReply
			if rf.sendAppendEntries(id, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// one of goroutines change the server state
				if rf.state != LEADER {
					// check state whether changed during broadcasting
					DPrintf("[broadcastHb| changed state] raft %d lost leadership | current term: %d | current state: %d\n",
						rf.me, rf.currentTerm, rf.state)
					return
				}

				if reply.Success {
					DPrintf("[broadcastHb | reply true] raft %d heartbeat to %d accepted | current term: %d | current state: %d\n",
						rf.me, id, rf.currentTerm, rf.state)
				} else {
					DPrintf("[broadcastHb | reply false] raft %d heartbeat to %d rejected | current term: %d | current state: %d | reply term: %d\n",
						rf.me, id, rf.currentTerm, rf.state, reply.Term)
					// case 1: lower term, step down
					if rf.currentTerm < reply.Term {
						rf.convertTo(FOLLOWER)
						rf.currentTerm = reply.Term
						return
					}
				}
			} else {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// no reply
				DPrintf("[broadcastHb | no reply] raft %d RPC to %d failed | current term: %d | current state: %d | reply term: %d\n",
					rf.me, id, rf.currentTerm, rf.state, reply.Term)
			}
		}(i)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	// Your code here (2A).
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
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

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// high term detected, turn to FOLLOWER and refresh the voteFor target
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertTo(FOLLOWER)
	}

	// do not grant vote for smaller term || already voted for another one
	if args.Term < rf.currentTerm || (rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		DPrintf("[RequestVote] raft %d reject vote for %d | current term: %d | current state: %d | recieved term: %d | voteFor: %d\n",
			rf.me, args.CandidateId, rf.currentTerm, rf.state, args.Term, rf.votedFor)

		return
	}

	// No vote yet || Voted for it before
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.elcTimerReset()
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		DPrintf("[RequestVote] raft %d accept vote for %d | current term: %d | current state: %d | recieved term: %d\n",
			rf.me, args.CandidateId, rf.currentTerm, rf.state, args.Term)
	}
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

// AppendEntries RPC

type AppendEntriesArgs struct {
	Term int // leader's term (2A)
}

type AppendEntriesReply struct {
	Term    int // currentTerm, for leader to update itself (2A)
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[AppendEntries|small term] raft %d reject append entries | current term: %d | current state: %d | received term: %d\n",
			rf.me, rf.currentTerm, rf.state, args.Term)
		return
	}

	// reset election timer
	rf.elcTimerReset()

	if args.Term > rf.currentTerm || rf.state != FOLLOWER {
		rf.convertTo(FOLLOWER)     //step down
		rf.currentTerm = args.Term //update the term
		DPrintf("[AppendEntries|big term or has leader] raft %d update term or state | current term: %d | current state: %d | recieved term: %d\n",
			rf.me, rf.currentTerm, rf.state, args.Term)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

// State conversion
// before using this function, make sure the writeLock is added beforehand
func (rf *Raft) convertTo(state int) {
	switch state {
	case FOLLOWER:
		rf.elcTimerReset()
		rf.votedFor = -1
		rf.state = FOLLOWER
	case CANDIDATE:
		rf.elcTimerReset()
		rf.state = CANDIDATE
		rf.currentTerm++
		rf.votedFor = rf.me
	case LEADER:
		rf.hbTimerReset()
		rf.state = LEADER
	}
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

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

//var globalInt = 0
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	//globalInt++
	//fmt.Println(globalInt)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.hbSignalChan = make(chan bool, 10)
	rf.elcSignalChan = make(chan bool, 10)
	rf.hbTimeout = 100 // ms
	rf.elcTimerReset()

	DPrintf("Starting raft %d\n", me)
	go rf.mainLoop()
	go rf.elcTimer()
	go rf.hbTimer()

	return rf
}
