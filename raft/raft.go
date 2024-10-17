// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"go.etcd.io/etcd/raft"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	electionTimeoutRandomized int

	isHardStateChanged bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic("InitialState failed with " + err.Error())
	}

	fi, err := c.Storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	// weird logic here.
	prs := make(map[uint64]*Progress, len(c.peers))
	for _, peer := range c.peers {
		prs[peer] = &Progress{Match: fi - 1}
		// prs[peer] = &Progress{Next: 1}
		// if fi > prs[peer].Next {
		// 	prs[peer].Next = fi
		// }
	}

	if len(prs) == 0 {
		for _, peer := range confState.Nodes {
			prs[peer] = &Progress{Match: fi - 1}
			// prs[n] = &Progress{Next: 1}
			// if fi > prs[n].Next {
			// 	prs[n].Next = fi
			// }
		}
	}

	raftLog := newLog(c.Storage)
	raftLog.committed = hardState.Commit
	raftLog.applied = c.Applied

	if raftLog.latestSnapIndex > 0 {
		raftLog.applied = max(raftLog.applied, raftLog.latestSnapIndex)
	}

	// for len(raftLog.entries) <= int(raftLog.stabled) {
	for raftLog.LastIndex()+1 <= raftLog.stabled {
		panic("weird logic triggered")
		// raftLog.entries = append(raftLog.entries, pb.Entry{Data: []byte("dummy entry")})
	}

	li, _ := c.Storage.LastIndex()

	fmt.Printf(
		"+++++[id=%d] new raft node created with peers:%+v, hardState:%v, confState:%v, first index:%d, last index:%d, l.latestEntryIndex:%d, raftLog.entries: %d\n",
		c.ID, prs, hardState, confState, fi, li, raftLog.latestSnapIndex, len(raftLog.entries),
	)
	for i, e := range raftLog.entries {
		fmt.Printf("+++++init entries[%d] %+v\n", i, e)
	}
	// Your Code Here (2A).

	electionTimeoutRandomized := generateRandomizedElectionTimeout(c.ElectionTick)
	r := &Raft{
		id:   c.ID,
		Term: hardState.Term,
		Lead: 0,
		Vote: hardState.Vote,

		// the log
		RaftLog: raftLog,

		// log replication progress of each peers
		Prs: prs,

		// this peer's role
		State: StateFollower,

		// // votes records
		// votes map[uint64]bool

		// msgs need to send
		msgs: nil, // []pb.Message{},

		// // the leader id
		// Lead uint64

		// heartbeat interval, should send
		heartbeatTimeout: c.HeartbeatTick,
		// baseline of election interval
		electionTimeout: c.ElectionTick,

		electionElapsed:           electionTimeoutRandomized,
		electionTimeoutRandomized: electionTimeoutRandomized,

		// applied: c.Applied,
	}
	return r
}

func (r *Raft) broadcastAppend() error {
	// no need to broadcast when there's only one node.
	if len(r.Prs) == 1 {
		r.maybeAdvanceCommit()
		return nil
	}

	for peerID := range r.Prs {
		if peerID == r.id {
			continue
		}
		r.sendAppend(peerID)
	}
	return nil
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	if r.Prs[to].Next <= r.RaftLog.latestSnapIndex {
		// send a snapshot that contains all the applied data.
		snap, err := r.RaftLog.storage.Snapshot()
		if err != nil {
			if errors.Is(err, ErrSnapshotTemporarilyUnavailable) {
				fmt.Printf("+++++[id=%d][term=%d] generating snap for id=%d (next=%d,match=%d) latestSnapIndex=%d, not available yet\n",
					r.id, r.Term, to, r.Prs[to].Next, r.Prs[to].Match, r.RaftLog.latestSnapIndex)
				return false
			} else {
				// fmt.Printf("Error type: %T, value: %+v, addr: %p\n", err, err, err)
				// fmt.Printf("Error type: %T, value: %+v, addr: %p\n", raft.ErrSnapshotTemporarilyUnavailable, raft.ErrSnapshotTemporarilyUnavailable, raft.ErrSnapshotTemporarilyUnavailable)
				panic(fmt.Sprintf("err: %+v, %v", err, err == raft.ErrSnapshotTemporarilyUnavailable))
			}
		}

		// nodes := []uint64{}
		// for n := range r.Prs {
		// 	nodes = append(nodes, n)
		// }

		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgSnapshot,
			To:      to,
			From:    r.id,
			Term:    r.Term,

			LogTerm: snap.Metadata.Term,
			Index:   snap.Metadata.Index,
			// Entries: entries,
			// Commit: r.RaftLog.committed,
			// Snapshot: &pb.Snapshot{
			// 	Metadata: &pb.SnapshotMetadata{
			// 		ConfState: &pb.ConfState{Nodes: nodes},
			// 		Index:     snapIdx,
			// 		Term:      snapTerm,
			// 	},
			// },
			Snapshot: &snap,
		})

		fmt.Printf("+++++[id=%d][term=%d] sending snap %v to %d (next=%d,match=%d) latestSnapIndex=%d\n",
			r.id, r.Term, snap, to, r.Prs[to].Next, r.Prs[to].Match, r.RaftLog.latestSnapIndex)
		return true
	}

	entries := []*pb.Entry{}
	if r.Prs[to].Match == 0 {
		// send everything. required by TestLeaderCommitPrecedingEntries2AB

		// for i := 1; int(i) < len(r.RaftLog.entries); i++ {
		// 	entries = append(entries, &r.RaftLog.entries[i])
		// }
		for _, entry := range r.RaftLog.allEntries() {
			entry := entry
			entries = append(entries, &entry)
		}
	} else if r.Prs[to].Next == r.Prs[to].Match+1 {
		// Bulk send
		for i := r.Prs[to].Next; i <= r.RaftLog.LastIndex(); i++ {
			entry := r.RaftLog.entry(i)
			entries = append(entries, &entry)
		}
	} else {
		// Probing mode
		entry := r.RaftLog.entry(r.Prs[to].Next)
		entries = append(entries, &entry)
	}

	if len(entries) == 0 {
		// fmt.Printf("+++++[id=%d][term=%d] just trying to send the latest commit index %d to %d:\n", r.id, r.Term, r.RaftLog.committed, to)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			To:      to,
			From:    r.id,
			Term:    r.Term,
			LogTerm: r.Term,
			Index:   r.RaftLog.LastIndex(),
			Entries: nil,
			Commit:  r.RaftLog.committed,
		})

		return false
	}

	// dedup?
	logIndex := entries[0].Index - 1
	logTerm := r.RaftLog.mustTerm(logIndex)

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,

		LogTerm: logTerm,
		Index:   logIndex,

		Entries: entries,
		Commit:  r.RaftLog.committed,
	})

	var toPrint string
	toPrint += fmt.Sprintf(
		"+++++[id=%d][term=%d] MsgAppend->r.msgs, (prev idx=%d,term=%d) "+
			"entries[%d,%d] ==> id=%d (next=%d,match=%d), my last_index=%d\n",
		r.id, r.Term, logIndex, logTerm,
		entries[0].Index, entries[len(entries)-1].Index, to, r.Prs[to].Next, r.Prs[to].Match, r.RaftLog.LastIndex(),
	)

	// for i, entry := range entries {
	// 	toPrint += fmt.Sprintf("+++++[id=%d][term=%d] \tentries[%d], index: %d, data len: %d\n", r.id, r.Term, i, entry.Index, len(entry.Data))
	// }
	fmt.Print(toPrint)

	return true
}

func (r *Raft) broadcastHeartbeat() {
	var toPrint string
	for peerID := range r.Prs {
		if peerID == r.id {
			continue
		}
		r.sendHeartbeat(peerID)
		toPrint += fmt.Sprintf(
			"+++++[id=%d][term=%d] pushing heartbeat to outbound messages for id=%d\n",
			r.id, r.Term, peerID,
		)
	}
	// fmt.Print(toPrint)
	// reduce the rate of heartbeat
	r.heartbeatElapsed = 0
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed, // This doesn't seem to be very useful though.
	})
}

func (r *Raft) broadcastVoteRequest() {
	logTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
	if err != nil {
		panic("cannot get latest log term")
	}

	for peerID := range r.Prs {
		if peerID == r.id {
			continue
		}

		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      peerID,
			From:    r.id,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			LogTerm: logTerm,
		})
	}

	// fmt.Printf("+++++[id=%d][term=%d] len(r.Prs): %d, # of messages to broadcast: %d\n", r.id, r.Term, len(r.Prs), len(r.msgs))
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).

	// Heartbeat sending logic. Only the leader needs this
	if r.State == StateLeader {
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed == r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// broadcast heartbeat messages to every peer except oneself
			// fmt.Printf("+++++[id=%d][term=%d]broadcasting heartbeat due to tick\n", r.id, r.Term)
			r.broadcastHeartbeat()
		}

		r.electionElapsed -= 1
		if r.electionElapsed == 0 {
			r.electionElapsed = r.electionTimeoutRandomized
			// Cancel any ongoing transfer ongoing
			r.leadTransferee = None
		}
	}

	if r.State == StateFollower || r.State == StateCandidate {
		r.electionElapsed -= 1
		if r.electionElapsed == 0 {
			// fmt.Printf("+++++[id=%d][term=%d] r.electionElapsed: %d\n", r.id, r.Term, r.electionElapsed)
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	fmt.Printf("+++++[id=%d][term=%d] become follower, lead %d\n", r.id, term, lead)
	// Your Code Here (2A).
	r.Lead = lead
	r.Term = term

	// Clear your vote
	r.Vote = None
	r.leadTransferee = None

	r.State = StateFollower
	r.electionElapsed = generateRandomizedElectionTimeout(r.electionTimeout)
	r.electionTimeoutRandomized = r.electionElapsed
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Move away from term 0 if this function is called. ? werid.
	// if r.Term == 0 {
	// 	r.Term = 1
	// }
	r.Term += 1

	// Your Code Here (2A).
	r.State = StateCandidate
	r.electionElapsed = generateRandomizedElectionTimeout(r.electionTimeout)
	r.electionTimeoutRandomized = r.electionElapsed

	fmt.Printf("+++++[id=%d] become candidate at term %d, rand timeout %d\n", r.id, r.Term, r.electionElapsed)
	// r.startNewElection()
}

func (r *Raft) startNewElection() {
	fmt.Printf("+++++[id=%d][term=%d] startNewElection\n", r.id, r.Term)
	// r.Term += 1

	// Vote for itself.
	r.Vote = r.id
	r.votes = map[uint64]bool{r.id: true}

	// Single-node scenario
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	// Request votes from others
	r.broadcastVoteRequest()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	// r.Term += 1
	fmt.Printf("+++++[id=%d] BECOME LEADER at term %d, proposing noop entry\n", r.id, r.Term)

	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	r.electionElapsed = generateRandomizedElectionTimeout(r.electionTimeout)
	r.electionTimeoutRandomized = r.electionElapsed

	// Update next index optimistically
	for _, pr := range r.Prs {
		// TODO: consider last index + 1, because of the noop entry.
		pr.Next = max(r.RaftLog.LastIndex()+1, 1)
		// if pr.Next == pr.Match {
		// 	// pr.Match is set to last entry during init. If they match, nothing needs to be sent.
		// 	pr.Next = pr.Match + 1
		// }
	}

	// Propose no-op entry.
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Entries: []*pb.Entry{{Data: nil}},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	if m.To != 0 && m.To != r.id {
		log.Errorf("msg id=%d->id=%d, but arrived at id=%d, %v", m.From, m.To, r.id, m)
		panic("bug bug bug")
	}

	// Your Code Here (2A).
	incomingTerm := m.Term
	if incomingTerm > r.Term {
		fmt.Printf("+++++[id=%d][term=%d] incoming term=%d from id=%d is higher\n", r.id, r.Term, m.Term, m.From)
		r.isHardStateChanged = true
		r.becomeFollower(m.Term, None)
	} else if incomingTerm < r.Term {
		if m.MsgType == pb.MessageType_MsgHup ||
			m.MsgType == pb.MessageType_MsgBeat ||
			m.MsgType == pb.MessageType_MsgPropose ||
			m.MsgType == pb.MessageType_MsgTransferLeader {
			// let local messages pass
		} else {
			// ignore messages with smaller terms
			fmt.Printf("+++++[id=%d][term=%d] ingore message %v from id=%d term=%d\n", r.id, r.Term, m.MsgType, m.From, m.Term)
			return nil
		}
	}

	if m.MsgType == pb.MessageType_MsgPropose && r.State != StateLeader {
		return errors.New("not leader")
	}

	// Handle vote requests
	if m.MsgType == pb.MessageType_MsgRequestVote {
		// fmt.Printf(
		// 	"+++++[id=%d][term=%d] received vote request from %d at term %d, m.Index: %d, m.LogTerm:%d, my li:%d, my li term:%d, r.Vote: %d\n",
		// 	r.id, r.Term, m.From, m.Term,
		// 	m.Index, m.LogTerm,
		// 	r.RaftLog.LastIndex(), r.RaftLog.mustTerm(r.RaftLog.LastIndex()),
		// 	r.Vote,
		// )

		prevVote := r.Vote
		if r.Vote == None && isUpToDate(m.Index, m.LogTerm, r.RaftLog.LastIndex(), r.RaftLog.mustTerm(r.RaftLog.LastIndex())) {
			r.Vote = m.From
			r.isHardStateChanged = true
		}

		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  r.Vote != m.From,
		})

		if r.Vote != m.From {
			if r.Vote == None {
				fmt.Printf(
					"+++++[id=%d][term=%d] received vote request from %d at term %d, REJECTED because not up-to-date!\n",
					r.id, r.Term, m.From, m.Term,
				)
			} else {
				fmt.Printf(
					"+++++[id=%d][term=%d] received vote request from %d at term %d, REJECTED! my vote: %d\n",
					r.id, r.Term, m.From, m.Term, prevVote,
				)
			}
		} else {
			fmt.Printf(
				"+++++[id=%d][term=%d] received vote request from %d at term %d, GRANTED\n",
				r.id, r.Term, m.From, m.Term,
			)
		}
		return nil
	}

	if m.MsgType == pb.MessageType_MsgHup && (r.State == StateFollower || r.State == StateCandidate) {
		fmt.Printf("+++++[id=%d][term=%d] received MsgHup\n", r.id, r.Term)
		r.becomeCandidate()
		r.startNewElection()
		return nil
	}

	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func isUpToDate(index, term, myIndex, myTerm uint64) bool {
	if term > myTerm {
		return true
	}

	if term == myTerm {
		return index >= myIndex
	}
	return false
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		fmt.Printf("+++++[id=%d][term=%d] receive vote from %d: accept=%v\n", r.id, r.Term, m.From, !m.Reject)
		if !m.Reject {
			r.votes[m.From] = true
		} else {
			r.votes[m.From] = false
		}

		if result, ready := electionOutCome(r.votes, r.Prs); ready {
			if result {
				r.becomeLeader()
			} else {
				fmt.Printf("+++++[id=%d][term=%d] losing the election, r.votes: %v\n", r.id, r.Term, r.votes)
				r.becomeFollower(r.Term, None)
			}
		}
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	}
	return nil
}

func electionOutCome(votes map[uint64]bool, prs map[uint64]*Progress) (bool, bool) {
	yes := 0
	no := 0
	for k := range votes {
		if votes[k] {
			yes += 1
		} else {
			no += 1
		}
	}

	if yes >= len(prs)/2+1 {
		// won the election
		return true, true
	}

	if no >= len(prs)/2+1 {
		return false, true
	}

	return false, false
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgTimeoutNow:
		fmt.Printf("+++++[id=%d][term=%d] (leader transfer) leader sent me %v, electing for leader\n", r.id, r.Term, m.MsgType)
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	case pb.MessageType_MsgTransferLeader:
		// Forward to leader
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgTransferLeader,
			To:      r.Lead,
			From:    r.id,
			Term:    r.Term,
		})
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	var toPrint string
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		fmt.Print("+++++broadcasting heartbeat due to beat msg\n")
		r.broadcastHeartbeat()
	case pb.MessageType_MsgTransferLeader:
		if r.leadTransferee != None {
			if r.leadTransferee == m.From {
				fmt.Printf(
					"+++++[id=%d][term=%d] leader transfer to id=%d is in progress, it's not ready yet!\n",
					r.id, r.Term, m.From,
				)
			} else {
				fmt.Printf(
					"+++++[id=%d][term=%d] leader transfer to another node (id=%d) is in progress, ignore leader transfer to %d\n",
					r.id, r.Term, r.leadTransferee, m.From,
				)
			}
			return nil
		}

		fmt.Printf(
			"+++++[id=%d][term=%d] (leader transfer) marking (id=%d) as the target\n",
			r.id, r.Term, m.From,
		)
		// Mark this node as the target
		r.leadTransferee = m.From
		r.electionElapsed = r.electionTimeoutRandomized
		if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
			// Make sure the follower is up-to-date before sending MsgTimeoutNow
			fmt.Printf(
				"+++++[id=%d][term=%d] transferring leader to id=%d, sending MsgTimeoutNow\n",
				r.id, r.Term, m.From,
			)
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgTimeoutNow,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
			})
		} else {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgPropose:
		if r.leadTransferee != None {
			errStr := fmt.Sprintf("rejecting propose because leader transfer to %d is in progress", r.leadTransferee)
			return errors.New(errStr)
		}

		// Make sure entry term is set.
		for _, e := range m.Entries {
			if e.Term == 0 {
				e.Term = r.Term
			}
		}

		r.RaftLog.appendEntries(m)

		// Update leader's progress
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

		toPrint += fmt.Sprintf(
			"+++++[id=%d][term=%d] leader receives propose:  <===============================\n",
			r.id, r.Term)
		for i, e := range m.Entries {
			entryStr := fmt.Sprintf("%+v", e)
			if e.Data == nil {
				entryStr = "(noop entry after winning compaign)"
			}
			toPrint += fmt.Sprintf(
				"+++++[id=%d][term=%d] \tentries[%d] -> %s\n",
				r.id, r.Term, int(r.RaftLog.LastIndex())-len(m.Entries)+i+1, entryStr)
		}

		// toPrint += fmt.Sprintf(
		// 	"+++++[id=%d][term=%d] post-propose latest entries, last index = %d:\n",
		// 	r.id, r.Term, )
		// for i, e := range r.RaftLog.entries {
		// 	var entryStr string
		// 	if e.Data == nil {
		// 		entryStr = "(noop entry)"
		// 	} else {
		// 		entryStr = string(e.Data)
		// 		if entryStr == "dummy entry" {
		// 			continue
		// 		} else {
		// 			entryStr = fmt.Sprintf("(data len %d)", len(entryStr))
		// 		}
		// 	}
		// 	// entryStr += fmt.Sprintf(" %v", e)

		// 	toPrint += fmt.Sprintf(
		// 		"+++++[id=%d][term=%d] \tlatest_entries[%d]: %s\n",
		// 		r.id, r.Term, i, entryStr)
		// }
		fmt.Print(toPrint)

		r.broadcastAppend()
	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			// prevNext := r.Prs[m.From].Next
			r.Prs[m.From].Next = m.Index
			if r.Prs[m.From].Next <= r.Prs[m.From].Match {
				// It's possible for Match to go backward.
				// Match doesn't mean it will never change.

				// reset match to 1 to enable probing.
				// TestPersistPartition2B found this bug.
				r.Prs[m.From].Match = 1
				// panic(
				// 	fmt.Sprintf(
				// 		"[id=%d][term=%d] unexpected append rejection. prevNext: %d, asking for: %d, m.From: %d, r.Prs[m.From].Match:%d",
				// 		r.id, r.Term, prevNext, m.Index, m.From, r.Prs[m.From].Match,
				// 	),
				// )
			}
			r.sendAppend(m.From)
		} else {
			toPrint += fmt.Sprintf("+++++[id=%d][term=%d] receive append response from %d, index=%d\n", r.id, r.Term, m.From, m.Index)
			fmt.Print(toPrint)
			// Update the progress of the follower
			if r.Prs[m.From].Match < m.Index {
				r.Prs[m.From].Match = m.Index
			}
			if r.Prs[m.From].Next < r.Prs[m.From].Match+1 {
				r.Prs[m.From].Next = r.Prs[m.From].Match + 1
			}
			r.maybeAdvanceCommit()
			if m.Index < r.RaftLog.LastIndex() {
				r.sendAppend(m.From)
			} else {
				if m.From == r.leadTransferee {
					// Make sure the follower is up-to-date before sending MsgTimeoutNow
					fmt.Printf(
						"+++++[id=%d][term=%d] (leader transfer) tranfer target id=%d has just caught up, sending MsgTimeoutNow\n",
						r.id, r.Term, m.From,
					)
					r.msgs = append(r.msgs, pb.Message{
						MsgType: pb.MessageType_MsgTimeoutNow,
						To:      m.From,
						From:    r.id,
						Term:    r.Term,
					})
				}
			}
		}
	case pb.MessageType_MsgHeartbeatResponse:
		// We should rely on append. <-- No, sometimes append messages can be lost

		// Update next if possible. Is this necessary?
		match := m.Commit
		if term, err := r.RaftLog.Term(m.Index); err != nil && term == m.Term {
			match = m.Index
		}
		if r.Prs[m.From].Match < match {
			r.Prs[m.From].Match = match
		}
		if r.Prs[m.From].Next < r.Prs[m.From].Match+1 {
			r.Prs[m.From].Next = r.Prs[m.From].Match + 1
		}

		// One test requires heartbeat to trigger append.
		if match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}

	return nil
}

func (r *Raft) maybeAdvanceCommit() {
	if len(r.Prs) == 0 {
		return
	}

	indexes := []uint64{}
	prss := []string{}
	for id, pr := range r.Prs {
		var idx uint64
		if id == r.id {
			idx = r.RaftLog.LastIndex()
		} else {
			idx = pr.Match
		}
		indexes = append(indexes, idx)
		prss = append(prss, fmt.Sprintf("%v=>%v", id, idx))
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] > indexes[j] }) // descending
	sort.Strings(prss)

	var toPrint string
	toPrint += fmt.Sprintf(
		"+++++[id=%d][term=%d] on leader, r.Prs:%+v, my commit:%v\n",
		r.id, r.Term, prss, r.RaftLog.committed)
	fmt.Print(toPrint)
	majorityPos := len(indexes) / 2
	canCommit := uint64(indexes[majorityPos])

	// A leader can only commit the entry that belongs to its term.
	if canCommit > r.RaftLog.committed && r.RaftLog.mustTerm(canCommit) == r.Term {
		prevCommit := r.RaftLog.committed
		r.RaftLog.committed = canCommit

		toPrint += fmt.Sprintf(
			"+++++[id=%d][term=%d] commit %d -> %d\n",
			r.id, r.Term, prevCommit, canCommit)

		// // Check if there's any special entry to handle. No, wrong place.
		// if r.State == StateLeader {
		// 	for i := prevCommit; i <= r.RaftLog.committed; i++ {
		// 		entry := r.RaftLog.entry(i)
		// 		if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		// 			// do it now

		// 		}
		// 	}
		// }

		// broadcast commit message to other peers
		if len(r.Prs) > 1 {
			r.broadcastAppend()
			// toPrint += fmt.Sprintf(
			// 	"+++++[id=%d][term=%d] broadcasting heartbeat to update commit to %d\n",
			// 	r.id, r.Term, r.RaftLog.committed)

			// Can't use broadcastHeartbeat because of TestLeaderCommitEntry2AB
			// r.broadcastHeartbeat()
		}
		fmt.Print(toPrint)
	} else {
		fmt.Print(toPrint)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.Lead = m.From
	var toPrint string
	// toPrint += fmt.Sprintf(
	// 	"+++++[id=%d][term=%d] follower receives append: m.Term=%d, m.Index=%d, m.LogTerm=%d, m.Commit=%d, entries: len %d\n",
	// 	r.id, r.Term, m.Term, m.Index, m.LogTerm, m.Commit, len(m.Entries))

	// Term match check

	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		var entriesStr string
		if len(m.Entries) == 0 {
			entriesStr = "0 entry"
		} else {
			entriesStr = fmt.Sprintf(
				"(idx=%d,term=%d)~(idx=%d,term=%d)",
				m.Entries[0].Index, m.Entries[0].Term,
				m.Entries[len(m.Entries)-1].Index, m.Entries[len(m.Entries)-1].Term,
			)
		}

		// reject
		fmt.Printf(
			"+++++[id=%d][term=%d] follower reject, m.Index=%d, m.LogTerm=%d, entries=%s, my log term=%v, my latest snap idx=%d, my li=%d, err=%v\n",
			r.id, r.Term, m.Index, m.LogTerm, entriesStr, term, r.RaftLog.latestSnapIndex, r.RaftLog.LastIndex(), err)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Index:   min(m.Index, r.RaftLog.LastIndex()+1), // Ask for a smaller index, the preceding entry
			Reject:  true,
		})
		return
	}

	// append
	r.RaftLog.appendEntries(m)

	toPrint += fmt.Sprintf(
		"+++++[id=%d][term=%d] post-append latest entries, last index=%d:\n",
		r.id, r.Term, r.RaftLog.LastIndex())
	// for i, e := range r.RaftLog.entries {
	// 	var entryStr string
	// 	if e.Data == nil {
	// 		entryStr = "(noop entry)"
	// 	} else {
	// 		entryStr = string(e.Data)
	// 		if entryStr == "dummy entry" {
	// 			continue
	// 		} else {
	// 			entryStr = fmt.Sprintf("(data len %d)", len(entryStr))
	// 		}
	// 	}
	// 	// entryStr += fmt.Sprintf(" %v", e)

	// 	toPrint += fmt.Sprintf(
	// 		"+++++[id=%d][term=%d] \tlatest_entries[%d]: %s\n",
	// 		r.id, r.Term, i, entryStr)
	// }

	latestEntryIndex := m.Index
	for _, e := range m.Entries {
		latestEntryIndex = max(latestEntryIndex, e.Index)
	}
	canCommit := min(min(m.Commit, r.RaftLog.LastIndex()), latestEntryIndex)
	if r.RaftLog.committed < canCommit {
		toPrint += fmt.Sprintf(
			"+++++[id=%d][term=%d] follower commit %d -> %d\n",
			r.id, r.Term, r.RaftLog.committed, canCommit)

		r.RaftLog.committed = canCommit
	}

	// fmt.Print(toPrint)

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		// Index:   r.RaftLog.LastIndex(),
		Index: latestEntryIndex,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).

	// Reset electioin elapsed.
	r.electionElapsed = r.electionTimeoutRandomized

	// The following is WRONG. You have to ensure the entries match.
	// // r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	r.Lead = m.From
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,

		Commit:  r.RaftLog.committed,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: r.RaftLog.mustTerm(r.RaftLog.LastIndex()),
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	snapIndex := m.Snapshot.Metadata.Index
	snapTerm := m.Snapshot.Metadata.Term
	if snapIndex <= r.RaftLog.committed || snapTerm < r.Term {
		fmt.Printf("+++++[id=%d][term=%d] ignore stale snapshot, snap index:%d, snap term=%d, my committed:%d\n",
			r.id, r.Term, snapIndex, snapTerm, r.RaftLog.committed)
		return
	}

	fmt.Printf("+++++[id=%d][term=%d] restored snapshot, snap index:%d, snap term=%d, prev committed:%d, prev applied:%d, prev stabled:%d\n",
		r.id, r.Term, snapIndex, snapTerm,
		r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.stabled,
	)
	r.Term = snapTerm
	r.Lead = m.From
	r.isHardStateChanged = true

	r.RaftLog.latestSnapIndex = snapIndex
	r.RaftLog.committed = snapIndex
	r.RaftLog.applied = snapIndex
	r.RaftLog.stabled = snapIndex

	r.RaftLog.entries = []pb.Entry{
		{Index: snapIndex, Term: snapTerm, Data: []byte("oooooo")}, // dummy entry
	}
	r.RaftLog.pendingSnapshot = m.Snapshot

	confState := m.Snapshot.Metadata.ConfState
	newPrs := make(map[uint64]*Progress)
	for _, n := range confState.Nodes {
		newPrs[n] = &Progress{Next: snapIndex}
	}
	r.Prs = newPrs

	// Send a message to the leader to indicate the snapshot has been applied?
	// actually, maybe this isn't strictly necessasry, in the next heartbeat the leader
	// will know.
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		// Index:   r.RaftLog.LastIndex(),
		Index: snapIndex,
	})
}

func (r *Raft) CompactLog(idx uint64) {
	log.Warnf(
		"[id=%d][term=%d] calling CompactLog, compact idx:%d, latestSnapIdx: %d, last idx: %d, committed:%d, applied:%v",
		r.id, r.Term, idx, r.RaftLog.latestSnapIndex, r.RaftLog.LastIndex(), r.RaftLog.committed, r.RaftLog.applied)
	r.RaftLog.maybeCompact(idx)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).

	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{
			// Match:
			Next: r.RaftLog.LastIndex(),
		}
	}
	// What's the catch?
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).

	// What's the catch? The quorum has changed, so there might be some entries
	// that can be committed now.

	log.Warnf("[id=%d] removing node id=%d", r.id, id)
	delete(r.Prs, id)
	delete(r.votes, id)
	r.maybeAdvanceCommit()
}

func generateRandomizedElectionTimeout(baseline int) int {
	return baseline + rand.Intn(baseline)
}
