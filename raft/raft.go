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

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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

	// applied uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	prs := make(map[uint64]*Progress, len(c.peers))
	for _, peer := range c.peers {
		prs[peer] = &Progress{Next: 1}
	}

	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		panic("InitialState failed with " + err.Error())
	}

	raftLog := newLog(c.Storage)
	raftLog.committed = hardState.Commit
	raftLog.applied = c.Applied
	fmt.Printf("+++++[id=%d] new raft node created\n", c.ID)
	// Your Code Here (2A).
	return &Raft{
		id:   c.ID,
		Term: hardState.Term,
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
		// // number of ticks since it reached last heartbeatTimeout.
		// // only leader keeps heartbeatElapsed.
		// heartbeatElapsed int
		// // Ticks since it reached last electionTimeout when it is leader or candidate.
		// // Number of ticks since it reached last electionTimeout or received a
		// // valid message from current leader when it is a follower.
		// electionElapsed int
		// applied: c.Applied,
	}
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
	entries := []*pb.Entry{}
	for i := r.Prs[to].Next; int(i) < len(r.RaftLog.entries); i++ {
		entries = append(entries, &r.RaftLog.entries[i])
	}

	if len(entries) == 0 {
		fmt.Printf("+++++[id=%d][term=%d] just trying to send the latest commit index %d to %d:\n", r.id, r.Term, r.RaftLog.committed, to)
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

	fmt.Printf("+++++[id=%d][term=%d] sending %d the following entries:\n", r.id, r.Term, to)

	for i, entry := range entries {
		fmt.Printf("+++++[id=%d][term=%d] \tentries[%d] %+v\n", r.id, r.Term, i, entry)
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: r.RaftLog.entries[entries[0].Index-1].Term,
		Index:   entries[0].Index - 1, // This might be -1
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

func (r *Raft) broadcastHeartbeat() {
	for peerID := range r.Prs {
		if peerID == r.id {
			continue
		}
		r.sendHeartbeat(peerID)
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
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
			r.broadcastHeartbeat()
		}
	}

	if r.State == StateFollower || r.State == StateCandidate {
		r.electionElapsed -= 1
		if r.electionElapsed == 0 {
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

	r.State = StateFollower
	r.electionElapsed = generateRandomizedElectionTimeout(r.electionTimeout)
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
	fmt.Printf("+++++[id=%d] become leader at term %d, proposing noop entry\n", r.id, r.Term)

	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.electionElapsed = generateRandomizedElectionTimeout(r.electionTimeout)

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
	// Your Code Here (2A).
	incomingTerm := m.Term
	if incomingTerm > r.Term {
		fmt.Printf("+++++[id=%d][term=%d] incoming term %d from %d is higher\n", r.id, r.Term, m.Term, m.From)
		r.becomeFollower(m.Term, None)
	} else if incomingTerm < r.Term {
		if m.MsgType == pb.MessageType_MsgHup ||
			m.MsgType == pb.MessageType_MsgBeat ||
			m.MsgType == pb.MessageType_MsgPropose {
			// let local messages pass
		} else {
			// ignore messages with smaller terms
			fmt.Printf("+++++[id=%d][term=%d] ingore message %v from %d, term %d is lower\n", r.id, r.Term, m.MsgType, m.From, m.Term)
			return nil
		}
	}

	// Handle vote requests
	if m.MsgType == pb.MessageType_MsgRequestVote {
		fmt.Printf(
			"+++++[id=%d][term=%d] received vote request from %d at term %d, my vote: %d\n",
			r.id, r.Term, m.From, m.Term, r.Vote,
		)

		if r.Vote == None && isUpToDate(m.Index, m.LogTerm, r.RaftLog.LastIndex(), r.RaftLog.entries[r.RaftLog.LastIndex()].Term) {
			r.Vote = m.From
		}

		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  r.Vote != m.From,
		})
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
		fmt.Printf("+++++[id=%d][term=%d] receive vote from %d: reject=%v\n", r.id, r.Term, m.From, m.Reject)
		if !m.Reject {
			r.votes[m.From] = true
		} else {
			r.votes[m.From] = false
		}

		if result, ready := electionOutCome(r.votes, r.Prs); ready {
			if result {
				r.becomeLeader()
			} else {
				fmt.Printf("+++++[id=%d][term=%d] losing the election\n", r.id, r.Term)
				r.becomeFollower(r.Term, None)
			}
		}
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
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
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.broadcastHeartbeat()
	case pb.MessageType_MsgPropose:
		fmt.Printf(
			"+++++[id=%d][term=%d] leader receives propose: idx=%d\n",
			r.id, r.Term, m.Index)
		for i, e := range m.Entries {
			entryStr := fmt.Sprintf("%+v", e)
			if e.Data == nil {
				entryStr = "(nil data)"
			}
			fmt.Printf(
				"+++++[id=%d][term=%d] \tincoming entries[%d]: %s\n",
				r.id, r.Term, i, entryStr)
		}

		// Make sure term is set.
		for _, e := range m.Entries {
			if e.Term == 0 {
				e.Term = r.Term
			}
		}

		r.RaftLog.appendEntries(m)

		// Update leader's progress
		r.Prs[r.id].Match = r.RaftLog.LastIndex()    // uint64(len(r.RaftLog.entries))
		r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1 // uint64(len(r.RaftLog.entries))

		fmt.Printf(
			"+++++[id=%d][term=%d] post-append latest entries:\n",
			r.id, r.Term)
		for i, e := range r.RaftLog.entries {
			var entryStr string
			if e.Data == nil {
				entryStr = "(nil data)"
			} else {
				entryStr = string(e.Data)
			}
			entryStr += fmt.Sprintf(" %v", e)

			fmt.Printf(
				"+++++[id=%d][term=%d] \tlatest_entries[%d]: %s\n",
				r.id, r.Term, i, entryStr)
		}

		r.broadcastAppend()
	case pb.MessageType_MsgAppendResponse:
		fmt.Printf("+++++[id=%d][term=%d] receive append response from %d, index=%d\n", r.id, r.Term, m.From, m.Index)
		// Update the progress of the follower
		if r.Prs[m.From].Match < m.Index {
			r.Prs[m.From].Match = m.Index
		}
		if r.Prs[m.From].Next < r.Prs[m.From].Match+1 {
			r.Prs[m.From].Next = r.Prs[m.From].Match + 1
		}
		r.maybeAdvanceCommit()
	case pb.MessageType_MsgHeartbeatResponse:
		if m.Index < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}
	return nil
}

func (r *Raft) maybeAdvanceCommit() {
	indexes := []uint64{}
	for id, pr := range r.Prs {
		if id == r.id {
			indexes = append(indexes, uint64(len(r.RaftLog.entries)-1))
		} else {
			indexes = append(indexes, pr.Match)
		}
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] > indexes[j] }) // descending
	fmt.Printf(
		"+++++[id=%d][term=%d] commit indexes %v\n",
		r.id, r.Term, indexes)

	majorityPos := len(indexes) / 2
	canCommit := uint64(indexes[majorityPos])
	if canCommit > r.RaftLog.committed && r.RaftLog.entries[canCommit].Term == r.Term {
		fmt.Printf(
			"+++++[id=%d][term=%d] commit %d -> %d\n",
			r.id, r.Term, r.RaftLog.committed, canCommit)

		r.RaftLog.committed = canCommit

		// broadcast commit message to other peers
		if len(r.Prs) > 1 {
			r.broadcastAppend()
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.Lead = m.From
	fmt.Printf(
		"+++++[id=%d][term=%d] follower receives append: m.Term=%d, m.Index=%d, m.LogTerm=%d, m.Commit=%d, entries: len %d, %v\n",
		r.id, r.Term, m.Term, m.Index, m.LogTerm, m.Commit, len(m.Entries), m.Entries)

	// Term match check
	if m.Index != 0 {
		term, err := r.RaftLog.Term(m.Index)
		if err != nil || term != m.LogTerm {
			// reject
			fmt.Printf(
				"+++++[id=%d][term=%d] follower reject because term=%v err=%v\n",
				r.id, r.Term, term, err)
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})
			return
		}
	}

	// append
	r.RaftLog.appendEntries(m)

	fmt.Printf(
		"+++++[id=%d][term=%d] post-append latest entries:\n",
		r.id, r.Term)
	for i, e := range r.RaftLog.entries {
		var entryStr string
		if e.Data == nil {
			entryStr = "(nil data)"
		} else {
			entryStr = string(e.Data)
		}
		entryStr += fmt.Sprintf(" %v", e)

		fmt.Printf(
			"+++++[id=%d][term=%d] \tlatest_entries[%d]: %s\n",
			r.id, r.Term, i, entryStr)
	}

	latestEntryIndex := m.Index
	for _, e := range m.Entries {
		latestEntryIndex = max(latestEntryIndex, e.Index)
	}
	canCommit := min(min(m.Commit, r.RaftLog.LastIndex()), latestEntryIndex)
	if r.RaftLog.committed < canCommit {
		fmt.Printf(
			"+++++[id=%d][term=%d] follower commit %d -> %d\n",
			r.id, r.Term, r.RaftLog.committed, canCommit)

		r.RaftLog.committed = canCommit
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
	})

}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.Lead = m.From
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: r.RaftLog.entries[r.RaftLog.LastIndex()].Term,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func generateRandomizedElectionTimeout(baseline int) int {
	return baseline + rand.Intn(baseline)
}
