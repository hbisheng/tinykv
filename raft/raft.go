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

	applied uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	prs := make(map[uint64]*Progress, len(c.peers))
	for _, peer := range c.peers {
		prs[peer] = &Progress{}
	}

	// Your Code Here (2A).
	return &Raft{
		id:   c.ID,
		Term: 0,

		// Vote uint64

		// the log
		RaftLog: newLog(c.Storage),

		// log replication progress of each peers
		Prs: prs,

		// this peer's role
		State: StateFollower,

		// // votes records
		// votes map[uint64]bool

		// msgs need to send
		msgs: []pb.Message{},

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
		applied: c.Applied,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
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
	for peerID := range r.Prs {
		if peerID == r.id {
			continue
		}

		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      peerID,
			From:    r.id,
			Term:    r.Term,
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
	fmt.Printf("+++++[id=%d] become follower at term %d, lead %d\n", r.id, term, lead)
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
	fmt.Printf("+++++[id=%d] become leader at term %d\n", r.id, r.Term)

	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.electionElapsed = generateRandomizedElectionTimeout(r.electionTimeout)
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
		if m.MsgType == pb.MessageType_MsgHup || m.MsgType == pb.MessageType_MsgBeat {
			// let local messages pass
		} else {
			// ignore messages with smaller terms
			fmt.Printf("+++++[id=%d][term=%d] ingore message %v from %d, term %d is lower\n", r.id, r.Term, m.MsgType, m.From, m.Term)
			return nil
		}
	}

	// Handle vote requests
	if m.MsgType == pb.MessageType_MsgRequestVote {
		if r.Vote == None {
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
		r.becomeCandidate()
		r.startNewElection()
		return nil
	}

	switch r.State {
	case StateFollower:
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		fmt.Printf("+++++[id=%d][term=%d] receive vote from %d: reject=%v\n", r.id, r.Term, m.From, m.Reject)
		if !m.Reject {
			r.votes[m.From] = true
			if len(r.votes) >= len(r.Prs)/2+1 {
				// won the election
				r.becomeLeader()
			}
		}
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.broadcastHeartbeat()
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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
