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

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	latestSnapIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic("storage.FirstIndex() " + err.Error())
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic("storage.LastIndex() " + err.Error())
	}
	entries := []pb.Entry{{Index: 0, Data: []byte("oooooo")}}

	var latestSnapIndex uint64
	// padding before first index
	if firstIndex > 0 {
		latestSnapIndex = firstIndex - 1
		entries[0].Index = latestSnapIndex
		// for i := 1; uint64(i) < firstIndex; i++ {
		// 	entries = append(entries, pb.Entry{Index: uint64(i), Data: []byte("oooooo")}) // dummy entry at position 0.
		// }
	}

	if firstIndex < lastIndex+1 {
		ee, err := storage.Entries(firstIndex, lastIndex+1)
		if err != nil {
			panic("storage.Entries() " + err.Error())
		}
		entries = append(entries, ee...)
	}

	// if len(entries) > 0 && int(entries[len(entries)-1].Index) != len(entries)-1 {
	// 	panic(
	// 		fmt.Sprintf(
	// 			"unexpected init, fi:%d, li:%d, len(entries):%d, entries[len(entries)-1].Index: %d",
	// 			firstIndex, lastIndex, len(entries), entries[len(entries)-1].Index,
	// 		),
	// 	)
	// }

	// if lastIndex != uint64(len(entries)-1) {
	if lastIndex != entries[len(entries)-1].Index {
		panic(
			fmt.Sprintf(
				"unexpected init, fi:%d, li:%d, len(entries):%d, entries[len(entries)-1].Index: %d",
				firstIndex, lastIndex, len(entries), entries[len(entries)-1].Index,
			),
		)
	}

	// hardState, confState, err := storage.InitialState()
	// if err != nil {
	// 	panic("storage.InitialState() " + err.Error())
	// }

	// fmt.Printf("+++++newLog hardState: %+v, confState: %+v, entries: %+v\n",
	// 	hardState, confState, entries)

	stabled := latestSnapIndex + uint64(len(entries)-1)
	if stabled != lastIndex {
		panic(
			fmt.Sprintf("weird logic triggered, fi: %d, li: %d, stabled: %d, entries: %v",
				firstIndex, lastIndex, stabled, entries),
		)
		// stabled = lastIndex
	}
	return &RaftLog{
		storage:         storage,
		entries:         entries,
		stabled:         stabled,
		latestSnapIndex: latestSnapIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries[1:]
}

// term uint64, entries []*pb.Entry

func (l *RaftLog) appendEntries(m pb.Message) {
	// if len(l.entries) != int(entries[0].Index) {
	// 	panic(fmt.Sprintf("len(l.entries) != entries[0].Index, %d != %d", len(l.entries), int(entries[0].Index)))
	// }
	// lastIndex, err := l.storage.LastIndex()
	// if err != nil {
	// 	fmt.Printf("+++++ warning, error getting l.storage.LastIndex()")
	// }

	// append empty entry on leader but not on a follower.
	if len(m.Entries) == 0 && m.MsgType == pb.MessageType_MsgPropose {
		// This is for proposing a no-op entry?

		// if m.Index+1 == uint64(len(l.entries)) {
		if m.Index == l.LastIndex() {
			newEntry := pb.Entry{
				EntryType: pb.EntryType_EntryNormal,
				Term:      m.Term,
				Index:     l.LastIndex() + 1,
				Data:      nil,
			}
			l.entries = append(l.entries, newEntry)

			if l.entries[len(l.entries)-1].Index != l.latestSnapIndex+uint64(len(l.entries))-1 {
				panic(fmt.Sprintf("%+v", l.entries))
			}
		}
		return
	}

	var toPrint string
	for _, e := range m.Entries {
		// Make sure related fields are set
		index := e.Index
		if index == 0 {
			// index = uint64(len(l.entries))
			index = l.LastIndex() + 1
		}

		newEntry := pb.Entry{
			EntryType: e.EntryType,
			Term:      e.Term,
			Index:     index, // e.Index, // uint64(len(l.entries)),
			Data:      e.Data,
		}

		if e.Index != 0 && e.Index <= l.LastIndex() {
			// Consider duplicate entries.
			if e.Term != l.entry(e.Index).Term {
				toPrint += fmt.Sprintf(
					"+++++overwriting existing entry, e.Index=%d, l.latestSnapIndex=%d, len(l.entries)=%d, old:%v, new:%v\n",
					e.Index, l.latestSnapIndex, len(l.entries), l.entry(e.Index), newEntry)

				// Override
				l.entries[e.Index-l.latestSnapIndex] = newEntry
				// Truncate
				l.entries = l.entries[:e.Index-l.latestSnapIndex+1]
				if l.stabled >= e.Index {
					l.stabled = e.Index - 1
				}
			} else {
				// Need to truncate as well
				// hmmm, TestFollowerAppendEntries2AB does not expect this to be truncated.
				// What fails if I don't do it? => nothing seems to =>
				// => found it: the leader sends a smaller index, the follower has extra entries and assume everything matches.
				// l.entries = l.entries[:e.Index+1]
				// if l.stabled >= e.Index {
				// 	l.stabled = e.Index
				// }
			}
		} else {
			l.entries = append(l.entries, newEntry)
		}
	}
	if len(toPrint) > 0 {
		fmt.Print(toPrint)
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	res := []pb.Entry{}
	for i := l.stabled + 1; i <= l.LastIndex(); i++ {
		res = append(res, l.entry(i))
	}
	return res
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	res := []pb.Entry{}
	for i := l.applied + 1; i <= l.committed; i++ {
		res = append(res, l.entry(i))
	}
	return res
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries)-1 >= 0 {
		return l.latestSnapIndex + uint64(len(l.entries)-1)
	} else {
		panic("there should always be some entries")
	}
}

func (l *RaftLog) entry(i uint64) pb.Entry {
	// i == l.latestSnapIndex is not ok, you can get the term but not the entry.
	if i <= l.latestSnapIndex {
		panic(fmt.Sprintf("asking entry for idx=%d while latestSnapIndex=%d", i, l.latestSnapIndex))
	}

	if i-l.latestSnapIndex >= uint64(len(l.entries)) {
		panic(
			fmt.Sprintf("asking entry for idx=%d while latestSnapIndex=%d and len(l.entries)=%d",
				i, l.latestSnapIndex, len(l.entries)),
		)
	}

	return l.entries[i-l.latestSnapIndex]
}

func (l *RaftLog) mustTerm(i uint64) uint64 {
	term, err := l.Term(i)
	if err != nil {
		panic(err.Error())
	}
	return term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i < l.latestSnapIndex {
		panic(fmt.Sprintf("asking term for idx=%d while latestSnapIndex=%d", i, l.latestSnapIndex))
	}

	if i-l.latestSnapIndex >= uint64(len(l.entries)) {
		return 0, errors.New("no entry")
	}

	return l.entries[i-l.latestSnapIndex].Term, nil
}
