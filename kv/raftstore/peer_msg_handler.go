package raftstore

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) debugRegionLocalState() {
	// Scan region meta to get saved regions.
	startKey := meta.RegionMetaMinKey
	endKey := meta.RegionMetaMaxKey

	kvEngine := d.ctx.engine.Kv

	kvEngine.View(func(txn *badger.Txn) error {
		// get all regions from RegionLocalState
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(startKey); it.Valid(); it.Next() {
			item := it.Item()
			if bytes.Compare(item.Key(), endKey) >= 0 {
				break
			}
			regionID, suffix, err := meta.DecodeRegionMetaKey(item.Key())
			if err != nil {
				return err
			}
			if suffix != meta.RegionStateSuffix {
				continue
			}
			val, err := item.Value()
			if err != nil {
				return errors.WithStack(err)
			}

			localState := new(rspb.RegionLocalState)
			err = localState.Unmarshal(val)
			if err != nil {
				return errors.WithStack(err)
			}

			log.Warnf("[id=%d][store=%d] region in key: %v, region state: %v", d.Meta.GetId(), d.storeID(), regionID, localState)
		}
		return nil
	})
}

func (d *peerMsgHandler) splitFromOldRegion(
	oldRegion *metapb.Region,
	splitRequest *raft_cmdpb.SplitRequest,
) *metapb.Region {
	newRegion := new(metapb.Region)
	newRegion.Id = splitRequest.NewRegionId
	newRegion.StartKey = splitRequest.SplitKey
	newRegion.EndKey = oldRegion.EndKey

	newEpoch := new(metapb.RegionEpoch)
	newEpoch.ConfVer = oldRegion.RegionEpoch.ConfVer
	newEpoch.Version = oldRegion.RegionEpoch.Version + 1 // Increment the epoch version
	newRegion.RegionEpoch = newEpoch

	sort.Slice(oldRegion.Peers, func(i, j int) bool { return oldRegion.Peers[i].Id < oldRegion.Peers[j].Id })
	for i, newId := range splitRequest.NewPeerIds {
		if i >= len(oldRegion.Peers) {
			break
		}

		newPeer := new(metapb.Peer)
		newPeer.Id = newId
		newPeer.StoreId = oldRegion.Peers[i].StoreId
		newRegion.Peers = append(newRegion.Peers, newPeer)
	}
	return newRegion
}

func keyInRange(region *metapb.Region, key []byte) bool {
	if bytes.Compare(key, region.StartKey) < 0 {
		return false
	}

	if engine_util.ExceedEndKey(key, region.EndKey) {
		return false
	}
	return true
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	hasReady := d.peer.RaftGroup.HasReady()
	if !hasReady {
		return
	}

	rd := d.peer.RaftGroup.Ready()

	// Handle stale proposals that will never be answered.

	// // Attempt 1: clear proposals when there are overwriting entries.
	// if len(rd.Entries) > 0 && rd.Entries[0].Index <= d.peer.peerStorage.raftState.LastIndex {
	// 	var toPrint string
	// 	for i := rd.Entries[0].Index; i <= d.peer.peerStorage.raftState.LastIndex; i++ {
	// 		if proposal := d.peer.popProposalByIndex(i); proposal != nil {
	// 			proposal.cb.Done(ErrRespStaleCommand(term))
	// 		}
	// 	}
	// 	fmt.Print(toPrint)
	// }

	// // Attempt 2: clear proposals when term changes
	// if rd.HardState.Term != 0 && rd.HardState.Term != d.peer.peerStorage.raftState.HardState.Term {
	// 	var toPrint string
	// 	newProposals := []*proposal{}
	// 	for _, p := range d.peer.proposals {

	// 	}
	// 	fmt.Print(toPrint)
	// }

	timeBeforeReady := time.Now()
	raftWB := &engine_util.WriteBatch{}
	kvWB := &engine_util.WriteBatch{}
	if rd.Snapshot.Metadata != nil {
		log.Warnf("+++++[id=%d] handling snapshot: %v, rd: %+v", d.PeerId(), rd.Snapshot.Metadata, rd)

		snapIdx := rd.Snapshot.Metadata.Index
		// Make an assumption that snap index will be smaller than the entries and commit entries.
		// Makes it easy to clean up stale states.
		if len(rd.Entries) > 0 && rd.Entries[0].Index < snapIdx {
			panic(fmt.Sprintf("entry idx %d, snap idx %d, ready: %v", rd.Entries[0].Index, snapIdx, rd.Snapshot))
		}

		if len(rd.CommittedEntries) > 0 && rd.CommittedEntries[0].Index < snapIdx {
			panic(fmt.Sprintf("committed entry idx %d, snap idx %d, ready: %v", rd.CommittedEntries[0].Index, snapIdx, rd.Snapshot))
		}

		// First apply the snapshot data, then persist the raft DB change.
		// Failure tolerance: if there's a restart, the raft DB state is old, it
		// will request a snapshot again.
		d.peer.peerStorage.ApplySnapshot(&rd.Snapshot, kvWB, raftWB)

		snapData := new(rspb.RaftSnapshotData)
		if err := snapData.Unmarshal(rd.Snapshot.Data); err != nil {
			panic(err.Error())
		}
		// Update storeMeta of the node itself.
		// This updates region in two places:
		// 1. d.ctx.storeMeta.regions
		// 2. d.peer.peerStorage.region
		func() {
			d.ctx.storeMeta.Lock()
			defer d.ctx.storeMeta.Unlock()

			d.ctx.storeMeta.setRegion(snapData.Region, d.peer)
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: snapData.Region})
		}()
	}

	_, err := d.peer.peerStorage.SaveReadyState(&rd, raftWB)
	if err != nil {
		panic("SaveReadyState returned error: %s" + err.Error())
	}
	timeSpentOnRaft := time.Since(timeBeforeReady)
	timeBeforeApply := time.Now()

	var toPrint string
	postWriteFuncs := []func(){}
	// Apply committed entries
	proposalsToRespond := []*proposal{}
	proposalReponses := []*raft_cmdpb.RaftCmdResponse{}
	cbsForRead := []*message.Callback{}

	// Make sure we have access to all peers, even though one of them may be in
	// the progress of removal.
	allPeers := d.peer.Region().GetPeers()
	var removedFromCluster bool
	if len(rd.CommittedEntries) > 0 {
		for _, e := range rd.CommittedEntries {
			// find the proposal, apply it, and remove, respond to the client.
			proposal := d.peer.popProposalByIndex(e.Index)
			// log.Warnf("+++++ [id=%v] processing committed entry %d, proposal:%v", d.Meta.Id, e.Index, proposal)

			// if proposal != nil {
			// 	toPrint += fmt.Sprintf("+++++[id=%d] committed entry: %v, matching proposal: %v, proposals: %v\n", d.PeerId(), e, proposal, d.peer.proposals)
			// }

			if proposal != nil && proposal.term != e.Term {
				proposal.cb.Done(ErrRespStaleCommand(e.Term))
				toPrint += fmt.Sprintf("[id=%d] found stale command at index %d, prev term %d, new term %d\n", d.PeerId(), e.Index, proposal.term, e.Term)
				proposal = nil
			}

			if e.EntryType == eraftpb.EntryType_EntryConfChange {
				cc := new(eraftpb.ConfChange)
				err := proto.Unmarshal(e.Data, cc)
				if err != nil {
					panic(err.Error())
				}

				if cc.NodeId == raft.None {
					log.Warnf("[id=%d] ignore empty conf change at idx=%d", d.Meta.Id, e.Index)
					continue
				}

				targetPeer := new(metapb.Peer)
				err = proto.Unmarshal(cc.Context, targetPeer)
				if err != nil {
					panic(err.Error())
				}

				peersLen := len(d.peerStorage.region.Peers)
				// Update d.peerStorage.region. It will be persisted later in this function
				switch cc.ChangeType {
				case eraftpb.ConfChangeType_AddNode:
					d.peerStorage.region.Peers = addPeer(d.peerStorage.region.Peers, targetPeer)
				case eraftpb.ConfChangeType_RemoveNode:
					d.peerStorage.region.Peers = removePeer(d.peerStorage.region.Peers, targetPeer)
					if targetPeer.Id == d.Meta.GetId() {
						removedFromCluster = true
					}
				default:
					panic("unexpected config change type")
				}

				if peersLen != len(d.peerStorage.region.Peers) {
					// Only increment ConfVer when the change has an effect.
					d.peerStorage.region.RegionEpoch.ConfVer += 1
				}

				// d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.peerStorage.region})

				postWriteFuncs = append(postWriteFuncs, func() {
					d.peer.RaftGroup.ApplyConfChange(*cc)

					if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode {
						if targetPeer.Id == d.Meta.GetId() {
							log.Errorf("[id=%d] detected self removal, destroying itself", d.Meta.GetId())
							// d.stopped = true
							// What if I don't do this? destroyPeer() already does this
							// destroyPeer() also deletes the region from d.ctx.storeMeta.

							// Need this condition to avoid destroying peer multiple times.
							if !d.stopped {
								d.destroyPeer()
							}

						} else {
							// Update d.ctx.storeMeta?
							// What if I don't?
							// log.Errorf("[id=%d] detected removal of id=%d, peerCache: %v", d.Meta.GetId(), targetPeer.Id, d.peerCache)

							// It's possible that the stale peer sends another message and gets inserted again.
							d.removePeerCache(targetPeer.GetId())
							// log.Errorf("[id=%d] after removal of id=%d, peerCache: %v", d.Meta.GetId(), targetPeer.Id, d.peerCache)
							// if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
							// 	panic(d.Tag + " meta corruption detected")
							// }
							// if _, ok := meta.regions[regionID]; !ok {
							// 	panic(d.Tag + " meta corruption detected")
							// }
							// delete(meta.regions, regionID)
						}
					}
				})

				// finish processing conf change.
				continue
			}

			cmdRequest := new(raft_cmdpb.RaftCmdRequest)
			err := proto.Unmarshal(e.Data, cmdRequest)
			if err != nil {
				panic(err.Error())
			}

			// Process admin request first.
			if cmdRequest.AdminRequest != nil {
				switch cmdRequest.AdminRequest.CmdType {
				case raft_cmdpb.AdminCmdType_CompactLog:
					request := cmdRequest.AdminRequest.CompactLog

					// there's no need to compact if compact idx <= latest snap idx
					if request.CompactIndex > d.peer.RaftGroup.Raft.RaftLog.LatestSnapIndex() && request.CompactIndex > d.scheduledCompactIdx {
						// d.scheduledCompactIdx is used to dedup compaction requests that come in the same batch.
						// the actual compaction (the change of latest snap idx) won't come until the batch completes.
						d.scheduledCompactIdx = request.CompactIndex

						d.peerStorage.applyState.TruncatedState.Index = request.CompactIndex
						d.peerStorage.applyState.TruncatedState.Term = request.CompactTerm
						// Raft apply state will be persisted into Raft KV later.

						log.Warnf("+++++ [id=%v] post write fn: scheduled compact log job (proposal=%d) with compact idx:%d", d.Meta.Id, e.Index, request.CompactIndex)
						postWriteFuncs = append(postWriteFuncs, func() {
							// After the update to applyState.TruncatedState,
							// peerStorage.FirstIndex() will return the latest
							// value. Raft can just look for it.

							// // Update the raft log state and assume everything <= compact index is gone.
							// d.peer.RaftGroup.Raft.CompactLog(request.CompactIndex)

							// schedule the truncate task.
							// check something using LastCompactedIdx?
							// LastCompactedIdx is the first available index after the last compaction
							if request.CompactIndex >= d.LastCompactedIdx {
								d.ScheduleCompactLog(request.CompactIndex)
							}
						})
					}
				case raft_cmdpb.AdminCmdType_Split:
					// TODO: What if there are duplicate split commands?
					// Then there will be multiple splits. This operation is not idempotent.
					splitRequest := cmdRequest.AdminRequest.Split
					err := util.CheckKeyInRegion(splitRequest.SplitKey, d.Region())
					if err != nil || bytes.Equal(splitRequest.SplitKey, d.Region().StartKey) {
						log.Errorf("[store=%d][id=%d][region=%d] found invalid split key %v during apply (region=%v), ignore it",
							d.storeID(), d.PeerId(), d.regionId, string(splitRequest.SplitKey), d.Region())
						continue
					}

					oldRegion := d.peerStorage.region
					newRegion := d.splitFromOldRegion(oldRegion, cmdRequest.AdminRequest.Split)
					// Update old region
					oldRegion.EndKey = cmdRequest.AdminRequest.Split.SplitKey
					oldRegion.RegionEpoch.Version += 1

					newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
					if err != nil {
						// this peer doesn't need to split
						continue
					}
					log.Errorf(
						"[id=%d][region=%d] new peer id=%d, region=%d created during split",
						d.PeerId(), d.regionId, newPeer.PeerId(), newRegion.Id,
					)

					func() {
						d.ctx.storeMeta.Lock()
						defer d.ctx.storeMeta.Unlock()
						d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
						d.ctx.storeMeta.regions[newRegion.Id] = newRegion
					}()

					// register region so it can start receiving message.
					d.ctx.router.register(newPeer)

					// cnt := 0
					// d.ctx.router.peers.Range(func(key, value interface{}) bool {
					// 	if d.storeID() == 1 {
					// 		peerState := value.(*peerState)
					// 		log.Errorf(
					// 			"[store=%d][id=%d][region=%d] (%d) region %v",
					// 			d.storeID(), d.PeerId(), d.regionId, cnt, peerState.peer.Region(),
					// 		)
					// 	}
					// 	cnt += 1
					// 	return true
					// })
					// log.Errorf(
					// 	"[store=%d][id=%d][region=%d] registered new peer %v, total registered:%v",
					// 	d.storeID(), d.PeerId(), d.regionId, newPeer.Meta.Id, cnt,
					// )
					// Send a message to it to activate it?
					d.ctx.router.send(newRegion.Id, message.Msg{RegionID: newRegion.Id, Type: message.MsgTypeStart})
					fromPeer := util.FindPeer(newRegion, d.storeID())
					if d.RaftGroup.Raft.State == raft.StateLeader {
						triggerLeaderMsg := &rspb.RaftMessage{
							RegionId: newRegion.Id,
							FromPeer: fromPeer,
							ToPeer:   fromPeer,
							Message: &eraftpb.Message{
								MsgType: eraftpb.MessageType_MsgTimeoutNow,
								Term:    meta.RaftInitLogTerm,
								From:    newPeer.PeerId(),
								To:      newPeer.PeerId(),
							},
							RegionEpoch: newRegion.RegionEpoch,
						}

						d.ctx.router.send(
							newRegion.Id,
							message.NewPeerMsg(message.MsgTypeRaftMessage, newRegion.Id, triggerLeaderMsg),
						)
					}
					d.sendPendingVote()

					// Respond to the client that the split succeeded? Is there a need?
					if proposal != nil {

					}
				default:
					panic(fmt.Sprintf("unexpected admin request type %v", cmdRequest.AdminRequest))
				}
				continue
			}

			cmdResp := newCmdResp()
			responses := []*raft_cmdpb.Response{}

			err = util.CheckRegionEpoch(cmdRequest, d.Region(), true)
			if err != nil {
				if proposal != nil {
					log.Errorf("[id=%d] CheckRegionEpoch returned %v, idx=%d, cmdRequest:%v", d.PeerId(), err, e.Index, cmdRequest)
					BindRespError(cmdResp, err)
				}
			} else {
				// do the operation and return results.
				for _, r := range cmdRequest.Requests {
					if r.CmdType == raft_cmdpb.CmdType_Put {
						kvWB.SetCF(r.Put.GetCf(), r.Put.GetKey(), r.Put.GetValue())
						// if proposal != nil {
						// toPrint += fmt.Sprintf("+++++[id=%d] writing [cf=%v,key=%v,val=%v]\n",
						// 	d.PeerId(), r.Put.GetCf(), string(r.Put.GetKey()), string(r.Put.GetValue()))
						// }
						responses = append(responses, &raft_cmdpb.Response{
							CmdType: r.CmdType,
							Put:     &raft_cmdpb.PutResponse{},
						})
					} else if r.CmdType == raft_cmdpb.CmdType_Delete {
						kvWB.DeleteCF(r.Delete.GetCf(), r.Delete.GetKey())
						// if proposal != nil {
						// toPrint += fmt.Sprintf("+++++[id=%d] deleting [cf=%v,key=%v]\n",
						// 	d.PeerId(), r.Delete.GetCf(), string(r.Delete.GetKey()))
						// }
						responses = append(responses, &raft_cmdpb.Response{
							CmdType: r.CmdType,
							Delete:  &raft_cmdpb.DeleteResponse{},
						})
					} else if r.CmdType == raft_cmdpb.CmdType_Snap {
						// if proposal != nil {
						// 	toPrint += fmt.Sprintf("+++++[id=%d] snapshot command, openning a new transaction! returning info %v \n", d.PeerId(), d.peerStorage.Region())
						// }

						regionRes := new(metapb.Region)
						err := util.CloneMsg(d.Region(), regionRes)
						if err != nil {
							panic(err.Error())
						}

						// readResponses[len(responses)] = true
						responses = append(responses, &raft_cmdpb.Response{
							CmdType: r.CmdType,
							Snap: &raft_cmdpb.SnapResponse{
								Region: regionRes,
							},
						})

						if proposal != nil {
							cbsForRead = append(cbsForRead, proposal.cb)
							// proposal.cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
						}
					} else if r.CmdType == raft_cmdpb.CmdType_Get {
						resp := &raft_cmdpb.GetResponse{}
						cf := r.Get.Cf
						key := r.Get.Key

						// populate the actual result later.
						postWriteFuncs = append(postWriteFuncs, func() {
							val, err := engine_util.GetCF(d.ctx.engine.Kv, cf, key)
							if err != nil {
								BindRespError(cmdResp, err)
							} else {
								resp.Value = val
							}
							// log.Errorf(
							// 	"[id=%d] engine_util.GetCF, idx=%d, cf=%v, key=%v, val=%v",
							// 	d.Meta.GetId(), e.Index, cf, string(key), string(val))
						})

						responses = append(responses, &raft_cmdpb.Response{
							CmdType: r.CmdType,
							Get:     resp,
						})
					} else {
						panic(fmt.Sprintf("unexpected command: %v", r))
					}
				}
			}

			if proposal != nil {
				proposalsToRespond = append(proposalsToRespond, proposal)
				cmdResp.Responses = responses
				proposalReponses = append(proposalReponses, cmdResp)
			}
		}

		d.peerStorage.applyState.AppliedIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}

	// Don't persist any state if a new peer hasn't received the snapshot. It
	// can send out messages of course. If removedFromCluster is true and d is
	// uninitialized, this peer is the last peer of the region and it's getting
	// removed.
	if d.isInitialized() && (rd.Snapshot.Metadata != nil || len(rd.CommittedEntries) > 0) {
		// Update the KVs and the apply/region at the same transaction
		regionLocalState := new(rspb.RegionLocalState)
		regionLocalState.State = rspb.PeerState_Normal // what if this node is removed?
		if removedFromCluster || d.stopped {
			regionLocalState.State = rspb.PeerState_Tombstone
		}
		regionLocalState.Region = d.peerStorage.region
		kvWB.SetMeta(meta.RegionStateKey(d.peerStorage.region.GetId()), regionLocalState)

		// d.peerStorage.applyState.TruncatedState is updated in admin command when needed.
		// Inefficiency: no need to write to disk if there's no change.
		kvWB.SetMeta(meta.ApplyStateKey(d.peerStorage.region.GetId()), d.peerStorage.applyState)
		kvWB.MustWriteToDB(d.ctx.engine.Kv)

		// toPrint += fmt.Sprintf("+++++[id=%d] what's in ready -> %v\n", d.PeerId(), rd)
		// toPrint += fmt.Sprintf("+++++[id=%d] finish writing to KV store, len(rd.CommittedEntries):%d \n", d.PeerId(), len(rd.CommittedEntries))

		toPrint += fmt.Sprintf("+++++[id=%d] latest RaftLocalState: %v, peerCache: %v\n",
			d.PeerId(), d.peerStorage.raftState, d.peerCache)
		toPrint += fmt.Sprintf("+++++[id=%d] d.ctx.storeMeta.regions: %v\n", d.PeerId(), d.ctx.storeMeta.regions)
		toPrint += fmt.Sprintf("+++++[id=%d][store_id=%d] persisted regionLocalState: %v\n", d.PeerId(), d.storeID(), regionLocalState)
		toPrint += fmt.Sprintf("+++++[id=%d] persisted RaftApplyState: %v, store id: %v\n",
			d.PeerId(), d.peerStorage.applyState, d.storeID())
		toPrint += fmt.Sprintf("+++++[id=%d] applied index: %v, region epoch: %v\n",
			d.PeerId(), d.peerStorage.applyState.AppliedIndex, d.peerStorage.region.RegionEpoch)
		// Opening a transaction here to ensure all previously writes are visible.
		for _, cb := range cbsForRead {
			cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
		}

		if len(rd.CommittedEntries) > 0 && (d.RaftGroup.Raft.State == raft.StateLeader || rd.Snapshot.Metadata != nil) {
			// if len(rd.CommittedEntries) > 0 {
			// if true {
			// if len(toPrint) > 0 && (d.RaftGroup.Raft.State == raft.StateLeader || rd.Snapshot.Metadata != nil || d.Meta.GetId() == 2 || d.Meta.GetId() == 3) {

			// fmt.Println(toPrint)

			// log.Warn(toPrint)
		}
	}
	timeSpentOnApply := time.Since(timeBeforeApply)

	timeBeforeSend := time.Now()
	d.sendPendingVote()
	for i := range rd.Messages {
		if removedFromCluster && d.RaftGroup.Raft.State != raft.StateLeader {
			// reduce sending some stale messages from removed follower
			// but allow leader to send out remaining messages before its own removal.
			continue
		}

		msg := rd.Messages[i] // must take a copy, cannot use the iter reference directly. otherwise message can be wrong.

		fromPeer, err := findPeerByID(allPeers, msg.From)
		if err != nil {
			fromPeer = d.getPeerFromCache(msg.From)
			if fromPeer == nil {
				fromPeer = d.peer.Meta
				if fromPeer == nil {
					panic(fmt.Sprintf("[id=%d] can't get fromPeer for %v", d.Meta.GetId(), msg.To))
				}
			}
		}

		toPeer, err := findPeerByID(allPeers, msg.To)
		if err != nil {
			// look for target in peer cache
			toPeer = d.getPeerFromCache(msg.To)
			if toPeer == nil {
				log.Warnf("[id=%d] ignore messge %v to peer %d, it may have been removed just now", d.Meta.GetId(), msg, msg.To)
				continue
			} else {
				// log.Warnf("[id=%d] got ToPeer %d from peerCache", d.Meta.GetId(), msg.To)
			}
		}

		if msg.MsgType == eraftpb.MessageType_MsgRequestVote || msg.MsgType == eraftpb.MessageType_MsgRequestVoteResponse {
			log.Warnf("[store=%d][region=%d][id=%d][term=%d] sent %v to %d",
				d.storeID(), d.regionId, d.PeerId(), msg.Term, msg.MsgType, msg.To)
		}
		raftMsg := &rspb.RaftMessage{
			RegionId:    d.peer.regionId,
			FromPeer:    fromPeer,
			ToPeer:      toPeer,
			Message:     &msg,
			RegionEpoch: d.peerStorage.region.RegionEpoch,
		}

		if err := d.ctx.trans.Send(raftMsg); err != nil {
			if !strings.Contains(err.Error(), "is dropped") {
				log.Warnf(
					"d.ctx.trans.Send() id=%d->id=%d msg: %v, returned error: %s",
					msg.From, msg.To, msg, err.Error(),
				)
			}
		}
	}
	timeSpentOnSend := time.Since(timeBeforeSend)
	if false {
		log.Errorf("[store=%d] \traft worker: peer %d, timeSpentOnRaft=%v, timeSpentOnApply=%v, timeSpentOnSend=%v",
			d.storeID(), d.PeerId(), timeSpentOnRaft, timeSpentOnApply, timeSpentOnSend)
	}
	d.peer.RaftGroup.Advance(rd)

	// Run these functions after the raft state (e.g. applied index) is updated.
	// CompactLog command may complain that the applied index isn't caught up.
	for _, fn := range postWriteFuncs {
		fn()
	}

	// Some responses (Get requests) are set in postWriteFuncs. So we want to respond to the
	// proposals later.
	for i := range proposalsToRespond {
		proposalsToRespond[i].cb.Done(proposalReponses[i])
		if d.RaftGroup.Raft.State == raft.StateLeader {
			toPrint += fmt.Sprintf("+++++[id=%d] responded to proposal: %v\n", d.PeerId(), proposalsToRespond[i])
		}
	}
}

func (d *peerMsgHandler) sendPendingVote() {
	d.ctx.storeMeta.Lock()
	defer d.ctx.storeMeta.Unlock()

	var remaining []*rspb.RaftMessage
	for i := range d.ctx.storeMeta.pendingVotes {
		msg := d.ctx.storeMeta.pendingVotes[i]

		peerState := d.ctx.router.get(msg.RegionId)
		if peerState != nil {
			d.ctx.router.send(
				msg.RegionId,
				message.NewPeerMsg(message.MsgTypeRaftMessage, msg.RegionId, msg),
			)
			log.Errorf(
				"[stored=%d][region=%d][id=%d] send pending vote to peer %d, msg %v",
				d.storeID(), d.regionId, d.PeerId(), peerState.peer.Meta.Id, msg,
			)
		} else {
			remaining = append(remaining, msg)
		}
	}
	d.ctx.storeMeta.pendingVotes = remaining
}

func addPeer(peers []*metapb.Peer, peer *metapb.Peer) []*metapb.Peer {
	for _, p := range peers {
		if p.Id == peer.Id {
			return peers
		}
	}
	return append(peers, peer)
}

func removePeer(peers []*metapb.Peer, peer *metapb.Peer) []*metapb.Peer {
	res := []*metapb.Peer{}
	for i := range peers {
		p := peers[i]
		if p.Id != peer.Id {
			res = append(res, p)
		}
	}
	return res
}

func mustFindPeerByID(peers []*metapb.Peer, id uint64) *metapb.Peer {
	p, err := findPeerByID(peers, id)
	if err != nil {
		panic(err.Error())
	}
	return p
}

func findPeerByID(peers []*metapb.Peer, id uint64) (*metapb.Peer, error) {
	for _, peer := range peers {
		if peer.Id == id {
			return peer, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("peer not found, id=%d", id))
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	// fmt.Printf("+++++ HandleMsg is called (type=%v)\n", msg.Type)

	// if d.Meta.GetId() == 2 {
	// log.Errorf("[id=2] HandleMsg is called, d info: %+v", d)
	// }
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v, msg: %v", d.Tag, err, raftMsg)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, string(split.SplitKey))
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	for _, request := range msg.Requests {
		var key []byte
		switch request.CmdType {
		case raft_cmdpb.CmdType_Put:
			key = request.Put.Key
		case raft_cmdpb.CmdType_Delete:
			key = request.Delete.Key
		case raft_cmdpb.CmdType_Get:
			key = request.Get.Key
		}

		if key != nil {
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				log.Warnf(
					"[id=%d] ignore propose, key not in range, header: %v, requests: %v, admin_req: %v",
					d.PeerId(), msg.Header, msg.Requests, msg.AdminRequest,
				)
				cb.Done(ErrResp(err))
				return
			}
		}
	}

	nextIndex := d.peer.nextProposalIndex()
	// Your Code Here (2B).
	if msg.AdminRequest != nil &&
		msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_TransferLeader &&
		(d.Meta.Id == msg.AdminRequest.TransferLeader.Peer.Id || d.RaftGroup.IsTransferInProgress(msg.AdminRequest.TransferLeader.Peer.Id)) {
		// no need to emit the log. The transfer is done. Just waiting for the propose to stop.
	} else {
		// if msg.AdminRequest != nil {
		log.Warnf(
			"[store=%d][id=%d] receive propose cmd, epoch:%v, header: %v, requests: %v, admin_req: %v, index: %d",
			d.storeID(), d.PeerId(), d.Region(), msg.Header, msg.Requests, msg.AdminRequest, nextIndex,
		)
		// }
	}

	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_TransferLeader:
			// no need to propose transfer leader admin command.

			// if msg.AdminRequest.TransferLeader.Peer.Id != d.PeerId() {
			// 	log.Warnf("[id=%d] admin command transfer leader, id mismatch! cmd peer id:%d", d.PeerId(), msg.AdminRequest.TransferLeader.Peer.Id)
			// 	cb.Done(ErrResp(err))
			// } else {
			// log.Warnf("[id=%d] admin command transfer leader -> id=%d", d.PeerId(), msg.AdminRequest.TransferLeader.Peer.Id)
			d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
			res := newCmdResp()
			res.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			}
			cb.Done(res)
			return
		case raft_cmdpb.AdminCmdType_ChangePeer:
			changeType := msg.AdminRequest.ChangePeer.ChangeType
			changePeer := msg.AdminRequest.ChangePeer.Peer

			if changeType == eraftpb.ConfChangeType_RemoveNode &&
				changePeer.Id == d.Meta.Id &&
				d.peer.RaftGroup.Raft.State == raft.StateLeader {
				// Removing a leader. must transfer the leadership out first.
				var maxProgress uint64
				var maxProgressPeer uint64
				for peer, pr := range d.peer.RaftGroup.GetProgress() {
					if pr.Match > uint64(maxProgress) && peer != d.Meta.GetId() {
						maxProgress = pr.Match
						maxProgressPeer = peer
					}
				}

				log.Errorf("[id=%d] transferring leader to %d because I'm being removed", d.Meta.Id, maxProgressPeer)
				d.RaftGroup.TransferLeader(maxProgressPeer)
				return
			}

			changePeerByte, err := changePeer.Marshal()
			if err != nil {
				panic(err.Error())
			}

			cc := eraftpb.ConfChange{
				ChangeType: changeType,
				NodeId:     changePeer.Id,
				Context:    changePeerByte,
			}
			err = d.peer.RaftGroup.ProposeConfChange(cc)
			if err != nil {
				leaderPeer := mustFindPeerByID(d.Region().Peers, d.peer.RaftGroup.Raft.Lead) // fix this if it panics
				cb.Done(ErrResp(&util.ErrNotLeader{RegionId: d.regionId, Leader: leaderPeer}))
				fmt.Printf("====================> [id=%d] failed to propose conf change, err: %v, msg: %v\n", d.PeerId(), err, msg)
			}
			// else {
			// do we need proposals?
			// d.peer.proposals = append(d.peer.proposals, &proposal{
			// 	index: nextIndex,
			// 	term:  d.peer.RaftGroup.Raft.Term,
			// 	cb:    cb,
			// })
			// }

			// res := newCmdResp()
			// res.AdminResponse = &raft_cmdpb.AdminResponse{
			// 	CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			// 	ChangePeer: &raft_cmdpb.ChangePeerResponse{},
			// }
			// cb.Done(res)

			return
		case raft_cmdpb.AdminCmdType_CompactLog:
			// continue with the normal proposal.
		case raft_cmdpb.AdminCmdType_Split:
			// continue with the split proposal.
			splitRequest := msg.AdminRequest.Split
			if len(d.peerStorage.region.Peers) != len(splitRequest.GetNewPeerIds()) {
				panic(fmt.Sprintf("hey bro it's not good, d.peerStorage.region: %v, splitRequest: %v", d.peerStorage.region, splitRequest))
			}

			// check the validity of the split key
			err := util.CheckKeyInRegion(splitRequest.SplitKey, d.Region())
			if err != nil || bytes.Equal(splitRequest.SplitKey, d.Region().StartKey) {
				log.Warnf(
					"[id=%d] ignore split propose, split key %v not valid, my region: %v",
					d.PeerId(), string(splitRequest.SplitKey), d.Region(),
				)
				cb.Done(ErrResp(err))
				return
			}

		default:
			panic(fmt.Sprintf("unknown raft admin command %v", msg.AdminRequest))
		}

	}

	cmdRequestBytes, err := msg.Marshal()
	if err != nil {
		panic(err.Error())
	}

	if err := d.peer.RaftGroup.Propose(cmdRequestBytes); err != nil {
		leaderPeer := mustFindPeerByID(d.Region().Peers, d.peer.RaftGroup.Raft.Lead) // fix this if it panics
		cb.Done(ErrResp(&util.ErrNotLeader{RegionId: d.regionId, Leader: leaderPeer}))
		fmt.Printf("====================> [id=%d] failed to propose, err: %v, msg: %v\n", d.PeerId(), err, msg)
	} else {
		d.peer.proposals = append(d.peer.proposals, &proposal{
			index: nextIndex,
			term:  d.peer.RaftGroup.Raft.Term,
			cb:    cb,
		})
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// log.Errorf("[id=%d] who sent me tombstone msg?! %v", d.Meta.GetId(), msg)
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}

	// if _, ok := d.peerCache[msg.GetFromPeer().GetId()]; !ok {
	// 	log.Infof("[id=%d] inserting peer %d to peer cache due to msg %v",
	// 		d.Meta.GetId(), msg.GetFromPeer().GetId(), msg)
	// }
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	// log.Errorf("[id=%d] util.IsEpochStale(fromEpoch, region.RegionEpoch): %v, util.FindPeer(region, fromStoreID):%v, region:%v, fromStoreID: %v, msg: %+v",
	// 	d.Meta.GetId(), util.IsEpochStale(fromEpoch, region.RegionEpoch), util.FindPeer(region, fromStoreID), region, fromStoreID, msg)
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// log.Errorf("[id=%d] handleStaleMsg will be called with msg: %+v", d.Meta.GetId(), msg)
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		log.Infof("[region %d] +++++ raft message %v", regionID, msg)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
