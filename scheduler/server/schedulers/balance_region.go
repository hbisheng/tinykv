// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap/log"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).

	stores := cluster.GetStores()
	// Filter the store by up time. A suitable store should be up and the down
	// time cannot be longer than `MaxStoreDownTime` of the cluster, which you
	// can get through `cluster.GetMaxStoreDownTime()`.
	var availableStores []*core.StoreInfo
	for i := range stores {
		store := stores[i]
		if store.IsOffline() || store.DownTime() >= cluster.GetMaxStoreDownTime() {
			continue
		}
		availableStores = append(availableStores, store)
	}

	if len(availableStores) <= 1 {
		return nil
	}

	if cluster.GetMaxReplicas() == len(availableStores) {
		return nil
	}

	// Sort the stores based on the region sizes.
	sort.Slice(availableStores, func(i, j int) bool {
		return availableStores[i].GetRegionSize() > availableStores[j].GetRegionSize()
	})

	log.Info("Available Stores:")
	for _, s := range availableStores {
		log.Info(fmt.Sprintf("id=%d, %+v", s.GetID(), s))
	}

	for i := 0; i < len(availableStores)-1; i++ {
		sourceStore := availableStores[i]
		log.Warn(fmt.Sprintf("consider moving a region out of store=%d", sourceStore.GetID()))

		// From this biggest store
		// 1. try to select a pending region
		// 2. it will try to find a follower region
		// 3. it will try to pick out one region
		var targetRegion *core.RegionInfo
		cluster.GetPendingRegionsWithLock(
			sourceStore.GetID(),
			func(rc core.RegionsContainer) {
				targetRegion = rc.RandomRegion(nil, nil)
				if targetRegion != nil {
					log.Warn(fmt.Sprintf("found a region with a pending peer %v on store=%d", targetRegion.GetMeta(), sourceStore.GetID()))
				}
			},
		)

		if targetRegion == nil {
			// if there's no pending region, consider moving a normal region
			cluster.GetFollowersWithLock(
				sourceStore.GetID(),
				func(rc core.RegionsContainer) {
					targetRegion = rc.RandomRegion(nil, nil)
					if targetRegion != nil {
						log.Warn(fmt.Sprintf("found a region with a follower peer %v on store=%d", targetRegion.GetMeta(), sourceStore.GetID()))
					}
				},
			)
		}

		if targetRegion == nil {
			cluster.GetLeadersWithLock(
				sourceStore.GetID(),
				func(rc core.RegionsContainer) {
					targetRegion = rc.RandomRegion(nil, nil)
					if targetRegion != nil {
						log.Warn(fmt.Sprintf("found a region with a leader peer %v on store=%d", targetRegion.GetMeta(), sourceStore.GetID()))
					}
				},
			)
		}

		if targetRegion != nil {
			for j := len(availableStores) - 1; j > i; j-- {
				targetStore := availableStores[j]
				log.Warn(fmt.Sprintf(
					"consider moving region=%d: store=%d -> store=%d", targetRegion.GetID(), sourceStore.GetID(), targetStore.GetID(),
				))

				if peer := targetRegion.GetStorePeer(targetStore.GetID()); peer != nil {
					// // Check if the region already exists on the store
					log.Warn("peer existed")
					continue
				}

				sizeDiff := sourceStore.GetRegionSize() - targetStore.GetRegionSize()
				if targetRegion.GetApproximateSize()*2 >= sizeDiff {
					// Any region you want to move should be smaller than half the size diff.
					log.Warn("diff too small")
					continue
				}

				peer, err := cluster.AllocPeer(targetStore.GetID())
				if err != nil {
					// Can't do anything if IDs cannot be allocated
					log.Error(err.Error())
					return nil
				}
				op, err := operator.CreateMovePeerOperator(
					"balance-region",
					cluster,
					targetRegion,
					operator.OpBalance,
					sourceStore.GetID(),
					targetStore.GetID(),
					peer.Id,
				)
				if err != nil {
					log.Error(err.Error())
				} else {
					log.Info(fmt.Sprintf(
						"returning operator, region=%d: store=%d -> store=%d", targetRegion.GetID(), sourceStore.GetID(), targetStore.GetID(),
					))
					return op
				}
			}
		}
	}
	return nil
}
