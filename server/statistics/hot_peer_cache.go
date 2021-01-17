// Copyright 2019 TiKV Project Authors.
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

package statistics

import (
	"math"
	"os"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/movingaverage"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

const (
	// TopNN is the threshold which means we can get hot threshold from store.
	TopNN = 60
	// HotThresholdRatio is used to calculate hot thresholds
	HotThresholdRatio = 0.8
	topNTTL           = 3 * RegionHeartBeatReportInterval * time.Second

	rollingWindowsSize = 5

	// HotRegionReportMinInterval is used for the simulator and test
	HotRegionReportMinInterval = 3

	hotRegionAntiCount = 2
)

var (
	minHotThresholds = [2][dimLen]float64{
		WriteFlow: {
			byteDim: 1 * 1024,
			keyDim:  32,
		},
		ReadFlow: {
			byteDim: 8 * 1024,
			keyDim:  128,
		},
	}
)

//TODO: jchen
type regionCache struct {
	regionId       uint64
	leaderId       uint64
	avgLeaderBytes uint64
	avgLeaderKeys  uint64
	avgPeerBytes   uint64
	avgPeerKeys    uint64
	startTs        int64
}

// hotPeerCache saves the hot peer's statistics.
type hotPeerCache struct {
	kind           FlowKind
	peersOfStore   map[uint64]*TopN               // storeID -> hot peers
	storesOfRegion map[uint64]map[uint64]struct{} // regionID -> storeIDs
	regionMap      map[uint64]regionCache         //regionId: regionCache
	mu             sync.Mutex
	config         *config.Config
}

// NewHotStoresStats creates a HotStoresStats
func NewHotStoresStats(kind FlowKind) *hotPeerCache {
	cfg := config.NewConfig()
	_ = cfg.Parse(os.Args[1:])

	return &hotPeerCache{
		kind:           kind,
		peersOfStore:   make(map[uint64]*TopN),
		storesOfRegion: make(map[uint64]map[uint64]struct{}),
		regionMap:      make(map[uint64]regionCache),
		mu:             sync.Mutex{},
		config:         cfg,
	}
}

// RegionStats returns hot items
func (f *hotPeerCache) RegionStats(minHotDegree int) map[uint64][]*HotPeerStat {
	res := make(map[uint64][]*HotPeerStat)
	for storeID, peers := range f.peersOfStore {
		values := peers.GetAll()
		stat := make([]*HotPeerStat, 0, len(values))
		for _, v := range values {
			if peer := v.(*HotPeerStat); peer.HotDegree >= minHotDegree {
				stat = append(stat, peer)
			}
		}
		res[storeID] = stat
	}
	return res
}

// Update updates the items in statistics.
func (f *hotPeerCache) Update(item *HotPeerStat) {
	if item.IsNeedDelete() {
		if peers, ok := f.peersOfStore[item.StoreID]; ok {
			peers.Remove(item.RegionID)
		}

		if stores, ok := f.storesOfRegion[item.RegionID]; ok {
			delete(stores, item.StoreID)
		}
	} else {
		peers, ok := f.peersOfStore[item.StoreID]
		if !ok {
			peers = NewTopN(dimLen, TopNN, topNTTL)
			f.peersOfStore[item.StoreID] = peers
		}
		peers.Put(item)

		stores, ok := f.storesOfRegion[item.RegionID]
		if !ok {
			stores = make(map[uint64]struct{})
			f.storesOfRegion[item.RegionID] = stores
		}
		stores[item.StoreID] = struct{}{}
	}
}

func (f *hotPeerCache) collectRegionMetrics(byteRate, keyRate float64, interval uint64) {
	regionHeartbeatIntervalHist.Observe(float64(interval))
	if interval == 0 {
		return
	}
	if f.kind == ReadFlow {
		readByteHist.Observe(byteRate)
		readKeyHist.Observe(keyRate)
	}
	if f.kind == WriteFlow {
		writeByteHist.Observe(byteRate)
		writeKeyHist.Observe(keyRate)
	}
}

// CheckRegionFlow checks the flow information of region.
func (f *hotPeerCache) CheckRegionFlow(region *core.RegionInfo) (ret []*HotPeerStat) {
	bytes := f.getRegionBytes(region)
	keys := f.getRegionKeys(region)

	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()

	//TODO: jchen, PD里需要对每个region的heartbeat做: 1.聚合 2.清理
	// pd-heartbeat-tick-interval: 1m
	// 现象：leader间隔很短, follower间隔很长
	// 先打印原始版本，看看现象？
	isTimeout := false
	var bytesF, keysF float64

	// 思路：每次保存一次字典里的平均值到cache里; 每隔config时间清空; 中间leader上报忽略?
	pdHeartbeatInterval := f.config.FollowerReadPeriod
	regionId := region.GetID()
	peerId := region.GetLeader().GetId()
	isLeader := region.GetApproximateSize() != 1
	rc, ok := f.regionMap[regionId]

	if !f.config.EnableFollowerRead {
		//默认没打开开关时
		//如果是peer数据，直接忽略
		if !isLeader {
			return []*HotPeerStat{}
		}
		//只更新leader数据
		bytesF = float64(bytes)
		keysF = float64(keys)
	} else {
		//打开follower-read开关后
		f.mu.Lock()
		defer f.mu.Unlock()

		now := time.Now().UnixNano()
		if ok {
			//超过config时间，清空cache
			if now-rc.startTs >= int64(pdHeartbeatInterval*1e9) {
				delete(f.regionMap, regionId)
				ok = false
				isTimeout = true
			}
		}

		log.Info("check region flow", zap.Uint64("peerId", peerId), zap.Uint64("regionId", regionId),
			zap.Bool("isLeader", isLeader), zap.Bool("isTimeout", isTimeout),
			zap.Bool("EnableFollowerRead", f.config.EnableFollowerRead),
		)
		if ok {
			//存到hash
			if isLeader {
				rc.avgLeaderBytes = (rc.avgLeaderBytes + bytes) >> 1
				rc.avgLeaderKeys = (rc.avgLeaderKeys + keys) >> 1
			} else {
				//TODO: jchen, 假设单个peer过热的概率比较小(某个peer过热或过冷怎么处理？)
				rc.avgPeerBytes = (rc.avgPeerBytes + bytes) >> 1
				rc.avgPeerKeys = (rc.avgPeerKeys + keys) >> 1
			}
			f.regionMap[regionId] = rc
		} else if isLeader {
			f.regionMap[regionId] = regionCache{
				regionId:       regionId,
				leaderId:       peerId,
				avgLeaderBytes: bytes,
				avgLeaderKeys:  keys,
				startTs:        time.Now().UnixNano(),
			}
		} else {
			return []*HotPeerStat{}
		}
		//TODO: jchen, 一个周期内，是每个leader心跳都更新是否过热，还是到周期再更新？
		//如果peer不更新cache, 更新map后退出
		if !isLeader {
			return []*HotPeerStat{}
		}
		//聚合计算
		if rc.avgPeerBytes < 1 {
			bytes = rc.avgLeaderBytes
			keys = rc.avgLeaderKeys
		} else {
			bytes = (rc.avgLeaderBytes + rc.avgPeerBytes) >> 1
			keys = (rc.avgLeaderKeys + rc.avgPeerKeys) >> 1
		}

		bytesF = float64(bytes)
		keysF = float64(keys)
	}
	//leader才来到这里，此时map已更新，继续走原有逻辑
	byteRate := bytesF / float64(interval)
	keyRate := keysF / float64(interval)

	f.collectRegionMetrics(byteRate, keyRate, interval)
	// old region is in the front and new region is in the back
	// which ensures it will hit the cache if moving peer or transfer leader occurs with the same replica number

	var peers []uint64
	for _, peer := range region.GetPeers() {
		peers = append(peers, peer.StoreId)
	}

	var tmpItem *HotPeerStat
	storeIDs := f.getAllStoreIDs(region)
	justTransferLeader := f.justTransferLeader(region)
	for _, storeID := range storeIDs {
		isExpired := f.isRegionExpired(region, storeID) // transfer read leader or remove write peer
		oldItem := f.getOldHotPeerStat(region.GetID(), storeID)
		if isExpired && oldItem != nil { // it may has been moved to other store, we save it to tmpItem
			tmpItem = oldItem
		}

		// This is used for the simulator and test. Ignore if report too fast.
		if !isExpired && Denoising && interval < HotRegionReportMinInterval {
			continue
		}

		thresholds := f.calcHotThresholds(storeID)

		newItem := &HotPeerStat{
			StoreID:            storeID,
			RegionID:           region.GetID(),
			Kind:               f.kind,
			ByteRate:           byteRate,
			KeyRate:            keyRate,
			LastUpdateTime:     time.Now(),
			needDelete:         isExpired,
			isLeader:           region.GetLeader().GetStoreId() == storeID,
			justTransferLeader: justTransferLeader,
			interval:           interval,
			peers:              peers,
			thresholds:         thresholds,
		}

		if oldItem == nil {
			if tmpItem != nil { // use the tmpItem cached from the store where this region was in before
				oldItem = tmpItem
			} else { // new item is new peer after adding replica
				for _, storeID := range storeIDs {
					oldItem = f.getOldHotPeerStat(region.GetID(), storeID)
					if oldItem != nil {
						break
					}
				}
			}
		}

		newItem = f.updateHotPeerStat(newItem, oldItem, bytesF, keysF, time.Duration(interval)*time.Second)
		if newItem != nil {
			ret = append(ret, newItem)
		}
	}

	log.Debug("region heartbeat info",
		zap.String("type", f.kind.String()),
		zap.Uint64("region", region.GetID()),
		zap.Uint64("leader", region.GetLeader().GetStoreId()),
		zap.Uint64s("peers", peers),
	)
	return ret
}

func (f *hotPeerCache) IsRegionHot(region *core.RegionInfo, hotDegree int) bool {
	switch f.kind {
	case WriteFlow:
		return f.isRegionHotWithAnyPeers(region, hotDegree)
	case ReadFlow:
		return f.isRegionHotWithPeer(region, region.GetLeader(), hotDegree)
	}
	return false
}

func (f *hotPeerCache) CollectMetrics(typ string) {
	for storeID, peers := range f.peersOfStore {
		store := storeTag(storeID)
		thresholds := f.calcHotThresholds(storeID)
		hotCacheStatusGauge.WithLabelValues("total_length", store, typ).Set(float64(peers.Len()))
		hotCacheStatusGauge.WithLabelValues("byte-rate-threshold", store, typ).Set(thresholds[byteDim])
		hotCacheStatusGauge.WithLabelValues("key-rate-threshold", store, typ).Set(thresholds[keyDim])
		// for compatibility
		hotCacheStatusGauge.WithLabelValues("hotThreshold", store, typ).Set(thresholds[byteDim])
	}
}

func (f *hotPeerCache) getRegionBytes(region *core.RegionInfo) uint64 {
	switch f.kind {
	case WriteFlow:
		return region.GetBytesWritten()
	case ReadFlow:
		return region.GetBytesRead()
	}
	return 0
}

func (f *hotPeerCache) getRegionKeys(region *core.RegionInfo) uint64 {
	switch f.kind {
	case WriteFlow:
		return region.GetKeysWritten()
	case ReadFlow:
		return region.GetKeysRead()
	}
	return 0
}

func (f *hotPeerCache) getOldHotPeerStat(regionID, storeID uint64) *HotPeerStat {
	if hotPeers, ok := f.peersOfStore[storeID]; ok {
		if v := hotPeers.Get(regionID); v != nil {
			return v.(*HotPeerStat)
		}
	}
	return nil
}

func (f *hotPeerCache) isRegionExpired(region *core.RegionInfo, storeID uint64) bool {
	switch f.kind {
	case WriteFlow:
		return region.GetStorePeer(storeID) == nil
	case ReadFlow:
		return region.GetLeader().GetStoreId() != storeID
	}
	return false
}

func (f *hotPeerCache) calcHotThresholds(storeID uint64) [dimLen]float64 {
	minThresholds := minHotThresholds[f.kind]
	tn, ok := f.peersOfStore[storeID]
	if !ok || tn.Len() < TopNN {
		return minThresholds
	}
	ret := [dimLen]float64{
		byteDim: tn.GetTopNMin(byteDim).(*HotPeerStat).GetByteRate(),
		keyDim:  tn.GetTopNMin(keyDim).(*HotPeerStat).GetKeyRate(),
	}
	for k := 0; k < dimLen; k++ {
		ret[k] = math.Max(ret[k]*HotThresholdRatio, minThresholds[k])
	}
	return ret
}

// gets the storeIDs, including old region and new region
func (f *hotPeerCache) getAllStoreIDs(region *core.RegionInfo) []uint64 {
	storeIDs := make(map[uint64]struct{})
	ret := make([]uint64, 0, len(region.GetPeers()))
	// old stores
	ids, ok := f.storesOfRegion[region.GetID()]
	if ok {
		for storeID := range ids {
			storeIDs[storeID] = struct{}{}
			ret = append(ret, storeID)
		}
	}

	// new stores
	for _, peer := range region.GetPeers() {
		//TODO: jchen!!!
		// ReadFlow no need consider the followers.
		if f.kind == ReadFlow && peer.GetStoreId() != region.GetLeader().GetStoreId() {
			continue
		}
		if _, ok := storeIDs[peer.GetStoreId()]; !ok {
			storeIDs[peer.GetStoreId()] = struct{}{}
			ret = append(ret, peer.GetStoreId())
		}
	}

	return ret
}
func (f *hotPeerCache) isOldColdPeer(oldItem *HotPeerStat, storeID uint64) bool {
	isOldPeer := func() bool {
		for _, id := range oldItem.peers {
			if id == storeID {
				return true
			}
		}
		return false
	}
	noInCache := func() bool {
		ids, ok := f.storesOfRegion[oldItem.RegionID]
		if ok {
			for id := range ids {
				if id == storeID {
					return false
				}
			}
		}
		return true
	}
	return isOldPeer() && noInCache()
}

func (f *hotPeerCache) justTransferLeader(region *core.RegionInfo) bool {
	ids, ok := f.storesOfRegion[region.GetID()]
	if ok {
		for storeID := range ids {
			oldItem := f.getOldHotPeerStat(region.GetID(), storeID)
			if oldItem == nil {
				continue
			}
			if oldItem.isLeader {
				return oldItem.StoreID != region.GetLeader().GetStoreId()
			}
		}
	}
	return false
}

func (f *hotPeerCache) isRegionHotWithAnyPeers(region *core.RegionInfo, hotDegree int) bool {
	for _, peer := range region.GetPeers() {
		if f.isRegionHotWithPeer(region, peer, hotDegree) {
			return true
		}
	}
	return false
}

//TODO: jchen, 判断peer所在store的regionId，是否过热
func (f *hotPeerCache) isRegionHotWithPeer(region *core.RegionInfo, peer *metapb.Peer, hotDegree int) bool {
	if peer == nil {
		return false
	}
	storeID := peer.GetStoreId()
	if peers, ok := f.peersOfStore[storeID]; ok {
		if stat := peers.Get(region.GetID()); stat != nil {
			return stat.(*HotPeerStat).HotDegree >= hotDegree
		}
	}
	return false
}

func (f *hotPeerCache) getDefaultTimeMedian() *movingaverage.TimeMedian {
	return movingaverage.NewTimeMedian(DefaultAotSize, rollingWindowsSize, RegionHeartBeatReportInterval*time.Second)
}

func (f *hotPeerCache) updateHotPeerStat(newItem, oldItem *HotPeerStat, bytes, keys float64, interval time.Duration) *HotPeerStat {
	if newItem == nil || newItem.needDelete {
		return newItem
	}

	if oldItem == nil {
		if interval == 0 {
			return nil
		}
		if interval.Seconds() >= RegionHeartBeatReportInterval {
			isHot := bytes/interval.Seconds() >= newItem.thresholds[byteDim] || keys/interval.Seconds() >= newItem.thresholds[keyDim]
			if !isHot {
				return nil
			}
			newItem.HotDegree = 1
			newItem.AntiCount = hotRegionAntiCount
		}
		newItem.isNew = true
		newItem.rollingByteRate = newDimStat(byteDim)
		newItem.rollingKeyRate = newDimStat(keyDim)
		newItem.rollingByteRate.Add(bytes, interval)
		newItem.rollingKeyRate.Add(keys, interval)
		if newItem.rollingKeyRate.isFull() {
			newItem.clearLastAverage()
		}
		return newItem
	}

	newItem.rollingByteRate = oldItem.rollingByteRate
	newItem.rollingKeyRate = oldItem.rollingKeyRate

	if newItem.justTransferLeader {
		newItem.HotDegree = oldItem.HotDegree
		newItem.AntiCount = oldItem.AntiCount
		// skip the first heartbeat interval after transfer leader
		return newItem
	}

	newItem.rollingByteRate.Add(bytes, interval)
	newItem.rollingKeyRate.Add(keys, interval)

	if !newItem.rollingKeyRate.isFull() {
		// not update hot degree and anti count
		newItem.HotDegree = oldItem.HotDegree
		newItem.AntiCount = oldItem.AntiCount
	} else {
		if f.isOldColdPeer(oldItem, newItem.StoreID) {
			if newItem.isHot() {
				newItem.HotDegree = 1
				newItem.AntiCount = hotRegionAntiCount
			} else {
				newItem.needDelete = true
			}
		} else {
			if newItem.isHot() {
				newItem.HotDegree = oldItem.HotDegree + 1
				newItem.AntiCount = hotRegionAntiCount
			} else {
				newItem.HotDegree = oldItem.HotDegree - 1
				newItem.AntiCount = oldItem.AntiCount - 1
				if newItem.AntiCount <= 0 {
					newItem.needDelete = true
				}
			}
		}
		newItem.clearLastAverage()
	}
	return newItem
}
