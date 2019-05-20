// Copyright 2016 PingCAP, Inc.
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

package tikv

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/btree"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/backoffutils"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	btreeDegree                = 32
	rcDefaultRegionCacheTTLSec = 600
	invalidatedLastAccessTime  = -1
	reloadRegionThreshold      = 5
)

var (
	tikvRegionCacheCounterWithDropRegionFromCacheOK = metrics.TiKVRegionCacheCounter.WithLabelValues("drop_region_from_cache", "ok")
	tikvRegionCacheCounterWithGetRegionByIDOK       = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region_by_id", "ok")
	tikvRegionCacheCounterWithGetRegionByIDError    = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region_by_id", "err")
	tikvRegionCacheCounterWithGetRegionOK           = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region", "ok")
	tikvRegionCacheCounterWithGetRegionError        = metrics.TiKVRegionCacheCounter.WithLabelValues("get_region", "err")
	tikvRegionCacheCounterWithGetStoreOK            = metrics.TiKVRegionCacheCounter.WithLabelValues("get_store", "ok")
	tikvRegionCacheCounterWithGetStoreError         = metrics.TiKVRegionCacheCounter.WithLabelValues("get_store", "err")
)

const (
	updated  int32 = iota // region is updated and no need to reload.
	needSync              //  need sync new region info.
)

// Region presents kv region
type Region struct {
	meta       *metapb.Region // raw region meta from PD immutable after init
	store      unsafe.Pointer // point to region store info, see RegionStore
	syncFlag   int32          // region need be sync in next turn
	lastAccess int64          // last region access time, see checkRegionCacheTTL
}

// RegionStore represents region stores info
// it will be store as unsafe.Pointer and be load at once
type RegionStore struct {
	workStoreIdx     int32    // point to current work peer in meta.Peers and work store in stores(same idx)
	stores           []*Store // stores in this region
	attemptAfterLoad uint8    // indicate switch peer attempts after load region info
}

// clone clones region store struct.
func (r *RegionStore) clone() *RegionStore {
	return &RegionStore{
		workStoreIdx:     r.workStoreIdx,
		stores:           r.stores,
		attemptAfterLoad: r.attemptAfterLoad,
	}
}

// init initializes region after constructed.
func (r *Region) init(c *RegionCache) {
	// region store pull used store from global store map
	// to avoid acquire storeMu in later access.
	rs := &RegionStore{
		workStoreIdx: 0,
		stores:       make([]*Store, 0, len(r.meta.Peers)),
	}
	for _, p := range r.meta.Peers {
		c.storeMu.RLock()
		store, exists := c.storeMu.stores[p.StoreId]
		c.storeMu.RUnlock()
		if !exists {
			store = c.getStoreByStoreID(p.StoreId)
		}
		rs.stores = append(rs.stores, store)
	}
	atomic.StorePointer(&r.store, unsafe.Pointer(rs))

	// mark region has been init accessed.
	r.lastAccess = time.Now().Unix()
}

func (r *Region) getStore() (store *RegionStore) {
	store = (*RegionStore)(atomic.LoadPointer(&r.store))
	return
}

func (r *Region) compareAndSwapStore(oldStore, newStore *RegionStore) bool {
	return atomic.CompareAndSwapPointer(&r.store, unsafe.Pointer(oldStore), unsafe.Pointer(newStore))
}

func (r *Region) checkRegionCacheTTL(ts int64) bool {
	for {
		lastAccess := atomic.LoadInt64(&r.lastAccess)
		if ts-lastAccess > rcDefaultRegionCacheTTLSec {
			return false
		}
		if atomic.CompareAndSwapInt64(&r.lastAccess, lastAccess, ts) {
			return true
		}
	}
}

// invalidate invalidates a region, next time it will got null result.
func (r *Region) invalidate() {
	atomic.StoreInt64(&r.lastAccess, invalidatedLastAccessTime)
}

// scheduleReload schedules reload region request in next LocateKey.
func (r *Region) scheduleReload() {
	oldValue := atomic.LoadInt32(&r.syncFlag)
	if oldValue != updated {
		return
	}
	atomic.CompareAndSwapInt32(&r.syncFlag, oldValue, needSync)
}

// needReload checks whether region need reload.
func (r *Region) needReload() bool {
	oldValue := atomic.LoadInt32(&r.syncFlag)
	if oldValue == updated {
		return false
	}
	return atomic.CompareAndSwapInt32(&r.syncFlag, oldValue, updated)
}

// RegionCache caches Regions loaded from PD.
type RegionCache struct {
	pdClient pd.Client

	mu struct {
		sync.RWMutex                         // mutex protect cached region
		regions      map[RegionVerID]*Region // cached regions be organized as regionVerID to region ref mapping
		sorted       *btree.BTree            // cache regions be organized as sorted key to region ref mapping
	}
	storeMu struct {
		sync.RWMutex
		stores map[uint64]*Store
	}
	notifyCheckCh chan struct{}
	closeCh       chan struct{}
}

// NewRegionCache creates a RegionCache.
func NewRegionCache(pdClient pd.Client) *RegionCache {
	c := &RegionCache{
		pdClient: pdClient,
	}
	c.mu.regions = make(map[RegionVerID]*Region)
	c.mu.sorted = btree.New(btreeDegree)
	c.storeMu.stores = make(map[uint64]*Store)
	c.notifyCheckCh = make(chan struct{}, 1)
	c.closeCh = make(chan struct{})
	go c.asyncCheckAndResolveLoop()
	return c
}

// Close releases region cache's resource.
func (c *RegionCache) Close() {
	close(c.closeCh)
}

// asyncCheckAndResolveLoop with
func (c *RegionCache) asyncCheckAndResolveLoop() {
	var needCheckStores []*Store
	for {
		select {
		case <-c.closeCh:
			return
		case <-c.notifyCheckCh:
			needCheckStores = needCheckStores[:0]
			c.checkAndResolve(needCheckStores)
		}
	}
}

// checkAndResolve checks and resolve addr of failed stores.
// this method isn't thread-safe and only be used by one goroutine.
func (c *RegionCache) checkAndResolve(needCheckStores []*Store) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(context.Background()).Error("panic in the checkAndResolve goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()

	c.storeMu.RLock()
	for _, store := range c.storeMu.stores {
		state := store.getState()
		if state.resolveState == needCheck {
			needCheckStores = append(needCheckStores, store)
		}
	}
	c.storeMu.RUnlock()

	for _, store := range needCheckStores {
		store.reResolve(c)
	}
}

// RPCContext contains data that is needed to send RPC to a region.
type RPCContext struct {
	Region RegionVerID
	Meta   *metapb.Region
	Peer   *metapb.Peer
	Store  *Store
	Addr   string
}

// GetStoreID returns StoreID.
func (c *RPCContext) GetStoreID() uint64 {
	if c.Store != nil {
		return c.Store.storeID
	}
	return 0
}

func (c *RPCContext) String() string {
	return fmt.Sprintf("region ID: %d, meta: %s, peer: %s, addr: %s",
		c.Region.GetID(), c.Meta, c.Peer, c.Addr)
}

// GetRPCContext returns RPCContext for a region. If it returns nil, the region
// must be out of date and already dropped from cache.
func (c *RegionCache) GetRPCContext(bo *Backoffer, id RegionVerID) (*RPCContext, error) {
	ts := time.Now().Unix()

	cachedRegion := c.getCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return nil, nil
	}

	if !cachedRegion.checkRegionCacheTTL(ts) {
		return nil, nil
	}

	store, peer, addr, err := c.routeStoreInRegion(bo, cachedRegion, ts)
	if err != nil {
		return nil, err
	}
	if store == nil || len(addr) == 0 {
		// Store not found, region must be out of date.
		cachedRegion.invalidate()
		return nil, nil
	}

	return &RPCContext{
		Region: id,
		Meta:   cachedRegion.meta,
		Peer:   peer,
		Store:  store,
		Addr:   addr,
	}, nil
}

// KeyLocation is the region and range that a key is located.
type KeyLocation struct {
	Region   RegionVerID
	StartKey []byte
	EndKey   []byte
}

// Contains checks if key is in [StartKey, EndKey).
func (l *KeyLocation) Contains(key []byte) bool {
	return bytes.Compare(l.StartKey, key) <= 0 &&
		(bytes.Compare(key, l.EndKey) < 0 || len(l.EndKey) == 0)
}

// LocateKey searches for the region and range that the key is located.
func (c *RegionCache) LocateKey(bo *Backoffer, key []byte) (*KeyLocation, error) {
	r, err := c.findRegionByKey(bo, key, false)
	if err != nil {
		return nil, err
	}
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
	}, nil
}

func (c *RegionCache) loadAndInsertRegion(bo *Backoffer, key []byte) (*Region, error) {
	r, err := c.loadRegion(bo, key, false)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.insertRegionToCache(r)
	c.mu.Unlock()
	return r, nil
}

// LocateEndKey searches for the region and range that the key is located.
// Unlike LocateKey, start key of a region is exclusive and end key is inclusive.
func (c *RegionCache) LocateEndKey(bo *Backoffer, key []byte) (*KeyLocation, error) {
	r, err := c.findRegionByKey(bo, key, true)
	if err != nil {
		return nil, err
	}
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
	}, nil
}

func (c *RegionCache) findRegionByKey(bo *Backoffer, key []byte, isEndKey bool) (r *Region, err error) {
	r = c.searchCachedRegion(key, isEndKey)
	if r == nil {
		// load region when it is not exists or expired.
		lr, err := c.loadRegion(bo, key, isEndKey)
		if err != nil {
			// no region data, return error if failure.
			return nil, err
		}
		r = lr
		c.mu.Lock()
		c.insertRegionToCache(r)
		c.mu.Unlock()
	} else if r.needReload() {
		// load region when it be marked as need reload.
		lr, err := c.loadRegion(bo, key, isEndKey)
		if err != nil {
			// ignore error and use old region info.
			logutil.Logger(bo.ctx).Error("load region failure",
				zap.ByteString("key", key), zap.Error(err))
		} else {
			r = lr
			c.mu.Lock()
			c.insertRegionToCache(r)
			c.mu.Unlock()
		}
	}
	return r, nil
}

// LocateRegionByID searches for the region with ID.
func (c *RegionCache) LocateRegionByID(bo *Backoffer, regionID uint64) (*KeyLocation, error) {
	c.mu.RLock()
	r := c.getRegionByIDFromCache(regionID)
	if r != nil {
		loc := &KeyLocation{
			Region:   r.VerID(),
			StartKey: r.StartKey(),
			EndKey:   r.EndKey(),
		}
		c.mu.RUnlock()
		return loc, nil
	}
	c.mu.RUnlock()

	r, err := c.loadRegionByID(bo, regionID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c.mu.Lock()
	c.insertRegionToCache(r)
	c.mu.Unlock()
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
	}, nil
}

// GroupKeysByRegion separates keys into groups by their belonging Regions.
// Specially it also returns the first key's region which may be used as the
// 'PrimaryLockKey' and should be committed ahead of others.
func (c *RegionCache) GroupKeysByRegion(bo *Backoffer, keys [][]byte) (map[RegionVerID][][]byte, RegionVerID, error) {
	groups := make(map[RegionVerID][][]byte)
	var first RegionVerID
	var lastLoc *KeyLocation
	for i, k := range keys {
		if lastLoc == nil || !lastLoc.Contains(k) {
			var err error
			lastLoc, err = c.LocateKey(bo, k)
			if err != nil {
				return nil, first, errors.Trace(err)
			}
		}
		id := lastLoc.Region
		if i == 0 {
			first = id
		}
		groups[id] = append(groups[id], k)
	}
	return groups, first, nil
}

// ListRegionIDsInKeyRange lists ids of regions in [start_key,end_key].
func (c *RegionCache) ListRegionIDsInKeyRange(bo *Backoffer, startKey, endKey []byte) (regionIDs []uint64, err error) {
	for {
		curRegion, err := c.LocateKey(bo, startKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		regionIDs = append(regionIDs, curRegion.Region.id)
		if curRegion.Contains(endKey) {
			break
		}
		startKey = curRegion.EndKey
	}
	return regionIDs, nil
}

// InvalidateCachedRegion removes a cached Region.
func (c *RegionCache) InvalidateCachedRegion(id RegionVerID) {
	cachedRegion := c.getCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return
	}
	tikvRegionCacheCounterWithDropRegionFromCacheOK.Inc()
	cachedRegion.invalidate()
}

// UpdateLeader update some region cache with newer leader info.
func (c *RegionCache) UpdateLeader(regionID RegionVerID, leaderStoreID uint64) {
	r := c.getCachedRegionWithRLock(regionID)
	if r == nil {
		logutil.Logger(context.Background()).Debug("regionCache: cannot find region when updating leader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Uint64("leaderStoreID", leaderStoreID))
		return
	}
	if !c.switchWorkStore(r, leaderStoreID) {
		logutil.Logger(context.Background()).Debug("regionCache: cannot find peer when updating leader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Uint64("leaderStoreID", leaderStoreID))
		r.invalidate()
	}
}

// insertRegionToCache tries to insert the Region to cache.
func (c *RegionCache) insertRegionToCache(cachedRegion *Region) {
	old := c.mu.sorted.ReplaceOrInsert(newBtreeItem(cachedRegion))
	if old != nil {
		delete(c.mu.regions, old.(*btreeItem).cachedRegion.VerID())
	}
	c.mu.regions[cachedRegion.VerID()] = cachedRegion
}

// searchCachedRegion finds a region from cache by key. Like `getCachedRegion`,
// it should be called with c.mu.RLock(), and the returned Region should not be
// used after c.mu is RUnlock().
// If the given key is the end key of the region that you want, you may set the second argument to true. This is useful when processing in reverse order.
func (c *RegionCache) searchCachedRegion(key []byte, isEndKey bool) *Region {
	ts := time.Now().Unix()
	var r *Region
	c.mu.RLock()
	c.mu.sorted.DescendLessOrEqual(newBtreeSearchItem(key), func(item btree.Item) bool {
		r = item.(*btreeItem).cachedRegion
		if isEndKey && bytes.Equal(r.StartKey(), key) {
			r = nil     // clear result
			return true // iterate next item
		}
		if !r.checkRegionCacheTTL(ts) {
			r = nil
			return true
		}
		return false
	})
	c.mu.RUnlock()
	if r != nil && (!isEndKey && r.Contains(key) || isEndKey && r.ContainsByEnd(key)) {
		if !c.hasAvailableStore(r, ts) {
			return nil
		}
		return r
	}
	return nil
}

// getRegionByIDFromCache tries to get region by regionID from cache. Like
// `getCachedRegion`, it should be called with c.mu.RLock(), and the returned
// Region should not be used after c.mu is RUnlock().
func (c *RegionCache) getRegionByIDFromCache(regionID uint64) *Region {
	for v, r := range c.mu.regions {
		if v.id == regionID {
			return r
		}
	}
	return nil
}

// loadRegion loads region from pd client, and picks the first peer as leader.
// If the given key is the end key of the region that you want, you may set the second argument to true. This is useful when processing in reverse order.
func (c *RegionCache) loadRegion(bo *Backoffer, key []byte, isEndKey bool) (*Region, error) {
	var backoffErr error
	searchPrev := false
	for {
		if backoffErr != nil {
			err := bo.Backoff(BoPDRPC, backoffErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		var meta *metapb.Region
		var leader *metapb.Peer
		var err error
		if searchPrev {
			meta, leader, err = c.pdClient.GetPrevRegion(bo.ctx, key)
		} else {
			meta, leader, err = c.pdClient.GetRegion(bo.ctx, key)
		}
		if err != nil {
			tikvRegionCacheCounterWithGetRegionError.Inc()
		} else {
			tikvRegionCacheCounterWithGetRegionOK.Inc()
		}
		if err != nil {
			backoffErr = errors.Errorf("loadRegion from PD failed, key: %q, err: %v", key, err)
			continue
		}
		if meta == nil {
			backoffErr = errors.Errorf("region not found for key %q", key)
			continue
		}
		if len(meta.Peers) == 0 {
			return nil, errors.New("receive Region with no peer")
		}
		if isEndKey && !searchPrev && bytes.Compare(meta.StartKey, key) == 0 && len(meta.StartKey) != 0 {
			searchPrev = true
			continue
		}
		region := &Region{meta: meta}
		region.init(c)
		if leader != nil {
			c.switchWorkStore(region, leader.StoreId)
		}
		return region, nil
	}
}

// loadRegionByID loads region from pd client, and picks the first peer as leader.
func (c *RegionCache) loadRegionByID(bo *Backoffer, regionID uint64) (*Region, error) {
	var backoffErr error
	for {
		if backoffErr != nil {
			err := bo.Backoff(BoPDRPC, backoffErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		meta, leader, err := c.pdClient.GetRegionByID(bo.ctx, regionID)
		if err != nil {
			tikvRegionCacheCounterWithGetRegionByIDError.Inc()
		} else {
			tikvRegionCacheCounterWithGetRegionByIDOK.Inc()
		}
		if err != nil {
			backoffErr = errors.Errorf("loadRegion from PD failed, regionID: %v, err: %v", regionID, err)
			continue
		}
		if meta == nil {
			backoffErr = errors.Errorf("region not found for regionID %q", regionID)
			continue
		}
		if len(meta.Peers) == 0 {
			return nil, errors.New("receive Region with no peer")
		}
		region := &Region{meta: meta}
		region.init(c)
		if leader != nil {
			c.switchWorkStore(region, leader.GetStoreId())
		}
		return region, nil
	}
}

func (c *RegionCache) getCachedRegionWithRLock(regionID RegionVerID) (r *Region) {
	c.mu.RLock()
	r = c.mu.regions[regionID]
	c.mu.RUnlock()
	return
}

// routeStoreInRegion ensures region have workable store and return it.
func (c *RegionCache) routeStoreInRegion(bo *Backoffer, region *Region, ts int64) (workStore *Store, workPeer *metapb.Peer, workAddr string, err error) {
retry:
	regionStore := region.getStore()
	cachedStore, cachedPeer, cachedIdx := region.WorkStorePeer(regionStore)

	// Most of the time, requests are directly routed to stable leader.
	// returns if store is stable leader and no need retry other node.
	state := cachedStore.getState()
	if cachedStore != nil && state.failedAttempt == 0 && state.lastFailedTime == 0 {
		workStore = cachedStore
		workAddr, err = c.getStoreAddr(bo, region, workStore, cachedIdx, state)
		workPeer = cachedPeer
		return
	}

	// try round-robin find & switch to other peers when old leader meet error.
	newIdx := -1
	storeNum := len(regionStore.stores)
	i := (cachedIdx + 1) % storeNum
	start := i
	for {
		store := regionStore.stores[i]
		state = store.getState()
		if state.Available(ts) {
			newIdx = i
			break
		}
		i = (i + 1) % storeNum
		if i == start {
			break
		}
	}
	if newIdx < 0 {
		return
	}
	newRegionStore := regionStore.clone()
	newRegionStore.workStoreIdx = int32(newIdx)
	newRegionStore.attemptAfterLoad++
	attemptOverThreshold := newRegionStore.attemptAfterLoad == reloadRegionThreshold
	if attemptOverThreshold {
		newRegionStore.attemptAfterLoad = 0
	}
	if !region.compareAndSwapStore(regionStore, newRegionStore) {
		goto retry
	}

	//  reload region info after attempts more than reloadRegionThreshold
	if attemptOverThreshold {
		region.scheduleReload()
	}

	workStore = newRegionStore.stores[newIdx]
	workAddr, err = c.getStoreAddr(bo, region, workStore, newIdx, state)
	workPeer = region.meta.Peers[newIdx]
	return
}

func (c *RegionCache) getStoreAddr(bo *Backoffer, region *Region, store *Store, storeIdx int, state storeState) (addr string, err error) {
	switch state.resolveState {
	case resolved, needCheck:
		addr = store.addr
		return
	case unresolved:
		addr, err = store.initResolve(bo, c)
		return
	case deleted:
		addr = c.changeToActiveStore(region, store, storeIdx)
		return
	default:
		panic("unsupported resolve state")
	}
}

func (c *RegionCache) changeToActiveStore(region *Region, store *Store, storeIdx int) (addr string) {
	c.storeMu.RLock()
	store = c.storeMu.stores[store.storeID]
	c.storeMu.RUnlock()
	for {
		oldRegionStore := region.getStore()
		newRegionStore := oldRegionStore.clone()
		newRegionStore.stores = make([]*Store, 0, len(oldRegionStore.stores))
		for i, s := range oldRegionStore.stores {
			if i == storeIdx {
				newRegionStore.stores = append(newRegionStore.stores, store)
			} else {
				newRegionStore.stores = append(newRegionStore.stores, s)
			}
		}
		if region.compareAndSwapStore(oldRegionStore, newRegionStore) {
			break
		}
	}
	addr = store.addr
	return
}

// hasAvailableStore checks whether region has available store.
// different to `routeStoreInRegion` just check and never change work store or peer.
func (c *RegionCache) hasAvailableStore(region *Region, ts int64) bool {
	regionStore := region.getStore()
	for _, store := range regionStore.stores {
		state := store.getState()
		if state.Available(ts) {
			return true
		}
	}
	return false
}

func (c *RegionCache) getStoreByStoreID(storeID uint64) (store *Store) {
	var ok bool
	c.storeMu.Lock()
	store, ok = c.storeMu.stores[storeID]
	if ok {
		c.storeMu.Unlock()
		return
	}
	store = &Store{storeID: storeID}
	c.storeMu.stores[storeID] = store
	c.storeMu.Unlock()
	return
}

// OnSendRequestFail is used for clearing cache when a tikv server does not respond.
func (c *RegionCache) OnSendRequestFail(ctx *RPCContext, err error) {
	failedStoreID := ctx.Store.storeID
	c.storeMu.RLock()
	store, exists := c.storeMu.stores[failedStoreID]
	c.storeMu.RUnlock()
	if !exists {
		return
	}
	store.markAccess(c.notifyCheckCh, false)
}

// OnRegionEpochNotMatch removes the old region and inserts new regions into the cache.
func (c *RegionCache) OnRegionEpochNotMatch(bo *Backoffer, ctx *RPCContext, currentRegions []*metapb.Region) error {
	// Find whether the region epoch in `ctx` is ahead of TiKV's. If so, backoff.
	for _, meta := range currentRegions {
		if meta.GetId() == ctx.Region.id &&
			(meta.GetRegionEpoch().GetConfVer() < ctx.Region.confVer ||
				meta.GetRegionEpoch().GetVersion() < ctx.Region.ver) {
			err := errors.Errorf("region epoch is ahead of tikv. rpc ctx: %+v, currentRegions: %+v", ctx, currentRegions)
			logutil.Logger(context.Background()).Info("region epoch is ahead of tikv", zap.Error(err))
			return bo.Backoff(BoRegionMiss, err)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	needInvalidateOld := true
	// If the region epoch is not ahead of TiKV's, replace region meta in region cache.
	for _, meta := range currentRegions {
		if _, ok := c.pdClient.(*codecPDClient); ok {
			if err := decodeRegionMetaKey(meta); err != nil {
				return errors.Errorf("newRegion's range key is not encoded: %v, %v", meta, err)
			}
		}
		region := &Region{meta: meta}
		region.init(c)
		c.switchWorkStore(region, ctx.Store.storeID)
		c.insertRegionToCache(region)
		if ctx.Region == region.VerID() {
			needInvalidateOld = false
		}
	}
	if needInvalidateOld {
		cachedRegion, ok := c.mu.regions[ctx.Region]
		if ok {
			tikvRegionCacheCounterWithDropRegionFromCacheOK.Inc()
			cachedRegion.invalidate()
		}
	}
	return nil
}

// PDClient returns the pd.Client in RegionCache.
func (c *RegionCache) PDClient() pd.Client {
	return c.pdClient
}

// btreeItem is BTree's Item that uses []byte to compare.
type btreeItem struct {
	key          []byte
	cachedRegion *Region
}

func newBtreeItem(cr *Region) *btreeItem {
	return &btreeItem{
		key:          cr.StartKey(),
		cachedRegion: cr,
	}
}

func newBtreeSearchItem(key []byte) *btreeItem {
	return &btreeItem{
		key: key,
	}
}

func (item *btreeItem) Less(other btree.Item) bool {
	return bytes.Compare(item.key, other.(*btreeItem).key) < 0
}

// GetID returns id.
func (r *Region) GetID() uint64 {
	return r.meta.GetId()
}

// WorkStorePeer returns current work store with work peer.
func (r *Region) WorkStorePeer(rs *RegionStore) (store *Store, peer *metapb.Peer, idx int) {
	idx = int(rs.workStoreIdx)
	store = rs.stores[rs.workStoreIdx]
	peer = r.meta.Peers[rs.workStoreIdx]
	return
}

// RegionVerID is a unique ID that can identify a Region at a specific version.
type RegionVerID struct {
	id      uint64
	confVer uint64
	ver     uint64
}

// GetID returns the id of the region
func (r *RegionVerID) GetID() uint64 {
	return r.id
}

// VerID returns the Region's RegionVerID.
func (r *Region) VerID() RegionVerID {
	return RegionVerID{
		id:      r.meta.GetId(),
		confVer: r.meta.GetRegionEpoch().GetConfVer(),
		ver:     r.meta.GetRegionEpoch().GetVersion(),
	}
}

// StartKey returns StartKey.
func (r *Region) StartKey() []byte {
	return r.meta.StartKey
}

// EndKey returns EndKey.
func (r *Region) EndKey() []byte {
	return r.meta.EndKey
}

// switchWorkStore switches current store to the one on specific store. It returns
// false if no peer matches the storeID.
func (c *RegionCache) switchWorkStore(r *Region, targetStoreID uint64) (switchToTarget bool) {
	if len(r.meta.Peers) == 0 {
		return
	}

	leaderIdx := 0
	for i, p := range r.meta.Peers {
		if p.GetStoreId() == targetStoreID {
			leaderIdx = i
			switchToTarget = true
			break
		}
	}

retry:
	// switch to new leader.
	oldRegionStore := r.getStore()
	if oldRegionStore.workStoreIdx == int32(leaderIdx) {
		return
	}
	newRegionStore := oldRegionStore.clone()
	newRegionStore.workStoreIdx = int32(leaderIdx)
	if !r.compareAndSwapStore(oldRegionStore, newRegionStore) {
		goto retry
	}
	return
}

// Contains checks whether the key is in the region, for the maximum region endKey is empty.
// startKey <= key < endKey.
func (r *Region) Contains(key []byte) bool {
	return bytes.Compare(r.meta.GetStartKey(), key) <= 0 &&
		(bytes.Compare(key, r.meta.GetEndKey()) < 0 || len(r.meta.GetEndKey()) == 0)
}

// ContainsByEnd check the region contains the greatest key that is less than key.
// for the maximum region endKey is empty.
// startKey < key <= endKey.
func (r *Region) ContainsByEnd(key []byte) bool {
	return bytes.Compare(r.meta.GetStartKey(), key) < 0 &&
		(bytes.Compare(key, r.meta.GetEndKey()) <= 0 || len(r.meta.GetEndKey()) == 0)
}

// Store contains a kv process's address.
type Store struct {
	addr         string     // loaded store address
	storeID      uint64     // store's id
	state        uint64     // unsafe store storeState
	resolveMutex sync.Mutex // protect pd from concurrent init requests
}

// storeState contains store's access info.
type storeState struct {
	lastFailedTime uint32
	failedAttempt  uint16
	resolveState   resolveState
	_Align         int8
}

type resolveState uint8

const (
	unresolved resolveState = iota
	resolved
	needCheck
	deleted
)

// initResolve resolves addr for store that never resolved.
func (s *Store) initResolve(bo *Backoffer, c *RegionCache) (addr string, err error) {
	s.resolveMutex.Lock()
	state := s.getState()
	defer s.resolveMutex.Unlock()
	if state.resolveState != unresolved {
		addr = s.addr
		return
	}
	var store *metapb.Store
	for {
		store, err = c.pdClient.GetStore(bo.ctx, s.storeID)
		if err != nil {
			tikvRegionCacheCounterWithGetStoreError.Inc()
		} else {
			tikvRegionCacheCounterWithGetStoreOK.Inc()
		}
		if err != nil {
			// TODO: more refine PD error status handle.
			if errors.Cause(err) == context.Canceled {
				return
			}
			err = errors.Errorf("loadStore from PD failed, id: %d, err: %v", s.storeID, err)
			if err = bo.Backoff(BoPDRPC, err); err != nil {
				return
			}
			continue
		}
		if store == nil {
			return
		}
		addr = store.GetAddress()
		s.addr = addr
	retry:
		state = s.getState()
		if state.resolveState != unresolved {
			addr = s.addr
			return
		}
		newState := state
		newState.resolveState = resolved
		if !s.compareAndSwapState(state, newState) {
			goto retry
		}
		return
	}
}

// reResolve try to resolve addr for store that need check.
func (s *Store) reResolve(c *RegionCache) {
	var addr string
	store, err := c.pdClient.GetStore(context.Background(), s.storeID)
	if err != nil {
		tikvRegionCacheCounterWithGetStoreError.Inc()
	} else {
		tikvRegionCacheCounterWithGetStoreOK.Inc()
	}
	if err != nil {
		logutil.Logger(context.Background()).Error("loadStore from PD failed", zap.Uint64("id", s.storeID), zap.Error(err))
		// we cannot do backoff in reResolve loop but try check other store and wait tick.
		return
	}
	if store == nil {
		return
	}

	addr = store.GetAddress()
	if s.addr != addr {
		var state storeState
		state.resolveState = resolved
		newStore := &Store{storeID: s.storeID, addr: addr}
		newStore.state = *(*uint64)(unsafe.Pointer(&state))
		c.storeMu.Lock()
		c.storeMu.stores[newStore.storeID] = newStore
		c.storeMu.Unlock()
	retryMarkDel:
		// all region used those
		oldState := s.getState()
		if oldState.resolveState == deleted {
			return
		}
		newState := oldState
		newState.resolveState = deleted
		if !s.compareAndSwapState(oldState, newState) {
			goto retryMarkDel
		}
		return
	}
retryMarkResolved:
	oldState := s.getState()
	if oldState.resolveState != needCheck {
		return
	}
	newState := oldState
	newState.resolveState = resolved
	if !s.compareAndSwapState(oldState, newState) {
		goto retryMarkResolved
	}
	return
}

const (
	// maxExponentAttempt before this blackout time is exponent increment.
	maxExponentAttempt = 10
	// startBlackoutAfterAttempt after continue fail attempts start blackout store.
	startBlackoutAfterAttempt = 20
)

func (s *Store) getState() storeState {
	var state storeState
	if s == nil {
		return state
	}
	x := atomic.LoadUint64(&s.state)
	*(*uint64)(unsafe.Pointer(&state)) = x
	return state
}

func (s *Store) compareAndSwapState(oldState, newState storeState) bool {
	oldValue, newValue := *(*uint64)(unsafe.Pointer(&oldState)), *(*uint64)(unsafe.Pointer(&newState))
	return atomic.CompareAndSwapUint64(&s.state, oldValue, newValue)
}

func (s *Store) storeState(newState storeState) {
	newValue := *(*uint64)(unsafe.Pointer(&newState))
	atomic.StoreUint64(&s.state, newValue)
}

// Available returns whether store be available for current.
func (state storeState) Available(ts int64) bool {
	if state.failedAttempt == 0 || state.lastFailedTime == 0 {
		// return quickly if it's continue success.
		return true
	}
	// first `startBlackoutAfterAttempt` can retry immediately.
	if state.failedAttempt < startBlackoutAfterAttempt {
		return true
	}
	// continue fail over than `startBlackoutAfterAttempt` start blackout store logic.
	// check blackout time window to determine store's reachable.
	if state.failedAttempt > startBlackoutAfterAttempt+maxExponentAttempt {
		state.failedAttempt = startBlackoutAfterAttempt + maxExponentAttempt
	}
	blackoutDeadline := int64(state.lastFailedTime) + 1*int64(backoffutils.ExponentBase2(uint(state.failedAttempt-startBlackoutAfterAttempt+1)))
	return blackoutDeadline <= ts
}

// markAccess marks the processing result.
func (s *Store) markAccess(notifyCheckCh chan struct{}, success bool) {
retry:
	var triggerCheck bool
	oldState := s.getState()
	if (oldState.failedAttempt == 0 && success) || (!success && oldState.failedAttempt >= (startBlackoutAfterAttempt+maxExponentAttempt)) {
		// return quickly if continue success, and no more mark when attempt meet max bound.
		return
	}
	state := oldState
	if !success {
		if state.lastFailedTime == 0 {
			state.lastFailedTime = uint32(time.Now().Unix())
		}
		state.failedAttempt = state.failedAttempt + 1
		if state.resolveState == resolved {
			state.resolveState = needCheck
			triggerCheck = true
		}
	} else {
		state.lastFailedTime = 0
		state.failedAttempt = 0
	}
	if !s.compareAndSwapState(oldState, state) {
		goto retry
	}
	if triggerCheck {
		select {
		case notifyCheckCh <- struct{}{}:
		default:
		}
	}
}

// markNeedCheck marks resolved store to be async resolve to check store addr change.
func (s *Store) markNeedCheck(notifyCheckCh chan struct{}) {
retry:
	oldState := s.getState()
	if oldState.resolveState != resolved {
		return
	}
	state := oldState
	state.resolveState = needCheck
	if !s.compareAndSwapState(oldState, state) {
		goto retry
	}
	select {
	case notifyCheckCh <- struct{}{}:
	default:
	}

}
