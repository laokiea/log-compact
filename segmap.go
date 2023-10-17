package compact

import (
	"fmt"
	"hash/fnv"
	"log"
	"runtime"
	"sync"
)

const MinShardsNum = 32

type SegmentShard struct {
	shard map[int]interface{}
	sync.RWMutex
}

// make a replica
func (shard *SegmentShard) copy() *SegmentShard {
	shardReplica := SegmentShard{
		shard: make(map[int]interface{}),
	}
	for key, val := range shard.shard {
		shardReplica.shard[key] = val
	}
	return &shardReplica
}

type SegmentMutex []*SegmentShard

func NewSegmentMutex(l int) SegmentMutex {
	if l < MinShardsNum {
		l = MinShardsNum
	}
	stack := make([]*SegmentShard, l)
	for i := 0; i < l; i++ {
		shard := new(SegmentShard)
		*shard = SegmentShard{
			shard: make(map[int]interface{}),
		}
		// used to debug panic whether it comes from GC
		runtime.SetFinalizer(shard, func(s *SegmentShard) {
			// runtime.KeepAlive(s)
			log.Printf("runtime gc obj, shards size: %d\n", len(s.shard))
		})
		stack[i] = shard
	}
	return stack
}

func (s SegmentMutex) hash(key int) uint {
	h := fnv.New64()
	_, _ = h.Write([]byte(fmt.Sprint(key)))
	return uint(h.Sum64())
}

func (s SegmentMutex) shard(key int) *SegmentShard {
	pos := s.hash(key) % uint(len(s))
	return s[pos]
}

func (s SegmentMutex) Set(key int, val interface{}) {
	shard := s.shard(key)
	shard.Lock()
	defer shard.Unlock()
	shard.shard[key] = val
}

func (s SegmentMutex) Get(key int) interface{} {
	shard := s.shard(key)
	shard.RLock()
	defer shard.RUnlock()
	return shard.shard[key]
}

func (s SegmentMutex) Delete(key int) {
	shard := s.shard(key)
	shard.Lock()
	defer shard.Unlock()
	delete(shard.shard, key)
}
