// Package Compactor
// 日志合并上报
// 减少elk的压力，防止太频繁的写kafka，日志延迟以及丢日志的情况
// 减少日志大小

package compact

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

// 数据存储：有并发需要锁，分段式map/sync.Map
// 只写一次，读偏多，不做删除处理
// 数据量较大
// 需要删除来减小map的大小吗
// 遍历map的时候，加锁会影响写，需要做副本吗，还是只加读锁
// 线上16个pod，单个map 2000000/16/32≈4000(平均均匀分布)
// map的大小加上监控/pprof，防止出现性能问题
// 结构：[]map -> haystack（单个用户）-> logs(多条数据)

// MaxCachedLogsNum
// 最大缓存消息数量
const MaxCachedLogsNum = 5

// MaxCachedExpire
// 最长缓存时间
const MaxCachedExpire = 2 * time.Minute

// NewCompactor return new *Compactor instance
// limit represents request channel buffer size
func NewCompactor(limit int, op func(int, []byte)) *Compactor {
	compactor := &Compactor{
		segmap:  NewSegmentMutex(-1),
		report:  op,
		reqChan: make(chan compactArgs, limit),
		bufPool: sync.Pool{New: func() interface{} { return new(bytes.Buffer) }},
		delay:   make(chan []byte, limit),
	}
	// clean cached logs
	go compactor.clean()
	// process channel
	go func() {
		for {
			select {
			case args := <-compactor.reqChan:
				go compactor.push(args.key, args.logs)
			case buf := <-compactor.delay:
				key := binary.LittleEndian.Uint32(buf)
				payload := buf[4:]
				compactor.report(int(key), payload)
			}
		}
	}()
	return compactor
}

func (compactor *Compactor) Push(key int, logs interface{}) {
	args := compactArgs{key: key, logs: logs}
	compactor.reqChan <- args
}

type compactArgs struct {
	key  int
	logs interface{}
}

type Compactor struct {
	segmap  SegmentMutex
	report  func(int, []byte)
	reqChan chan compactArgs
	bufPool sync.Pool
	delay   chan []byte
}

type JSONTime time.Time

func (t JSONTime) MarshalJSON() ([]byte, error) {
	stamp := fmt.Sprintf(`"%s"`, time.Time(t).Format("2006-01-02 15:04:05"))
	return []byte(stamp), nil
}

type compactLogs struct {
	Logs    interface{} `json:"logs"`
	Time    JSONTime    `json:"time"`
	version int         // 记录变更历史，防止数据更改操作被覆盖, 从1开始
}

type haystack struct {
	syncs        []*compactLogs
	lastPushTime time.Time
	version      int // 记录变更历史，防止数据更改操作被覆盖, 从1开始
}

// Push
// 写入日志数据
// 在向segmap中写入时，需要判断segmap中是否有新的写入，通过version来校验
func (compactor *Compactor) push(key int, syncLogs interface{}) {
	defer func() {
		if v := recover(); v != nil {
			stack := debug.Stack()
			log.Printf("Compactor log push panic, key: %d, err: %v, stack: %s\n", key, v, stack)
		}
	}()
	var stack haystack
	val := compactor.segmap.Get(key)
	// not exists
	if val == nil {
		val = compactor.segmap.Get(key)
		if val == nil {
			stack.lastPushTime = time.Now()
			stack.version = 1
			stack.syncs = []*compactLogs{{Logs: syncLogs, Time: JSONTime(time.Now()), version: stack.version}}
			compactor.segmap.Set(key, stack)
			return
		}
	}
	stack = val.(haystack)
	// 合并日志
	stack.syncs = append(stack.syncs, &compactLogs{Logs: syncLogs, Time: JSONTime(time.Now())})
	if len(stack.syncs) >= MaxCachedLogsNum {
		payload, _ := json.Marshal(stack.syncs)
		compactor.report(key, payload)
		stack.syncs = []*compactLogs{}
	}
	// 检查segmap中是否又有写入操作
	stack = compactor.checkVersion(key, stack)
	stack.lastPushTime = time.Now()
	compactor.segmap.Set(key, stack)
	return
}

// 防止在写回segmap时，另外的goroutine中对segmap有新的操作
// 从而导致新操作被覆盖，日志有漏报的情况
func (compactor *Compactor) checkVersion(key int, stack haystack) haystack {
	var newSyncInfo []*compactLogs
	stackCopy := compactor.segmap.Get(key).(haystack)
	if stackCopy.version != stack.version {
		// 有新地写入，做一次合并
		for _, si := range stackCopy.syncs {
			if si.version != stack.version {
				tmpVar := *si
				newSyncInfo = append(newSyncInfo, &tmpVar)
			}
		}
		stack.syncs = append(stack.syncs, newSyncInfo...)
		stack.version = stackCopy.version + 1
	} else {
		stack.version++
	}
	for _, si := range stack.syncs {
		tmpVar := si
		tmpVar.version = stack.version
	}
	return stack
}

// 定时遍历segmap
// 上报超出缓存时间的日志
func (compactor *Compactor) clean() {
	ticker := time.NewTicker(MaxCachedExpire)
	for {
		select {
		case <-ticker.C:
			for index, shard := range compactor.segmap {
				// 遍历副本，防止锁竞争
				replica := shard.copy()
				go func(index int, shard *SegmentShard) {
					shard.RLock()
					for key, val := range shard.shard {
						if val == nil {
							continue
						}
						stack := val.(haystack)
						if len(stack.syncs) == 0 {
							continue
						}
						if time.Since(stack.lastPushTime) >= MaxCachedExpire {
							stack = compactor.checkVersion(key, stack)
							syncs, _ := json.Marshal(stack.syncs)
							// append key and payload to the buffer
							buf := compactor.bufPool.Get().(*bytes.Buffer)
							buf.Reset()
							buf.WriteByte(byte(key))
							buf.WriteByte(byte(key >> 8))
							buf.WriteByte(byte(key >> 16))
							buf.WriteByte(byte(key >> 24))
							buf.Write(syncs)
							logPayload := make([]byte, buf.Len())
							copy(logPayload, buf.Bytes())
							compactor.bufPool.Put(buf)
							compactor.delay <- logPayload
							stack.syncs = []*compactLogs{}
							compactor.segmap.Set(key, stack)
						}
					}
					shard.RUnlock()
				}(index, replica)
			}
		}
	}
}
