package compact

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestCompact(t *testing.T) {
	var wg sync.WaitGroup
	keys := []int{1, 10, 100, 1000, 10000}
	for i := 0; i <= 50000; i++ {
		key := keys[rand.Intn(len(keys))]
		wg.Add(1)
		go doCompact(key, &wg)
	}
	wg.Wait()
}

func doCompact(key int, wg *sync.WaitGroup) {
	for i := 0; i < 100; i++ {
		CompactInstance.reqChan <- compactArgs{key: key, logs: []int{1}}
	}
	wg.Done()
}

func TestBuffer(t *testing.T) {
	pool := sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}
	delay := make(chan []byte, 10)
	// write
	for i := 0; i < 10; i++ {
		buf := pool.Get().(*bytes.Buffer)
		buf.Reset()
		buf.WriteString("hello" + strconv.Itoa(i))
		payload := make([]byte, buf.Len())
		copy(payload, buf.Bytes())
		delay <- payload
		pool.Put(buf)
	}
	for {
		select {
		case buf := <-delay:
			time.Sleep(5000 * time.Millisecond)
			fmt.Println(string(buf))
		}
	}
}
