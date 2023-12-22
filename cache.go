package timecache

// #include <unistd.h>
import "C"

import (
	"container/list"
	"log"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

type blockType interface {
	string | []byte
}

type Key interface {
	GenerateKey() string
	ParseKey(string) (time.Time, time.Time)
}

type Cache[K comparable, V blockType, A proto.Message] struct {
	maxMemoryBytes int64
	maxEntries     int
	blockSize      int64
	blockManager   BlockManager[K, V, A]

	lock        sync.RWMutex
	activeBlock *activeBlock[A]
	entries     map[K]*list.Element
	ll          *list.List
	size        int64
	stats       Stats
}

type activeBlock[A any] struct {
	first, last time.Time
	values      []A
	size        int64
}

var (
	maxMemory  int64 = int64(getSystemMemory() / 10)
	blockSize  int64 = int64(maxMemory / 100)
	maxEntries int   = 1000
)

func getSystemMemory() uint64 {
	return uint64(C.sysconf(C._SC_PHYS_PAGES) * C.sysconf(C._SC_PAGE_SIZE))
}

func (cc *CacheConfig[K, V, A]) NewCache() *Cache[K, V, A] {
	return NewTypedCache[K, V, A](cc)
}

func NewTypedCache[K comparable, V blockType, A proto.Message](
	cfg *CacheConfig[K, V, A],
) *Cache[K, V, A] {

	if cfg.MaxMemory != "" {
		bytes, err := convertToBytes(cfg.MaxMemory)
		if err != nil {
			log.Fatalln("Failed to convert max memory:", err)
		}
		maxMemory = bytes
	}

	if cfg.MaxEntries != 0 {
		maxEntries = cfg.MaxEntries
	}

	if cfg.BlockSize != "" {
		bytes, err := convertToBytes(cfg.BlockSize)
		if err != nil {
			log.Fatalln("Failed to convert cache block size:", err)
		}
		if bytes > maxMemory {
			log.Fatalln("Block size cannot be larger than max memory")
		}
		blockSize = bytes
	}

	return &Cache[K, V, A]{
		maxMemoryBytes: maxMemory,
		maxEntries:     maxEntries,
		blockSize:      blockSize,
		blockManager:   cfg.BlockManager,

		activeBlock: &activeBlock[A]{
			first:  time.Now(),
			last:   time.Now(),
			values: []A{},
			size:   0,
		},
		entries: make(map[K]*list.Element),
		ll:      list.New(),
	}
}

func (c *Cache[K, V, A]) Active() *activeBlock[A] {
	return c.activeBlock
}

func (c *Cache[K, V, A]) ActiveLen() int {
	return len(c.activeBlock.values)
}

func (c *Cache[K, V, A]) Fetch(key string) []V {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var values []V
	for e := c.ll.Front(); e != nil; e = e.Next() {
		values = append(values, e.Value.(*Block[K, V]).value)
	}

	return values
}

func (c *Cache[K, V, A]) Keys() []K {
	keys := make([]K, 0, len(c.entries))
	for key := range c.entries {
		keys = append(keys, key)
	}
	return keys
}

func (c *Cache[K, V, A]) Get(key K) (value *V, cacheHit bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	var element *list.Element
	element, cacheHit = c.entries[key]
	if cacheHit {
		c.stats.cacheHits++
		value = &element.Value.(*Block[K, V]).value
		return
	}

	c.stats.cacheMisses++
	return
}

func (c *Cache[K, V, A]) Newest() (*Block[K, V], bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	elmt := c.ll.Front()
	if elmt != nil {
		c.stats.cacheHits++
		return elmt.Value.(*Block[K, V]), true
	}

	return nil, false
}

func (c *Cache[K, V, A]) Add(createdAt time.Time, value A, size int64) {
	if c.activeBlock.size+size > c.blockSize {
		block := c.blockManager.CreateBlock(c.activeBlock.values)
		c.put(
			c.blockManager.CreateKey(c.activeBlock.first, c.activeBlock.last),
			block,
			int64(len(block)),
		)
		c.activeBlock.first = createdAt
		c.activeBlock.last = createdAt
		c.activeBlock.values = []A{value}
		c.activeBlock.size = size
	}

	c.activeBlock.last = createdAt
	c.activeBlock.values = append(c.activeBlock.values, value)
	c.activeBlock.size += size

	for {
		if c.ll.Len() < c.maxEntries && c.size+size < c.maxMemoryBytes {
			return
		}

		c.RemoveOldest()
	}
}

func (c *Cache[K, V, A]) Put(key K, value V, size int64) {
	c.put(key, value, size)
	for {
		if c.ll.Len() < c.maxEntries && c.size+size < c.maxMemoryBytes {
			return
		}
		c.RemoveOldest()
	}
}

func (c *Cache[K, V, A]) put(key K, value V, size int64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	entity := NewBlock[K, V](key, value, size)
	c.entries[key] = c.ll.PushFront(entity)

	c.stats.blocksAdded++
	c.size += int64(entity.size)
}

func (c *Cache[K, V, A]) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.ll = nil
	c.entries = nil
	c.activeBlock = nil
	c.size = 0
	c.stats = Stats{}
}

func (c *Cache[K, V, A]) Remove(key K) {
	c.lock.Lock()
	defer c.lock.Unlock()
	e := c.entries[key]
	c.ll.Remove(e)
	delete(c.entries, key)
}

func (c *Cache[K, V, A]) RemoveOldest() {
	c.lock.Lock()
	defer c.lock.Unlock()
	elmt := c.ll.Back()
	if elmt != nil {
		c.ll.Remove(elmt)
		delete(c.entries, elmt.Value.(*Block[K, V]).key)
		s := elmt.Value.(*Block[K, V]).size
		c.size -= int64(s)
	}
}

func (c *Cache[K, V, A]) Len() int {
	return len(c.entries)
}

func (c *Cache[K, V, A]) Size() int64 {
	return c.size
}

type Block[K comparable, V any] struct {
	key   K
	value V
	size  int64
}

func NewBlock[K comparable, V any](key K, value V, size int64) *Block[K, V] {
	return &Block[K, V]{
		key:   key,
		value: value,
		size:  size,
	}
}

type Stats struct {
	cacheHits   int
	cacheMisses int
	blocksAdded int
}

func (c *Cache[K, V, A]) Stats() {
	log.Printf("Cache hits: %d, misses: %d, blocks: %d", c.stats.cacheHits, c.stats.cacheMisses, c.stats.blocksAdded)
}
