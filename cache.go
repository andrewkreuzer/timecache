package main

// #include <unistd.h>
import "C"

import (
	"container/list"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)

type CacheInterface[K comparable, V any, A any] interface {
	Active() *activeBlock[A]
	Get(key K) (*V, bool)
	Add(createdAt time.Time, value A, size int64) error
	Remove(key K)
	Newest() (*Block[K, V], bool)
	Fetch(key string) []V
	Size() int64
	Len() int
	Stats()
}

type CacheConfig[K comparable, V any, A any] struct {
	MaxMemory    string
	MaxEntries   int
	BlockSize    string
	BlockManager BlockManager[K, V, A]
}

type Cache[K comparable, V any, A any] struct {
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

func (cc *CacheConfig[K, V, A]) NewCache() CacheInterface[K, V, A] {
	return NewTypedCache[K, V, A](cc)
}

func NewTypedCache[K comparable, V any, A any](cfg *CacheConfig[K, V, A]) *Cache[K, V, A] {

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

func (c *Cache[K, V, A]) Fetch(key string) []V {
	c.lock.RLock()
	defer c.lock.RUnlock()
	defer getTimer().timer("fetch")()
	var values []V
	for e := c.ll.Front(); e != nil; e = e.Next() {
		values = append(values, e.Value.(*Block[K, V]).value)
	}

	return values
}

func (c *Cache[K, V, A]) Keys() []K {
	var keys []K
	for key := range c.entries {
		keys = append(keys, key)
	}
	return keys
}

func (c *Cache[K, V, A]) Get(key K) (*V, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	element, ok := c.entries[key]
	if ok {
		c.stats.cacheHits++
		entity := element.Value.(*Block[K, V])
		return &entity.value, true
	}

	c.stats.cacheMisses++
	return nil, false
}

func (c *Cache[K, V, A]) Newest() (*Block[K, V], bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	defer getTimer().timer("newest")()
	elmt := c.ll.Front()
	if elmt != nil {
		c.stats.cacheHits++
		return elmt.Value.(*Block[K, V]), true
	}

	return nil, false
}

func (c *Cache[K, V, A]) Add(createdAt time.Time, value A, size int64) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	defer getTimer().timer("add")()
	if c.activeBlock.size+size > c.blockSize {
		blockValue, blockSize := c.blockManager.CreateValue(c.activeBlock.values)
		go c.put(
			c.blockManager.CreateKey(c.activeBlock.first, c.activeBlock.last),
			blockValue,
			blockSize,
		)
		c.activeBlock = &activeBlock[A]{
			first:  createdAt,
			last:   createdAt,
			values: []A{value},
			size:   size,
		}
		return nil
	}

	c.activeBlock.last = createdAt
	c.activeBlock.values = append(c.activeBlock.values, value)
	c.activeBlock.size += size

	return nil
}

func (c *Cache[K, V, A]) put(key K, value V, size int64) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	defer getTimer().timer("put")()

	if c.maxEntries != 0 && c.ll.Len() >= c.maxEntries {
		go c.RemoveOldest()
	}

	if c.size+size > c.maxMemoryBytes {
		go c.RemoveOldest()
	}

	entity := NewBlock[K, V](key, value, size)
	c.entries[key] = c.ll.PushFront(entity)

	c.stats.blocksAdded++
	c.size += int64(entity.size)
	return nil
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
	defer getTimer().timer("removeOldest")()
	elmt := c.ll.Back()
	if elmt != nil {
		c.ll.Remove(elmt)
		delete(c.entries, elmt.Value.(*Block[K, V]).key)
		s := elmt.Value.(*Block[K, V]).size
		c.size -= int64(s)
	}
}

func (c *Cache[K, V, A]) ActiveLen() int {
	return len(c.activeBlock.values)
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

func convertToBytes(size string) (int64, error) {
	size, unit := parseSize(size)
	switch unit {
	case "":
		s, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			return 0, err
		}
		return s, nil
	case "KiB":
		s, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			return 0, err
		}
		return s * 1024, nil
	case "KB":
		s, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			return 0, err
		}
		return s * 1000, nil
	case "MiB":
		s, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			return 0, err
		}
		return s * 1024 * 1024, nil
	case "MB":
		s, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			return 0, err
		}
		return s * 1000 * 1000, nil
	case "GB":
		s, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			return 0, err
		}
		return s * 1000 * 1000 * 1000, nil
	case "GiB":
		s, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			return 0, err
		}
		return s * 1024 * 1024 * 1024, nil
	default:
		return 0, fmt.Errorf("unknown unit")
	}
}

func parseSize(size string) (string, string) {
	var unit string
	var s string
	for i := len(size) - 1; i >= 0; i-- {
		if size[i] <= '9' {
			unit = size[i+1:]
			s = size[:i+1]
			break
		}
	}
	return s, unit
}
