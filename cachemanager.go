package main

// type CacheManager[K comparable, V any, A any] struct {
// 	caches map[string]*Cache[K, V, A]
// }

// func NewCacheManager[K comparable, V any, A any]() *CacheManager[K, V, A] {
// 	return &CacheManager[K, V, A]{
// 		caches: make(map[string]*Cache[K, V, A]),
// 	}
// }

// func (cm *CacheManager[K, V, A]) GetCache(name string) *Cache[K, V, A] {
// 	return cm.caches[name]
// }

// func (cm *CacheManager[K, V, A]) AddCache(name string, cache *Cache[K, V, A]) {
// 	cm.caches[name] = cache
// }

// func (cm *CacheManager[K, V, A]) RemoveCache(name string) {
// 	delete(cm.caches, name)
// }

// func (cm *CacheManager[K, V, A]) ClearCache(name string) {
// 	cm.caches[name].Clear()
// }

// func (cm *CacheManager[K, V, A]) ClearAll() {
// 	for _, cache := range cm.caches {
// 		cache.Clear()
// 	}
// }

// func (cm *CacheManager[K, V, A]) RemoveOldest(name string) {
// 	cm.caches[name].RemoveOldest()
// }

// func (cm *CacheManager[K, V, A]) Remove(name string, key K) {
// 	cm.caches[name].Remove(key)
// }
