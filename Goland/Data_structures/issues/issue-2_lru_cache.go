// Задача 2: LRU-кэш
// Реализуйте кэш с политикой вытеснения LRU (Least Recently Used) 
// - при переполнении удаляются наименее используемые элементы.

package main

import (
    "fmt"
    "sync"
)

type CacheEntry struct {
    Key   string
    Value interface{}
}

type LRUCache struct {
    mu       sync.RWMutex
    capacity int
    data     map[string]*CacheEntry
    order    []string   // [0] — самый старый, [len-1] — самый недавний
    stats    CacheStats
}

type CacheStats struct {
    Hits      int
    Misses    int
    Evictions int
}


func NewLRUCache(capacity int) *LRUCache {
    return &LRUCache{
        capacity: capacity,
        data:     make(map[string]*CacheEntry),
        order:    make([]string, 0, capacity),
        stats:    CacheStats{},
    }
}

func (c *LRUCache) Get(key string) (interface{}, bool) {
    // Lock, а не RLock: Get мутирует order (двигает ключ в конец) и stats.
    c.mu.Lock()
    defer c.mu.Unlock()

    if entry, ok := c.data[key]; ok {
        for i, k := range c.order {
            if k == key {
                c.order = append(c.order[:i], c.order[i+1:]...)
                c.order = append(c.order, key)
                c.stats.Hits++
                return entry.Value, true
            }
        }
    }
    c.stats.Misses++
    return nil, false
}

func (c *LRUCache) Put(key string, value interface{}) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if entry, ok := c.data[key]; ok {
        entry.Value = value
        for i, k := range c.order {
            if k == key {
                c.order = append(c.order[:i], c.order[i+1:]...)
                c.order = append(c.order, key)
                break
            }
        }
    } else {
        if len(c.order) >= c.capacity {
            oldKey := c.order[0]
            delete(c.data, oldKey)
            c.order = c.order[1:]
            c.stats.Evictions++
        }
        c.data[key] = &CacheEntry{Key: key, Value: value}
        c.order = append(c.order, key)
    }
}

func (c *LRUCache) Len() int {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return len(c.order)
}

func (c *LRUCache) Stats() CacheStats {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return c.stats
}

func (c *LRUCache) HitRate() float64 {
    c.mu.RLock()
    defer c.mu.RUnlock()

    total := c.stats.Hits + c.stats.Misses
    if total == 0 {
        return 0.0
    }
    return float64(c.stats.Hits) / float64(total)
}

func (c *LRUCache) Keys() []string {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return append([]string(nil), c.order...)
}

func (c *LRUCache) Clear() {
    c.mu.Lock()
    defer c.mu.Unlock()

    c.data = make(map[string]*CacheEntry)
    c.order = c.order[:0]
}

func (c *LRUCache) Delete(key string) bool {
    c.mu.Lock()
    defer c.mu.Unlock()

    if _, ok := c.data[key]; ok {
        delete(c.data, key)
        for i, k := range c.order {
            if k == key {
                c.order = append(c.order[:i], c.order[i+1:]...)
                return true
            }
        }
    }
    return false
}

func (c *LRUCache) Contains(key string) bool {
    c.mu.RLock()
    defer c.mu.RUnlock()

    _, ok := c.data[key]
    return ok
}

func (c *LRUCache) Clone() *LRUCache {
    c.mu.RLock()
    defer c.mu.RUnlock()

    newCache := &LRUCache{
        capacity: c.capacity,
        data:     make(map[string]*CacheEntry, len(c.data)),
        order:    make([]string, len(c.order), c.capacity),
        stats:    c.stats,
    }
    copy(newCache.order, c.order)
    for k, v := range c.data {
        newCache.data[k] = &CacheEntry{Key: v.Key, Value: v.Value}
    }
    return newCache
}


func main() {
    cache := NewLRUCache(3)

    cache.Put("a", "one")
    cache.Put("b", "two")
    cache.Put("c", "three")

    fmt.Println(cache.Get("a")) // "one", true - теперь "a" недавно использован

    cache.Put("d", "four") // Должно вытеснить "b" (наименее использованный)

    fmt.Println(cache.Get("b")) // nil, false
    fmt.Println(cache.Get("a")) // "one", true

    fmt.Println(cache.Stats())    // {2 1 1} — Hits, Misses, Evictions
    fmt.Println(cache.HitRate())  // 0.666...

    clone := cache.Clone()
    clone.Put("z", "zzz")
    fmt.Println(cache.Contains("z"))  // false — оригинал не тронут
    fmt.Println(clone.Contains("z"))  // true
    fmt.Println(cache.Keys())          // без "z"
    fmt.Println(clone.Keys())          // с "z"

    // go run -race issues/issue-2_lru_cache.go
    // Мешаем writer'ов (Put) и reader'ов (Get/Stats) в куче горутин.
    var wg sync.WaitGroup
    for i := 0; i < 100; i++ {
        key := fmt.Sprintf("k%d", i%5)

        wg.Add(1)
        go func(v int) {
            defer wg.Done()
            cache.Put(key, v)
        }(i)

        wg.Add(1)
        go func() {
            defer wg.Done()
            cache.Get(key)
            _ = cache.Stats()
        }()
    }
    wg.Wait()
}