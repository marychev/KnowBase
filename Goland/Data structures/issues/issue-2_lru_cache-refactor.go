// Задача 2: LRU-кэш — рефакторинг оригинального ../issue-2.go
//
// Что изменилось по сравнению с оригиналом:
//   1. Исправлен баг эффективности в Put: c.order = c.order[1:] терял cap
//      backing array; заменено на copy + truncate, cap сохраняется.
//   2. Удалено мёртвое поле CacheEntry.Key — оно только писалось и никогда
//      не читалось (ключ уже есть в мапе).
//   3. Дубликат "найти в order, вырезать, дописать в конец" вынесен в
//      приватный метод touch — используется в Get и Put.
//   4. Delete упрощён: после проверки в мапе инвариант гарантирует, что ключ
//      есть в order, поэтому защитная проверка внутри цикла удалена.
//   5. interface{} → any (стандарт с Go 1.18).
//   6. Ручные append-идиомы заменены на slices.Index, slices.Delete, slices.Clone.
//   7. Clear использует clear(map) — сохраняет bucket array (Go 1.21+),
//      симметрично с c.order[:0] для слайса.
//   8. В Clone у order выделена cap = capacity, чтобы клон не рос при доборе.
//   9. Убран избыточный zero init stats: CacheStats{} в конструкторе.

package main

import (
	"fmt"
	"slices"
	"sync"
)

type CacheEntry struct {
	Value any
}

type LRUCache struct {
	mu       sync.RWMutex
	capacity int
	data     map[string]*CacheEntry
	order    []string // [0] — самый старый, [len-1] — самый недавний
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
	}
}

// touch перемещает существующий ключ в конец order (самый недавний).
// Инварианты: key уже присутствует в c.order И вызывающий держит c.mu.Lock.
// Свой лок touch не берёт намеренно — RWMutex не рекурсивен, повторный Lock
// из-под уже захваченного лока привёл бы к deadlock.
func (c *LRUCache) touch(key string) {
	i := slices.Index(c.order, key)
	c.order = append(slices.Delete(c.order, i, i+1), key)
}

func (c *LRUCache) Get(key string) (any, bool) {
	// Lock, а не RLock: Get через touch мутирует order и меняет stats.
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.data[key]; ok {
		c.touch(key)
		c.stats.Hits++
		return entry.Value, true
	}
	c.stats.Misses++
	return nil, false
}

func (c *LRUCache) Put(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.data[key]; ok {
		entry.Value = value
		c.touch(key)
		return
	}
	if len(c.order) >= c.capacity {
		oldKey := c.order[0]
		delete(c.data, oldKey)
		// Сдвиг влево вместо c.order = c.order[1:] — сохраняем cap
		// и не перевыделяем backing array при следующем append.
		copy(c.order, c.order[1:])
		c.order = c.order[:len(c.order)-1]
		c.stats.Evictions++
	}
	c.data[key] = &CacheEntry{Value: value}
	c.order = append(c.order, key)
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
	return slices.Clone(c.order)
}

func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	clear(c.data)
	c.order = c.order[:0]
}

func (c *LRUCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.data[key]; !ok {
		return false
	}
	delete(c.data, key)
	i := slices.Index(c.order, key)
	c.order = slices.Delete(c.order, i, i+1)
	return true
}

func (c *LRUCache) Contains(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, ok := c.data[key]
	return ok
}

// Clone создаёт независимую копию кэша.
// maps.Clone здесь не подходит: он даёт shallow-копию, где обе мапы указывали
// бы на одни и те же *CacheEntry — мутация Value в клоне протекала бы в оригинал.
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
		newCache.data[k] = &CacheEntry{Value: v.Value}
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

	fmt.Println(cache.Stats())   // {2 1 1} — Hits, Misses, Evictions
	fmt.Println(cache.HitRate()) // 0.666...

	clone := cache.Clone()
	clone.Put("z", "zzz")
	fmt.Println(cache.Contains("z")) // false — оригинал не тронут
	fmt.Println(clone.Contains("z")) // true
	fmt.Println(cache.Keys())        // без "z"
	fmt.Println(clone.Keys())        // с "z"

	// go run -race issues/issue-2_lru_cache-refactor.go
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
