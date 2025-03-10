package executor

import (
	"container/heap"
	"context"
	"github.com/petermattis/goid"
	"math"
	"sync"
	"time"
)

const maxTimeDuration = math.MaxUint16 * time.Hour
const invalidIndex = -1

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// SafeSlice 是一个使用读写锁和泛型的线程安全切片结构体
type SafeSlice[T any] struct {
	data  []T
	rwMux sync.RWMutex
}

// Append 向切片中追加元素
func (s *SafeSlice[T]) Append(value T) {
	s.rwMux.Lock()
	defer s.rwMux.Unlock()
	s.data = append(s.data, value)
}

// Get 获取切片中指定索引的元素
func (s *SafeSlice[T]) Get(index int) (T, bool) {
	s.rwMux.RLock()
	defer s.rwMux.RUnlock()
	var zero T
	if index < 0 || index >= len(s.data) {
		return zero, false
	}
	return s.data[index], true
}

// Len 获取切片的长度
func (s *SafeSlice[T]) Len() int {
	s.rwMux.RLock()
	defer s.rwMux.RUnlock()
	return len(s.data)
}

// Remove 删除切片中指定索引的元素
func (s *SafeSlice[T]) Remove(index int) bool {
	s.rwMux.Lock()
	defer s.rwMux.Unlock()
	if index < 0 || index >= len(s.data) {
		return false
	}
	// 删除指定索引的元素
	s.data = append(s.data[:index], s.data[index+1:]...)
	return true
}

func GetGoroutineID() int64 {
	//更换为第三方获取，性能更高
	gid := goid.Get()
	//buf := make([]byte, 64)
	//buf = buf[:runtime.Stack(buf, false)]
	//fields := strings.Fields(strings.TrimPrefix(string(buf), "goroutine "))
	//gid, _ = strconv.ParseInt(fields[0], 10, 64)
	return gid
}

// contextInfo Context的上下文信息集合体
type contextInfo struct {
	ctx         context.Context
	goroutineId int64
	cancel      context.CancelFunc
}

// CallableAdapter 适配器，把Runnable包装成Callable
type CallableAdapter struct {
	runnable Runnable
}

func (c *CallableAdapter) Call() any {
	c.runnable.Run()
	return nil
}

// 获取一个Callable
func callable(runnable Runnable) Callable {
	return &CallableAdapter{
		runnable: runnable,
	}
}

// NewHeap 通过此方式创建的堆，只能使用标准库的 heap 中的方法和 .Peek（非标准库）来调用
func NewHeap(valueLess func(a, b any) bool) *Heap {
	return &Heap{valueLess: valueLess}
}

// NewSafeHeap 创建一个线程安全的堆，只能通过使用该结构体提供的方法
func NewSafeHeap(heap *Heap) *SafeHeap {
	return &SafeHeap{heap: heap}
}

type SafeHeap struct {
	heap *Heap
	mux  sync.Mutex
}

func (s *SafeHeap) Push(heapElement *HeapElement) {
	s.mux.Lock()
	defer s.mux.Unlock()
	heap.Push(s.heap, heapElement)
}

func (s *SafeHeap) Pop() (*HeapElement, bool) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.heap.Len() == 0 {
		return nil, false
	} else {
		return heap.Pop(s.heap).(*HeapElement), true
	}
}

func (s *SafeHeap) Len() int {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.heap.Len()
}

func (s *SafeHeap) Peek() *HeapElement {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.heap.Peek().(*HeapElement)
}

func (s *SafeHeap) Remove(heapElement *HeapElement) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if heapElement.Index >= 0 {
		heap.Remove(s.heap, heapElement.Index)
	}
}

func (s *SafeHeap) RemoveAll() []*HeapElement {
	s.mux.Lock()
	defer s.mux.Unlock()
	var heapElements = make([]*HeapElement, 0, s.Len())
	for {
		heapElement, ok := s.Pop()
		if ok {
			heapElements = append(heapElements, heapElement)
		} else {
			return heapElements
		}
	}
}

type HeapElement struct {
	Value any
	Index int
}

// Heap 堆结构体，使用 any 类型的泛型
type Heap struct {
	elements  []*HeapElement
	valueLess func(a, b any) bool
}

func (h *Heap) Push(element any) {
	h.elements = append(h.elements, element.(*HeapElement))
}

func (h *Heap) Len() int {
	return len(h.elements)
}

func (h *Heap) Pop() any {
	old := h.elements
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	h.elements = old[:n-1]
	x.Index = invalidIndex
	return x
}

func (h *Heap) Less(i, j int) bool {
	return h.valueLess(h.elements[i].Value, h.elements[j].Value)
}

// Swap 交换两个元素的位置，并更新它们的下标
func (h *Heap) Swap(i, j int) {
	h.elements[i], h.elements[j] = h.elements[j], h.elements[i]
	h.elements[i].Index = i
	h.elements[j].Index = j
}

func (h *Heap) Peek() any {
	return h.elements[0]
}
