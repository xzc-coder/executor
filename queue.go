package executor

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

func NewLinkedQueue[T any](capacity int) Queue[T] {
	if capacity < 0 {
		capacity = 0
	}
	queue := &LinkedQueue[T]{
		capacity: capacity,
	}
	return queue
}

func newLockQueue[T any](q Queue[T]) Queue[T] {
	return &LockQueue[T]{
		queue: q,
	}
}

func NewLinkedBlockingRunnableQueue(capacity int) BlockingQueue[Runnable] {
	return NewLinkedBlockingQueue[Runnable](capacity)
}

func NewLinkedBlockingQueue[T any](capacity int) BlockingQueue[T] {
	if capacity <= 0 {
		capacity = 0
	}
	return &LinkedBlockingQueue[T]{
		likedQueue:    &LinkedQueue[T]{capacity: capacity},
		ctxLikedQueue: &LinkedQueue[*contextInfo]{capacity: math.MaxInt},
	}
}

func NewChanBlockingRunnableQueue(capacity int) BlockingQueue[Runnable] {
	return NewChanBlockingQueue[Runnable](capacity)
}

func NewChanBlockingQueue[T any](capacity int) BlockingQueue[T] {
	if capacity <= 0 {
		capacity = 0
	}
	return &ChanBlockingQueue[T]{
		capacity: capacity,
		ch:       make(chan T, capacity),
	}
}

type ChanBlockingQueue[T any] struct {
	capacity   int
	ch         chan T
	mux        sync.Mutex
	ctxInfoMap sync.Map
}

func (c *ChanBlockingQueue[T]) Close() {
	close(c.ch)
}

func (c *ChanBlockingQueue[T]) IsEmpty() bool {
	return len(c.ch) == 0
}

func (c *ChanBlockingQueue[T]) IsFull() bool {
	return len(c.ch) == c.capacity
}

func (c *ChanBlockingQueue[T]) Size() int {
	return len(c.ch)
}

func (c *ChanBlockingQueue[T]) Capacity() int {
	return c.capacity
}

func (c *ChanBlockingQueue[T]) Enqueue(item T) bool {
	//不支持
	return false
}

func (c *ChanBlockingQueue[T]) Dequeue() (T, bool) {
	//不支持
	var t T
	return t, false
}

func (c *ChanBlockingQueue[T]) RemoveAll() []T {
	result := make([]T, 0, len(c.ch))
	for {
		select {
		case item, ok := <-c.ch:
			if !ok {
				return result
			} else {
				result = append(result, item)
			}
		default:
			return result
		}
	}
}

func (c *ChanBlockingQueue[T]) Refresh(newCapacity int) bool {
	return false
}

func (c *ChanBlockingQueue[T]) Put(item T) bool {
	return c.Offer(item, maxTimeDuration)
}

func (c *ChanBlockingQueue[T]) Offer(item T, timeout time.Duration) bool {
	ok := false
	if timeout <= 0 {
		select {
		case c.ch <- item:
			ok = true
		default:
		}
	} else {
		goroutineId := GetGoroutineID()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		ctxInfo := &contextInfo{ctx: ctx, cancel: cancel}
		c.ctxInfoMap.Store(goroutineId, ctxInfo)
		select {
		case c.ch <- item:
			ok = true
		case <-ctx.Done():
		}
		cancel()
		c.ctxInfoMap.Delete(goroutineId)
	}
	return ok
}

func (c *ChanBlockingQueue[T]) Take() (T, bool) {
	return c.Poll(maxTimeDuration)
}

func (c *ChanBlockingQueue[T]) Poll(timeout time.Duration) (T, bool) {
	ok := false
	var item T
	if timeout <= 0 {
		select {
		case item = <-c.ch:
			ok = true
		default:
		}
	} else {
		goroutineId := GetGoroutineID()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		ctxInfo := &contextInfo{ctx: ctx, cancel: cancel}
		c.ctxInfoMap.Store(goroutineId, ctxInfo)
		select {
		case item = <-c.ch:
			ok = true
		case <-ctx.Done():
		}
		cancel()
		c.ctxInfoMap.Delete(goroutineId)
	}
	return item, ok
}

// Interrupt 极端情况下，对于切片的打断，返回值结果并不准确
func (c *ChanBlockingQueue[T]) Interrupt(goroutineId int64) {
	ctxInfoAny, ok := c.ctxInfoMap.LoadAndDelete(goroutineId)
	if ok {
		ctxInfo := ctxInfoAny.(*contextInfo)
		ctxInfo.cancel()
	}
}

const itemKey = ""
const interruptKey = " "

type LinkedBlockingQueue[T any] struct {
	mux           sync.Mutex
	likedQueue    *LinkedQueue[T]
	ctxLikedQueue *LinkedQueue[*contextInfo]
}

func (l *LinkedBlockingQueue[T]) IsEmpty() bool {
	l.mux.Lock()
	defer l.mux.Unlock()
	return l.likedQueue.IsEmpty()
}

func (l *LinkedBlockingQueue[T]) IsFull() bool {
	l.mux.Lock()
	defer l.mux.Unlock()
	return l.likedQueue.IsFull()
}

func (l *LinkedBlockingQueue[T]) Size() int {
	l.mux.Lock()
	defer l.mux.Unlock()
	return l.likedQueue.Size()
}

func (l *LinkedBlockingQueue[T]) Capacity() int {
	l.mux.Lock()
	defer l.mux.Unlock()
	return l.likedQueue.Capacity()
}

func (l *LinkedBlockingQueue[T]) Interrupt(goroutineId int64) {
	l.mux.Lock()
	defer l.mux.Unlock()
	ctxInfo, ok := l.ctxLikedQueue.delete(goroutineId, func(ctxInfoItem *contextInfo, goroutineId any) bool {
		return ctxInfoItem.goroutineId == goroutineId.(int64)
	})
	if ok {
		interruptPtr := ctxInfo.ctx.Value(interruptKey).(*atomic.Bool)
		interruptPtr.Store(true)
		ctxInfo.cancel()
	}
}

func (l *LinkedBlockingQueue[T]) Enqueue(item T) bool {
	return l.likedQueue.Enqueue(item)
}

func (l *LinkedBlockingQueue[T]) Dequeue() (T, bool) {
	return l.likedQueue.Dequeue()
}

func (l *LinkedBlockingQueue[T]) RemoveAll() []T {
	l.mux.Lock()
	defer l.mux.Unlock()
	result := make([]T, 0, l.likedQueue.size+l.ctxLikedQueue.Size())
	if l.likedQueue.IsFull() {
		//如果满了，取完后，再唤醒阻塞的且获取
		result = append(result, l.likedQueue.RemoveAll()...)
		for {
			blockCtxInfo, ok := l.ctxLikedQueue.Dequeue()
			if ok {
				//如果存在阻塞的协程，则唤醒，把添加的元素放进队列
				itemPtr := blockCtxInfo.ctx.Value(itemKey).(*atomic.Pointer[T])
				item := *itemPtr.Load()
				blockCtxInfo.cancel()
				//是没有被打断则被取消的才放入队列中
				interruptPtr := blockCtxInfo.ctx.Value(interruptKey).(*atomic.Bool)
				if errors.Is(blockCtxInfo.ctx.Err(), context.Canceled) && !interruptPtr.Load() {
					result = append(result, item)
				}
			} else {
				break
			}
		}
	} else {
		//如果没有满，则直接返回
		result = append(result, l.likedQueue.RemoveAll()...)
	}
	return result
}

func (l *LinkedBlockingQueue[T]) Refresh(newCapacity int) bool {
	l.mux.Lock()
	defer l.mux.Unlock()
	if newCapacity >= l.likedQueue.Capacity() {
		if l.likedQueue.IsFull() {
			yes := l.likedQueue.Refresh(newCapacity)
			if yes {
				notifyNumber := newCapacity - l.likedQueue.Capacity()
				for i := 0; i < notifyNumber; i++ {
					blockCtxInfo, ok := l.ctxLikedQueue.Dequeue()
					if ok {
						//如果存在阻塞的协程，则唤醒，把添加的元素放进队列
						itemPtr := blockCtxInfo.ctx.Value(itemKey).(*atomic.Pointer[T])
						item := *itemPtr.Load()
						blockCtxInfo.cancel()
						//是没有被打断则被取消的才放入队列中
						interruptPtr := blockCtxInfo.ctx.Value(interruptKey).(*atomic.Bool)
						if errors.Is(blockCtxInfo.ctx.Err(), context.Canceled) && !interruptPtr.Load() {
							l.likedQueue.Enqueue(item)
						}
					} else {
						break
					}
				}
			}
			return yes
		} else {
			return l.likedQueue.Refresh(newCapacity)
		}
	} else {
		return l.likedQueue.Refresh(newCapacity)
	}
}

func (l *LinkedBlockingQueue[T]) Put(item T) bool {
	return l.Offer(item, maxTimeDuration)
}

func (l *LinkedBlockingQueue[T]) Offer(item T, timeout time.Duration) bool {
	var ctxInfo *contextInfo
	ok := false
	func() {
		l.mux.Lock()
		defer l.mux.Unlock()
		if l.likedQueue.IsFull() {
			if timeout > 0 {
				itemPtr := &atomic.Pointer[T]{}
				itemPtr.Store(&item)
				interruptPtr := &atomic.Bool{}
				ctx, cancel := context.WithTimeout(context.WithValue(context.WithValue(context.Background(), itemKey, itemPtr), interruptKey, interruptPtr), timeout)
				ctxInfo = &contextInfo{ctx: ctx, cancel: cancel, goroutineId: GetGoroutineID()}
				l.ctxLikedQueue.Enqueue(ctxInfo)
			}
		} else {
			ok = true
			for {
				//判断有没有阻塞，有阻塞，则通过context传值和唤醒
				blockCtxInfo, have := l.ctxLikedQueue.Dequeue()
				if have {
					itemPtr := blockCtxInfo.ctx.Value(itemKey).(*atomic.Pointer[T])
					//没有超时和取消时，才通过context传值
					if blockCtxInfo.ctx.Err() == nil {
						itemPtr.Store(&item)
						blockCtxInfo.cancel()
						return
					}
				} else {
					l.likedQueue.Enqueue(item)
					return
				}
			}
		}
	}()
	if ctxInfo != nil {
		select {
		case <-ctxInfo.ctx.Done():
			l.mux.Lock()
			defer l.mux.Unlock()
			if errors.Is(ctxInfo.ctx.Err(), context.Canceled) {
				//被唤醒，且没有被打断，则为添加成功
				interruptPtr := ctxInfo.ctx.Value(interruptKey).(*atomic.Bool)
				//判断是否是被打断
				if !interruptPtr.Load() {
					ok = true
				}
			} else {
				//超时，则需要从阻塞上下文队列中删除
				l.ctxLikedQueue.delete(ctxInfo, func(ctxInfoItem *contextInfo, deleteCtxInfo any) bool {
					return ctxInfoItem == deleteCtxInfo.(*contextInfo)
				})
			}
		}
	}
	return ok
}

func (l *LinkedBlockingQueue[T]) Take() (T, bool) {
	return l.Poll(maxTimeDuration)
}

func (l *LinkedBlockingQueue[T]) Poll(timeout time.Duration) (T, bool) {
	var item T
	var ok = false
	var ctxInfo *contextInfo
	func() {
		l.mux.Lock()
		defer l.mux.Unlock()
		if l.likedQueue.IsEmpty() {
			if timeout > 0 {
				itemPtr := &atomic.Pointer[T]{}
				interruptPtr := &atomic.Bool{}
				ctx, cancel := context.WithTimeout(context.WithValue(context.WithValue(context.Background(), itemKey, itemPtr), interruptKey, interruptPtr), timeout)
				ctxInfo = &contextInfo{ctx: ctx, cancel: cancel, goroutineId: GetGoroutineID()}
				l.ctxLikedQueue.Enqueue(ctxInfo)
			}
		} else {
			ok = true
			for {
				//判断有没有阻塞
				blockCtxInfo, have := l.ctxLikedQueue.Dequeue()
				if have {
					itemPtr := blockCtxInfo.ctx.Value(itemKey).(*atomic.Pointer[T])
					//没有超时或者被取消时，才放入
					if blockCtxInfo.ctx.Err() == nil {
						//用队列的返回
						item, _ = l.likedQueue.Dequeue()
						//把阻塞的放回队列
						newItem := *itemPtr.Load()
						l.likedQueue.Enqueue(newItem)
						blockCtxInfo.cancel()
						return
					}
				} else {
					item, _ = l.likedQueue.Dequeue()
					return
				}
			}
		}
	}()
	if ctxInfo != nil {
		select {
		case <-ctxInfo.ctx.Done():
			l.mux.Lock()
			defer l.mux.Unlock()
			if errors.Is(ctxInfo.ctx.Err(), context.Canceled) {
				//被唤醒。则取出数据
				itemPtr := ctxInfo.ctx.Value(itemKey).(*atomic.Pointer[T])
				interruptPtr := ctxInfo.ctx.Value(interruptKey).(*atomic.Bool)
				if !interruptPtr.Load() {
					item = *itemPtr.Load()
					ok = true
				}
			} else {
				//超时，则需要从阻塞上下文队列中删除
				l.ctxLikedQueue.delete(ctxInfo, func(ctxInfoItem *contextInfo, deleteCtxInfo any) bool {
					return ctxInfoItem == deleteCtxInfo.(*contextInfo)
				})
			}
		}
	}
	return item, ok
}

type BlockingQueue[T any] interface {
	Queue[T]

	Put(item T) bool

	Offer(item T, timeout time.Duration) bool

	Take() (T, bool)

	Interrupt(goroutineId int64)

	Poll(timeout time.Duration) (T, bool)
}

// Node 定义链表节点结构体
type Node[T any] struct {
	data T
	next *Node[T]
}

type LinkedQueue[T any] struct {
	head     *Node[T]
	tail     *Node[T]
	size     int
	capacity int
}

// InsertAtEnd 在链表尾部插入新节点
func (l *LinkedQueue[T]) insertAtEnd(data T) bool {
	if l.IsFull() {
		return false
	}
	newNode := &Node[T]{data: data, next: nil}
	if l.head == nil {
		l.head = newNode
		l.tail = newNode
	} else {
		l.tail.next = newNode
		l.tail = newNode
	}
	l.size++
	return true
}

// DeleteAtBeginning 删除链表头部节点
func (l *LinkedQueue[T]) deleteAtHead() (T, bool) {
	if l.head == nil {
		var zero T
		return zero, false
	}
	data := l.head.data
	l.head = l.head.next
	if l.head == nil {
		l.tail = nil
	}
	l.size--
	return data, true
}

func (l *LinkedQueue[T]) delete(data any, compare func(T, any) bool) (T, bool) {
	var result T
	var ok bool
	// 处理头节点就是要删除的节点的情况
	if l.head != nil && compare(l.head.data, data) {
		result = l.head.data
		ok = true
		l.head = l.head.next
		if l.head == nil {
			l.tail = nil
		}
		l.size--
	} else {
		// 遍历链表查找要删除的节点
		current := l.head
		for current != nil && current.next != nil {
			if compare(current.next.data, data) {
				result = current.next.data
				ok = true
				if current.next == l.tail {
					l.tail = current
				}
				current.next = current.next.next
				l.size--
				break
			}
			current = current.next
		}
	}
	return result, ok
}

// IsEmpty 判断链表是否为空
func (l *LinkedQueue[T]) IsEmpty() bool {
	return l.size == 0
}

func (l *LinkedQueue[T]) IsFull() bool {
	return l.size == l.capacity
}

// Size 返回链表的大小
func (l *LinkedQueue[T]) Size() int {
	return l.size
}

// Capacity 返回链表的容量
func (l *LinkedQueue[T]) Capacity() int {
	return l.capacity
}

// Enqueue 入队操作
func (l *LinkedQueue[T]) Enqueue(item T) bool {
	return l.insertAtEnd(item)
}

// Dequeue 出队操作
func (l *LinkedQueue[T]) Dequeue() (T, bool) {
	return l.deleteAtHead()
}

func (l *LinkedQueue[T]) RemoveAll() []T {
	result := make([]T, 0, l.size)
	for {
		t, ok := l.Dequeue()
		if ok {
			result = append(result, t)
		} else {
			break
		}
	}
	return result
}

func (l *LinkedQueue[T]) Refresh(newCapacity int) bool {
	ok := true
	if newCapacity >= l.capacity {
		l.capacity = newCapacity
	} else {
		if l.size > newCapacity {
			ok = false
		} else {
			l.capacity = newCapacity
		}
	}
	return ok
}

type LockQueue[T any] struct {
	queue Queue[T]
	rwMux sync.RWMutex
}

func (l *LockQueue[T]) IsEmpty() bool {
	l.rwMux.RLock()
	defer l.rwMux.RUnlock()
	return l.queue.IsEmpty()
}

func (l *LockQueue[T]) IsFull() bool {
	l.rwMux.RLock()
	defer l.rwMux.RUnlock()
	return l.queue.IsFull()
}

func (l *LockQueue[T]) Size() int {
	l.rwMux.RLock()
	defer l.rwMux.RUnlock()
	return l.queue.Size()
}

func (l *LockQueue[T]) Capacity() int {
	l.rwMux.RLock()
	defer l.rwMux.RUnlock()
	return l.queue.Capacity()
}

func (l *LockQueue[T]) Enqueue(item T) bool {
	l.rwMux.Lock()
	defer l.rwMux.Unlock()
	return l.queue.Enqueue(item)
}

func (l *LockQueue[T]) Dequeue() (T, bool) {
	l.rwMux.Lock()
	defer l.rwMux.Unlock()
	return l.queue.Dequeue()
}

func (l *LockQueue[T]) RemoveAll() []T {
	l.rwMux.Lock()
	defer l.rwMux.Unlock()
	return l.queue.RemoveAll()
}

func (l *LockQueue[T]) Refresh(newCapacity int) bool {
	l.rwMux.Lock()
	defer l.rwMux.Unlock()
	ok := l.queue.Refresh(newCapacity)
	return ok
}

type Queue[T any] interface {
	IsEmpty() bool

	IsFull() bool

	Size() int

	Capacity() int

	Enqueue(item T) bool

	Dequeue() (T, bool)

	RemoveAll() []T

	Refresh(newCapacity int) bool
}
