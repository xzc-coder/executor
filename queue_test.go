package executor

import (
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 测试 Put 方法
func TestBlockingQueuePut(t *testing.T) {
	test := func(queue BlockingQueue[int]) {
		item := 1
		// 调用 Put 方法将元素放入队列
		queue.Put(item)
		// 检查队列长度是否为 1
		require.Equal(t, 1, queue.Size())
	}
	queue := NewChanBlockingQueue[int](1)
	test(queue)
	queue = NewLinkedBlockingQueue[int](1)
	test(queue)

}

// 测试 Offer 方法
func TestBlockingQueueOffer(t *testing.T) {
	// 测试正常情况
	test := func(queue BlockingQueue[int]) {
		item := 1
		timeout := 100 * time.Millisecond
		// 调用 Offer 方法并获取结果
		result := queue.Offer(item, timeout)
		require.Equal(t, true, result)
		require.Equal(t, 1, queue.Size())

		// 测试超时情况
		queue = NewLinkedBlockingQueue[int](1)
		result = queue.Offer(item, timeout)
		//应该超时
		result = queue.Offer(item, timeout)
		require.Equal(t, false, result)
	}
	queue := NewChanBlockingQueue[int](1)
	test(queue)
	queue = NewLinkedBlockingQueue[int](1)
	test(queue)
}

// 测试 Take 方法
func TestBlockingQueueTake(t *testing.T) {
	test := func(queue BlockingQueue[int]) {
		item := 1
		// 调用 Put 方法将元素放入队列
		queue.Put(item)
		// 调用 Take 方法从队列中取出元素
		result, ok := queue.Take()
		require.Equal(t, true, ok)
		require.Equal(t, result, item)
		// 检查队列是否为空
		require.Equal(t, true, queue.IsEmpty())
	}
	queue := NewChanBlockingQueue[int](1)
	test(queue)
	queue = NewLinkedBlockingQueue[int](1)
	test(queue)
}

// 测试 Poll 方法
func TestBlockingQueuePoll(t *testing.T) {
	// 测试正常情况
	test := func(queue BlockingQueue[int]) {
		item := 1
		timeout := 100 * time.Millisecond
		// 调用 Put 方法将元素放入队列
		queue.Put(item)
		// 调用 Poll 方法从队列中取出元素
		result, ok := queue.Poll(timeout)
		require.Equal(t, true, ok)
		require.Equal(t, result, item)
		// 检查队列是否为空
		require.Equal(t, true, queue.IsEmpty())

		// 测试超时情况
		queue = NewLinkedBlockingQueue[int](1)
		result, ok = queue.Poll(timeout)
		require.Equal(t, false, ok)
	}
	queue := NewChanBlockingQueue[int](1)
	test(queue)
	queue = NewLinkedBlockingQueue[int](1)
	test(queue)
}

// 测试 Offer 方法在高并发下的超时情况
func TestBlockingQueueOfferConcurrentTimeout(t *testing.T) {
	test := func(queue BlockingQueue[int]) {
		var wg sync.WaitGroup
		timeout := 100 * time.Millisecond
		successCount := 0

		// 启动多个并发 Offer 操作
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if queue.Offer(1, timeout) {
					successCount++
				}
			}()
		}

		wg.Wait()
		require.Equal(t, 1, successCount)
	}
	queue := NewChanBlockingQueue[int](1)
	test(queue)
	queue = NewLinkedBlockingQueue[int](1)
	test(queue)
}

// 测试 Poll 方法在队列空时的超时情况
func TestBlockingQueuePollEmptyTimeout(t *testing.T) {
	test := func(queue BlockingQueue[int]) {
		timeout := 100 * time.Millisecond
		startTime := time.Now()
		_, ok := queue.Poll(timeout)
		require.Equal(t, false, ok)
		if ok {
			t.Errorf("Expected Poll to fail due to empty queue and timeout, but it succeeded")
		}
		elapsed := time.Since(startTime)
		require.Greater(t, elapsed, timeout)
	}
	queue := NewChanBlockingQueue[int](1)
	test(queue)
	queue = NewLinkedBlockingQueue[int](1)
	test(queue)
}

// 测试打断
func TestBlockingQueueInterrupt(t *testing.T) {
	test := func(queue BlockingQueue[int]) {
		number := atomic.Uint32{}
		var gid int64
		go func() {
			gid = GetGoroutineID()
			queue.Put(1)
			number.Add(1)
			//此处应该阻塞
			queue.Put(1)
			number.Add(1)
		}()
		time.Sleep(200 * time.Millisecond)
		require.Equal(t, uint32(1), number.Load())
		//打断
		queue.Interrupt(gid)
		time.Sleep(200 * time.Millisecond)
		require.Equal(t, uint32(2), number.Load())
	}
	queue := NewChanBlockingQueue[int](1)
	test(queue)
	queue = NewLinkedBlockingQueue[int](1)
	test(queue)
}

// 测试移除所有
func TestBlockingQueueRemoveAll(t *testing.T) {
	test := func(queue BlockingQueue[int]) {
		//满移除
		queue.Put(1)
		queue.Put(1)
		notify := atomic.Bool{}
		go func() {
			//应该会阻塞
			queue.Put(1)
			notify.Store(true)
		}()
		time.Sleep(200 * time.Millisecond)
		rs := queue.RemoveAll()
		time.Sleep(200 * time.Millisecond)
		require.Equal(t, 3, len(rs))
		require.Equal(t, true, queue.IsEmpty())
		require.Equal(t, true, notify.Load())
		//再次移除
		queue.Put(1)
		queue.Put(1)
		rs = queue.RemoveAll()
		require.Equal(t, 2, len(rs))
		require.Equal(t, true, queue.IsEmpty())

		//空移除
		rs = queue.RemoveAll()
		require.Equal(t, 0, len(rs))
		require.Equal(t, true, queue.IsEmpty())
	}
	queue := NewChanBlockingQueue[int](2)
	test(queue)
	queue = NewLinkedBlockingQueue[int](2)
	test(queue)
}

// 测试刷新容量
func TestBlockingQueueRefresh(t *testing.T) {
	queue := NewLinkedBlockingQueue[int](1)

	queue.Put(1)
	notify := atomic.Bool{}
	go func() {
		//应该会阻塞
		queue.Put(1)
		notify.Store(true)
	}()
	//向上扩容，应该阻塞元素会被唤醒，且会扩容成功
	ok := queue.Refresh(4)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, true, ok)
	require.Equal(t, true, notify.Load())
	require.Equal(t, 4, queue.Capacity())
	//向下扩容，当队列数量大于扩容值时应该失败
	ok = queue.Refresh(1)
	require.Equal(t, false, ok)
	require.Equal(t, 4, queue.Capacity())

	//向下扩容，当队列数量小于扩容数量时，应该成功
	ok = queue.Refresh(3)
	require.Equal(t, true, ok)
	require.Equal(t, 3, queue.Capacity())
}

func TestLockQueue(t *testing.T) {
	lockQueue := newLockQueue[int](NewLinkedQueue[int](10000))
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			for j := 0; j < 100; j++ {
				lockQueue.Enqueue(1)
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	require.Equal(t, 10000, lockQueue.Size())

	for i := 0; i < 100; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			for j := 0; j < 100; j++ {
				lockQueue.Dequeue()
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	wg.Wait()
	require.Equal(t, 0, lockQueue.Size())
}
