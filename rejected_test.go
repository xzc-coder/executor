package executor

import (
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

// TestAbortPolicy 测试AbortPolicy 放弃任务执行，直接返回错误信息，默认的拒绝策略
func TestAbortPolicy(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1), WithTaskBlockingQueue(NewLinkedBlockingRunnableQueue(1)), WithRejectedHandler(AbortPolicy))
	number := atomic.Int32{}
	executor.Execute(func() {
		//模拟任务耗时
		time.Sleep(100 * time.Millisecond)
		number.Add(1)
	})
	f, _ := executor.Execute(func() {
		//模拟任务耗时
		time.Sleep(100 * time.Millisecond)
		number.Add(1)
	})
	_, err := executor.Execute(func() {
		//模拟任务耗时
		time.Sleep(100 * time.Millisecond)
		number.Add(1)
	})
	require.NotNil(t, err)
	f.Get()
	//任务应该执行了2次，第三次返回异常
	require.Equal(t, int32(2), number.Load())
}

// TestCallerRunsPolicy 测试CallerRunsPolicy 用当前提交的协程去进行执行
func TestCallerRunsPolicy(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1), WithTaskBlockingQueue(NewLinkedBlockingRunnableQueue(1)), WithRejectedHandler(CallerRunsPolicy))
	executor.Execute(func() {
		//模拟任务耗时
		time.Sleep(100 * time.Millisecond)
	})
	executor.Execute(func() {
		//模拟任务耗时
		time.Sleep(100 * time.Millisecond)
	})
	var id int64
	_, err := executor.Execute(func() {
		id = GetGoroutineID()
		//模拟任务耗时
		time.Sleep(100 * time.Millisecond)
	})
	time.Sleep(200)
	gid := GetGoroutineID()
	//协程ID应该相同
	require.Equal(t, gid, id)
	//不返回错误
	require.Nil(t, err)
}

// TestBlockPolicy 测试BlockPolicy 阻塞拒绝处理器，阻塞调用方直到任务队列有空余
func TestBlockPolicy(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1), WithTaskBlockingQueue(NewLinkedBlockingRunnableQueue(1)), WithRejectedHandler(BlockPolicy))
	number := atomic.Int32{}
	executor.Execute(func() {
		//模拟任务耗时
		time.Sleep(100 * time.Millisecond)
		number.Add(1)
	})
	executor.Execute(func() {
		//模拟任务耗时
		time.Sleep(100 * time.Millisecond)
		number.Add(1)
	})
	f, err := executor.Execute(func() {
		//模拟任务耗时
		time.Sleep(100 * time.Millisecond)
		number.Add(1)
	})
	require.Nil(t, err)
	f.Get()
	//任务应该都被执行
	require.Equal(t, int32(3), number.Load())
}

// TestDiscardOldestPolicy 测试DiscardOldestPolicy 丢弃最老的任务
func TestDiscardOldestPolicy(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1), WithTaskBlockingQueue(NewLinkedBlockingRunnableQueue(1)), WithRejectedHandler(DiscardOldestPolicy))
	number := atomic.Int32{}
	executor.Execute(func() {
		//模拟任务耗时
		time.Sleep(100 * time.Millisecond)
		number.Add(1)
	})
	executor.Execute(func() {
		//模拟任务耗时
		time.Sleep(100 * time.Millisecond)
		number.Add(1)
	})
	f, err := executor.Execute(func() {
		//模拟任务耗时
		time.Sleep(100 * time.Millisecond)
		number.Add(2)
	})
	require.Nil(t, err)
	f.Get()
	//如果第二个被丢弃了，应该值是3
	require.Equal(t, int32(3), number.Load())
}
