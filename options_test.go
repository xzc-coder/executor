package executor

import (
	"errors"
	"github.com/stretchr/testify/require"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// TestCorePoolSize 测试核心协程数设置
func TestCorePoolSize(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1))
	require.Equal(t, 1, executor.CorePoolSize())
}

// TestMaxPoolSize 测试最大协程数设置
func TestMaxPoolSize(t *testing.T) {
	executor := NewExecutor(WithMaxPoolSize(1))
	require.Equal(t, 1, executor.MaxPoolSize())
}

// TestKeepAliveTime 测试非核心协程活跃时间
func TestKeepAliveTime(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(0), WithMaxPoolSize(1), WithTaskBlockingQueueDefault(0), WithKeepAliveTime(100*time.Millisecond))
	f, _ := executor.Execute(func() {
	})
	f.Get()
	require.Equal(t, 1, len(executor.WorkerInfos()))
	time.Sleep(150 * time.Millisecond)
	require.Equal(t, 0, len(executor.WorkerInfos()))
}

type executeExpandOption struct {
	number atomic.Int32
}

func (e *executeExpandOption) TaskBeforeExecute(workerName string, runnable Runnable) {
	e.number.Add(1)
}

func (e *executeExpandOption) TaskAfterExecute(workerName string, runnable Runnable) {
	e.number.Add(1)
}

// TestExecuteExpand 测试执行扩展
func TestExecuteExpand(t *testing.T) {
	executeExpand := &executeExpandOption{}
	executor := NewExecutor(WithCorePoolSize(1), WithExecuteExpand(executeExpand))
	f, _ := executor.Execute(func() {

	})
	f.Get()
	require.Equal(t, int32(2), executeExpand.number.Load())
}

// TestGoroutineNameFactory 测试协程命名工厂
func TestGoroutineNameFactory(t *testing.T) {
	gNameFactory := func(number int) string {
		return "test-" + strconv.Itoa(number)
	}
	executor := NewExecutor(WithCorePoolSize(2), WithGoroutineNameFactory(gNameFactory))
	f, _ := executor.Execute(func() {
		time.Sleep(50 * time.Millisecond)
	})
	f, _ = executor.Execute(func() {
		time.Sleep(100 * time.Millisecond)
	})
	f.Get()
	workerInfos := executor.WorkerInfos()
	require.Equal(t, 2, len(workerInfos))
	workerInfo := workerInfos[0]
	workerInfo1 := workerInfos[1]
	require.Equal(t, "test-1", workerInfo.WorkerName)
	require.Equal(t, "test-2", workerInfo1.WorkerName)
}

// TestRejectedHandler 测试拒绝处理器
func TestRejectedHandler(t *testing.T) {
	rHandler := func(Runnable, Executor) error {
		return errors.New("TestRejectedHandler")
	}
	executor := NewExecutor(WithCorePoolSize(0), WithTaskBlockingQueueDefault(0), WithRejectedHandler(rHandler))
	_, err := executor.Execute(func() {

	})
	require.Equal(t, "TestRejectedHandler", err.Error())
}

// TestPanicHandler 测试恐慌处理器
func TestPanicHandler(t *testing.T) {
	number := atomic.Int32{}
	pHandler := func(runnable Runnable, workerInfo *WorkerInfo, v any) {
		number.Add(v.(int32))
	}
	executor := NewExecutor(WithCorePoolSize(1), WithPanicHandler(pHandler))
	f, _ := executor.Execute(func() {
		panic(int32(100))
	})
	f.Get()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int32(100), number.Load())
}
