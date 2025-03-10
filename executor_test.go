package executor

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

type executeExpandTest struct {
}

func (e executeExpandTest) TaskBeforeExecute(workerName string, runnable Runnable) {
	fmt.Println("TaskBeforeExecuteTest")
}

func (e executeExpandTest) TaskAfterExecute(workerName string, runnable Runnable) {
	fmt.Println("TaskAfterExecuteTest")
}

var goroutineNameFactory = func(number int) string {
	return "test-" + strconv.Itoa(number)
}

var panicHandlerNumber = atomic.Int32{}

var panicHandler = func(runnable Runnable, workerInfo *WorkerInfo, v any) {
	fmt.Println("panicHandlerTest")
	panicHandlerNumber.Add(1)
}

var rejectedHandler = func(Runnable, Executor) error {
	return errors.New("test")
}

// TestExecutorOptions 测试 Execute的参数方法
func TestExecutorOptions(t *testing.T) {
	executeExpand := &executeExpandTest{}
	linkedBlockingQueue := NewLinkedBlockingRunnableQueue(10)
	executor := NewExecutor(WithCorePoolSize(2), WithMaxPoolSize(3), WithKeepAliveTime(10*time.Second), WithTaskBlockingQueue(linkedBlockingQueue), WithGoroutineNameFactory(goroutineNameFactory), WithRejectedHandler(rejectedHandler), WithExecuteExpand(executeExpand), WithPanicHandler(panicHandler)).(*GoroutinePoolExecutor)
	corePoolSize := executor.corePoolSize
	require.Equal(t, 2, corePoolSize)
	maxPoolSize := executor.maxPoolSize
	require.Equal(t, 3, maxPoolSize)
	keepAliveTime := executor.keepAliveTime
	require.Equal(t, 10*time.Second, keepAliveTime)
	taskBlockingQueue := executor.taskBlockingQueue
	require.Equal(t, linkedBlockingQueue, taskBlockingQueue)
	gNameFactory := executor.goroutineNameFactory
	name1 := gNameFactory(1)
	name2 := goroutineNameFactory(1)
	require.Equal(t, name1, name2)
	rHandler := executor.rejectedHandler
	r1 := rHandler(nil, nil)
	r2 := rejectedHandler(nil, nil)
	require.Equal(t, r1.Error(), r2.Error())
	eExpand := executor.executeExpand
	require.Equal(t, executeExpand, eExpand)
	pHandler := executor.panicHandler
	pHandler(nil, nil, nil)
	require.Equal(t, int(panicHandlerNumber.Load()), 1)
}

// TestExecutorExecute 测试 Execute 方法
func TestExecutorExecute(t *testing.T) {
	number := atomic.Int32{}
	executor := NewExecutor(WithCorePoolSize(1))
	job := func() {
		// 模拟任务执行
		time.Sleep(100 * time.Millisecond)
		number.Add(1)
	}
	f, err := executor.Execute(job)
	require.Equal(t, nil, err)
	f.Get()
	require.Equal(t, 1, int(number.Load()))
	executor.AwaitTermination(maxTimeDuration)
}

var runnableNumber = atomic.Int32{}

type runnableTest struct {
}

func (r runnableTest) Run() {
	// 模拟任务执行
	time.Sleep(100 * time.Millisecond)
	runnableNumber.Add(1)
}

// TestExecutorExecuteRunnable 测试 ExecuteRunnable 方法
func TestExecutorExecuteRunnable(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1))
	runnable := &runnableTest{}
	f, err := executor.ExecuteRunnable(runnable)
	require.Equal(t, nil, err)
	f.Get()
	require.Equal(t, 1, int(runnableNumber.Load()))
	executor.AwaitTermination(maxTimeDuration)
}

// TestExecutorSubmit 测试 Submit 方法
func TestExecutorSubmit(t *testing.T) {
	result := "test"
	executor := NewExecutor(WithCorePoolSize(1))
	job := func() any {
		// 模拟任务执行
		time.Sleep(100 * time.Millisecond)
		return result
	}
	f, err := executor.Submit(job)
	require.Equal(t, nil, err)
	r := f.Get()
	require.Equal(t, result, r)
	executor.AwaitTermination(maxTimeDuration)
}

var callableResult = "test"

type callableTest struct {
}

func (c callableTest) Call() any {
	// 模拟任务执行
	time.Sleep(100 * time.Millisecond)
	return callableResult
}

// TestExecutorSubmitCallable 测试 SubmitCallable 方法
func TestExecutorSubmitCallable(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1))
	c := &callableTest{}
	f, err := executor.SubmitCallable(c)
	require.Equal(t, nil, err)
	r := f.Get()
	require.Equal(t, callableResult, r)
	executor.AwaitTermination(maxTimeDuration)
}

// TestExecutorShutdown 测试 Shutdown 方法
func TestExecutorShutdown(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1))
	executor.Shutdown()
	require.Equal(t, true, executor.IsShutdown())
	require.Equal(t, true, executor.IsTerminated())
	number := atomic.Int32{}
	executor = NewExecutor(WithCorePoolSize(1))
	executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(200 * time.Millisecond)
		number.Add(1)
	})
	executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(200 * time.Millisecond)
		number.Add(1)
	})
	require.Equal(t, false, executor.IsShutdown())
	executor.Shutdown()
	require.Equal(t, true, executor.IsShutdown())
	require.Equal(t, false, executor.IsTerminated())
	time.Sleep(500 * time.Millisecond)
	_, err := executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(200 * time.Millisecond)
		number.Add(1)
	})
	require.NotNil(t, err)
	require.Equal(t, 2, int(number.Load()))
	require.Equal(t, true, executor.IsTerminated())
}

// TestExecutorAwaitTermination 测试 AwaitTermination 方法
func TestExecutorAwaitTermination(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1))
	number := atomic.Int32{}
	executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(200 * time.Millisecond)
		number.Add(1)
	})
	executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(200 * time.Millisecond)
		number.Add(1)
	})
	executor.AwaitTermination(300 * time.Millisecond)
	require.Equal(t, true, executor.IsShutdown())
	require.Equal(t, false, executor.IsTerminated())
	require.Equal(t, 1, int(number.Load()))
	executor.AwaitTermination(200 * time.Millisecond)
	require.Equal(t, 2, int(number.Load()))
	require.Equal(t, true, executor.IsTerminated())
	_, err := executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(200 * time.Millisecond)
		number.Add(1)
	})
	require.NotNil(t, err)
}

// TestExecutorShutdownNow 测试 ShutdownNow 方法
func TestExecutorShutdownNow(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1))
	number := atomic.Int32{}
	executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(200 * time.Millisecond)
		number.Add(1)
	})
	f, _ := executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(200 * time.Millisecond)
		number.Add(1)
	})
	rs := executor.ShutdownNow()
	require.Equal(t, 1, len(rs))
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, false, f.IsSuccess())
	require.Equal(t, false, f.IsDone())
	require.Equal(t, true, executor.IsShutdown())
	require.Equal(t, true, executor.IsTerminated())
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, 1, int(number.Load()))
	time.Sleep(500 * time.Millisecond)
	_, err := executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(200 * time.Millisecond)
		number.Add(1)
	})
	require.NotNil(t, err)
}

// TestSetCorePoolSize 测试 SetCorePoolSize 和 CorePoolSize 方法
func TestSetCorePoolSize(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1))
	executor.SetCorePoolSize(3)
	require.Equal(t, 3, executor.CorePoolSize())
}

// TestCorePoolChange 测试 CorePool 的变化
func TestCorePoolChange(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1))
	start := time.Now()
	executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(110 * time.Millisecond)
	})
	f, _ := executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(110 * time.Millisecond)
	})
	f.Get()
	elapsed := time.Since(start)
	elapsedMs := elapsed.Milliseconds()
	require.Greater(t, elapsedMs, int64(200))
	//修改核心协程数后，执行时间小于200
	start = time.Now()
	executor.SetCorePoolSize(2)
	executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(110 * time.Millisecond)
	})
	f, _ = executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(110 * time.Millisecond)
	})
	f.Get()
	elapsed = time.Since(start)
	elapsedMs = elapsed.Milliseconds()
	require.Less(t, elapsedMs, int64(200))
}

// TestSetMaxPoolSize 测试 SetMaxPoolSize 和 MaxPoolSize方法
func TestSetMaxPoolSize(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1), WithMaxPoolSize(1))
	executor.SetMaxPoolSize(3)
	require.Equal(t, 3, executor.MaxPoolSize())
}

// TestMaxPoolSizeChange 测试 MaxPoolSize 的变化
func TestMaxPoolSizeChange(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1), WithMaxPoolSize(0), WithTaskBlockingQueue(NewLinkedBlockingRunnableQueue(1)))
	start := time.Now()
	executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(110 * time.Millisecond)
	})
	f, _ := executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(110 * time.Millisecond)
	})
	f.Get()
	elapsed := time.Since(start)
	elapsedMs := elapsed.Milliseconds()
	require.Greater(t, elapsedMs, int64(200))
	//修改最大协程数后（非核心协程数=最大协程数-核心县城出），执行3个的执行时间小于300
	start = time.Now()
	executor.SetMaxPoolSize(2)
	executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(110 * time.Millisecond)
	})
	executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(110 * time.Millisecond)
	})
	f, _ = executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(110 * time.Millisecond)
	})
	f.Get()
	elapsed = time.Since(start)
	elapsedMs = elapsed.Milliseconds()
	require.Less(t, elapsedMs, int64(300))
}

// TestSetKeepAliveTime 测试 KeepAliveTime 和 SetKeepAliveTime 方法
func TestSetKeepAliveTime(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1), WithKeepAliveTime(1*time.Second))
	executor.SetKeepAliveTime(2 * time.Second)
	require.Equal(t, 2*time.Second, executor.KeepAliveTime())
}

// TestKeepAliveTimeChange 测试 KeepAliveTime 的 变化
func TestKeepAliveTimeChange(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(0), WithMaxPoolSize(1), WithTaskBlockingQueue(NewLinkedBlockingRunnableQueue(0)), WithKeepAliveTime(100*time.Millisecond))
	f, _ := executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(110 * time.Millisecond)
	})
	f.Get()
	time.Sleep(200 * time.Millisecond)
	workerLen := len(executor.WorkerInfos())
	require.Equal(t, 0, workerLen)
	//修改活跃时间
	executor.SetKeepAliveTime(300 * time.Millisecond)
	f, _ = executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(110 * time.Millisecond)
	})
	f.Get()
	time.Sleep(200 * time.Millisecond)
	workerLen = len(executor.WorkerInfos())
	require.Equal(t, 1, workerLen)
}

// TestSetBlockingTaskQueueCapacity 测试 SetBlockingTaskQueueCapacity 和 方法
func TestSetBlockingTaskQueueCapacity(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(0), WithMaxPoolSize(0), WithTaskBlockingQueue(NewLinkedBlockingRunnableQueue(0)))
	executor.SetBlockingTaskQueueCapacity(1)
	blockingTaskQueueCapacity := executor.BlockingTaskQueueCapacity()
	require.Equal(t, 1, blockingTaskQueueCapacity)
}

// 测试 BlockingTaskQueueCapacityChange 的变化
func TestBlockingTaskQueueCapacityChange(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(0), WithMaxPoolSize(0), WithTaskBlockingQueue(NewLinkedBlockingRunnableQueue(0)))
	_, err := executor.Execute(func() {})
	require.Equal(t, ErrTaskQueueFull, err)
	executor.SetBlockingTaskQueueCapacity(1)
	_, err = executor.Execute(func() {})
	require.Equal(t, nil, err)
}

// TestActiveCount 测试 ActiveCount 方法
func TestActiveCount(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1), WithKeepAliveTime(20*time.Second))
	require.Equal(t, 0, executor.ActiveCount())
	executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(200 * time.Millisecond)
	})
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, executor.ActiveCount())

}

// TestTaskCount 测试 TaskCount 方法
func TestTaskCount(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1), WithKeepAliveTime(20*time.Second))
	require.Equal(t, 0, executor.TaskCount())
	executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(200 * time.Millisecond)
	})
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, executor.TaskCount())
}

// TestCompletedTaskCount 测试 CompletedTaskCount 方法
func TestCompletedTaskCount(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1), WithKeepAliveTime(20*time.Second))
	require.Equal(t, int64(0), executor.CompletedTaskCount())
	f, _ := executor.Execute(func() {
	})
	f.Get()
	require.Equal(t, int64(1), executor.CompletedTaskCount())
}

// TestWorkerInfos 测试 WorkerInfos 方法
func TestWorkerInfos(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1), WithKeepAliveTime(20*time.Second))
	require.Equal(t, 0, len(executor.WorkerInfos()))
	f, _ := executor.Execute(func() {
	})
	f.Get()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, len(executor.WorkerInfos()))
	workerInfo := executor.WorkerInfos()[0]
	require.Equal(t, "executor-pool-1", workerInfo.WorkerName)
	require.Equal(t, uint8(workerStateIdle), workerInfo.WorkerState)
	require.Equal(t, true, workerInfo.IsCore)
	require.Equal(t, 1, workerInfo.CompletedTaskCount)
}

// TestExecutorListener 测试 Executor的监听 方法
func TestExecutorListener(t *testing.T) {
	executor := NewExecutor(WithCorePoolSize(1), WithKeepAliveTime(20*time.Second))
	require.Equal(t, 0, len(executor.WorkerInfos()))
	number := atomic.Int32{}
	f, _ := executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(100 * time.Millisecond)
	})
	f.(ListenableFuture).AddListener(func(future Future) {
		if future.IsSuccess() {
			number.Add(1)
		}
	})
	time.Sleep(150 * time.Millisecond)
	require.Equal(t, int32(1), number.Load())
	f, _ = executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(100 * time.Millisecond)
	})
	f, _ = executor.Execute(func() {
		// 模拟任务执行
		time.Sleep(100 * time.Millisecond)
	})
	f.(ListenableFuture).AddListener(func(future Future) {
		if future.IsSuccess() {
			number.Add(1)
		} else {
			number.Add(-1)
		}
	})
	f.Cancel()
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(0), number.Load())

}
