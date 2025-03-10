# executor

## 介绍

executor 项目是基于Go的goroutine及Go的并发编程实现的协程复用池，一比一复刻Java线程池的大部分功能，并能对大量协程进行管理和监控，复用goroutine以达到减少GC开销的目的，并提供动态修改协程池参数及延迟调度的功能。

## 功能

- 协程复用，节省高并发情况下协程频繁创建和销毁开销
- 支持多种核心参数配置（核心协程数、最大协程数、非核心协程活跃时间、阻塞队列、拒绝策略、恐慌处理器、执行扩展器、协程命名工厂）
- 默认提供多种拒绝策略（放弃执行、阻塞拒绝、抛弃旧任务、当前协程执行），也可自行实现
- 提供核心参数动态配置，支持运行时修改（核心协程数、最大协程数、非核心协程活跃时间、阻塞队列大小）
- 支持异步结果（Future）及超时处理，可对提交的任务进行监控和取消
- 支持任务监听，当任务完成时、取消时进行回调
- 提供默认的协程池，包括固定协程数量的协程池、单协程的协程池、无限缓存的协程池
- 支持协程命名功能，并提供协程池及工作池监控信息，便于数据统计
- 支持协程池渐进式关闭，更友好的释放资源
- 支持单次的延迟任务、固定频率的延迟任务、固定延迟的延迟任务
- 代码全中文注释，且包含详细的测试用例



## 快速开始
### 导入
```
go get github.com/xzc-coder/executor
```

### 普通协程池

```
func main() {
	//创建
	pool := executor.NewExecutor(executor.WithCorePoolSize(1))
	//添加任务，无返回值
	pool.Execute(func() {
		//模拟任务运行
		time.Sleep(100 * time.Millisecond)
	})
	//添加任务，有返回值，获取future
	f, err := pool.Submit(func() any {
		//模拟任务运行
		time.Sleep(100 * time.Millisecond)
		return "test"
	})
	if err == nil {
		//阻塞至完成并获取返回值
		result := f.Get()
		//打印返回值
		fmt.Println(result)
	}
	//结束任务，渐进式关闭
	pool.Shutdown()
}
```

### 延迟协程池

```
func main() {
	//创建
	schedule := executor.NewSchedule(executor.WithCorePoolSize(1))
	//添加延迟任务，无返回值，100毫秒后运行一次
	schedule.Schedule(func() {
		time.Sleep(100 * time.Millisecond)
	}, 100*time.Millisecond)
	//添加任务，有返回值，100毫秒后运行一次，获取future
	f, err := schedule.Schedules(func() any {
		//模拟任务运行
		time.Sleep(100 * time.Millisecond)
		return "test"
	}, 100*time.Millisecond)
	if err == nil {
		//阻塞至完成并获取返回值
		result := f.Get()
		//打印返回值
		fmt.Println(result)
	}
	//添加固定速率任务，延迟100毫秒执行，然后每隔100毫秒执行一次（不管上次任务是否完成）
	schedule.ScheduleAtFixedRate(func() {
		fmt.Println(time.Now())
	}, 100*time.Millisecond, 100*time.Millisecond)

	//添加固定速率任务，延迟100毫秒执行，等任务执行完后，每隔100毫秒执行一次
	schedule.ScheduleWithFixedDelay(func() {
		fmt.Println(time.Now())
	}, 100*time.Millisecond, 100*time.Millisecond)

	time.Sleep(1000 * time.Millisecond)
	//结束任务，渐进式关闭
	schedule.Shutdown()
}
```

## 协程池配置

### corePoolSize（核心协程数）

核心协程数，当提交任务时，当前协程数量达不到该值时，会开启新协程。

通过 **WithCorePoolSize(corePoolSize int) **配置，默认值0.

### MaxPoolSize（最大协程数）

最大协程数，当核心协程数达到最大值，且阻塞队列任务已满时，会开启非核心协程处理任务，非核心协程的数量=最大协程数-核心协程数。

通过 **WithMaxPoolSize(maxPoolSize int)** 配置，默认值0，且不能小于核心协程数

### KeepAliveTime（非核心协程活跃时间）

非核心协程活跃时间，当开启非核心协程处理任务时，空闲时间超过该值时则会关闭该协程。

通过 **WithKeepAliveTime(keepAliveTime time.Duration)** 配置，默认值0

### TaskBlockingQueue（任务阻塞队列）

阻塞队列，用于存储执行任务的阻塞队列，目前提供基于数组和chan实现的 **ChanBlockingQueue** ，基于链表的 **LinkedBlockingQueue**  ；**ChanBlockingQueue** 不支持修改队列任务大小，并且初始化时就已分配内存；**LinkedBlockingQueue**  支持修改队列任务大小，不预先分配内存；

**ChanBlockingQueue**  适用于小量且不修改任务大小的协程池；**LinkedBlockingQueue**  适用于大量且要修改任务大小的协程池。

```
//通过该方式创建时，当小于等于1W的队列大小时，会使用ChanBlockingQueue，大于1W的队列大小会使用LinkedBlockingQueue
executor.WithTaskBlockingQueueDefault(1000)
//指定使用链表阻塞队列
executor.WithTaskBlockingQueue(executor.NewLinkedBlockingRunnableQueue(1000))
//指定使用数组阻塞队列
executor.WithTaskBlockingQueue(executor.NewChanBlockingRunnableQueue(1000))
```

默认值为 **NewLinkedBlockingRunnableQueue(math.MaxInt)**，即无限大小的链表阻塞队列

### ExecuteExpand（执行扩展器）

执行扩展器，在每个任务的执行前后进行埋点。

```
type ExecuteExpand interface {
	// TaskBeforeExecute 任务执行之前
	TaskBeforeExecute(workerName string, runnable Runnable)
	// TaskAfterExecute 任务执行之后
	TaskAfterExecute(workerName string, runnable Runnable)
}
```

通过 **WithExecuteExpand(executeExpand ExecuteExpand)** 配置，实现 **ExecuteExpand** 接口即可，默认值 无

### GoroutineNameFactory（协程命名工厂）

协程命名工厂，用于给协程中执行任务的 worker 命名。

```
//传入该方法的定义即可，number 每次新建都会加1,
type GoroutineNameFactory func(number int) string
```

通过 **WithGoroutineNameFactory(goroutineNameFactory GoroutineNameFactory)** 配置，默认值 **executor-pool-{number}**

### RejectedHandler（拒绝处理器）

拒绝策略，当核心线程数、阻塞任务队列及最大线程数都达到最大值时如何处理，提供四种 **AbortPolicy**、**BlockPolicy**、**CallerRunsPolicy**、**DiscardOldestPolicy**

1. **AbortPolicy**：放弃任务执行，直接返回错误信息，默认的拒绝策略
2. **BlockPolicy**：阻塞调用方直到任务队列有空余
3. **CallerRunsPolicy**：用当前提交的协程去进行执行
4. **DiscardOldestPolicy**：丢弃最老的任务，然后再将提交的任务尝试放入

```
//传入该方法的定义，即可自定义拒绝策略
type RejectedHandler func(Runnable, Executor) error
```

通过 **WithRejectedHandler(rejectedHandler RejectedHandler)** 配置，默认值 **AbortPolicy**

### PanicHandler（恐慌处理器）

恐慌处理器，当提交的任务发生恐慌时，如何处理，默认控制台打印，最好自己实现或不抛出恐慌

```
//传入该方法的定义，即可自定义恐慌处理
type PanicHandler func(runnable Runnable, workerInfo *WorkerInfo, v any)
```

通过 **WithPanicHandler(panicHandler PanicHandler)** 配置，默认值 控制台打印

## 异步结果（Future）

### API说明

```
// Cancel 取消任务 bool 是否取消成功
Cancel() bool

// IsCancelled 是否任务被取消
IsCancelled() bool

// IsDone 任务是否完成，取消、异常、完成都算完成
IsDone() bool

// IsSuccess 任务是否成功执行，只有正常完成才算成功
IsSuccess() bool

// PanicError 获取恐慌信息，当发生恐慌时才有
PanicError() any

// Get 阻塞等待至任务完成
Get() any

// GetTimeout 阻塞等待至任务完成或超时
GetTimeout(timeout time.Duration) any
```

### 监听器

监听器只有普通协程池的异步结果实现了，延迟协程池中未实现。

```
func main() {
	//创建
	pool := executor.NewExecutor(executor.WithCorePoolSize(1))
	//添加任务，无返回值
	f, _ := pool.Execute(func() {
		//模拟任务运行
		time.Sleep(100 * time.Millisecond)
	})
	//添加监听
	f.(executor.ListenableFuture).AddListener(func(future executor.Future) {
		if future.IsDone() {
			fmt.Println("test AddListener")
		}
	})
	//结束任务，渐进式关闭
	pool.Shutdown()
}
```

需要将Future强转为ListenableFuture，即可添加监听器。

## 示例

### 普通协程池

#### 创建

```
//执行扩展器
type executeExpandTest struct {
}

func (e executeExpandTest) TaskBeforeExecute(workerName string, runnable Runnable) {
	fmt.Println("TaskBeforeExecuteTest")
}

func (e executeExpandTest) TaskAfterExecute(workerName string, runnable Runnable) {
	fmt.Println("TaskAfterExecuteTest")
}
//协程命名工厂
var goroutineNameFactory = func(number int) string {
	return "test-" + strconv.Itoa(number)
}
//恐慌处理器
var panicHandler = func(runnable Runnable, workerInfo *WorkerInfo, v any) {
	fmt.Println("panicHandlerTest")
	panicHandlerNumber.Add(1)
}
//拒绝策略
var rejectedHandler = func(Runnable, Executor) error {
	return errors.New("test")
}

func main() {
	linkedBlockingQueue := executor.NewLinkedBlockingRunnableQueue(10)
	//设置所有参数创建协程池
	pool := executor.NewExecutor(executor.WithCorePoolSize(2),
		executor.WithMaxPoolSize(3),
		executor.WithKeepAliveTime(10*time.Second),
		executor.WithTaskBlockingQueue(linkedBlockingQueue),
		executor.WithGoroutineNameFactory(goroutineNameFactory),
		executor.WithRejectedHandler(rejectedHandler),
		executor.WithExecuteExpand(&executeExpandTest{}),
		executor.WithPanicHandler(panicHandler))
	//添加任务，无返回值
	pool.Execute(func() {
		//模拟任务运行
		time.Sleep(100 * time.Millisecond)
	})
	//结束任务，渐进式关闭
	pool.Shutdown()
}
```

#### 提交任务

##### API

```
// Execute 提交一个无返回值的任务  Future 异步结果 error 异常信息
Execute(job func()) (Future, error)

// ExecuteRunnable 提交一个无返回值的任务  Future 异步结果 error 异常信息
ExecuteRunnable(runnable Runnable) (Future, error)

// Submit 提交一个有返回值的任务 Future 异步结果 error 异常信息
Submit(job func() any) (Future, error)

// SubmitCallable 提交一个有返回值的任务 Future 异步结果 error 异常信息
SubmitCallable(callable Callable) (Future, error)
```

##### 不带返回值

```
func main() {
	//创建
	pool := executor.NewExecutor(executor.WithCorePoolSize(2))
	//提交任务，不带返回值
	future,err := pool.Execute(func() {
		time.Sleep(100 * time.Millisecond)
	})
	if err == nil {
		//等待执行完毕
		future.Get()
	}
	//渐进式关闭
	pool.Shutdown()
}
```

##### 带返回值

```
func main() {
	//创建
	pool := executor.NewExecutor(executor.WithCorePoolSize(2))
	//提交任务，不带返回值
	future, err := pool.Submit(func() any {
		time.Sleep(100 * time.Millisecond)
		return "test"
	})
	if err == nil {
		//等待执行完毕
		name := future.Get()
		fmt.Println(name)
	}
	//渐进式关闭
	pool.Shutdown()
}
```

#### 动态配置

API

```
// SetCorePoolSize 设置核心协程数
SetCorePoolSize(corePoolSize int)

// SetMaxPoolSize 设置最大协程数
SetMaxPoolSize(maxPoolSize int)

// SetKeepAliveTime 设置非核心协程的活跃时间
SetKeepAliveTime(keepAliveTime time.Duration)

// SetBlockingTaskQueueCapacity 设置阻塞任务队列的容量，如果小于当前的任务数，会设置失败，目前仅支持链表的阻塞队列设置
SetBlockingTaskQueueCapacity(capacity int) bool
```

示例

```
func main() {
	//创建
	pool := executor.NewExecutor(executor.WithCorePoolSize(2))
	//修改核心协程数
	pool.SetCorePoolSize(100)
	//修改最大协程数
	pool.SetMaxPoolSize(200)
	//修改非核心协程活跃时间
	pool.SetKeepAliveTime(5 * time.Second)
	//修改阻塞队列大小，仅支持链表的阻塞队列
	pool.SetBlockingTaskQueueCapacity(1000)
	//渐进式关闭
	pool.Shutdown()
}
```

#### 数据统计

API

```
// CorePoolSize 获取核心协程数
CorePoolSize() int

// MaxPoolSize 获取最大协程数
MaxPoolSize() int

// KeepAliveTime 获取非核心协程数的活跃时间
KeepAliveTime() time.Duration

// BlockingTaskQueueCapacity 获取当前的阻塞任务队列的容量
BlockingTaskQueueCapacity() int

// ActiveCount 协程池中活跃的协程数量
ActiveCount() int

// TaskCount 协程池中的任务总数，大概数量
TaskCount() int

// CompletedTaskCount 协程池中完成的任务数
CompletedTaskCount() int64

// WorkerInfos 所有协程的信息
WorkerInfos() []*WorkerInfo
```

#### 关闭

API

```
// Shutdown 渐进式关闭协程池，直至所有任务执行完毕
Shutdown()

// AwaitTermination 等待至关闭完成或超时，如果返回ok表示关闭完成
AwaitTermination(timeout time.Duration) bool

// ShutdownNow 立马关闭，返回剩余的任务
ShutdownNow() []Runnable

// IsShutdown 协程池是否是关闭
IsShutdown() bool

// IsTerminated 协程池是否终止
IsTerminated() bool
```



### 延迟协程池

#### 创建

```
linkedBlockingQueue := executor.NewLinkedBlockingRunnableQueue(10)
//设置所有参数创建延迟协程池，参数内容同上
schedule := executor.NewSchedule(executor.WithCorePoolSize(2),
	executor.WithMaxPoolSize(3),
	executor.WithKeepAliveTime(10*time.Second),
	executor.WithTaskBlockingQueue(linkedBlockingQueue),
	executor.WithGoroutineNameFactory(goroutineNameFactory),
	executor.WithRejectedHandler(rejectedHandler),
	executor.WithExecuteExpand(&executeExpandTest{}),
	executor.WithPanicHandler(panicHandler))
```

#### 使用

##### 单次延迟

API

```
// Schedule 提交一个无返回值的延迟任务 ScheduledFuture 异步结果 error 异常信息
Schedule(job func(), delay time.Duration) (ScheduledFuture, error)

// ScheduleRunnable 提交一个无返回值的延迟任务  ScheduledFuture 异步结果 error 异常信息
ScheduleRunnable(runnable Runnable, delay time.Duration) (ScheduledFuture, error)

// Schedules  提交一个有返回值的延迟任务  ScheduledFuture 异步结果 error 异常信息
Schedules(job func() any, delay time.Duration) (ScheduledFuture, error)

// SchedulesCallable 提交一个有返回值的延迟任务  ScheduledFuture 异步结果 error 异常信息
SchedulesCallable(callable Callable, delay time.Duration) (ScheduledFuture, error)
```

示例

```
//不带返回值
//创建
schedule := executor.NewSchedule(executor.WithCorePoolSize(2))
f,err := schedule.Schedule(func() {
	time.Sleep(100 * time.Millisecond)
}, 100*time.Millisecond)
if err == nil {
	//阻塞至完成
	f.Get()
}
//带返回值
f, err = schedule.Schedules(func() any {
	time.Sleep(100 * time.Millisecond)
	return "test"
}, 100*time.Millisecond)
if err == nil {
	//阻塞至完成
	result := f.Get()
	fmt.Println(result)
}
```

##### 固定速率

API

```
// ScheduleAtFixedRate 以固定的速率执行任务，不管上一个任务是否完成 ScheduledFuture 异步结果 error 异常信息
ScheduleAtFixedRate(job func(), initialDelay time.Duration, delay time.Duration) (ScheduledFuture, error)

// ScheduleAtFixedRateRunnable 以固定的速率执行任务，不管上一个任务是否完成 ScheduledFuture 异步结果 error 异常信息
ScheduleAtFixedRateRunnable(runnable Runnable, initialDelay time.Duration, delay time.Duration) (ScheduledFuture, error)
```

示例

```
schedule := executor.NewSchedule(executor.WithCorePoolSize(1))
//固定速率执行，不会等到上一次执行完成再计算延迟时间去执行，第一次执行的延迟为50毫秒，后面都是100毫秒
schedule.ScheduleAtFixedRate(func() {
		fmt.Println(time.Now())
		time.Sleep(100 * time.Millisecond)
	}, 50*time.Millisecond, 100*time.Millisecond)
```

##### 固定延迟

API

```
// ScheduleWithFixedDelay 以固定的延迟时间执行任务，上一个任务完成后，会等待设定的延迟时间，然后再启动下一个任务。
ScheduleWithFixedDelay(job func(), initialDelay time.Duration, delay time.Duration) (ScheduledFuture, error)

// ScheduleWithFixedDelayRunnable 以固定的延迟时间执行任务，上一个任务完成后，会等待设定的延迟时间，然后再启动下一个任务。
ScheduleWithFixedDelayRunnable(runnable Runnable, initialDelay time.Duration, delay time.Duration) (ScheduledFuture, error)
```

示例

```
schedule := executor.NewSchedule(executor.WithCorePoolSize(1))
//固定延迟执行，会等到上一次执行完毕后，再计算延迟时间去执行，第一次执行的延迟为50毫秒，后面都是100毫秒
schedule.ScheduleWithFixedDelay(func() {
		fmt.Println(time.Now())
		time.Sleep(100 * time.Millisecond)
	}, 50*time.Millisecond, 100*time.Millisecond)
```



## 原理

该项目参考Java协程池 **ThreadPoolExecutor** 实现

运行流程如下图：

<img width="480" alt="1741615245099" src="https://github.com/user-attachments/assets/a8ac6cfe-f494-4e13-b166-03a507a10fb6" />

