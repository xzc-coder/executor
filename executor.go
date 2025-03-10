package executor

import (
	"context"
	"errors"
	"log"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// PanicHandler 恐慌处理器
type PanicHandler func(runnable Runnable, workerInfo *WorkerInfo, v any)

// GoroutineNameFactory 协程命名工厂,num是当前号码，从1开始递增
type GoroutineNameFactory func(number int) string

// 默认值
var (
	//默认的协程命名工厂
	defaultNameGoroutineFactory = func(number int) string {
		return "executor-pool-" + strconv.Itoa(number)
	}
	//默认的拒绝策略,使用放弃执行返回异常
	defaultRejectedHandler = AbortPolicy
	//默认的恐慌处理器，控制台打印
	defaultPanicHandler = func(runnable Runnable, workerInfo *WorkerInfo, v any) {
		log.Printf("workerName: %v , there was a panic,value: %v \n", workerInfo, v)
	}
)

// 协程池状态
const (
	//运行状态，可以提交任务
	executorStateRunning = iota
	//关闭状态，不可以提交任务，会执行完队列中的任务
	executorStateShutdown
	//结束状态，不可以提交任务，并且不执行队列中的任务
	executorStateTerminated
)

// 最小池数量
const minPoolSize = 0

// NewExecutor 新建一个协程池
func NewExecutor(opts ...Option) PoolExecutor {
	//配置参数
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}
	//核心协程数
	corePoolSize := Max(options.CorePoolSize, minPoolSize)
	//最大协程数，不能比核心协程数小
	maxPoolSize := Max(options.MaxPoolSize, corePoolSize)
	//任务阻塞队列，默认是链表的
	taskBlockingQueue := options.TaskBlockingQueue
	if options.TaskBlockingQueue == nil {
		taskBlockingQueue = NewLinkedBlockingRunnableQueue(math.MaxInt)
	}
	//非核心协程活跃时间
	var keepAliveTime time.Duration = 0
	if options.KeepAliveTime > 0 {
		keepAliveTime = options.KeepAliveTime
	}
	//拒绝策略
	rejectedHandler := options.RejectedHandler
	if rejectedHandler == nil {
		rejectedHandler = defaultRejectedHandler
	}
	//恐慌处理器
	panicHandler := options.PanicHandler
	if panicHandler == nil {
		panicHandler = defaultPanicHandler
	}
	//命名工厂
	goroutineNameFactory := options.GoroutineNameFactory
	if goroutineNameFactory == nil {
		goroutineNameFactory = defaultNameGoroutineFactory
	}
	//执行扩展
	executeExpand := options.ExecuteExpand
	//shutdown的上下文，用于唤醒AwaitTermination阻塞的协程
	ctx, cancel := context.WithCancel(context.Background())
	shutdownCtxInfo := &contextInfo{ctx: ctx, cancel: cancel}
	//创建协程池
	executor := &GoroutinePoolExecutor{
		state:                executorStateRunning,
		corePoolSize:         corePoolSize,
		maxPoolSize:          maxPoolSize,
		keepAliveTime:        keepAliveTime,
		executeExpand:        executeExpand,
		goroutineNameFactory: goroutineNameFactory,
		workers:              make([]*worker, 0),
		nonCoreWorkers:       make([]*worker, 0),
		taskBlockingQueue:    taskBlockingQueue,
		rejectedHandler:      rejectedHandler,
		panicHandler:         panicHandler,
		shutdownCtxInfo:      shutdownCtxInfo,
	}
	return executor
}

// NewFixedGoroutinePool 创建一个固定协程数量的协程池
func NewFixedGoroutinePool(goroutineNumber int) PoolExecutor {
	return NewExecutor(WithCorePoolSize(goroutineNumber), WithMaxPoolSize(goroutineNumber))
}

// NewSingleGoroutineExecutor 创建一个单协程的协程池
func NewSingleGoroutineExecutor() PoolExecutor {
	return NewExecutor(WithCorePoolSize(1))
}

// NewCachedGoroutinePool 创建一个有任务时就创建协程处理的协程池，并且尽可能的复用之前的
func NewCachedGoroutinePool() PoolExecutor {
	return NewExecutor(WithCorePoolSize(0), WithMaxPoolSize(math.MaxInt32), WithKeepAliveTime(60*time.Second), WithTaskBlockingQueue(NewChanBlockingQueue[Runnable](0)))
}

type Pool struct {
	*GoroutinePoolExecutor
}

func (p *Pool) Executor() Executor {
	return p.GoroutinePoolExecutor
}

func (p *Pool) PoolExecutor() PoolExecutor {
	return p.GoroutinePoolExecutor
}

// GoroutinePoolExecutor 协程池，用于将协程池化，复用协程执行任务，避免重复创建，实现了 Executor 接口
// 协程池中不会持有任何worker的锁操作，只通过CAS保证，避免死锁
type GoroutinePoolExecutor struct {
	//协程池工厂号，递增的，每创建一个协程时 +1，用于给worker命名
	goroutineFactoryNum atomic.Int32
	//协程命名工厂，通过该方式给worker命名
	goroutineNameFactory GoroutineNameFactory
	//状态
	state uint8
	//读写锁
	rwMux sync.RWMutex
	//核心协程数
	corePoolSize int
	//最大协程数
	maxPoolSize int
	//非核心协程的活跃时间
	keepAliveTime time.Duration
	//执行扩展，任务执行前后扩展记录
	executeExpand ExecuteExpand
	//核心协程切片，只在锁中使用
	workers []*worker
	//非核心协程切片，只在锁中使用
	nonCoreWorkers []*worker
	//任务阻塞队列
	taskBlockingQueue BlockingQueue[Runnable]
	//拒绝策略，当核心协程数和非核心协程数以及任务队列都达到最大值时，要怎么处理，rejected.go 中提供了4个实现策略
	rejectedHandler RejectedHandler
	//恐慌处理器，任务发生恐慌时如何处理，默认控制台打印
	panicHandler PanicHandler
	//协程池在运行过程中同时存在的最大协程数量
	largestPoolSize int
	//完成的任务数量，大概值
	completedTaskCount atomic.Int64
	//shutdown的上下文，用于唤醒AwaitTermination阻塞的协程
	shutdownCtxInfo *contextInfo
	//只执行一次
	closeOnce sync.Once
}

func (g *GoroutinePoolExecutor) Execute(job func()) (Future, error) {
	future, err := g.doExecuteLock(callable(FuncRunnable(job)))
	return future, err
}

func (g *GoroutinePoolExecutor) ExecuteRunnable(runnable Runnable) (Future, error) {
	return g.doExecuteLock(callable(runnable))
}

func (g *GoroutinePoolExecutor) Submit(job func() any) (Future, error) {
	future, err := g.doExecuteLock(FuncCallable(job))
	return future, err
}

func (g *GoroutinePoolExecutor) SubmitCallable(callable Callable) (Future, error) {
	return g.doExecuteLock(callable)
}

func (g *GoroutinePoolExecutor) CorePoolSize() int {
	g.rwMux.RLock()
	defer g.rwMux.RUnlock()
	return g.corePoolSize
}

func (g *GoroutinePoolExecutor) SetCorePoolSize(corePoolSize int) {
	g.rwMux.Lock()
	defer g.rwMux.Unlock()
	//不能小于0
	corePoolSize = Max(minPoolSize, corePoolSize)
	currentWorkerNum := len(g.workers)
	// 如果当前的核心协程池的数量大于设值后的核心协程数数量，则需要关闭
	if currentWorkerNum > corePoolSize {
		//关闭多余的
		closeWorkerNum := corePoolSize - currentWorkerNum
		//排序，让空闲的在最前面
		sort.Slice(g.workers, func(i, j int) bool {
			return g.workers[i].state.Load() < g.workers[j].state.Load()
		})
		closeWorkerSlice := g.workers[0:closeWorkerNum]
		for _, w := range closeWorkerSlice {
			w.casClose(workerTerminatedClose, true)
		}
	}
	g.corePoolSize = corePoolSize
	//核心协程数超过最大协程数时，设置为核心协程数
	if g.corePoolSize > g.maxPoolSize {
		g.maxPoolSize = g.corePoolSize
	}
}

func (g *GoroutinePoolExecutor) SetMaxPoolSize(maxPoolSize int) {
	g.rwMux.Lock()
	defer g.rwMux.Unlock()
	//最大协程数不能小于设置的核心协程数
	maxPoolSize = Max(maxPoolSize, g.corePoolSize)
	currentWorkerNum := len(g.nonCoreWorkers)
	//当前的非核心协程数大于设置后的非核心协程数，则需要关闭
	if maxNonCoreWorkerNum := maxPoolSize - g.corePoolSize; currentWorkerNum > maxNonCoreWorkerNum {
		//关闭多余的
		closeWorkerNum := currentWorkerNum - maxNonCoreWorkerNum
		//排序，让空闲的在最前面
		sort.Slice(g.nonCoreWorkers, func(i, j int) bool {
			return g.nonCoreWorkers[i].state.Load() < g.nonCoreWorkers[j].state.Load()
		})
		closeWorkerSlice := g.nonCoreWorkers[0:closeWorkerNum]
		for _, w := range closeWorkerSlice {
			w.casClose(workerTerminatedClose, true)
		}
	}
	g.maxPoolSize = maxPoolSize
}

func (g *GoroutinePoolExecutor) MaxPoolSize() int {
	g.rwMux.RLock()
	defer g.rwMux.RUnlock()
	return g.maxPoolSize
}

func (g *GoroutinePoolExecutor) SetKeepAliveTime(keepAliveTime time.Duration) {
	g.rwMux.Lock()
	defer g.rwMux.Unlock()
	if keepAliveTime <= 0 {
		g.keepAliveTime = 0
	} else {
		g.keepAliveTime = keepAliveTime
	}
}

func (g *GoroutinePoolExecutor) Shutdown() {
	g.rwMux.Lock()
	defer g.rwMux.Unlock()
	g.doClose(false)
}

func (g *GoroutinePoolExecutor) AwaitTermination(timeout time.Duration) (ok bool) {
	g.writeLockOperate(func() {
		g.doClose(false)
		//如果完成，则跳过超时处理
		if g.state == executorStateTerminated {
			timeout = 0
			ok = true
		}
	})
	//阻塞超时处不能加锁
	if timeout > 0 {
		t := time.NewTimer(timeout)
		select {
		//完成或者等待超时则唤醒
		case <-t.C:
		case <-g.shutdownCtxInfo.ctx.Done():
			t.Stop()
		}
		//超时后，判断是否成功
		g.readLockOperate(func() {
			ok = g.state == executorStateTerminated
		})
	}
	return ok
}

func (g *GoroutinePoolExecutor) ShutdownNow() []Runnable {
	g.rwMux.Lock()
	defer g.rwMux.Unlock()
	g.doClose(true)
	//返回剩余的任务
	runnableTasks := g.taskBlockingQueue.RemoveAll()
	return runnableTasks
}

func (g *GoroutinePoolExecutor) IsShutdown() bool {
	g.rwMux.RLock()
	defer g.rwMux.RUnlock()
	return g.state == executorStateShutdown || g.state == executorStateTerminated
}

func (g *GoroutinePoolExecutor) IsTerminated() bool {
	g.rwMux.RLock()
	defer g.rwMux.RUnlock()
	return g.state == executorStateTerminated
}

func (g *GoroutinePoolExecutor) ActiveCount() int {
	g.rwMux.RLock()
	defer g.rwMux.RUnlock()
	activeCount := 0
	for _, w := range g.workers {
		if w.state.Load() == workerStateWorking {
			activeCount++
		}
	}
	for _, w := range g.nonCoreWorkers {
		if w.state.Load() == workerStateWorking {
			activeCount++
		}
	}
	return activeCount
}

func (g *GoroutinePoolExecutor) TaskCount() int {
	return g.ActiveCount() + g.taskBlockingQueue.Size()

}

func (g *GoroutinePoolExecutor) CompletedTaskCount() int64 {
	return g.completedTaskCount.Load()
}

func (g *GoroutinePoolExecutor) KeepAliveTime() time.Duration {
	g.rwMux.RLock()
	defer g.rwMux.RUnlock()
	return g.keepAliveTime
}

func (g *GoroutinePoolExecutor) BlockingTaskQueueCapacity() int {
	return g.taskBlockingQueue.Capacity()
}

func (g *GoroutinePoolExecutor) SetBlockingTaskQueueCapacity(capacity int) bool {
	//不需要加锁，不改指针
	return g.taskBlockingQueue.Refresh(capacity)
}

func (g *GoroutinePoolExecutor) WorkerInfos() []*WorkerInfo {
	g.rwMux.RLock()
	defer g.rwMux.RUnlock()
	workerInfos := make([]*WorkerInfo, 0)
	for _, w := range g.workers {
		workerInfos = append(workerInfos, w.WorkerInfo())
	}
	for _, w := range g.nonCoreWorkers {
		workerInfos = append(workerInfos, w.WorkerInfo())
	}
	return workerInfos
}

// doExecuteLock 加锁执行提交的任务 error 异常信息 Future 未来的结果
func (g *GoroutinePoolExecutor) doExecuteLock(callable Callable) (future Future, err error) {
	//拒绝策略，需要在锁释放之后执行
	defer func() {
		if errors.Is(err, ErrNeedRejectedHandle) {
			taskFuture := future.(*TaskFuture)
			//如果拒绝策略实现返回错误，覆盖返回值的错误
			err = g.rejectedHandler(taskFuture, g)
		}
	}()
	g.rwMux.Lock()
	defer g.rwMux.Unlock()
	//状态检查，必须是运行状态才能添加
	err = g.runStateCheck()
	if err != nil {
		return nil, err
	}
	//生成一个TaskFuture
	taskFuture := NewTaskFuture(callable)
	if err != nil {
		return nil, err
	}
	//是否需要添加进阻塞队列中，如果是第一次启动协程的任务则不需要
	addToQueue := true
	//核心协程数没满，继续创建
	if len(g.workers) < g.corePoolSize {
		g.createWorker(true, taskFuture)
		addToQueue = false
	} else {
		//核心协程数满了，判断任务队列
		if g.taskBlockingQueue.IsFull() {
			//任务队列满了，判断非核心协程数
			if len(g.nonCoreWorkers) < g.nonCorePoolSize() {
				g.createWorker(false, taskFuture)
				addToQueue = false
			} else {
				//非核心协程数满了，需要拒绝策略处理，拒绝策略在defer中
				return taskFuture, ErrNeedRejectedHandle
			}
		}
	}
	//添加任务
	if addToQueue {
		err = g.addRunnableTask(taskFuture, 0)
	}
	return taskFuture, err
}

// addRunnableTask 添加一个任务到队列中
func (g *GoroutinePoolExecutor) addRunnableTask(runnable Runnable, timeout time.Duration) (err error) {
	//任务入队，等待执行
	ok := g.taskBlockingQueue.Offer(runnable, timeout)
	if !ok {
		//入队失败
		err = ErrTaskEnqueueFailed
	}
	return
}

// removeRunnableTask 移除一个任务出队列
func (g *GoroutinePoolExecutor) removeRunnableTask(timeout time.Duration) (Runnable, bool) {
	return g.taskBlockingQueue.Poll(timeout)
}

// writeLockOperate 加写锁进行操作
func (g *GoroutinePoolExecutor) writeLockOperate(f func()) {
	g.rwMux.Lock()
	defer g.rwMux.Unlock()
	f()
}

// readLockOperate 加读锁进行操作
func (g *GoroutinePoolExecutor) readLockOperate(f func()) {
	g.rwMux.RLock()
	defer g.rwMux.RUnlock()
	f()
}

// doClose 执行关闭协程池操作
func (g *GoroutinePoolExecutor) doClose(nowClose bool) {
	//如果已经终止，则不能改状态
	if g.state != executorStateTerminated {
		//修改状态
		if nowClose {
			g.state = executorStateTerminated
		} else {
			g.state = executorStateShutdown
		}
	} else {
		//已经是终止，则结束
		return
	}
	//关闭
	g.closeOnce.Do(func() {
		var closeType uint8 = workerShutdownClose
		if nowClose {
			closeType = workerTerminatedClose
		}
		//如果关闭时worker和任务都是空，则不用给worker发送关闭信号
		if g.workerAndTaskEmpty() {
			g.state = executorStateTerminated
			if chanBlockingQueue, ok := g.taskBlockingQueue.(*ChanBlockingQueue[Runnable]); ok {
				chanBlockingQueue.Close()
			}
			g.shutdownCtxInfo.cancel()
		} else {
			//worker发送关闭信号
			for _, w := range g.workers {
				w.casClose(closeType, true)
			}
			for _, w := range g.nonCoreWorkers {
				w.casClose(closeType, true)
			}
		}
	})
}

// keepAliveTimeLock 加锁获取非核心协程活跃时间
func (g *GoroutinePoolExecutor) keepAliveTimeLock() time.Duration {
	g.rwMux.RLock()
	defer g.rwMux.RUnlock()
	return g.keepAliveTime
}

// nonCorePoolSize 获取非核心协程数的大小
func (g *GoroutinePoolExecutor) nonCorePoolSize() int {
	return g.maxPoolSize - g.corePoolSize
}

// runStateCheck 运行状态检查，可继续运行则返回nil
func (g *GoroutinePoolExecutor) runStateCheck() error {
	switch g.state {
	case executorStateRunning:
		return nil
	case executorStateShutdown:
		return ErrExecutorShutdown
	case executorStateTerminated:
		return ErrExecutorTerminated
	default:
		return ErrExecutorUnknownState
	}
}

// runStateCheck 运行状态加锁检查，可继续运行则返回nil
func (g *GoroutinePoolExecutor) runStateCheckLock() error {
	g.rwMux.RLock()
	defer g.rwMux.RUnlock()
	switch g.state {
	case executorStateRunning:
		return nil
	case executorStateShutdown:
		return ErrExecutorShutdown
	case executorStateTerminated:
		return ErrExecutorTerminated
	default:
		return ErrExecutorUnknownState
	}
}

// createWorker 创建一个协程worker执行任务
func (g *GoroutinePoolExecutor) createWorker(core bool, firstTask Runnable) {
	//创建
	w := createWorker(g, g.goroutineNameFactory(int(g.goroutineFactoryNum.Add(1))), core)
	if core {
		g.workers = append(g.workers, w)
	} else {
		g.nonCoreWorkers = append(g.nonCoreWorkers, w)
	}
	//统计池中最大的数量
	largestPoolSize := len(g.workers) + len(g.nonCoreWorkers)
	if largestPoolSize > g.largestPoolSize {
		g.largestPoolSize = largestPoolSize
	}
	w.startGoroutine(firstTask)
}

// removeWorker 移除一个协程worker，只有当worker终止时，才移除，只发送通知信号等待worker终止后调用移除
func (g *GoroutinePoolExecutor) removeWorker(core bool, removeWorker *worker) {
	if core {
		for index, w := range g.workers {
			if w == removeWorker {
				g.workers = append(g.workers[:index], g.workers[index+1:]...)
				return
			}
		}
	} else {
		for index, w := range g.nonCoreWorkers {
			if w == removeWorker {
				g.nonCoreWorkers = append(g.nonCoreWorkers[:index], g.nonCoreWorkers[index+1:]...)
				return
			}
		}
	}

}

// workerAndTaskEmpty 判断任何和worker是否都为空
func (g *GoroutinePoolExecutor) workerAndTaskEmpty() bool {
	if g.taskBlockingQueue.IsEmpty() && len(g.workers) == 0 && len(g.nonCoreWorkers) == 0 {
		return true
	} else {
		return false
	}
}

// handleWorkerCloseLock 处理worker关闭
func (g *GoroutinePoolExecutor) handleWorkerCloseLock(worker *worker) {
	g.rwMux.Lock()
	defer g.rwMux.Unlock()
	//移除worker
	g.removeWorker(worker.core, worker)
	//如果是通过shutdown的方式关闭的，并且所有的任务和worker都已关闭，则为结束，唤醒所有shutdown等待的
	if worker.closeType.Load() == workerShutdownClose && worker.gpe.workerAndTaskEmpty() {
		g.state = executorStateTerminated
		if chanBlockingQueue, ok := g.taskBlockingQueue.(*ChanBlockingQueue[Runnable]); ok {
			chanBlockingQueue.Close()
		}
		g.shutdownCtxInfo.cancel()
	}
}

// FuncRunnable Runnable的Func方式简写
type FuncRunnable func()

func (f FuncRunnable) Run() {
	f()
}

// FuncCallable Callable 的Func方式简写
type FuncCallable func() any

func (f FuncCallable) Call() any {
	return f()
}

// FuncCallableMultiple CallableMultiple 的Func方式简写
type FuncCallableMultiple func() []any

func (f FuncCallableMultiple) call() []any {
	return f()
}

// PoolExecutor 池化的执行器，额外提供了，池相关的设置和操作
type PoolExecutor interface {
	// Executor 执行器
	Executor

	// SetCorePoolSize 设置核心协程数
	SetCorePoolSize(corePoolSize int)

	// CorePoolSize 获取核心协程数
	CorePoolSize() int

	// SetMaxPoolSize 设置最大协程数
	SetMaxPoolSize(maxPoolSize int)

	// MaxPoolSize 获取最大协程数
	MaxPoolSize() int

	// KeepAliveTime 获取非核心协程数的活跃时间
	KeepAliveTime() time.Duration

	// SetKeepAliveTime 设置非核心协程的活跃时间
	SetKeepAliveTime(keepAliveTime time.Duration)

	// BlockingTaskQueueCapacity 获取当前的阻塞任务队列的容量
	BlockingTaskQueueCapacity() int

	// SetBlockingTaskQueueCapacity 设置阻塞任务队列的容量，如果小于当前的任务数，会设置失败
	SetBlockingTaskQueueCapacity(capacity int) bool

	// ActiveCount 协程池中活跃的协程数量
	ActiveCount() int

	// TaskCount 协程池中的任务总数，大概数量
	TaskCount() int

	// CompletedTaskCount 协程池中完成的任务数
	CompletedTaskCount() int64

	// WorkerInfos 所有协程的信息
	WorkerInfos() []*WorkerInfo
}

// Executor 执行器，用于提交任务
type Executor interface {
	// Execute 提交一个无返回值的任务  Future 异步结果 error 异常信息
	Execute(job func()) (Future, error)

	// ExecuteRunnable 提交一个无返回值的任务  Future 异步结果 error 异常信息
	ExecuteRunnable(runnable Runnable) (Future, error)

	// Submit 提交一个有返回值的任务 Future 异步结果 error 异常信息
	Submit(job func() any) (Future, error)

	// SubmitCallable 提交一个有返回值的任务 Future 异步结果 error 异常信息
	SubmitCallable(callable Callable) (Future, error)

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
}

// Callable 有返回值的Callable
type Callable interface {
	// Call 调用任务
	Call() any
}

// Runnable 无返回值的Callable
type Runnable interface {
	// Run 调用任务
	Run()
}

// ExecuteExpand 执行扩展
type ExecuteExpand interface {
	// TaskBeforeExecute 任务执行之前
	TaskBeforeExecute(workerName string, runnable Runnable)

	// TaskAfterExecute 任务执行之后
	TaskAfterExecute(workerName string, runnable Runnable)
}
