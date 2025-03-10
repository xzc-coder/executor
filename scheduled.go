package executor

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// NewSchedule 构建一个Schedule，支持协程池的配置，但是不支持配置使用的任务队列
func NewSchedule(options ...Option) ScheduledPoolExecutor {
	//创建一个协程池
	options = append(options, WithTaskBlockingQueue(NewLinkedBlockingQueue[Runnable](math.MaxInt32)))
	pool := NewExecutor(options...)
	//创建一个线程安全的堆，存储待执行的定时任务
	h := NewHeap(func(a, b any) bool {
		return a.(*ScheduledTask).executeTimePtr.Load().Before(*b.(*ScheduledTask).executeTimePtr.Load())
	})
	taskHeap := NewSafeHeap(h)
	//创建一个延迟调度结构体
	s := &ScheduledPool{
		taskHeap:       taskHeap,
		pool:           pool,
		addTaskChan:    make(chan *HeapElement),
		closeChan:      make(chan struct{}),
		cancelTaskChan: make(chan *HeapElement),
	}
	//启动调度 会开启一个协程去将即将要调度的任务添加到协程池中运行
	s.start()
	return s
}

// ScheduledPool 延迟调度池
type ScheduledPool struct {
	//任务堆，按时间排序
	taskHeap *SafeHeap
	//任务运行池
	pool PoolExecutor
	//添加任务Chan
	addTaskChan chan *HeapElement
	//关闭延迟调度的Chan
	closeChan chan struct{}
	//取消任务Chan
	cancelTaskChan chan *HeapElement
	stopSignalOnce sync.Once
}

func (s *ScheduledPool) Execute(job func()) (Future, error) {
	return s.pool.Execute(job)
}

func (s *ScheduledPool) ExecuteRunnable(runnable Runnable) (Future, error) {
	return s.pool.ExecuteRunnable(runnable)
}

func (s *ScheduledPool) Submit(job func() any) (Future, error) {
	return s.pool.Submit(job)
}

func (s *ScheduledPool) SubmitCallable(callable Callable) (Future, error) {
	return s.pool.SubmitCallable(callable)
}

func (s *ScheduledPool) AwaitTermination(timeout time.Duration) (ok bool) {
	ok = s.pool.AwaitTermination(timeout)
	s.stopSignalOnce.Do(s.sendStopSignal)
	return
}

func (s *ScheduledPool) Shutdown() {
	s.pool.Shutdown()
	s.stopSignalOnce.Do(s.sendStopSignal)
}

func (s *ScheduledPool) ShutdownNow() []Runnable {
	result := s.pool.ShutdownNow()
	s.stopSignalOnce.Do(s.sendStopSignal)
	return result
}

func (s *ScheduledPool) IsShutdown() bool {
	return s.pool.IsShutdown()
}

func (s *ScheduledPool) IsTerminated() bool {
	return s.pool.IsTerminated()
}

func (s *ScheduledPool) Schedules(job func() any, delay time.Duration) (ScheduledFuture, error) {
	return s.doSchedule(FuncCallable(job), 0, delay, ScheduledTaskTypeOnce)
}

func (s *ScheduledPool) SchedulesCallable(callable Callable, delay time.Duration) (ScheduledFuture, error) {
	return s.doSchedule(callable, 0, delay, ScheduledTaskTypeOnce)
}

func (s *ScheduledPool) ScheduleAtFixedRate(job func(), initialDelay time.Duration, delay time.Duration) (ScheduledFuture, error) {
	return s.doSchedule(callable(FuncRunnable(job)), initialDelay, delay, ScheduledTaskTypeFixedRate)
}

func (s *ScheduledPool) ScheduleAtFixedRateRunnable(runnable Runnable, initialDelay time.Duration, delay time.Duration) (ScheduledFuture, error) {
	return s.doSchedule(callable(runnable), initialDelay, delay, ScheduledTaskTypeFixedRate)
}

func (s *ScheduledPool) ScheduleWithFixedDelay(job func(), initialDelay time.Duration, delay time.Duration) (ScheduledFuture, error) {
	return s.doSchedule(callable(FuncRunnable(job)), initialDelay, delay, ScheduledTaskTypeFixedDelay)
}

func (s *ScheduledPool) ScheduleWithFixedDelayRunnable(runnable Runnable, initialDelay time.Duration, delay time.Duration) (ScheduledFuture, error) {
	return s.doSchedule(callable(runnable), initialDelay, delay, ScheduledTaskTypeFixedDelay)
}

func (s *ScheduledPool) Schedule(job func(), delay time.Duration) (ScheduledFuture, error) {
	return s.doSchedule(callable(FuncRunnable(job)), 0, delay, ScheduledTaskTypeOnce)
}

func (s *ScheduledPool) ScheduleRunnable(runnable Runnable, delay time.Duration) (ScheduledFuture, error) {
	return s.doSchedule(callable(runnable), 0, delay, ScheduledTaskTypeOnce)
}

// doSchedule 添加延迟任务的具体实现，用于创建返回值并把延迟任务放到延迟协程中执行
func (s *ScheduledPool) doSchedule(callable Callable, initialDelay time.Duration, delay time.Duration, taskType ScheduledTaskType) (ScheduledFuture, error) {
	//检查协程池的状态，和其一致，不新建状态处理
	if !s.pool.IsShutdown() {
		//如果是负数 只执行一次
		if delay <= 0 {
			taskType = ScheduledTaskTypeOnce
		}
		//创建堆任务和异步结果
		taskHeapElement, scheduledTaskFuture := s.createTaskHeapElementAndFuture(callable, initialDelay, delay, taskType)
		//发送添加任务通知
		s.addTaskChan <- taskHeapElement
		return scheduledTaskFuture, nil
	} else {
		return nil, ErrScheduleShutdown
	}
}

// sendStopSignal 发送延迟任务池停止信号，并关闭chan
func (s *ScheduledPool) sendStopSignal() {
	s.closeChan <- struct{}{}
	close(s.closeChan)
	close(s.addTaskChan)
	close(s.cancelTaskChan)
}

// start 启动延迟任务调度
func (s *ScheduledPool) start() {
	go func() {
		for {
			now := time.Now()
			var timer *time.Timer
			//如果没有任务提交，睡眠等待任务
			if s.taskHeap.Len() == 0 {
				timer = time.NewTimer(maxTimeDuration)
			} else {
				//获取时间最早的堆任务元素
				heapElement := s.taskHeap.Peek()
				executeTime := heapElement.Value.(*ScheduledTask).executeTimePtr.Load()
				//设置执行间隔
				timer = time.NewTimer(executeTime.Sub(now))
			}
			select {
			case <-timer.C:
				//超时，则说明达到执行时间
				heapElement, ok := s.taskHeap.Pop()
				if ok {
					//执行任务
					s.handleAndExecuteTask(heapElement)
				}
			case taskHeapElement := <-s.cancelTaskChan: //取消任务
				timer.Stop()
				//从堆中删除取消的任务
				task := taskHeapElement.Value.(*ScheduledTask)
				if taskHeapElement.Index != invalidIndex {
					s.taskHeap.Remove(taskHeapElement)
				}
				//调用异步结果的取消，唤醒阻塞的协程
				if task.scheduledTaskFuture != nil {
					task.scheduledTaskFuture.cancelCtxInfo.cancel()
					task.scheduledTaskFuture.state.Store(ScheduledTaskFutureStateCancel)
				}
			case taskHeapElement := <-s.addTaskChan: //添加任务
				timer.Stop()
				//直接堆中 添加任务，会自动排序
				s.taskHeap.Push(taskHeapElement)
			case <-s.closeChan:
				timer.Stop()
				//关闭资源
				s.close()
				return
			}
		}
	}()
}

// handleAndExecuteTask 处理执行的任务，根据不同的任务类别做不同的处理
func (s *ScheduledPool) handleAndExecuteTask(heapElement *HeapElement) {
	task := heapElement.Value.(*ScheduledTask)
	//如果任务被取消，则不执行
	if task.scheduledTaskFuture != nil && task.scheduledTaskFuture.state.Load() == ScheduledTaskFutureStateCancel {
		return
	}
	switch task.taskType {
	case ScheduledTaskTypeOnce: //单次执行
		//提交到协程池执行
		future, err := s.pool.SubmitCallable(task.callable)
		if err == nil {
			//添加监听
			listenableFuture := future.(ListenableFuture)
			//单次执行的完成了，调用取消
			if task.scheduledTaskFuture != nil {
				//设置Future
				task.scheduledTaskFuture.futurePtr.Store(&future)
				listenableFuture.AddListener(func(f Future) {
					if f.IsDone() {
						//完成了，则唤醒阻塞的协程
						task.scheduledTaskFuture.cancelCtxInfo.cancel()
					}
				})
			}
		} else {
			//返回异常 唤醒阻塞的协程（不可能出现的情况）
			task.scheduledTaskFuture.cancelCtxInfo.cancel()
		}
	case ScheduledTaskTypeFixedDelay: //固定延迟执行
		future, err := s.pool.SubmitCallable(task.callable)
		if err == nil {
			listenableFuture := future.(ListenableFuture)
			//添加任务完成的监听，完成后才继续添加
			listenableFuture.AddListener(func(f Future) {
				s.pool.Execute(func() {
					//任务完成后修改时间添加继续执行
					if f.IsDone() && !s.pool.IsShutdown() {
						executeTime := time.Now().Add(task.delay)
						task.executeTimePtr.Store(&executeTime)
						//此处只能放在协程池中，如果再定时任务的协程中，极端情况下会导致死锁
						s.addTaskChan <- heapElement
					}
				})
			})
		} else {
			//异常也继续添加（不可能出现的情况）
			executeTime := time.Now().Add(task.delay)
			task.executeTimePtr.Store(&executeTime)
			s.taskHeap.Push(heapElement)
		}
	case ScheduledTaskTypeFixedRateOnce: //固定速率生成的单次执行的任务
		//固定速率一次的，只管提交任务
		_, _ = s.pool.SubmitCallable(task.callable)
	case ScheduledTaskTypeFixedRate: //固定速率执行
		newExecuteTime := task.executeTimePtr.Load().Add(task.delay)
		//生成下次要执行的新任务
		newTask := &ScheduledTask{
			callable: task.callable,
			taskType: ScheduledTaskTypeFixedRateOnce,
		}
		//添加2个任务
		task.executeTimePtr.Store(&newExecuteTime)
		s.taskHeap.Push(heapElement)
		newHeapElement := &HeapElement{Value: newTask, Index: invalidIndex}
		//递归添加当次任务到池中
		s.handleAndExecuteTask(newHeapElement)
	}
}

// close 关闭Schedule资源和协程池的资源
func (s *ScheduledPool) close() {
	heapElements := s.taskHeap.RemoveAll()
	//唤醒所有的任务。避免关闭了还有协程阻塞
	for _, heapElement := range heapElements {
		task := heapElement.Value.(*ScheduledTask)
		if task.scheduledTaskFuture != nil {
			task.scheduledTaskFuture.state.Store(ScheduledTaskFutureStateCancel)
			task.scheduledTaskFuture.cancelCtxInfo.cancel()
		}
	}
}

// createTaskHeapElementAndFuture 创建堆的任务元素以及异步结果
func (s *ScheduledPool) createTaskHeapElementAndFuture(callable Callable, initialDelay time.Duration, delay time.Duration, taskType ScheduledTaskType) (*HeapElement, *ScheduledTaskFuture) {
	task := &ScheduledTask{
		callable: callable,
		taskType: taskType,
		delay:    delay,
	}
	executeTime := time.Now()
	//有初始延迟则第一次按初始延迟来
	if initialDelay > 0 {
		executeTime = time.Now().Add(initialDelay)
	} else {
		executeTime = time.Now().Add(delay)
	}
	task.executeTimePtr.Store(&executeTime)
	heapElement := &HeapElement{
		Value: task,
		Index: invalidIndex,
	}
	scheduledTaskFuture := &ScheduledTaskFuture{
		taskHeapElement: heapElement,
		scheduledPool:   s,
	}
	//创建取消上下文，当结束或者被取消时调用
	ctx, cancel := context.WithCancel(context.Background())
	scheduledTaskFuture.cancelCtxInfo = &contextInfo{ctx: ctx, cancel: cancel}
	scheduledTaskFuture.state.Store(ScheduledTaskFutureStateRunning)
	scheduledTaskFuture.taskType = taskType
	task.scheduledTaskFuture = scheduledTaskFuture
	return heapElement, scheduledTaskFuture
}

// ScheduledTaskType 任务类别
type ScheduledTaskType uint8

// 任务类别
const (
	ScheduledTaskTypeOnce ScheduledTaskType = iota
	ScheduledTaskTypeFixedRate
	ScheduledTaskTypeFixedRateOnce
	ScheduledTaskTypeFixedDelay
)

// ScheduledTask 调度任务结构体，包含任务调度信息
type ScheduledTask struct {
	// 执行的时间，每次执行完，如果重复调度就重新计算
	executeTimePtr atomic.Pointer[time.Time]
	// 周期间隔
	delay time.Duration
	//要执行的任务
	callable Callable
	// 是否只执行一次
	taskType ScheduledTaskType
	//异步结果
	scheduledTaskFuture *ScheduledTaskFuture
}

// 调度任务Future状态
const (
	ScheduledTaskFutureStateRunning = iota
	ScheduledTaskFutureStateCancel
)

// ScheduledTaskFuture 调度任务的异步结果
type ScheduledTaskFuture struct {
	taskHeapElement  *HeapElement
	futurePtr        atomic.Pointer[Future]
	scheduledPool    *ScheduledPool
	state            atomic.Uint32
	cancelCtxInfo    *contextInfo
	taskType         ScheduledTaskType
	cancelSignalOnce sync.Once
}

func (s *ScheduledTaskFuture) GetDelay() time.Duration {
	return s.taskHeapElement.Value.(*ScheduledTask).delay
}

func (s *ScheduledTaskFuture) Cancel() bool {
	ok := false
	sendCancelSignal := false
	if s.taskType != ScheduledTaskTypeOnce {
		//循环执行的通过cas修改状态，修改成功则为发送信号取消
		if s.state.CompareAndSwap(ScheduledTaskFutureStateRunning, ScheduledTaskFutureStateCancel) {
			sendCancelSignal = true
			ok = true
		}
	} else {
		//只执行一次,,如果是运行状态，判断是否存在Future（任务提交到协程池）
		if s.state.Load() == ScheduledTaskFutureStateRunning {
			future := s.futurePtr.Load()
			//提交到协程池，则调用协程池的取消
			if future != nil {
				ok = (*future).Cancel()
				if ok {
					s.state.Store(ScheduledTaskFutureStateCancel)
				}
			} else {
				//未提交到协程池，修改状态，则发送信号取消
				s.state.Store(ScheduledTaskFutureStateCancel)
				sendCancelSignal = true
				ok = true
			}
		} else {
			ok = true
		}
	}
	if sendCancelSignal {
		s.cancelSignalOnce.Do(func() {
			if !s.scheduledPool.IsShutdown() {
				s.scheduledPool.cancelTaskChan <- s.taskHeapElement
			}
		})
	}
	return ok
}

func (s *ScheduledTaskFuture) IsCancelled() bool {
	future := s.futurePtr.Load()
	if future != nil {
		return (*future).IsCancelled()
	} else {
		return s.state.Load() == ScheduledTaskFutureStateCancel
	}
}

func (s *ScheduledTaskFuture) IsDone() bool {
	future := s.futurePtr.Load()
	if future != nil {
		return (*future).IsDone()
	} else {
		return s.state.Load() == ScheduledTaskFutureStateCancel
	}
}

func (s *ScheduledTaskFuture) IsSuccess() bool {
	future := s.futurePtr.Load()
	if future != nil {
		return (*future).IsSuccess()
	} else {
		return false
	}
}

func (s *ScheduledTaskFuture) PanicError() any {
	future := s.futurePtr.Load()
	if future != nil {
		return (*future).PanicError()
	} else {
		return nil
	}
}

func (s *ScheduledTaskFuture) Get() any {
	return s.GetTimeout(maxTimeDuration)
}

func (s *ScheduledTaskFuture) GetTimeout(timeout time.Duration) any {
	if s.state.Load() == ScheduledTaskFutureStateRunning {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		select {
		case <-timeoutCtx.Done():
		case <-s.cancelCtxInfo.ctx.Done():
		}
	}
	future := s.futurePtr.Load()
	if future != nil && (*future).IsSuccess() {
		return (*future).Get()
	} else {
		return nil
	}
}

// ScheduledPoolExecutor 延迟任务执行器池接口，用于提交和执行延迟任务
type ScheduledPoolExecutor interface {
	Executor

	// Schedule 提交一个无返回值的延迟任务 ScheduledFuture 异步结果 error 异常信息
	Schedule(job func(), delay time.Duration) (ScheduledFuture, error)

	// ScheduleRunnable 提交一个无返回值的延迟任务  ScheduledFuture 异步结果 error 异常信息
	ScheduleRunnable(runnable Runnable, delay time.Duration) (ScheduledFuture, error)

	// Schedules  提交一个有返回值的延迟任务  ScheduledFuture 异步结果 error 异常信息
	Schedules(job func() any, delay time.Duration) (ScheduledFuture, error)

	// SchedulesCallable 提交一个有返回值的延迟任务  ScheduledFuture 异步结果 error 异常信息
	SchedulesCallable(callable Callable, delay time.Duration) (ScheduledFuture, error)

	// ScheduleAtFixedRate 以固定的速率执行任务，不管上一个任务是否完成 ScheduledFuture 异步结果 error 异常信息
	ScheduleAtFixedRate(job func(), initialDelay time.Duration, delay time.Duration) (ScheduledFuture, error)

	// ScheduleAtFixedRateRunnable 以固定的速率执行任务，不管上一个任务是否完成 ScheduledFuture 异步结果 error 异常信息
	ScheduleAtFixedRateRunnable(runnable Runnable, initialDelay time.Duration, delay time.Duration) (ScheduledFuture, error)

	// ScheduleWithFixedDelay 以固定的延迟时间执行任务，上一个任务完成后，会等待设定的延迟时间，然后再启动下一个任务。
	ScheduleWithFixedDelay(job func(), initialDelay time.Duration, delay time.Duration) (ScheduledFuture, error)

	// ScheduleWithFixedDelayRunnable 以固定的延迟时间执行任务，上一个任务完成后，会等待设定的延迟时间，然后再启动下一个任务。
	ScheduleWithFixedDelayRunnable(runnable Runnable, initialDelay time.Duration, delay time.Duration) (ScheduledFuture, error)
}

// ScheduledFuture 延迟任务的异步结果
type ScheduledFuture interface {
	Future
	// GetDelay 获取延迟
	GetDelay() time.Duration
}
