package executor

import (
	"sync"
	"sync/atomic"
	"time"
)

// worker状态
const (
	workerStateIdle = iota
	workerStateWorking
	workerStateShutdown
	workerStateTerminated
)

// worker关闭的方式
const (
	workerNormalClose = iota
	workerTimeoutClose
	workerShutdownClose
	workerTerminatedClose
)

// 核心协程活跃时间
const coreKeepAliveTime = maxTimeDuration

// worker  一个协程对应一个worker，用于执行协程池任务队列的任务
type worker struct {
	//协程池
	gpe *GoroutinePoolExecutor
	//协程ID，启动时获取
	id atomic.Int64
	//协程名
	name string
	//是否核心协程
	core bool
	//最后的工作时间，用于空闲检测
	lastWorkTimePtr atomic.Pointer[time.Time]
	//读写锁
	rwMux sync.RWMutex
	//状态
	state atomic.Int32
	//关闭方式
	closeType atomic.Uint32
	//此worker完成的任务数
	completedTaskCount atomic.Int32
}

// close 关闭worker,只在worker的协程中调用
func (w *worker) close() {
	//协程池中处理协程的关闭，从协程中移除
	w.gpe.handleWorkerCloseLock(w)
}

// createWorker 创建一个worker
func createWorker(gpe *GoroutinePoolExecutor, name string, core bool) *worker {
	now := time.Now()
	w := &worker{
		name: name,
		gpe:  gpe,
		core: core,
	}
	w.lastWorkTimePtr.Store(&now)
	//默认空闲和正常关闭
	w.state.Store(workerStateIdle)
	w.closeType.Store(workerNormalClose)
	return w
}

// startGoroutine 启动一个协程工作
func (w *worker) startGoroutine(firstTask Runnable) {
	go func() {
		//获取并设置协程ID
		w.id.Store(GetGoroutineID())
		//执行第一个任务,新建worker时，需要执行这个，因为启动协程是异步的，当阻塞队列满时，放入有可能失败
		w.doRun(firstTask)
		//开始运行
		w.Run()
	}()
}

// writeLockOperate 加写锁进行操作
func (w *worker) writeLockOperate(f func()) {
	w.rwMux.Lock()
	defer w.rwMux.Unlock()
	f()
}

// readLockOperate 加读锁进行操作
func (w *worker) readLockOperate(f func()) {
	w.rwMux.RLock()
	defer w.rwMux.RUnlock()
	f()
}

// Run for循环运行协程获取任务执行
func (w *worker) Run() {
	for {
		ws := w.state.Load()
		var task Runnable
		if ws == workerStateIdle {
			deadline := w.deadline()
			ok := false
			//超时等待获取任务，阻塞前是空闲的
			task, ok = w.gpe.removeRunnableTask(deadline.Sub(time.Now()))
			if !ok {
				//获取不到任务，判断是否超时，如果超时直接结束
				if deadline.Before(time.Now()) {
					w.casClose(workerTimeoutClose, false)
				}
			}
		} else if ws == workerStateShutdown {
			//获取任务，如果获取不到，直接关闭
			ok := false
			task, ok = w.gpe.taskBlockingQueue.Poll(0)
			if !ok {
				w.state.Store(workerStateTerminated)
			}
		} else if ws == workerStateTerminated {
			//关闭，并结束循环
			w.close()
			return
		} else {
			//异常状态，工作中的状态不在这里处理，如果有外部将状态改为工作中，则设为结束
			w.state.Store(workerStateTerminated)
		}
		//处理任务，如果是空闲状态，则会更新为工作中，任务执行完以后更新为空闲，Shutdown状态不更新，只执行任务
		if task != nil {
			w.doRun(task)
		}
	}
}

// closeState 只能通过该方式修改状态进行关闭
func (w *worker) casClose(closeType uint8, interruptIdle bool) bool {
	closeState := workerStateTerminated
	if closeType == workerShutdownClose {
		closeState = workerStateShutdown
	} else {
		closeState = workerStateTerminated
	}
	for {
		ws := w.state.Load()
		//已经关闭，则不在关了
		if ws == workerStateShutdown || ws == workerStateTerminated {
			return false
		} else {
			if w.state.CompareAndSwap(workerStateIdle, int32(closeState)) {
				if interruptIdle {
					w.gpe.taskBlockingQueue.Interrupt(w.id.Load())
				}
				w.closeType.Store(uint32(closeType))
				return true
			}
			if w.state.CompareAndSwap(workerStateWorking, int32(closeState)) {
				w.closeType.Store(uint32(closeType))
				return true
			}
		}
	}
}

// timeout 获取协程超时时间
func (w *worker) deadline() time.Time {
	var keepAliveTime time.Duration
	if w.core {
		keepAliveTime = maxTimeDuration
	} else {
		keepAliveTime = w.gpe.keepAliveTimeLock()
	}
	return w.lastWorkTimePtr.Load().Add(keepAliveTime)
}

// doRun 执行某个任务
func (w *worker) doRun(task Runnable) {
	defer func() {
		if r := recover(); r != nil {
			//恐慌处理
			w.gpe.panicHandler(task, w.WorkerInfo(), r)
		}
	}()
	//更新状态和工作时间，只有空闲时才跟新为work，因为shutdown也能执行任务
	w.state.CompareAndSwap(workerStateIdle, workerStateWorking)
	now := time.Now()
	w.lastWorkTimePtr.Store(&now)
	//执行前扩展调用
	executeExpand := w.gpe.executeExpand
	if executeExpand != nil {
		executeExpand.TaskBeforeExecute(w.name, task)
	}
	//执行任务
	task.Run()
	//统计完成的任务数
	w.completedTaskCount.Add(1)
	w.gpe.completedTaskCount.Add(1)
	//更新工作时间
	now = time.Now()
	w.lastWorkTimePtr.Store(&now)
	//执行后扩展调用
	if executeExpand != nil {
		executeExpand.TaskAfterExecute(w.name, task)
	}
	//修改为空闲中，继续获取任务
	w.state.CompareAndSwap(workerStateWorking, workerStateIdle)
}

// WorkerInfo 获取worker的信息
func (w *worker) WorkerInfo() *WorkerInfo {
	w.rwMux.RLock()
	defer w.rwMux.RUnlock()
	wi := &WorkerInfo{
		WorkerID:           w.id.Load(),
		WorkerName:         w.name,
		IsCore:             w.core,
		CloseType:          uint8(w.closeType.Load()),
		LastWorkTime:       *w.lastWorkTimePtr.Load(),
		CompletedTaskCount: int(w.completedTaskCount.Load()),
		WorkerState:        uint8(w.state.Load()),
	}
	return wi
}

// WorkerInfo worker的信息
type WorkerInfo struct {
	WorkerID           int64
	WorkerName         string
	IsCore             bool
	CloseType          uint8
	LastWorkTime       time.Time
	CompletedTaskCount int
	WorkerState        uint8
}
