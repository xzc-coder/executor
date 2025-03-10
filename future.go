package executor

import (
	"context"
	"math"
	"sync"
	"time"
)

// 任务的状态
const (
	taskStateNew = iota
	taskStateRunning
	taskStateComplete
	taskStateCancel
	taskStateError
)

// NewTaskFuture 创建一个TaskFuture
func NewTaskFuture(callable Callable) *TaskFuture {
	taskFuture := &TaskFuture{
		callable:           callable,
		state:              taskStateNew,
		listenerQueue:      newLockQueue[Listener](NewLinkedQueue[Listener](math.MaxInt32)),
		contextCancelQueue: newLockQueue[context.CancelFunc](NewLinkedQueue[context.CancelFunc](math.MaxInt32)),
	}
	return taskFuture
}

// TaskFuture 代表一个任务，且有着执行后的信息，callable、callableMultiple、runnable 3个只会存在一个有值
type TaskFuture struct {
	callable Callable
	//任务状态
	state uint8
	//单个返回值结果
	result any
	//恐慌错误信息
	panicError any
	//读写锁
	rwMux sync.RWMutex
	//监听器队列
	listenerQueue Queue[Listener]
	//取消阻塞队列
	contextCancelQueue Queue[context.CancelFunc]
}

func (t *TaskFuture) Run() {
	//如果被取消则结束
	var cancel bool
	var ctxCancels []context.CancelFunc
	var listeners []Listener
	t.writeLockOperate(func() {
		//如果被取消，则不继续运行
		if t.state == taskStateCancel {
			cancel = true
		} else {
			//不被取消
			if t.state == taskStateNew {
				//设置为运行状态
				t.state = taskStateRunning
				cancel = false
			}
		}
	})
	//不被取消则继续运行，被取消成功的已经回调了
	if !cancel {
		//只有运行状态才可以继续，只运行一次
		if t.stateLock() == taskStateRunning {
			defer func() {
				//该层不处理恐慌，只唤醒和设置异常
				if r := recover(); r != nil {
					//设置为错误，唤醒和回调
					t.writeLockOperate(func() {
						t.state = taskStateError
						t.panicError = r
						ctxCancels = t.contextCancelQueue.RemoveAll()
						listeners = t.listenerQueue.RemoveAll()
					})
					t.doCancelAndOnDone(listeners, ctxCancels)
					//向上抛出恐慌，该层只记录
					panic(r)
				}
			}()
			//执行运行
			t.doRun()
		}
	}
}

func (t *TaskFuture) Cancel() bool {
	ok := false
	var ctxCancels []context.CancelFunc
	var listeners []Listener
	t.writeLockOperate(func() {
		if t.state == taskStateNew {
			t.state = taskStateCancel
			//取消成功，则回调，只通过该方式回调一次
			ctxCancels = t.contextCancelQueue.RemoveAll()
			listeners = t.listenerQueue.RemoveAll()
			ok = true
		} else {
			ok = t.state == taskStateCancel
		}
	})
	t.doCancelAndOnDone(listeners, ctxCancels)
	return ok
}

func (t *TaskFuture) IsCancelled() bool {
	t.rwMux.RLock()
	defer t.rwMux.RUnlock()
	return t.state == taskStateCancel
}

func (t *TaskFuture) IsDone() bool {
	t.rwMux.RLock()
	defer t.rwMux.RUnlock()
	return t.isDone()
}

func (t *TaskFuture) IsSuccess() bool {
	t.rwMux.RLock()
	defer t.rwMux.RUnlock()
	return t.state == taskStateComplete
}

func (t *TaskFuture) PanicError() any {
	t.rwMux.RLock()
	defer t.rwMux.RUnlock()
	return t.panicError
}

func (t *TaskFuture) Get() any {
	return t.timeoutHandle(maxTimeDuration)
}

func (t *TaskFuture) GetTimeout(timeout time.Duration) any {
	return t.timeoutHandle(timeout)
}

func (t *TaskFuture) AddListener(listenerFunc ListenerFunc) {
	t.AddListeners(Listener(listenerFunc))
}

func (t *TaskFuture) AddListeners(listeners ...Listener) {
	// 任务未完成，添加进队列中
	ok := t.noDoneOperateLock(func() {
		for _, listener := range listeners {
			t.listenerQueue.Enqueue(listener)
		}
	})
	//任务已完成回调
	if !ok {
		t.doOnDone(listeners)
	}

}

// noDoneOperateLock 任务状态未完成时，加锁进行操作，保证并发安全
func (t *TaskFuture) noDoneOperateLock(f func()) (ok bool) {
	t.writeLockOperate(func() {
		if !t.isDone() {
			f()
			ok = true
		}
	})
	return
}

// isDone 任务是否完成，不带锁
func (t *TaskFuture) isDone() bool {
	return t.state == taskStateComplete || t.state == taskStateCancel || t.state == taskStateError
}

// timeoutHandle 超时处理
func (t *TaskFuture) timeoutHandle(timeout time.Duration) any {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	//任务未完成时加锁写入，保证状态和队列信息一致
	ok := t.noDoneOperateLock(func() {
		t.contextCancelQueue.Enqueue(cancel)
	})
	if !ok {
		return t.resultLock()
	} else {
		//任务未完成，等待超时或完成唤醒
		select {
		case <-ctx.Done():
			return t.resultLock()
		}
	}
}

// doRun 执行任务
func (t *TaskFuture) doRun() {
	//开始执行任务，只执行一个
	result := t.callable.Call()
	//回调信息
	var ctxCancels []context.CancelFunc
	var listeners []Listener
	t.writeLockOperate(func() {
		t.state = taskStateComplete
		t.result = result
		ctxCancels = t.contextCancelQueue.RemoveAll()
		listeners = t.listenerQueue.RemoveAll()
	})
	//唤醒和回调
	t.doCancelAndOnDone(listeners, ctxCancels)
}

// writeLockOperate 加写锁进行操作
func (t *TaskFuture) writeLockOperate(f func()) {
	t.rwMux.Lock()
	defer t.rwMux.Unlock()
	f()
}

// resultLock 加读锁获取结果
func (t *TaskFuture) resultLock() any {
	t.rwMux.RLock()
	defer t.rwMux.RUnlock()
	return t.result
}

// readLockOperate 加读锁进行操作
func (t *TaskFuture) readLockOperate(f func()) {
	t.rwMux.RLock()
	defer t.rwMux.RUnlock()
	f()
}

// stateLock 加锁获取状态
func (t *TaskFuture) stateLock() uint8 {
	t.rwMux.RLock()
	defer t.rwMux.RUnlock()
	return t.state
}

// doOnDone 执行监听回调
func (t *TaskFuture) doOnDone(listeners []Listener) {
	for _, listener := range listeners {
		listener.OnDone(t)
	}
}

// doCancel 执行任务取消
func (t *TaskFuture) doCancel(cancels []context.CancelFunc) {
	for _, cancel := range cancels {
		cancel()
	}
}

// doCancelAndOnDone 执行任务取消和监听回调
func (t *TaskFuture) doCancelAndOnDone(listeners []Listener, cancels []context.CancelFunc) {
	t.doOnDone(listeners)
	t.doCancel(cancels)
}

// ListenerFunc 监听器 简写
type ListenerFunc func(Future)

func (l ListenerFunc) OnDone(future Future) {
	l(future)
}

// ListenableFuture 监听器的Future 提供监听回调的功能
type ListenableFuture interface {
	Future
	// AddListener 添加一个监听器
	AddListener(listenerFunc ListenerFunc)

	// AddListeners 添加多个监听器
	AddListeners(listeners ...Listener)
}

// Future 异步结果
type Future interface {
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
}

// Listener 监听器，任务完成时回调
type Listener interface {
	// OnDone 回调的方法中不能抛出恐慌
	OnDone(future Future)
}
