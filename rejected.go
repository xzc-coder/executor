package executor

// RejectedHandler 拒绝策略处理器 参数是任务（TaskFuture）、协程池
type RejectedHandler func(Runnable, Executor) error

// AbortPolicy 放弃任务执行，直接返回错误信息，默认的拒绝策略
var AbortPolicy RejectedHandler = func(runnable Runnable, executor Executor) error {
	return ErrTaskQueueFull
}

// BlockPolicy 阻塞拒绝处理器，阻塞调用方直到任务队列有空余
var BlockPolicy RejectedHandler = func(runnable Runnable, executor Executor) (err error) {
	g := executor.(*GoroutinePoolExecutor)
	g.readLockOperate(func() {
		err = g.runStateCheck()
	})
	if err == nil {
		err = g.addRunnableTask(runnable, maxTimeDuration)
	}
	return
}

// CallerRunsPolicy 用当前提交的协程去进行执行
var CallerRunsPolicy RejectedHandler = func(runnable Runnable, executor Executor) (err error) {
	g := executor.(*GoroutinePoolExecutor)
	g.readLockOperate(func() {
		err = g.runStateCheck()
	})
	if err == nil {
		runnable.Run()
	}
	return
}

// DiscardOldestPolicy 丢弃最老的任务
var DiscardOldestPolicy RejectedHandler = func(runnable Runnable, executor Executor) (err error) {
	g := executor.(*GoroutinePoolExecutor)
	g.writeLockOperate(func() {
		g.runStateCheck()
		r, _ := g.removeRunnableTask(0)
		//取消任务
		cancel := r.(*TaskFuture).Cancel()
		if cancel {
			//执行TaskFuture的run方法，跑完流程唤醒回调（实际任务已被取消不会再被执行）
			r.Run()
		}
		//添加当前任务
		err = g.addRunnableTask(runnable, 0)
	})
	return
}
