package executor

import "errors"

var (
	ErrExecutorShutdown = errors.New("executor is shutdown")

	ErrExecutorTerminated = errors.New("executor is terminated")

	ErrExecutorUnknownState = errors.New("executor is unknown state")

	ErrTaskEnqueueFailed = errors.New("task enqueue failed")

	ErrTaskQueueFull = errors.New("task queue is full")

	ErrNeedRejectedHandle = errors.New("need rejected handle")
)

var (
	// ErrScheduleShutdown 延迟任务调度器已关闭错误
	ErrScheduleShutdown = errors.New("schedule: schedule is already in shutdown")
)
