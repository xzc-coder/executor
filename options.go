package executor

import "time"

type Option func(options *Options)

// Options 协程池创建时的设置参数
type Options struct {
	//核心协程数
	CorePoolSize int
	//最大协程数
	MaxPoolSize int
	//非核心协程活跃时间
	KeepAliveTime time.Duration
	//任务阻塞队列
	TaskBlockingQueue BlockingQueue[Runnable]
	//执行扩展
	ExecuteExpand ExecuteExpand
	//协程命名工厂
	GoroutineNameFactory GoroutineNameFactory
	//拒绝策略
	RejectedHandler RejectedHandler
	//恐慌处理器
	PanicHandler PanicHandler
}

// WithOptions 设置参数
func WithOptions(options Options) Option {
	return func(opts *Options) {
		*opts = options
	}
}

// WithCorePoolSize 设置核心协程数
func WithCorePoolSize(corePoolSize int) Option {
	return func(opts *Options) {
		opts.CorePoolSize = corePoolSize
	}
}

// WithMaxPoolSize 设置最大协程数
func WithMaxPoolSize(maxPoolSize int) Option {
	return func(opts *Options) {
		opts.MaxPoolSize = maxPoolSize
	}
}

// WithKeepAliveTime 设置非核心协程活跃时间
func WithKeepAliveTime(keepAliveTime time.Duration) Option {
	return func(opts *Options) {
		opts.KeepAliveTime = keepAliveTime
	}
}

// WithTaskBlockingQueue 设置任务的阻塞队列
func WithTaskBlockingQueue(taskBlockingQueue BlockingQueue[Runnable]) Option {
	return func(opts *Options) {
		opts.TaskBlockingQueue = taskBlockingQueue
	}
}

var blockSplitCapacity = 10000

// WithTaskBlockingQueueDefault 设置默认的任务的阻塞队列大小
func WithTaskBlockingQueueDefault(capacity int) Option {
	if capacity <= blockSplitCapacity {
		return func(opts *Options) {
			opts.TaskBlockingQueue = NewChanBlockingRunnableQueue(capacity)
		}
	} else {
		return func(opts *Options) {
			opts.TaskBlockingQueue = NewLinkedBlockingRunnableQueue(capacity)
		}
	}
}

// WithExecuteExpand 设置执行扩展，在任务执行前后扩展
func WithExecuteExpand(executeExpand ExecuteExpand) Option {
	return func(opts *Options) {
		opts.ExecuteExpand = executeExpand
	}
}

// WithGoroutineNameFactory 设置协程命名工厂
func WithGoroutineNameFactory(goroutineNameFactory GoroutineNameFactory) Option {
	return func(opts *Options) {
		opts.GoroutineNameFactory = goroutineNameFactory
	}
}

// WithRejectedHandler 设置拒绝处理器，当达到最大协程数且任务队列满时的处理策略，默认返回异常
func WithRejectedHandler(rejectedHandler RejectedHandler) Option {
	return func(opts *Options) {
		opts.RejectedHandler = rejectedHandler
	}
}

// WithPanicHandler 设置恐慌处理器，当提交的任务发生恐慌时处理，默认打印恐慌信息
func WithPanicHandler(panicHandler PanicHandler) Option {
	return func(opts *Options) {
		opts.PanicHandler = panicHandler
	}
}
