package executor

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

// TestCancel 测试 Cancel和IsCancelled方法
func TestCancel(t *testing.T) {
	future := NewTaskFuture(callable(FuncRunnable(func() {
	})))
	require.Equal(t, false, future.IsCancelled())
	cancelled := future.Cancel()
	require.Equal(t, true, cancelled)
	require.Equal(t, true, future.IsCancelled())
	require.Equal(t, true, future.IsDone())
	//执行时取消
	future = NewTaskFuture(callable(FuncRunnable(func() {
		//模拟任务执行
		time.Sleep(100 * time.Millisecond)
	})))
	go future.Run()
	time.Sleep(70 * time.Millisecond)
	//取消失败
	cancelled = future.Cancel()
	require.Equal(t, false, cancelled)
	require.Equal(t, false, future.IsCancelled())
	require.Equal(t, false, future.IsDone())
	time.Sleep(70 * time.Millisecond)
	require.Equal(t, true, future.IsDone())
}

// TestIsDone 测试 IsDone 方法
func TestIsDone(t *testing.T) {
	future := NewTaskFuture(callable(FuncRunnable(func() {
		//模拟任务执行
		time.Sleep(100 * time.Millisecond)
	})))
	require.Equal(t, false, future.IsDone())
	go future.Run()
	time.Sleep(150 * time.Millisecond)
	require.Equal(t, false, future.IsCancelled())
	require.Equal(t, true, future.IsDone())
}

// TestIsSuccess 测试 IsSuccess 方法
func TestIsSuccess(t *testing.T) {
	future := NewTaskFuture(callable(FuncRunnable(func() {
		//模拟任务执行
		time.Sleep(100 * time.Millisecond)
	})))
	go future.Run()
	require.Equal(t, false, future.IsSuccess())
	time.Sleep(120 * time.Millisecond)
	require.Equal(t, true, future.IsSuccess())
	require.Equal(t, true, future.IsDone())
	//中途取消
	future = NewTaskFuture(callable(FuncRunnable(func() {
		//模拟任务执行
		time.Sleep(100 * time.Millisecond)
	})))
	future.Cancel()
	require.Equal(t, false, future.IsSuccess())
	require.Equal(t, true, future.IsDone())
}

// TestPanicError 测试 PanicError 方法
func TestPanicError(t *testing.T) {
	future := NewTaskFuture(callable(FuncRunnable(func() {
		//模拟任务执行
		time.Sleep(100 * time.Millisecond)
		panic("test")
	})))
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println(r)
			}
		}()
		future.Run()
	}()
	time.Sleep(140 * time.Millisecond)
	require.Equal(t, false, future.IsSuccess())
	require.Equal(t, true, future.IsDone())
	require.Equal(t, "test", future.PanicError())
}

// TestGet 测试 Get 方法
func TestGet(t *testing.T) {
	future := NewTaskFuture(FuncCallable(func() any {
		//模拟任务执行
		time.Sleep(100 * time.Millisecond)
		return "test"
	}))
	go future.Run()
	result := future.Get()
	require.Equal(t, "test", result)
	require.Equal(t, true, future.IsDone())
	require.Equal(t, true, future.IsSuccess())
}

// TestGetTimeout 测试 GetTimeout 方法
func TestGetTimeout(t *testing.T) {
	future := NewTaskFuture(FuncCallable(func() any {
		//模拟任务执行
		time.Sleep(100 * time.Millisecond)
		return "test"
	}))
	go future.Run()
	result := future.GetTimeout(50 * time.Millisecond)
	require.Equal(t, nil, result)
	require.Equal(t, false, future.IsDone())
	require.Equal(t, false, future.IsSuccess())
	result = future.GetTimeout(80 * time.Millisecond)
	require.Equal(t, "test", result)
	require.Equal(t, true, future.IsDone())
	require.Equal(t, true, future.IsSuccess())
}

type ListenerTest struct {
	f func(Future)
}

func (l *ListenerTest) OnDone(future Future) {
	l.f(future)
}

// TestListenableSuccessFuture  测试成功监听
func TestListenableSuccessFuture(t *testing.T) {
	number := atomic.Int32{}
	future := NewTaskFuture(FuncCallable(func() any {
		//模拟任务执行
		time.Sleep(100 * time.Millisecond)
		return "test"
	}))
	go future.Run()
	future.AddListener(func(future Future) {
		if future.IsSuccess() {
			number.Add(1)
		}
	})
	future.AddListener(func(future Future) {
		if future.IsSuccess() {
			number.Add(1)
		}
	})
	future.Get()
	require.Equal(t, 2, int(number.Load()))
	//测试多个
	number.Store(0)
	future = NewTaskFuture(FuncCallable(func() any {
		//模拟任务执行
		time.Sleep(100 * time.Millisecond)
		return "test"
	}))
	go future.Run()
	l1 := &ListenerTest{func(future Future) {
		if future.IsSuccess() {
			number.Add(1)
		}
	}}
	l2 := &ListenerTest{func(future Future) {
		if future.IsSuccess() {
			number.Add(1)
		}
	}}
	l3 := &ListenerTest{func(future Future) {
		if future.IsSuccess() {
			number.Add(1)
		}
	}}
	future.AddListeners(l1, l2, l3)
	future.Get()
	require.Equal(t, 3, int(number.Load()))
}

// TestListenableCancelFuture  测试取消监听
func TestListenableCancelFuture(t *testing.T) {
	number := atomic.Int32{}
	future := NewTaskFuture(FuncCallable(func() any {
		//模拟任务执行
		time.Sleep(100 * time.Millisecond)
		return "test"
	}))
	future.Cancel()
	future.AddListener(func(future Future) {
		if future.IsCancelled() {
			number.Add(1)
		}
	})
	future.AddListener(func(future Future) {
		if future.IsCancelled() {
			number.Add(1)
		}
	})
	future.Get()
	require.Equal(t, 2, int(number.Load()))
	//测试多个
	number.Store(0)
	future = NewTaskFuture(FuncCallable(func() any {
		//模拟任务执行
		time.Sleep(100 * time.Millisecond)
		return "test"
	}))
	future.Cancel()
	l1 := &ListenerTest{func(future Future) {
		if future.IsCancelled() {
			number.Add(1)
		}
	}}
	l2 := &ListenerTest{func(future Future) {
		if future.IsCancelled() {
			number.Add(1)
		}
	}}
	l3 := &ListenerTest{func(future Future) {
		if future.IsCancelled() {
			number.Add(1)
		}
	}}
	future.AddListeners(l1, l2, l3)
	future.Get()
	require.Equal(t, 3, int(number.Load()))
}
