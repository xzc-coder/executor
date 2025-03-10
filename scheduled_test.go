package executor

import (
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

// TestSchedule 测试延迟任务
func TestSchedule(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(1))
	number := atomic.Int32{}
	f, _ := scheduledPool.Schedule(func() {
		number.Add(1)
	}, 100*time.Millisecond)

	time.Sleep(60 * time.Millisecond)
	require.Equal(t, int32(0), number.Load())
	require.Equal(t, false, f.IsDone())
	require.Equal(t, false, f.IsSuccess())
	f.Get()
	require.Equal(t, int32(1), number.Load())
	require.Equal(t, true, f.IsDone())
	require.Equal(t, true, f.IsSuccess())
}

// TestScheduleRunnable 测试延迟任务
func TestScheduleRunnable(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(1))
	number := atomic.Int32{}
	f, _ := scheduledPool.ScheduleRunnable(FuncRunnable(func() {
		number.Add(1)
	}), 100*time.Millisecond)

	time.Sleep(60 * time.Millisecond)
	require.Equal(t, int32(0), number.Load())
	require.Equal(t, false, f.IsDone())
	require.Equal(t, false, f.IsSuccess())
	f.Get()
	require.Equal(t, int32(1), number.Load())
	require.Equal(t, true, f.IsDone())
	require.Equal(t, true, f.IsSuccess())
}

// TestSchedules 测试延迟任务，带返回值
func TestSchedules(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(1))
	f, _ := scheduledPool.Schedules(func() any {
		return "test"
	}, 100*time.Millisecond)

	time.Sleep(60 * time.Millisecond)
	require.Equal(t, false, f.IsDone())
	require.Equal(t, false, f.IsSuccess())
	result := f.Get()
	require.Equal(t, "test", result)
	require.Equal(t, true, f.IsDone())
	require.Equal(t, true, f.IsSuccess())
}

// TestSchedulesCallable 测试延迟任务，带返回值
func TestSchedulesCallable(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(1))
	f, _ := scheduledPool.SchedulesCallable(FuncCallable(func() any {
		return "test"
	}), 100*time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	require.Equal(t, false, f.IsDone())
	require.Equal(t, false, f.IsSuccess())
	result := f.Get()
	require.Equal(t, "test", result)
	require.Equal(t, true, f.IsDone())
	require.Equal(t, true, f.IsSuccess())
}

// TestScheduleCancel 测试延迟任务的取消
func TestScheduleCancel(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(1))
	number := atomic.Int32{}
	f, _ := scheduledPool.Schedule(func() {
		number.Add(1)
	}, 100*time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, false, f.IsDone())
	require.Equal(t, false, f.IsCancelled())
	f.Cancel()
	//等待延迟任务执行时间
	time.Sleep(60 * time.Millisecond)
	require.Equal(t, true, f.IsCancelled())
	require.Equal(t, true, f.IsDone())
	require.Equal(t, false, f.IsSuccess())
	require.Equal(t, int32(0), number.Load())
}

// TestSchedulePanicError 测试延迟任务的恐慌异常
func TestSchedulePanicError(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(1))
	f, _ := scheduledPool.Schedule(func() {
		panic("test")
	}, 100*time.Millisecond)
	f.Get()
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, true, f.IsDone())
	require.Equal(t, false, f.IsSuccess())
	require.Equal(t, "test", f.PanicError())
}

// TestSchedulesCancel 测试延迟任务的取消，带返回值
func TestSchedulesCancel(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(1))
	f, _ := scheduledPool.Schedules(func() any {
		return "test"
	}, 100*time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, false, f.IsDone())
	require.Equal(t, false, f.IsCancelled())
	f.Cancel()
	//等待延迟任务执行时间
	time.Sleep(60 * time.Millisecond)
	require.Equal(t, true, f.IsCancelled())
	require.Equal(t, true, f.IsDone())
	require.Equal(t, false, f.IsSuccess())
	require.Equal(t, nil, f.Get())

}

// TestScheduleFutureGetDelay 测试ScheduleFuture
func TestScheduleFutureGetDelay(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(1))
	number := atomic.Int32{}
	f, _ := scheduledPool.ScheduleRunnable(FuncRunnable(func() {
		number.Add(1)
	}), 100*time.Millisecond)
	require.Equal(t, 100*time.Millisecond, f.GetDelay())
}

// TestScheduleTimeout 测试延迟任务的超时
func TestScheduleTimeout(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(1))
	number := atomic.Int32{}
	f, _ := scheduledPool.Schedule(func() {
		number.Add(1)
	}, 100*time.Millisecond)
	f.GetTimeout(60 * time.Millisecond)
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, false, f.IsDone())
	require.Equal(t, false, f.IsCancelled())
	//等待延迟任务执行时间
	f.GetTimeout(60 * time.Millisecond)
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, true, f.IsDone())
	require.Equal(t, true, f.IsSuccess())
	require.Equal(t, int32(1), number.Load())

	number.Store(0)
	f, _ = scheduledPool.Schedule(func() {
		number.Add(1)
	}, 100*time.Millisecond)
	//阻塞至完成
	f.Get()
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, true, f.IsDone())
	require.Equal(t, true, f.IsSuccess())
	require.Equal(t, int32(1), number.Load())
}

// TestScheduleTaskOrder 测试延迟任务的顺序
func TestScheduleTaskOrder(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(10))
	number := atomic.Int32{}
	number1 := atomic.Int32{}
	f, _ := scheduledPool.Schedule(func() {
		number.Add(1)
	}, 120*time.Millisecond)
	f1, _ := scheduledPool.Schedule(func() {
		number1.Add(1)
	}, 60*time.Millisecond)
	f1.Get()
	require.Equal(t, false, f1.IsCancelled())
	require.Equal(t, true, f1.IsDone())
	require.Equal(t, true, f1.IsSuccess())
	require.Equal(t, int32(1), number1.Load())
	require.Equal(t, int32(0), number.Load())
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, false, f.IsDone())
	require.Equal(t, false, f.IsSuccess())
}

// TestSchedulesTimeout 测试延迟任务的超时，带返回值
func TestSchedulesTimeout(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(1))
	f, _ := scheduledPool.Schedules(FuncCallable(func() any {
		return "test"
	}), 100*time.Millisecond)
	f.GetTimeout(60 * time.Millisecond)
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, false, f.IsDone())
	require.Equal(t, false, f.IsCancelled())
	//等待延迟任务执行时间
	f.GetTimeout(60 * time.Millisecond)
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, true, f.IsDone())
	require.Equal(t, true, f.IsSuccess())
	require.Equal(t, "test", f.Get())

	f, _ = scheduledPool.Schedules(FuncCallable(func() any {
		return "test"
	}), 100*time.Millisecond)
	//阻塞至完成
	f.Get()
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, true, f.IsDone())
	require.Equal(t, true, f.IsSuccess())
	require.Equal(t, "test", f.Get())
}

// TestScheduleAtFixedRate 测试固定速率
func TestScheduleAtFixedRate(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(10))
	number := atomic.Int32{}
	f, _ := scheduledPool.ScheduleAtFixedRate(func() {
		//模拟任务耗时
		time.Sleep(20 * time.Millisecond)
		number.Add(1)
	}, 50*time.Millisecond, 100*time.Millisecond)
	//按固定速率 和初始延迟 600秒内应该执行了6次 50 150 250 350 450 550（+20为570毫秒执行完毕）
	time.Sleep(600 * time.Millisecond)
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, false, f.IsDone())
	require.Equal(t, false, f.IsSuccess())
	require.Equal(t, int32(6), number.Load())
}

// TestScheduleAtFixedRateRunnable 测试固定速率
func TestScheduleAtFixedRateRunnable(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(10))
	number := atomic.Int32{}
	f, _ := scheduledPool.ScheduleAtFixedRate(FuncRunnable(func() {
		//模拟任务耗时
		time.Sleep(20 * time.Millisecond)
		number.Add(1)
	}), 50*time.Millisecond, 100*time.Millisecond)
	//按固定速率 和初始延迟 600秒内应该执行了6次 50 150 250 350 450 550（+20为570毫秒执行完毕）
	time.Sleep(600 * time.Millisecond)
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, false, f.IsDone())
	require.Equal(t, false, f.IsSuccess())
	require.Equal(t, int32(6), number.Load())
}

// TestScheduleWithFixedDelay 测试固定延迟
func TestScheduleWithFixedDelay(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(10))
	number := atomic.Int32{}
	f, _ := scheduledPool.ScheduleWithFixedDelay(func() {
		//模拟任务耗时
		time.Sleep(20 * time.Millisecond)
		number.Add(1)
	}, 50*time.Millisecond, 100*time.Millisecond)
	//按固定延迟 和初始延迟 600秒内应该执行了5次  50（70） 170（190） 290（310） 410（430） 530（550）
	time.Sleep(600 * time.Millisecond)
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, false, f.IsDone())
	require.Equal(t, false, f.IsSuccess())
	require.Equal(t, int32(5), number.Load())
}

// TestScheduleWithFixedDelayRunnable 测试固定延迟
func TestScheduleWithFixedDelayRunnable(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(10))
	number := atomic.Int32{}
	f, _ := scheduledPool.ScheduleWithFixedDelayRunnable(FuncRunnable(func() {
		//模拟任务耗时
		time.Sleep(20 * time.Millisecond)
		number.Add(1)
	}), 50*time.Millisecond, 100*time.Millisecond)
	//按固定延迟 和初始延迟 600秒内应该执行了5次  50（70） 170（190） 290（310） 410（430） 530（550）
	time.Sleep(600 * time.Millisecond)
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, false, f.IsDone())
	require.Equal(t, false, f.IsSuccess())
	require.Equal(t, int32(5), number.Load())
}

// TestSchedulePoolShutdown 测试延迟任务的Shutdown
func TestSchedulePoolShutdown(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(1))
	number := atomic.Int32{}
	f, _ := scheduledPool.Schedule(func() {
		time.Sleep(50 * time.Millisecond)
		number.Add(1)
	}, 50*time.Millisecond)
	f, _ = scheduledPool.Schedule(func() {
		time.Sleep(50 * time.Millisecond)
		number.Add(1)
	}, 50*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	scheduledPool.Shutdown()
	require.Equal(t, true, scheduledPool.IsShutdown())
	require.Equal(t, false, scheduledPool.IsTerminated())
	f.Get()
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, true, f.IsDone())
	require.Equal(t, true, f.IsSuccess())
	require.Equal(t, int32(2), number.Load())
	_, err := scheduledPool.Schedule(func() {
		time.Sleep(50 * time.Millisecond)
		number.Add(1)
	}, 50*time.Millisecond)
	require.NotNil(t, err)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, true, scheduledPool.IsShutdown())
	require.Equal(t, true, scheduledPool.IsTerminated())
}

// TestSchedulePoolAwaitTermination 测试延迟任务的AwaitTermination
func TestSchedulePoolAwaitTermination(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(1))
	number := atomic.Int32{}
	f, _ := scheduledPool.Schedule(func() {
		time.Sleep(50 * time.Millisecond)
		number.Add(1)
	}, 50*time.Millisecond)
	f, _ = scheduledPool.Schedule(func() {
		time.Sleep(50 * time.Millisecond)
		number.Add(1)
	}, 50*time.Millisecond)
	time.Sleep(70 * time.Millisecond)
	scheduledPool.AwaitTermination(50 * time.Millisecond)
	require.Equal(t, true, scheduledPool.IsShutdown())
	require.Equal(t, false, scheduledPool.IsTerminated())
	f.Get()
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, true, f.IsDone())
	require.Equal(t, true, f.IsSuccess())
	require.Equal(t, true, scheduledPool.IsShutdown())
	require.Equal(t, true, scheduledPool.IsTerminated())
	require.Equal(t, int32(2), number.Load())
	_, err := scheduledPool.Schedule(func() {
		time.Sleep(50 * time.Millisecond)
		number.Add(1)
	}, 50*time.Millisecond)
	require.NotNil(t, err)
}

// TestSchedulePoolShutdownNow( 测试延迟任务的ShutdownNow
func TestSchedulePoolShutdownNow(t *testing.T) {
	scheduledPool := NewSchedule(WithCorePoolSize(1))
	scheduledPool.Schedule(func() {
		time.Sleep(50 * time.Millisecond)
	}, 50*time.Millisecond)
	f, _ := scheduledPool.Schedule(func() {
		time.Sleep(50 * time.Millisecond)
	}, 50*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	rs := scheduledPool.ShutdownNow()
	require.Equal(t, 1, len(rs))
	require.Equal(t, true, scheduledPool.IsShutdown())
	require.Equal(t, true, scheduledPool.IsTerminated())
	require.Equal(t, false, f.IsCancelled())
	require.Equal(t, false, f.IsDone())
	require.Equal(t, false, f.IsSuccess())
	_, err := scheduledPool.Schedule(func() {
		time.Sleep(50 * time.Millisecond)
	}, 50*time.Millisecond)
	require.NotNil(t, err)
}
