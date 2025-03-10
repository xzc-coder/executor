package executor

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// TestPool 测试协程池案例
func TestPool(t *testing.T) {
	//创建
	pool := NewExecutor(WithCorePoolSize(1))
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
		require.Equal(t, "test", result)
	}
	//结束任务，渐进式关闭
	pool.Shutdown()
}

// TestScheduledPool 测试延迟协程池案例
func TestScheduledPool(t *testing.T) {
	//创建
	schedule := NewSchedule(WithCorePoolSize(1))
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
		require.Equal(t, "test", result)
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
