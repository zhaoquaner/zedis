package timewheel

import "time"

// oneTimeUnit 为一个时间单元
var oneTimeUnit = time.Millisecond * 2000

var tw = NewTimeWheel(oneTimeUnit, 3600)

func init() {
	tw.Start()
}

// Delay 执行任务，在d时间后
func Delay(d time.Duration, key string, job func()) {
	tw.AddJob(d, key, job)
}

// At 在at时刻，执行任务
func At(at time.Time, key string, job func()) {
	if time.Now().After(at) {
		return
	}
	now := time.Now()
	tw.AddJob(at.Sub(now), key, job)
}

// Cancel 停止一个任务
func Cancel(key string) {
	tw.RemoveJob(key)
}
