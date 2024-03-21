package timewheel

import "time"

// oneTimeUnit 为一个时间单元
var oneTimeUnit = time.Millisecond * 200

var tw = NewTimeWheel(oneTimeUnit, 3600)

//var anotherTw = NewTimeWheel(oneTimeUnit*2, 3600)

func init() {
	tw.Start()
	//anotherTw.Start()
}

// Delay 执行任务，在d时间后
func Delay(d time.Duration, key string, job func()) {
	tw.AddJob(d, key, job)
	//anotherTw.AddJob(d, key, job)
}

// At 在at时刻，执行任务
func At(at time.Time, key string, job func()) {
	if time.Now().After(at) {
		return
	}
	now := time.Now()
	tw.AddJob(at.Sub(now), key, job)
	//anotherTw.AddJob(at.Sub(now), key, job)
}

// Cancel 停止一个任务
func Cancel(key string) {
	tw.RemoveJob(key)
	//anotherTw.RemoveJob(key)
}
