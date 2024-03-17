package timewheel

import (
	"container/list"
	"strings"
	"time"
	"zedis/logger"
)

// task 一个延迟任务
type task struct {
	delay  time.Duration
	circle int // 表示全局定时任务执行circle次以后，才执行该任务，也就是delay的另一种表示方式
	key    string
	job    func() // 延时任务要运行的函数
}

// location 表示一个包装后的定时任务，slotIndex表示槽索引，elementTask表示包装后放入list.List的对象
type location struct {
	slotIndex   int
	elementTask *list.Element
}

// TimeWheel 全局定时任务对象
// 这里说明定时任务的执行流程：
// 1. 每隔 interval 时间 执行一次定时任务
// 2. 每次执行定时任务时，只扫描slots数组中的其中一个槽，currentSlotPos表示本次执行时要扫描的槽索引
// 3. 扫描slots其中一个槽时，遍历该槽的所有定时任务，如果任务的circle为0，表示应该执行该任务了；否则circle减一，本次不执行
// 4. 当slots扫描一遍以后，从第一个槽重新开始扫描
//
// 因此可以得出结论：
//
//  1. 对于一个任务/slot槽来说，如果本次扫描到它，circle - 1，再经过 slotNum * interval 时间，才会第二次扫描到它
//
//  2. 如果要添加一个新的任务，delay秒以后执行，那么它的circle和slotIndex的计算方式为:
//     每隔 slotNum * second(interval) 秒，任务会被扫描到一次，因此扫描 int(delay / (slotNum * second(interval)) ) 次后 就应该执行该任务，即circle的值
//
//     假设currentSlotPos=0，即从slots开始遍历，那么由于 扫描 circle 次才执行该任务，而每经过一个 interval 时间就换一个槽，
//     所以circle也表示，从当前槽开始，需要切换多少次槽，才应该执行该任务，但槽的总数为slotNum，因此可以理解为slots是一个圈，那么slotIndex就应该是 circle % slotNum
type TimeWheel struct {
	interval time.Duration // 全局定时任务的执行间隔时间，例如interval = time.Seconds,表示一秒执行一次
	ticker   *time.Ticker  // 用于定时任务执行
	slots    []*list.List  // 任务槽，一个二维列表，每个List都放入一个包装后的task element，每次定时任务执行扫描其中一个slot

	slotNum           int                  // 表示 任务槽的数量
	timer             map[string]*location // 任务key -> location
	currentSlotPos    int                  // 表示当前全局定时任务扫描的slot槽索引
	addTaskChannel    chan task            // 添加定时任务通道
	removeTaskChannel chan string          // 删除定时任务通道
	stopChannel       chan bool            // 定时任务停止通道
}

func NewTimeWheel(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval:          interval,
		slots:             make([]*list.List, slotNum),
		slotNum:           slotNum,
		timer:             make(map[string]*location),
		currentSlotPos:    0,
		addTaskChannel:    make(chan task),
		removeTaskChannel: make(chan string),
		stopChannel:       make(chan bool),
	}

	tw.initSlots()
	return tw
}

// initSlots 初始化任务槽，每个槽都是一个延时任务列表
func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

// Start 开始执行全局定时任务
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

// Stop 停止执行全局定时任务
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

// AddJob 添加任务，表示经过delay时间后，执行job
func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}

	// 移除旧的
	tw.removeTaskChannel <- key

	tw.addTaskChannel <- task{
		delay: delay,
		key:   key,
		job:   job,
	}
}

// RemoveJob 移除任务，根据任务key
func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskChannel <- key
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case task := <-tw.addTaskChannel:
			tw.addTask(&task)
		case key := <-tw.removeTaskChannel:
			tw.removeTask(key)
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickHandler() {
	l := tw.slots[tw.currentSlotPos]
	if tw.currentSlotPos == tw.slotNum-1 {
		tw.currentSlotPos = 0
	} else {
		tw.currentSlotPos += 1
	}
	go tw.scanAndRunTasks(l)
}

func (tw *TimeWheel) scanAndRunTasks(l *list.List) {
	e := l.Front()
	for e != nil {
		t := e.Value.(*task)
		if t.circle > 0 {
			t.circle--
			e = e.Next()
			continue
		}

		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()

			job := t.job
			job()
		}()

		next := e.Next()
		tw.removeTask(t.key)
		e = next
	}
}

func (tw *TimeWheel) addTask(t *task) {
	slotIndex, circle := tw.getSlotIndexAndCircleByOneUnit(t.delay)
	t.circle = circle
	e := tw.slots[slotIndex].PushBack(t)

	loc := &location{
		slotIndex:   slotIndex,
		elementTask: e,
	}

	_, ok := tw.timer[t.key]
	if ok {
		tw.removeTask(t.key)
	}
	tw.timer[t.key] = loc
}

func (tw *TimeWheel) removeTask(key string) {
	loc, ok := tw.timer[key]
	if !ok {
		return
	}
	l := tw.slots[loc.slotIndex]
	l.Remove(loc.elementTask)
	delete(tw.timer, key)
}

// getSlotIndexAndCircle 根据任务的延时时间，计算该任务的circle字段，以及要放入slot的槽索引
func (tw *TimeWheel) getSlotIndexAndCircle(d time.Duration) (slotIndex int, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	scanSeconds := tw.slotNum * intervalSeconds // 扫描两次任务的间隔时间
	steps := delaySeconds % scanSeconds         // 扫描该任务所在槽之前，应扫描steps个槽
	circle = (delaySeconds - steps) / scanSeconds
	slotIndex = (tw.currentSlotPos + steps) % tw.slotNum
	return
}

// getSlotIndexAndCircle 根据任务的延时时间，利用自定义的时间单元，计算该任务的circle字段，以及要放入slot的槽索引
func (tw *TimeWheel) getSlotIndexAndCircleByOneUnit(d time.Duration) (slotIndex int, circle int) {
	oneTimeUnitInt64 := oneTimeUnit.Microseconds()
	currentSlotPos := int64(tw.currentSlotPos)
	slotNum := int64(tw.slotNum)
	delayTimeUnits := d.Microseconds() / oneTimeUnitInt64
	intervalTimeUnits := tw.interval.Microseconds() / oneTimeUnitInt64
	scanTimeUnits := slotNum * intervalTimeUnits // 扫描两次任务的间隔时间
	steps := delayTimeUnits % scanTimeUnits      // 扫描该任务所在槽之前，应扫描steps个槽
	circle = int((delayTimeUnits - steps) / scanTimeUnits)
	slotIndex = int((currentSlotPos + steps) % slotNum)
	return
}
