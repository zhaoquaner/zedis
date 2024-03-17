package timewheel

import (
	"testing"
	"time"
)

func TestNewTimeWheel(t *testing.T) {
	tw := NewTimeWheel(time.Second, 3)
	pos, circle := tw.getSlotIndexAndCircle(time.Second * 7)
	t.Logf("pos: %d, circle: %d", pos, circle)
}
