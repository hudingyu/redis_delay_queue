package util

import (
	"github.com/redis_delay_queue/logs"
	"time"
)

func Retry(count int, sleep time.Duration, f func() (success bool)) bool {
	for retry := 0; retry < count; retry++ {
		success := f()
		if success {
			return true
		} else {
			left := count - retry - 1
			if left == 0 {
				return false
			} else {
				logs.Warn("[Retry]", logs.Int("sleep", int(sleep/1e6)), logs.Int("left", left))
				time.Sleep(sleep)
			}
		}
	}
	return false
}
