package test

import (
	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
	"github.com/redis_delay_queue"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// 初始化单元测试的miniRedis
	miniRedisServer, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer miniRedisServer.Close()
	redisClient := redis.NewClient(&redis.Options{
		Addr: miniRedisServer.Addr(),
		DB:   0, // use default DB
	})
	delay_queue.NewDelayQueue("test", redisClient).InitOnce(
		&DemoDelayTask{},
	)

	// convey在TestMain场景下的入口
	SuppressConsoleStatistics()
	result := m.Run()
	time.Sleep(20 * time.Second)
	// convey在TestMain场景下的结果打印
	PrintConsoleStatistics()
	os.Exit(result)
}
