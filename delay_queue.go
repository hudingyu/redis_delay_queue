package delay_queue

import (
	"context"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/redis_delay_queue/logs"
	"github.com/redis_delay_queue/util"
	"github.com/redis_delay_queue/util/maps"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
)

type PersistFn func(event *EventEntity) error

var (
	DelayQueueImpl *DelayQueue
)

type DelayQueue struct {
	namespace   string
	redisClient *redis.Client
	once        sync.Once
	wg          sync.WaitGroup
	isRunning   int32
	stop        chan struct{}
	persistFn   PersistFn
}

func init() {
	DelayQueueImpl = &DelayQueue{}
}

func GetDelayQueue() *DelayQueue {
	return DelayQueueImpl
}

func NewDelayQueue(namespace string, redisClient *redis.Client) *DelayQueue {
	DelayQueueImpl.namespace = namespace
	DelayQueueImpl.redisClient = redisClient
	DelayQueueImpl.stop = make(chan struct{})
	return DelayQueueImpl
}

func (q *DelayQueue) WithPersistForUnhandledEvent(fn PersistFn) {
	q.persistFn = fn
}

// gracefully shudown
func (q *DelayQueue) ShutDown() {
	if !atomic.CompareAndSwapInt32(&q.isRunning, 1, 0) {
		return
	}
	close(q.stop)
	q.wg.Wait()
}

func (q *DelayQueue) genBucketKey(topic string) string {
	return fmt.Sprintf("BUCKET_%v_%v", q.namespace, topic)
}

func (q *DelayQueue) genPoolKey(topic string) string {
	return fmt.Sprintf("POOL_%v_%v", q.namespace, topic)
}

func (q *DelayQueue) genQueueKey(topic string) string {
	return fmt.Sprintf("QUEUE_%v_%v", q.namespace, topic)
}

func (q *DelayQueue) InitOnce(subscriber IEventSubscriber, others ...IEventSubscriber) {
	if !atomic.CompareAndSwapInt32(&q.isRunning, 0, 1) {
		return
	}

	list := append([]IEventSubscriber{subscriber}, others...)
	topicConsumerMap := make(map[string][]IEventSubscriber)
	for _, s := range list {
		topicConsumerMap[s.Topic()] = append(topicConsumerMap[s.Topic()], s)
	}
	topicList := maps.Keys(topicConsumerMap).([]string)
	q.once.Do(func() {
		for _, t := range topicList {
			topic := t
			// 定时topic扫描到期的事件
			util.GoSafe(func() {
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						_ = q.carryEventToQueue(topic)
					case <-q.stop:
						return
					}
				}
			})

			// 消费topic队列的事件
			util.GoSafe(func() {
				_ = q.runConsumer(topic, topicConsumerMap[topic])
			})
		}
	})
}

// 扫描zset中到期的任务，添加到对应topic的待消费队列里
func (q *DelayQueue) carryEventToQueue(topic string) error {
	ctx := context.Background()
	members, err := q.redisClient.WithContext(ctx).ZRangeByScoreWithScores(q.genBucketKey(topic), redis.ZRangeBy{Min: "0", Max: util.ToString(time.Now().Unix())}).Result()
	if err != nil && err != redis.Nil {
		logs.CtxWarn(ctx, "[carryEventToQueue] ZRangeByScoreWithScores", logs.String("err", err.Error()))
		return err
	}
	if len(members) == 0 {
		return nil
	}

	errMap := make(map[string]error)
	for _, m := range members {
		eventId := m.Member.(string)
		err = q.redisClient.WithContext(ctx).LPush(q.genQueueKey(topic), eventId).Err()
		if err != nil {
			logs.CtxWarn(ctx, "[carryEventToQueue] LPush", logs.String("err", err.Error()))
			errMap[eventId] = err
		}
	}

	// 从Bucket中删除已进入待消费队列的事件
	var doneMembers []interface{}
	for _, m := range members {
		eventId := m.Member.(string)
		if _, ok := errMap[eventId]; !ok {
			doneMembers = append(doneMembers, eventId)
		}
	}
	if len(doneMembers) == 0 {
		return nil
	}

	err = q.redisClient.WithContext(ctx).ZRem(q.genBucketKey(topic), doneMembers...).Err()
	if err != nil {
		logs.CtxWarn(ctx, "[carryEventToQueue] ZRem", logs.String("err", err.Error()))
	}
	return nil
}

func (q *DelayQueue) runConsumer(topic string, subscriberList []IEventSubscriber) error {
	for {
		if atomic.LoadInt32(&q.isRunning) == 0 {
			return nil
		}
		q.wg.Add(1)
		ctx := context.Background()
		kvPair, err := q.redisClient.WithContext(ctx).BLPop(60*time.Second, q.genQueueKey(topic)).Result()
		if err != nil {
			logs.CtxWarn(ctx, "[InitOnce] BLPop", logs.String("err", err.Error()))
			q.wg.Done()
			continue
		}
		if len(kvPair) < 2 {
			q.wg.Done()
			continue
		}

		eventId := kvPair[1]
		data, err := q.redisClient.WithContext(ctx).HGet(q.genPoolKey(topic), eventId).Result()
		if err != nil && err != redis.Nil {
			logs.CtxWarn(ctx, "[InitOnce] HGet", logs.String("err", err.Error()))
			if q.persistFn != nil {
				_ = q.persistFn(&EventEntity{
					EventId: util.String2Int64(eventId),
					Topic:   topic,
				})
			}
			q.wg.Done()
			continue
		}
		event := &EventEntity{}
		_ = jsoniter.UnmarshalFromString(data, event)

		for _, subscriber := range subscriberList {
			util.Retry(3, 0, func() (success bool) {
				err = subscriber.Handle(ctx, event)
				if err != nil {
					logs.CtxWarn(ctx, "[InitOnce] subscriber.Handle", logs.String("err", err.Error()))
					return false
				}
				return true
			})
		}

		err = q.redisClient.WithContext(ctx).HDel(q.genPoolKey(topic), eventId).Err()
		if err != nil {
			logs.CtxWarn(ctx, "[InitOnce] HDel", logs.String("err", err.Error()))
		}
		q.wg.Done()
	}
}

func (q *DelayQueue) PublishEvent(ctx context.Context, event *EventEntity) error {
	pipeline := q.redisClient.WithContext(ctx).Pipeline()
	defer pipeline.Close()

	pipeline.HSet(q.genPoolKey(event.Topic), strconv.FormatInt(event.EventId, 10), util.ToJsonString(event))
	pipeline.ZAdd(q.genBucketKey(event.Topic), redis.Z{
		Member: strconv.FormatInt(event.EventId, 10),
		Score:  float64(event.EffectTime.Unix()),
	})
	_, err := pipeline.Exec()
	if err != nil {
		logs.CtxWarn(ctx, "pipeline.Exec", logs.String("err", err.Error()))
		return err
	}
	logs.CtxInfo(ctx, "publish event success", logs.String("event", util.ToJsonString(event)))
	return nil
}
