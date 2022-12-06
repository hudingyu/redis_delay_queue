package test

import (
	"context"
	delay_queue "github.com/redis_delay_queue"
	"github.com/redis_delay_queue/logs"
	"github.com/redis_delay_queue/util"
)

// 模拟处理延时任务
type DemoDelayTask struct{}

func (p *DemoDelayTask) Topic() string {
	return "test"
}

func (p *DemoDelayTask) Handle(ctx context.Context, event *delay_queue.EventEntity) error {
	logs.CtxInfo(ctx, "handle event success", logs.String("event", util.ToJsonString(event)))
	return nil
}
