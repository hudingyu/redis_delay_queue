package delay_queue

import (
	"context"
	"time"
)

type EventEntity struct {
	EventId    int64
	Topic      string
	Body       string
	EffectTime time.Time
}

type IDelayQueue interface {
	PublishEvent(ctx context.Context, event *EventEntity) error
}

type IEventSubscriber interface {
	Topic() string
	Handle(ctx context.Context, event *EventEntity) error
}
