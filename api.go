package delay_queue

import "context"

func PublishEvent(ctx context.Context, event *EventEntity) error {
	return GetDelayQueue().PublishEvent(ctx, event)
}
