package test

import (
	"context"
	"fmt"
	"github.com/redis_delay_queue"
	"github.com/redis_delay_queue/util"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestPublishEvent(t *testing.T) {
	type args struct {
		ctx   context.Context
		event *delay_queue.EventEntity
	}
	type testConfig struct {
		args    args
		wantErr bool
	}
	// 模拟每隔5秒发送一次事件
	Convey("test", t, func() {
		//your mock code...
		i := 1
		var errList []error
		Convey(fmt.Sprintf("test case%v", i), func() {
			tt := testConfig{
				args: args{
					ctx: context.Background(),
					event: &delay_queue.EventEntity{
						EventId:    int64(i),
						Topic:      "test",
						EffectTime: time.Now().Add(time.Duration(i) * 5 * time.Second),
						Body:       util.ToString(i),
					},
				},
				wantErr: false,
			}
			err := delay_queue.PublishEvent(tt.args.ctx, tt.args.event)
			if err != nil {
				errList = append(errList, err)
			}
			i++
			Convey(fmt.Sprintf("test case%v", i), func() {
				tt := testConfig{
					args: args{
						ctx: context.Background(),
						event: &delay_queue.EventEntity{
							EventId:    int64(i),
							Topic:      "test",
							EffectTime: time.Now().Add(time.Duration(i) * 5 * time.Second),
							Body:       util.ToString(i),
						},
					},
					wantErr: false,
				}
				err := delay_queue.PublishEvent(tt.args.ctx, tt.args.event)
				if err != nil {
					errList = append(errList, err)
				}
				i++
				Convey(fmt.Sprintf("test case%v", i), func() {
					tt := testConfig{
						args: args{
							ctx: context.Background(),
							event: &delay_queue.EventEntity{
								EventId:    int64(i),
								Topic:      "test",
								EffectTime: time.Now().Add(time.Duration(i) * 5 * time.Second),
								Body:       util.ToString(i),
							},
						},
						wantErr: false,
					}
					err := delay_queue.PublishEvent(tt.args.ctx, tt.args.event)
					if err != nil {
						errList = append(errList, err)
					}
					So(len(errList) == 0, ShouldEqual, true)
				})
			})
		})
	})
}
